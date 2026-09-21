from __future__ import annotations

from base64 import b64decode, urlsafe_b64encode
from dataclasses import dataclass
from hashlib import sha256
from logging import Logger
from math import ceil
from time import monotonic
from typing import Final, Literal, overload
from uuid import UUID, uuid4

ENVELOPE_VERSION: Final = 1
ENVELOPE_PREFIX: Final = b"#APS"
FRAGMENT_SIZE: Final = 4096
MAX_PACKET_SIZE: Final = 8192
DEFAULT_MAX_EVENT_SIZE: Final = 1024 * 1024
DEFAULT_FRAGMENT_TIMEOUT: Final = 65.0
DEFAULT_MAX_PENDING_EVENTS: Final = 1000
DEFAULT_MAX_FRAGMENT_BUFFER_SIZE: Final = 10 * 1024 * 1024

LEGACY_FORMAT: Final = object()


@dataclass
class _PendingEvent:
    total_length: int
    fragment_count: int
    checksum: bytes
    deadline: float
    fragments: dict[int, bytes]
    buffered_bytes: int = 0

    @property
    def complete(self) -> bool:
        return len(self.fragments) == self.fragment_count


class FragmentAssembler:
    def __init__(
        self,
        *,
        logger: Logger,
        max_event_size: int = DEFAULT_MAX_EVENT_SIZE,
        fragment_timeout: float = DEFAULT_FRAGMENT_TIMEOUT,
        max_pending_events: int = DEFAULT_MAX_PENDING_EVENTS,
        max_fragment_buffer_size: int = DEFAULT_MAX_FRAGMENT_BUFFER_SIZE,
    ) -> None:
        self._logger = logger
        self.max_event_size = max_event_size
        self.fragment_timeout = fragment_timeout
        self.max_pending_events = max_pending_events
        self.max_fragment_buffer_size = max_fragment_buffer_size
        self._pending: dict[UUID, _PendingEvent] = {}
        self._delivered: dict[object, float] = {}

    @property
    def pending_events(self) -> int:
        return len(self._pending) + len(self._delivered)

    @property
    def buffered_bytes(self) -> int:
        return sum(event.buffered_bytes for event in self._pending.values())

    def sweep_expired(self, now: float | None = None) -> None:
        current = monotonic() if now is None else now
        expired = [
            event_id
            for event_id, event in self._pending.items()
            if event.deadline <= current
        ]
        for event_id in expired:
            del self._pending[event_id]
        delivered_expired = [
            delivered_key
            for delivered_key, deadline in self._delivered.items()
            if deadline <= current
        ]
        for delivered_key in delivered_expired:
            del self._delivered[delivered_key]

    def assemble(self, packet: bytes | str, *, text: bool = False) -> object:
        current = monotonic()
        self.sweep_expired(current)

        try:
            if text:
                if not isinstance(packet, str):
                    raise TypeError
                raw_packet = packet.encode("ascii")
            else:
                if not isinstance(packet, bytes):
                    raise TypeError
                raw_packet = packet
        except (UnicodeEncodeError, TypeError):
            self._logger.warning("Received a non-ASCII fragmented notification")
            return None

        if not raw_packet.startswith(ENVELOPE_PREFIX):
            if len(raw_packet) > self.max_event_size:
                self._logger.warning("Received an oversized legacy notification")
                return None
            return LEGACY_FORMAT

        if len(raw_packet) > MAX_PACKET_SIZE:
            self._logger.warning("Received an oversized fragmented notification")
            return None

        try:
            (
                marker,
                event_id_token,
                total_length_token,
                fragment_index_token,
                fragment_count_token,
                checksum_token,
                encoded_fragment,
            ) = raw_packet.split(b" ", 6)
            if not marker.startswith(ENVELOPE_PREFIX):
                raise ValueError

            version_text = marker[len(ENVELOPE_PREFIX) :]
            if not version_text.isdigit():
                raise ValueError

            version = int(version_text)
            if version != ENVELOPE_VERSION:
                raise ValueError(f"unsupported envelope version: {version}")

            event_id = UUID(event_id_token.decode("ascii"))
            total_length = self._positive_int(total_length_token)
            fragment_index = self._non_negative_int(fragment_index_token)
            fragment_count = self._positive_int(fragment_count_token)
            checksum = self._decode_base64(checksum_token)
            fragment = self._decode_base64(encoded_fragment)
        except (ValueError, TypeError):
            self._logger.warning("Received a malformed fragmented notification")
            return None

        if len(checksum) != sha256().digest_size:
            self._logger.warning(
                "Received a fragmented notification with a bad checksum"
            )
            return None

        expected_count = ceil(total_length / FRAGMENT_SIZE)
        expected_length = min(
            FRAGMENT_SIZE, total_length - fragment_index * FRAGMENT_SIZE
        )
        if (
            total_length > self.max_event_size
            or fragment_count != expected_count
            or fragment_index >= fragment_count
            or len(fragment) != expected_length
            or expected_length < 0
        ):
            self._logger.warning("Received a fragmented notification outside budget")
            return None

        delivered_deadline = self._delivered.get(event_id)
        if delivered_deadline is not None and delivered_deadline > current:
            return None

        pending = self._pending.get(event_id)
        if pending is None:
            required_bytes = total_length
            self.sweep_expired(current)
            if self.pending_events >= self.max_pending_events:
                self._logger.warning("Fragment cache event limit reached")
                return None

            if self.buffered_bytes + required_bytes > self.max_fragment_buffer_size:
                self._logger.warning("Fragment cache byte limit reached")
                return None

            pending = _PendingEvent(
                total_length=total_length,
                fragment_count=fragment_count,
                checksum=checksum,
                deadline=current + self.fragment_timeout,
                fragments={},
            )
            self._pending[event_id] = pending
        elif (
            pending.total_length != total_length
            or pending.fragment_count != fragment_count
            or pending.checksum != checksum
        ):
            self._logger.warning(
                "Received conflicting fragments for event %s", event_id
            )
            self._pending.pop(event_id, None)
            return None

        existing = pending.fragments.get(fragment_index)
        if existing is not None:
            if existing != fragment:
                self._logger.warning(
                    "Received a conflicting duplicate fragment for event %s", event_id
                )
                self._pending.pop(event_id, None)
            return None

        pending.fragments[fragment_index] = fragment
        pending.buffered_bytes += len(fragment)
        if not pending.complete:
            return None

        payload = b"".join(
            pending.fragments[index] for index in range(pending.fragment_count)
        )
        if (
            len(payload) != pending.total_length
            or sha256(payload).digest() != checksum
        ):
            self._logger.warning(
                "Received fragmented event %s with a bad checksum", event_id
            )
            self._pending.pop(event_id, None)
            return None

        self._pending.pop(event_id, None)
        self._delivered[event_id] = current + self.fragment_timeout
        return payload

    def mark_delivered(self, payload: bytes, *, now: float | None = None) -> bool:
        current = monotonic() if now is None else now
        self.sweep_expired(current)
        checksum = sha256(payload).digest()
        deadline = self._delivered.get(checksum)
        if deadline is not None and deadline > current:
            return False

        if self.pending_events >= self.max_pending_events:
            return True

        self._delivered[checksum] = current + self.fragment_timeout
        return True

    @staticmethod
    def _positive_int(value: bytes) -> int:
        if not value.isdigit():
            raise ValueError

        result = int(value)
        if result <= 0:
            raise ValueError

        return result

    @staticmethod
    def _non_negative_int(value: bytes) -> int:
        if not value.isdigit():
            raise ValueError

        return int(value)

    @staticmethod
    def _decode_base64(value: bytes) -> bytes:
        padding = b"=" * (-len(value) % 4)
        return b64decode(value + padding, altchars=b"-_", validate=True)


@overload
def split_payload(
    payload: bytes, *, text: Literal[False] = ...
) -> tuple[bytes, ...]:
    ...


@overload
def split_payload(payload: bytes, *, text: Literal[True]) -> tuple[str, ...]:
    ...


def split_payload(payload: bytes, *, text: bool = False) -> tuple[bytes | str, ...]:
    event_id = uuid4()
    checksum = sha256(payload).digest()
    fragment_count = ceil(len(payload) / FRAGMENT_SIZE)
    packets: list[bytes | str] = []

    for fragment_index in range(fragment_count):
        start = fragment_index * FRAGMENT_SIZE
        fragment = payload[start : start + FRAGMENT_SIZE]
        packet = b" ".join(
            (
                f"#APS{ENVELOPE_VERSION}".encode("ascii"),
                str(event_id).encode("ascii"),
                str(len(payload)).encode("ascii"),
                str(fragment_index).encode("ascii"),
                str(fragment_count).encode("ascii"),
                urlsafe_b64encode(checksum).rstrip(b"="),
                urlsafe_b64encode(fragment).rstrip(b"="),
            )
        )
        if len(packet) > MAX_PACKET_SIZE:
            raise ValueError("fragment packet exceeds maximum packet size")

        packets.append(packet.decode("ascii") if text else packet)

    return tuple(packets)
