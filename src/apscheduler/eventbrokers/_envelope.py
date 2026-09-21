from __future__ import annotations

import re
import zlib
from base64 import b64decode, b64encode
from collections.abc import Callable
from time import monotonic
from uuid import uuid4

import attrs

from .._exceptions import SerializationError

#: The magic prefix that distinguishes enveloped notifications from legacy ones.
#: Legacy notifications start with an event class name, which never matches this
#: pattern.
ENVELOPE_MAGIC = "APS"

#: The envelope version implemented by this module
ENVELOPE_VERSION = 1

#: The full prefix of envelopes produced by this module (magic + version)
ENVELOPE_PREFIX = f"{ENVELOPE_MAGIC}{ENVELOPE_VERSION}"

_envelope_prefix_re = re.compile(rf"{ENVELOPE_MAGIC}(\d+)$")
_event_id_re = re.compile(r"[0-9a-f]{32}$")
_crc_re = re.compile(r"[0-9a-f]{8}$")


class EnvelopeError(ValueError):
    """Raised when a notification envelope fails validation."""


def is_envelope(payload: bytes | str) -> bool:
    """
    Return True if the payload looks like a versioned envelope rather than a
    legacy notification.
    """
    if isinstance(payload, bytes):
        try:
            first_token = payload.split(b" ", 1)[0].decode("ascii")
        except UnicodeDecodeError:
            return False
    else:
        first_token = payload.split(" ", 1)[0]

    return bool(_envelope_prefix_re.match(first_token))


@attrs.define(frozen=True)
class Envelope:
    """A parsed notification envelope."""

    event_id: str
    fragment_index: int
    fragment_count: int
    total_length: int
    crc32: int
    event_type: str
    fragment: bytes


def parse_envelope(line: str) -> Envelope:
    """
    Parse an envelope line into its components.

    Only syntactic validation is performed here; semantic validation (budgets,
    consistency) is done by :class:`FragmentReassembler`.

    :raises EnvelopeError: if the envelope is malformed or uses an unsupported
        version
    """
    parts = line.split(" ")
    match = _envelope_prefix_re.match(parts[0])
    if not match:
        raise EnvelopeError("missing or malformed envelope prefix")

    version = int(match.group(1))
    if version != ENVELOPE_VERSION:
        raise EnvelopeError(f"unsupported envelope version: {version}")

    if len(parts) != 8:
        raise EnvelopeError(f"expected 8 envelope fields, got {len(parts)}")

    _, event_id, index_s, count_s, total_s, crc_s, event_type, fragment_s = parts
    if not _event_id_re.match(event_id):
        raise EnvelopeError(f"malformed event ID: {event_id!r}")

    if not _crc_re.match(crc_s):
        raise EnvelopeError(f"malformed checksum: {crc_s!r}")

    try:
        fragment_index = int(index_s)
        fragment_count = int(count_s)
        total_length = int(total_s)
    except ValueError:
        raise EnvelopeError("malformed fragment index/count or total length") from None

    if fragment_index < 0:
        raise EnvelopeError(f"negative fragment index: {fragment_index}")

    if fragment_count < 1:
        raise EnvelopeError(f"invalid fragment count: {fragment_count}")

    if fragment_index >= fragment_count:
        raise EnvelopeError(
            f"fragment index {fragment_index} out of range "
            f"(fragment count: {fragment_count})"
        )

    if total_length < 1:
        raise EnvelopeError(f"invalid total length: {total_length}")

    try:
        fragment = b64decode(fragment_s, validate=True)
    except ValueError:
        raise EnvelopeError("malformed base64 fragment data") from None

    if not fragment:
        raise EnvelopeError("empty fragment data")

    return Envelope(
        event_id=event_id,
        fragment_index=fragment_index,
        fragment_count=fragment_count,
        total_length=total_length,
        crc32=int(crc_s, 16),
        event_type=event_type,
        fragment=fragment,
    )


def fragment_payload(
    event_type: str,
    payload: bytes,
    max_notification_size: int,
    max_payload_size: int,
    max_fragments: int,
) -> list[str]:
    """
    Split a serialized event payload into envelope lines, each no longer than
    max_notification_size characters.

    :param event_type: the event class name
    :param payload: the serialized event
    :param max_notification_size: maximum length of each generated line
    :param max_payload_size: maximum allowed size of the serialized payload
    :param max_fragments: maximum allowed number of fragments per event
    :raises SerializationError: if the payload exceeds the size budget or cannot
        be fragmented within the given constraints
    """
    if len(payload) > max_payload_size:
        raise SerializationError(
            f"Serialized event object exceeds the maximum payload size of "
            f"{max_payload_size} bytes ({len(payload)} bytes)"
        )

    event_id = uuid4().hex
    crc32 = zlib.crc32(payload)
    total_length = len(payload)

    def _header_length(digits: int) -> int:
        return (
            len(ENVELOPE_PREFIX)
            + 1  # space
            + 32  # event ID
            + 1  # space
            + digits  # fragment index
            + 1  # space
            + digits  # fragment count
            + 1  # space
            + len(str(total_length))  # total length
            + 1  # space
            + 8  # CRC-32
            + 1  # space
            + len(event_type)
            + 1  # space
        )

    def _chunk_size(digits: int) -> int:
        # Base64 expands 3 bytes into 4 characters
        return (max_notification_size - _header_length(digits)) // 4 * 3

    # First size the chunks from the worst-case digit count, then recompute
    # with the actual fragment count so the generated lines pack tightly while
    # never exceeding the notification size limit
    worst_case_digits = len(str(max_fragments))
    chunk_size = _chunk_size(worst_case_digits)
    if chunk_size < 1:
        raise SerializationError(
            f"The maximum notification size ({max_notification_size}) leaves no "
            f"room for fragment data (header requires "
            f"{_header_length(worst_case_digits)} characters)"
        )

    fragment_count = (total_length + chunk_size - 1) // chunk_size
    if fragment_count > max_fragments:
        raise SerializationError(
            f"Serialized event object would require {fragment_count} fragments, "
            f"exceeding the maximum of {max_fragments}"
        )

    chunk_size = _chunk_size(len(str(fragment_count)))
    chunks = [
        payload[offset : offset + chunk_size]
        for offset in range(0, total_length, chunk_size)
    ]
    fragment_count = len(chunks)
    return [
        f"{ENVELOPE_PREFIX} {event_id} {index} {fragment_count} {total_length} "
        f"{crc32:08x} {event_type} {b64encode(chunk).decode('ascii')}"
        for index, chunk in enumerate(chunks)
    ]


@attrs.define
class _ReassemblyState:
    event_type: str
    fragment_count: int
    total_length: int
    crc32: int
    expires_at: float
    fragments: dict[int, bytes] = attrs.field(factory=dict)
    received_bytes: int = 0


@attrs.define(eq=False)
class FragmentReassembler:
    """
    Reassembles fragmented notification envelopes into complete payloads.

    Memory use is bounded in two ways:

    * at most max_events incomplete events are buffered at any time (excess
      states evict the ones closest to expiring)
    * incomplete events expire after timeout seconds, and completed event IDs
      are remembered for dedup_ttl seconds to suppress duplicate deliveries
      (e.g. from publisher retries or MQTT QoS 1 redelivery)

    :param max_events: maximum number of concurrently buffered incomplete events
    :param max_payload_size: maximum accepted reassembled payload size in bytes
    :param max_fragments: maximum accepted number of fragments per event
    :param timeout: maximum time (in seconds) to wait for all fragments of an
        event to arrive
    :param dedup_ttl: how long (in seconds) to remember completed event IDs for
        duplicate suppression
    :param clock: monotonic clock callable (injectable for tests)
    """

    max_events: int = 1000
    max_payload_size: int = 1024 * 1024
    max_fragments: int = 1024
    timeout: float = 10.0
    dedup_ttl: float = 60.0
    clock: Callable[[], float] = monotonic

    _pending: dict[str, _ReassemblyState] = attrs.field(init=False, factory=dict)
    _delivered: dict[str, float] = attrs.field(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        if self.max_events < 1:
            raise ValueError("max_events must be at least 1")

        if self.max_payload_size < 1:
            raise ValueError("max_payload_size must be at least 1")

        if self.max_fragments < 1:
            raise ValueError("max_fragments must be at least 1")

        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

        if self.dedup_ttl <= 0:
            raise ValueError("dedup_ttl must be positive")

    @property
    def buffered_events(self) -> int:
        """The number of incomplete events currently buffered."""
        return len(self._pending)

    def _purge_expired(self, now: float) -> None:
        expired = [
            event_id
            for event_id, state in self._pending.items()
            if state.expires_at <= now
        ]
        for event_id in expired:
            del self._pending[event_id]

        expired_delivered = [
            event_id for event_id, expiry in self._delivered.items() if expiry <= now
        ]
        for event_id in expired_delivered:
            del self._delivered[event_id]

    def receive(self, line: str) -> tuple[str, bytes] | None:
        """
        Process one envelope line.

        :return: None if the event is still incomplete (or was a duplicate),
            otherwise an (event_type, payload) tuple
        :raises EnvelopeError: if the envelope fails validation
        """
        envelope = parse_envelope(line)
        now = self.clock()
        self._purge_expired(now)

        if envelope.fragment_count > self.max_fragments:
            raise EnvelopeError(
                f"fragment count {envelope.fragment_count} exceeds the maximum of "
                f"{self.max_fragments}"
            )

        if envelope.total_length > self.max_payload_size:
            raise EnvelopeError(
                f"declared total length {envelope.total_length} exceeds the "
                f"maximum payload size of {self.max_payload_size}"
            )

        if len(envelope.fragment) > envelope.total_length:
            raise EnvelopeError("fragment is larger than the declared total length")

        # Suppress duplicates of already delivered events (e.g. publisher retries)
        if envelope.event_id in self._delivered:
            return None

        state = self._pending.get(envelope.event_id)
        if state is None:
            if len(self._pending) >= self.max_events:
                # Evict the state closest to expiring to keep memory bounded
                oldest = min(
                    self._pending,
                    key=lambda event_id: self._pending[event_id].expires_at,
                )
                del self._pending[oldest]

            state = self._pending[envelope.event_id] = _ReassemblyState(
                event_type=envelope.event_type,
                fragment_count=envelope.fragment_count,
                total_length=envelope.total_length,
                crc32=envelope.crc32,
                expires_at=now + self.timeout,
            )
        elif (
            state.fragment_count != envelope.fragment_count
            or state.total_length != envelope.total_length
            or state.crc32 != envelope.crc32
            or state.event_type != envelope.event_type
        ):
            raise EnvelopeError(
                f"fragment is inconsistent with the buffered state of event "
                f"{envelope.event_id}"
            )

        if envelope.fragment_index in state.fragments:
            # Duplicate fragment; ignore
            return None

        state.fragments[envelope.fragment_index] = envelope.fragment
        state.received_bytes += len(envelope.fragment)
        if state.received_bytes > state.total_length:
            del self._pending[envelope.event_id]
            raise EnvelopeError(
                f"received more data than the declared total length of event "
                f"{envelope.event_id}"
            )

        if len(state.fragments) < state.fragment_count:
            return None

        if state.received_bytes != state.total_length:
            del self._pending[envelope.event_id]
            raise EnvelopeError(
                f"reassembled size ({state.received_bytes}) does not match the "
                f"declared total length ({state.total_length}) of event "
                f"{envelope.event_id}"
            )

        payload = b"".join(
            state.fragments[index] for index in range(state.fragment_count)
        )
        del self._pending[envelope.event_id]
        if zlib.crc32(payload) != state.crc32:
            raise EnvelopeError(
                f"checksum mismatch on reassembled event {envelope.event_id}"
            )

        self._delivered[envelope.event_id] = now + self.dedup_ttl
        if len(self._delivered) > self.max_events * 2:
            # Keep the dedup cache bounded as well
            oldest = min(
                self._delivered, key=lambda event_id: self._delivered[event_id]
            )
            del self._delivered[oldest]

        return state.event_type, payload

    def __len__(self) -> int:
        return len(self._pending)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(pending={len(self._pending)}, "
            f"delivered={len(self._delivered)})"
        )
