from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from hashlib import sha256
from typing import Any

import pytest
from anyio import sleep

from apscheduler import Event, ScheduleAdded
from apscheduler._exceptions import SerializationError
from apscheduler.eventbrokers._fragments import (
    ENVELOPE_PREFIX,
    ENVELOPE_VERSION,
    FragmentAssembler,
    split_payload,
)
from apscheduler.eventbrokers.base import BaseExternalEventBroker
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer

pytestmark = pytest.mark.anyio


class DummyExternalEventBroker(BaseExternalEventBroker):
    async def publish(self, event: Event) -> None:
        pass


class FlakyExternalEventBroker(DummyExternalEventBroker):
    def __init__(self, **kwargs: Any) -> None:
        defaults = {
            "fragment_timeout": 10,
            "max_pending_events": 10,
            "max_fragment_buffer_size": 100_000,
        }
        defaults.update(kwargs)
        super().__init__(**defaults)
        self.published: list[bytes] = []
        self.delivered: list[Event] = []
        self.attempt = 0
        self.failed_attempts = 1

    async def retry_notification(self, notification: bytes) -> None:
        self.published.append(notification)
        event = self.reconstitute_event(notification)
        if event is not None:
            self.delivered.append(event)
        self.attempt += 1
        if self.attempt <= self.failed_attempts:
            raise ConnectionError


@pytest.fixture
def assembler() -> FragmentAssembler:
    return FragmentAssembler(
        logger=logging.getLogger(__name__),
        max_event_size=20_000,
        fragment_timeout=0.1,
        max_pending_events=2,
        max_fragment_buffer_size=15_000,
    )


@pytest.fixture
async def broker() -> AsyncGenerator[DummyExternalEventBroker, None]:
    broker = DummyExternalEventBroker(
        max_notification_size=100,
        max_event_size=20_000,
        fragment_timeout=0.1,
        max_pending_events=2,
        max_fragment_buffer_size=25_000,
        fragment_cleanup_interval=0.02,
    )
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logging.getLogger(__name__))
        yield broker


def large_event(size: int = 10_000) -> ScheduleAdded:
    return ScheduleAdded(
        schedule_id="x" * size,
        task_id="task",
        next_fire_time=None,
    )


def assemble_all(assembler: FragmentAssembler, packets, *, text: bool = False):
    result = None
    for packet in packets:
        result = assembler.assemble(packet, text=text)
    return result


@pytest.mark.parametrize("text", [False, True], ids=["bytes", "str"])
def test_assembles_large_payload_out_of_order_and_deduplicates(
    assembler: FragmentAssembler, text: bool
) -> None:
    payload = b"EventType " + bytes(range(256)) * 20
    packets = list(split_payload(payload, text=text))
    shuffled = list(reversed(packets))

    assembled = assemble_all(assembler, shuffled, text=text)
    assert assembled == payload
    assert assembler.buffered_bytes == 0

    for packet in shuffled:
        assert assembler.assemble(packet, text=text) is None


def test_rejects_unknown_version(assembler: FragmentAssembler) -> None:
    packets = list(split_payload(b"EventType payload"))
    packet = bytearray(packets[0])
    packet[len(ENVELOPE_PREFIX)] = ord("2")

    assert assembler.assemble(bytes(packet)) is None
    assert assembler.pending_events == 0


def test_rejects_bad_checksum(assembler: FragmentAssembler) -> None:
    payload = b"EventType " + b"x" * 5_000
    packets = list(split_payload(payload))
    (
        marker,
        event_id,
        total_length,
        fragment_index,
        fragment_count,
        _checksum,
        fragment,
    ) = packets[-1].split(b" ", 6)
    packets[-1] = b" ".join(
        (
            marker,
            event_id,
            total_length,
            fragment_index,
            fragment_count,
            b"A" * 43,
            fragment,
        )
    )

    assert assemble_all(assembler, packets) is None
    assert assembler.pending_events == 0


def test_rejects_overbudget_input(assembler: FragmentAssembler) -> None:
    payload = b"EventType " + b"x" * (20_001 - len(b"EventType "))
    packets = split_payload(payload)

    assert assemble_all(assembler, packets) is None
    assert assembler.pending_events == 0
    assert assembler.buffered_bytes == 0


async def test_publisher_rejects_overbudget_input(
    broker: DummyExternalEventBroker,
) -> None:
    event = large_event(19_900)
    with pytest.raises(SerializationError):
        broker.generate_notifications(event)


async def test_timeout_clears_incomplete_fragments(
    assembler: FragmentAssembler,
) -> None:
    packets = split_payload(b"EventType " + b"x" * 9_000)
    assert assembler.assemble(packets[0]) is None
    assert assembler.pending_events == 1
    assert assembler.buffered_bytes > 0

    await sleep(0.12)
    assembler.sweep_expired()

    assert assembler.pending_events == 0
    assert assembler.buffered_bytes == 0


async def test_background_sweeps_completed_cache(
    broker: DummyExternalEventBroker,
) -> None:
    packets = broker.generate_notifications(large_event())
    assert len(packets) > 1
    assembler = broker._get_fragment_assembler()
    assert assemble_all(assembler, packets)
    assert assembler.pending_events == 1

    await sleep(0.15)

    assert assembler.pending_events == 0
    assert assembler.buffered_bytes == 0


def test_cache_event_limit_is_bounded(assembler: FragmentAssembler) -> None:
    packets = split_payload(b"EventType " + b"a" * 5_000)
    assembler.assemble(packets[0])
    second = split_payload(b"EventType " + b"b" * 5_000)
    assembler.assemble(second[0])
    third = split_payload(b"EventType " + b"c" * 5_000)

    assert assembler.assemble(third[0]) is None
    assert assembler.pending_events == 2
    assert assembler.buffered_bytes == 2 * 4096


def test_cache_byte_limit_is_bounded(assembler: FragmentAssembler) -> None:
    packets = split_payload(b"EventType " + b"a" * 10_000)
    assembler.assemble(packets[0])
    other = split_payload(b"EventType " + b"b" * 12_000)

    assert assembler.assemble(other[0]) is None
    assert assembler.buffered_bytes == 4096


@pytest.mark.parametrize(
    "serializer_class", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
@pytest.mark.parametrize("text", [False, True], ids=["bytes", "str"])
async def test_large_event_roundtrip(
    broker: DummyExternalEventBroker, serializer_class, text: bool
) -> None:
    broker.serializer = serializer_class()
    event = large_event()
    packets = (
        broker.generate_notifications_str(event)
        if text
        else broker.generate_notifications(event)
    )
    assert len(packets) > 1

    result = None
    for packet in reversed(packets):
        result = (
            broker.reconstitute_event_str(packet)
            if text
            else broker.reconstitute_event(packet)
        )

    assert result == event
    for packet in packets:
        repeated = (
            broker.reconstitute_event_str(packet)
            if text
            else broker.reconstitute_event(packet)
        )
        assert repeated is None


@pytest.mark.parametrize(
    "serializer_class", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
def test_small_events_remain_legacy_single_packet(
    serializer_class,
) -> None:
    broker = DummyExternalEventBroker(serializer=serializer_class())
    event = ScheduleAdded(
        schedule_id="schedule", task_id="task", next_fire_time=None
    )

    assert broker.generate_notifications(event) == (
        broker.generate_notification(event),
    )
    assert broker.generate_notifications_str(event) == (
        broker.generate_notification_str(event),
    )


async def test_legacy_payload_interoperates_and_deduplicates(
    broker: DummyExternalEventBroker,
) -> None:
    event = ScheduleAdded(
        schedule_id="legacy", task_id="task", next_fire_time=None
    )
    packet = broker.generate_notification(event)

    assert broker.reconstitute_event(packet) == event
    assert broker.reconstitute_event(packet) is None

    packet_str = broker.generate_notification_str(event)
    assert broker.reconstitute_event_str(packet_str) == event
    assert broker.reconstitute_event_str(packet_str) is None


def test_unstarted_broker_supports_legacy_payload() -> None:
    broker = DummyExternalEventBroker()
    event = ScheduleAdded(schedule_id="legacy", task_id="task", next_fire_time=None)

    assert broker.reconstitute_event(broker.generate_notification(event)) == event


async def test_retried_notification_delivers_only_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = FlakyExternalEventBroker()
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logging.getLogger(__name__))
        event = ScheduleAdded(
            schedule_id="retry", task_id="task", next_fire_time=None
        )
        notification = broker.generate_notification(event)

        with pytest.raises(ConnectionError):
            await broker.retry_notification(notification)

        await broker.retry_notification(notification)

    assert broker.delivered == [event]
    assert caplog.text == ""


async def test_retried_fragment_batch_delivers_only_once() -> None:
    broker = FlakyExternalEventBroker(
        max_notification_size=100,
        max_event_size=20_000,
        fragment_timeout=10,
        max_pending_events=10,
        max_fragment_buffer_size=100_000,
    )
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logging.getLogger(__name__))
        event = large_event()
        packets = broker.generate_notifications(event)
        broker.failed_attempts = len(packets)

        for packet in packets:
            with pytest.raises(ConnectionError):
                await broker.retry_notification(packet)

        for packet in packets:
            await broker.retry_notification(packet)

    assert broker.delivered == [event]


def test_packet_header_contains_verifiable_metadata() -> None:
    packet = split_payload(b"EventType " + b"x" * 5_000)[0]
    marker, event_id, total_length, fragment_index, fragment_count, checksum, _ = (
        packet.split(b" ", 6)
    )

    assert marker == f"#APS{ENVELOPE_VERSION}".encode("ascii")
    assert event_id.count(b"-") == 4
    assert total_length == b"5010"
    assert fragment_index == b"0"
    assert fragment_count == b"2"
    assert len(checksum) == 43
    assert len(sha256(b"").hexdigest()) == 64
