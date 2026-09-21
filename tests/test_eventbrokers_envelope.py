from __future__ import annotations

import logging
import random
from base64 import b64decode
from contextlib import AsyncExitStack

import attrs
import pytest
from anyio import fail_after

import apscheduler._events as events_mod
from apscheduler import Event
from apscheduler._exceptions import SerializationError
from apscheduler.eventbrokers._envelope import EnvelopeError, fragment_payload
from apscheduler.eventbrokers.base import BaseExternalEventBroker
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer

pytestmark = pytest.mark.anyio


@attrs.define(kw_only=True, frozen=True)
class MetadataEvent(Event):
    metadata: str = ""


@pytest.fixture(autouse=True)
def _register_metadata_event(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(events_mod, "MetadataEvent", MetadataEvent, raising=False)


class RecordingBroker(BaseExternalEventBroker):
    """External broker stub whose "wire" is an in-process method call."""

    async def publish(self, event: Event) -> None:
        for notification in self.generate_notifications_str(event):
            # Simulate the transport listener receiving the notification
            reconstituted = self.reconstitute_event_str(notification)
            if reconstituted is not None:
                await self.publish_local(reconstituted)


@pytest.fixture
def broker_factory():
    def _factory(serializer=None, **kwargs):
        broker = RecordingBroker(
            serializer=serializer or JSONSerializer(),
            max_notification_size=1000,
            **kwargs,
        )
        broker._logger = logging.getLogger("test.envelope")
        return broker

    return _factory


@pytest.mark.parametrize(
    "serializer_cls", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
async def test_small_event_stays_legacy_single_packet(
    broker_factory, serializer_cls
) -> None:
    broker = broker_factory(serializer=serializer_cls())
    event = MetadataEvent(metadata="small payload")
    notifications = broker.generate_notifications_str(event)

    # No fragmentation and no envelope marker: old clients keep working
    assert len(notifications) == 1
    assert notifications[0].startswith("MetadataEvent ")
    assert not notifications[0].startswith("APS")

    received = broker.reconstitute_event_str(notifications[0])
    assert received == event

    # And a legacy client parser still understands it
    event_type, b64_serialized = notifications[0].split(" ", 1)
    assert event_type == "MetadataEvent"
    assert (
        broker.serializer.deserialize(b64decode(b64_serialized))["metadata"]
        == "small payload"
    )


@pytest.mark.parametrize(
    "serializer_cls", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
async def test_large_event_fragments_and_reassembles(
    broker_factory, serializer_cls
) -> None:
    broker = broker_factory(serializer=serializer_cls())
    event = MetadataEvent(metadata="abcdefgh" * 10000)
    notifications = broker.generate_notifications_str(event)

    assert len(notifications) > 1
    assert all(n.startswith("APS1 ") for n in notifications)
    assert all(len(n) <= 1000 for n in notifications)

    received = None
    for notification in notifications:
        received = broker.reconstitute_event_str(notification) or received
    assert received == event
    assert broker._reassembler.buffered_events == 0


async def test_fragment_roundtrip_on_bytes_channel(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="Z" * 8000)
    notifications = broker.generate_notifications(event)
    assert len(notifications) > 1
    for notification in notifications[:-1]:
        assert broker.reconstitute_event(notification) is None
    received = broker.reconstitute_event(notifications[-1])
    assert isinstance(received, MetadataEvent)
    assert received.metadata == event.metadata


async def test_out_of_order_and_duplicate_fragments(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="data-" * 3000)
    notifications = broker.generate_notifications_str(event)

    rng = random.Random(7)
    shuffled = notifications.copy()
    rng.shuffle(shuffled)
    # Duplicate several fragments (as a retrying publisher might produce)
    feed = shuffled[:2] + shuffled[5:7] + shuffled

    results = []
    for notification in feed:
        received = broker.reconstitute_event_str(notification)
        if received is not None:
            results.append(received)

    assert len(results) == 1
    assert results[0] == event
    assert broker._reassembler.buffered_events == 0


async def test_full_redelivery_is_deduplicated(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="once" * 2000)
    notifications = broker.generate_notifications_str(event)

    first = [broker.reconstitute_event_str(n) for n in notifications]
    assert any(first)

    # Every packet re-sent, e.g. Redis/asyncpg publisher retry storm:
    second = [broker.reconstitute_event_str(n) for n in notifications]
    assert not any(second)


async def test_incomplete_fragments_expire_and_buffer_can_recover(
    broker_factory,
) -> None:
    broker = broker_factory()
    now = [0.0]
    broker._reassembler = type(broker._reassembler)(
        timeout=1.0, dedup_ttl=2.0, clock=lambda: now[0]
    )
    event = MetadataEvent(metadata="T" * 3000)
    notifications = broker.generate_notifications_str(event)

    assert broker.reconstitute_event_str(notifications[0]) is None
    assert broker._reassembler.buffered_events == 1

    now[0] = 1.1  # exceed the reassembly timeout
    # Late fragments must be discarded and the stale state reaped
    assert broker.reconstitute_event_str(notifications[1]) is None
    assert broker._reassembler.buffered_events == 1

    # A fresh delivery of the whole event after the timeout still succeeds
    now[0] = 2.0
    received = None
    for notification in notifications:
        received = broker.reconstitute_event_str(notification) or received
    assert received == event


async def test_delivered_event_ids_expire_after_dedup_ttl(broker_factory) -> None:
    broker = broker_factory()
    now = [0.0]
    broker._reassembler = type(broker._reassembler)(
        timeout=1.0, dedup_ttl=5.0, clock=lambda: now[0]
    )
    event = MetadataEvent(metadata="dedup" * 500)
    notifications = broker.generate_notifications_str(event)

    for notification in notifications:
        broker.reconstitute_event_str(notification)
    assert len(broker._reassembler._delivered) == 1

    # Duplicate inside the TTL window is suppressed
    assert broker.reconstitute_event_str(notifications[0]) is None

    now[0] = 5.1
    # After the TTL the stale entry is reaped on the next receive
    broker.reconstitute_event_str(notifications[0])
    assert len(broker._reassembler._delivered) == 0


async def test_buffer_is_bounded_under_partial_flood(broker_factory) -> None:
    broker = broker_factory(max_buffered_events=3)
    reassembler = type(broker._reassembler)(max_events=3)
    broker._reassembler = reassembler

    for i in range(50):
        event = MetadataEvent(metadata=str(i) * 2000)
        notifications = broker.generate_notifications_str(event)
        broker.reconstitute_event_str(notifications[0])  # first fragment only

    assert reassembler.buffered_events <= 3
    # Finished events are also kept in a bounded dedup cache
    completed = 0
    for i in range(50):
        event = MetadataEvent(metadata=f"complete-{i}" * 2000)
        notifications = broker.generate_notifications_str(event)
        for notification in notifications:
            result = broker.reconstitute_event_str(notification)
            completed += result is not None
    assert completed == 50
    assert len(reassembler._delivered) <= 6


@pytest.mark.parametrize(
    "mutator,description",
    [
        (lambda line: line.replace("APS1 ", "APS9 ", 1), "unknown version"),
        (lambda line: _replace_field(line, 5, "deadbeef"), "bad checksum"),
        (lambda line: _replace_field(line, 4, "2"), "wrong total length"),
        (lambda line: _replace_field(line, 2, "12"), "index past count"),
        (lambda line: line.rsplit(" ", 1)[0] + " !!!", "bad base64"),
        (lambda line: line + " extra", "unexpected fields"),
        (lambda line: _replace_field(line, 1, "z" * 32), "bad event id"),
    ],
)
async def test_bad_envelopes_are_rejected(
    broker_factory, caplog, mutator, description
) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="bad" * 1000)
    notifications = broker.generate_notifications_str(event)
    corrupted = [mutator(line) for line in notifications]

    with caplog.at_level(logging.ERROR):
        results = [broker.reconstitute_event_str(line) for line in corrupted]

    assert not any(results), description
    assert broker._reassembler.buffered_events == 0
    assert "malformed" in caplog.text


async def test_declared_total_length_over_budget_is_rejected(
    broker_factory, caplog
) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="X" * 3000)
    notifications = broker.generate_notifications_str(event)
    oversized = [_replace_field(line, 4, "999999") for line in notifications]
    with caplog.at_level(logging.ERROR):
        for notification in oversized:
            assert broker.reconstitute_event_str(notification) is None
    assert broker._reassembler.buffered_events == 0


async def test_fragment_count_over_budget_is_rejected(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="Y" * 3000)

    # The sender must refuse events that cannot fit in the fragment budget
    with pytest.raises(SerializationError):
        fragment_payload(
            "MetadataEvent",
            broker.serializer.serialize(event.marshal()),
            max_notification_size=1000,
            max_payload_size=broker.max_payload_size,
            max_fragments=2,
        )

    # The receiver must likewise reject fragment counts above its budget
    broker._reassembler = type(broker._reassembler)(max_fragments=2)
    notifications = broker.generate_notifications_str(event)
    for notification in notifications:
        assert broker.reconstitute_event_str(notification) is None
    assert broker._reassembler.buffered_events == 0


async def test_inconsistent_fragments_are_rejected(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="W" * 3000)
    notifications = broker.generate_notifications_str(event)
    other = broker.generate_notifications_str(MetadataEvent(metadata="V" * 3000))

    broker.reconstitute_event_str(notifications[0])
    # Claim a different checksum/total for the same event ID
    event_id = notifications[1].split(" ")[1]
    foreign = _set_event_id(other[1], event_id)
    with pytest.raises(EnvelopeError):
        broker._reassembler.receive(foreign)


async def test_publish_rejects_payload_over_size_budget() -> None:
    broker = RecordingBroker(
        serializer=JSONSerializer(),
        max_notification_size=500,
        max_payload_size=100,
    )
    broker._logger = logging.getLogger("test.envelope")
    event = MetadataEvent(metadata="Q" * 500)
    with pytest.raises(SerializationError):
        broker.generate_notifications_str(event)


async def test_legacy_format_interoperates_both_ways(broker_factory) -> None:
    broker = broker_factory()
    event = MetadataEvent(metadata="legacy interop")

    # Legacy notifications produced by old code are still accepted
    legacy_str = broker.generate_notification_str(event)
    legacy_bytes = broker.generate_notification(event)
    assert broker.reconstitute_event_str(legacy_str) == event
    assert broker.reconstitute_event(legacy_bytes) == event

    # New small-event notifications are byte-for-byte the old format
    assert broker.generate_notifications(event) == [legacy_bytes]
    assert broker.generate_notifications_str(event) == [legacy_str]


async def test_publisher_retries_cause_single_callback(broker_factory, logger) -> None:
    broker = broker_factory()
    events_seen = []
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(events_seen.append)

        notifications = broker.generate_notifications_str(
            MetadataEvent(metadata="retry" * 2000)
        )
        # Every fragment delivered twice, like a duplicated retry
        with fail_after(3):
            for notification in notifications:
                for _ in range(2):
                    reconstituted = broker.reconstitute_event_str(notification)
                    if reconstituted is not None:
                        await broker.publish_local(reconstituted)

    assert len(events_seen) == 1
    assert events_seen[0].metadata == "retry" * 2000


async def test_postgres_notify_packets_never_exceed_limit() -> None:
    broker_cls = None
    dsn = None
    try:
        from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
    except ImportError:
        pass
    else:
        broker_cls = AsyncpgEventBroker
        dsn = "postgres://localhost"

    if broker_cls is None:
        try:
            from apscheduler.eventbrokers.psycopg import PsycopgEventBroker
        except ImportError:
            pytest.skip("neither asyncpg nor psycopg is installed")
        else:
            broker_cls = PsycopgEventBroker
            dsn = "postgres://localhost"

    broker = broker_cls(dsn, serializer=JSONSerializer())
    event = MetadataEvent(metadata="L" * 1_000_000)
    notifications = broker.generate_notifications_str(event)
    assert len(notifications) > 1
    assert all(len(n) <= 7999 for n in notifications)

    received = None
    for notification in notifications:
        received = broker.reconstitute_event_str(notification) or received
    assert isinstance(received, MetadataEvent)
    assert len(received.metadata) == 1_000_000


def _replace_field(line: str, index: int, value: str) -> str:
    parts = line.split(" ")
    parts[index] = value
    return " ".join(parts)


def _set_event_id(line: str, event_id: str) -> str:
    parts = line.split(" ")
    parts[1] = event_id
    return " ".join(parts)
