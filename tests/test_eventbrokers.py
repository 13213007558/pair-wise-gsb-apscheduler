from __future__ import annotations

import sys
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger
from typing import Any

import anyio
import pytest
from _pytest.logging import LogCaptureFixture
from anyio import CancelScope, create_memory_object_stream, fail_after

from apscheduler import Event, ScheduleAdded
from apscheduler.abc import EventBroker
from apscheduler.eventbrokers.local import LocalEventBroker

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = timezone.utc

pytestmark = pytest.mark.anyio


async def test_publish_subscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
        event_broker.subscribe(send.send)
        event_broker.subscribe(send.send_nowait)
        event = ScheduleAdded(
            schedule_id="schedule1",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
        )
        await event_broker.publish(event)

        with fail_after(3):
            event1 = await receive.receive()
            event2 = await receive.receive()

    assert event1 == event2
    assert isinstance(event1, ScheduleAdded)
    assert isinstance(event1.timestamp, datetime)
    assert event1.schedule_id == "schedule1"
    assert event1.task_id == "task1"
    assert event1.next_fire_time == datetime(2021, 9, 11, 12, 31, 56, 254867, UTC)


async def test_subscribe_one_shot(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
        event_broker.subscribe(send.send, one_shot=True)
        event = ScheduleAdded(
            schedule_id="schedule1",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
        )
        await event_broker.publish(event)
        event = ScheduleAdded(
            schedule_id="schedule2",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 12, 8, 42, 11, 968481, UTC),
        )
        await event_broker.publish(event)

        with fail_after(3):
            received_event = await receive.receive()

        with pytest.raises(TimeoutError), fail_after(0.1):
            await receive.receive()

    assert isinstance(received_event, ScheduleAdded)
    assert received_event.schedule_id == "schedule1"
    assert received_event.task_id == "task1"


async def test_unsubscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
        subscription = event_broker.subscribe(send.send)
        await event_broker.publish(Event())
        with fail_after(3):
            await receive.receive()

        subscription.unsubscribe()
        await event_broker.publish(Event())
        with pytest.raises(TimeoutError), fail_after(0.1):
            await receive.receive()


async def test_publish_no_subscribers(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    await event_broker.publish(Event())
    assert not caplog.text


async def test_publish_exception(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    def bad_subscriber(event: Event) -> None:
        raise Exception("foo")

    timestamp = datetime.now(UTC)
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
        event_broker.subscribe(bad_subscriber)
        event_broker.subscribe(send.send)
        await event_broker.publish(Event(timestamp=timestamp))

        received_event = await receive.receive()
        assert received_event.timestamp == timestamp
        assert "Error delivering Event" in caplog.text


async def test_cancel_start(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        scope.cancel()
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)


async def test_cancel_stop(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)
            scope.cancel()


@pytest.fixture
async def local_event_broker(
    logger: Logger,
) -> AsyncGenerator[LocalEventBroker, Any]:
    broker = LocalEventBroker()
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        yield broker


async def test_local_one_shot_concurrent_publishers(
    local_event_broker: LocalEventBroker,
) -> None:
    async_one_shot_calls: list[Event] = []
    sync_one_shot_calls: list[Event] = []
    async_normal_calls: list[Event] = []
    sync_normal_calls: list[Event] = []
    filtered_calls: list[Event] = []

    async def async_one_shot(event: Event) -> None:
        async_one_shot_calls.append(event)

    def sync_one_shot(event: Event) -> None:
        sync_one_shot_calls.append(event)

    async def async_normal(event: Event) -> None:
        async_normal_calls.append(event)

    def sync_normal(event: Event) -> None:
        sync_normal_calls.append(event)

    async_sub = local_event_broker.subscribe(async_one_shot, one_shot=True)
    sync_sub = local_event_broker.subscribe(
        sync_one_shot, is_async=False, one_shot=True
    )
    local_event_broker.subscribe(async_normal)
    local_event_broker.subscribe(sync_normal, is_async=False)
    local_event_broker.subscribe(
        lambda event: filtered_calls.append(event), event_types=[ScheduleAdded]
    )

    # Release both publishers from behind a barrier at the same time
    barrier = anyio.Event()

    async def publisher() -> None:
        await barrier.wait()
        await local_event_broker.publish_local(Event())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publisher)
        task_group.start_soon(publisher)
        await anyio.sleep(0.01)
        barrier.set()

    # Flush all scheduled deliveries with a sentinel event
    await local_event_broker.publish_local(Event())
    with fail_after(3):
        while len(async_normal_calls) < 3 or len(sync_normal_calls) < 3:
            await anyio.sleep(0.01)

    assert len(async_one_shot_calls) == 1
    assert len(sync_one_shot_calls) == 1
    assert len(async_normal_calls) == 3
    assert len(sync_normal_calls) == 3
    assert not filtered_calls
    assert async_sub.token not in local_event_broker._subscriptions
    assert sync_sub.token not in local_event_broker._subscriptions


async def test_local_one_shot_reentrant_publish(
    local_event_broker: LocalEventBroker,
) -> None:
    one_shot_calls: list[Event] = []
    normal_calls: list[Event] = []

    async def reentrant_callback(event: Event) -> None:
        one_shot_calls.append(event)
        await local_event_broker.publish_local(Event())

    local_event_broker.subscribe(reentrant_callback, one_shot=True)
    local_event_broker.subscribe(lambda event: normal_calls.append(event))
    await local_event_broker.publish_local(Event())

    # Flush all scheduled deliveries (initial + reentrant + sentinel)
    await local_event_broker.publish_local(Event())
    with fail_after(3):
        while len(normal_calls) < 3:
            await anyio.sleep(0.01)

    assert len(one_shot_calls) == 1
    assert len(normal_calls) == 3


async def test_local_one_shot_blocked_callback_not_reselected(
    local_event_broker: LocalEventBroker,
) -> None:
    entered = anyio.Event()
    release = anyio.Event()
    one_shot_calls: list[Event] = []
    normal_calls: list[Event] = []

    async def blocking_callback(event: Event) -> None:
        one_shot_calls.append(event)
        entered.set()
        await release.wait()

    local_event_broker.subscribe(blocking_callback, one_shot=True)
    local_event_broker.subscribe(lambda event: normal_calls.append(event))
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(local_event_broker.publish_local, Event())
        with fail_after(3):
            await entered.wait()

        # This publish happens while the one_shot callback is still blocked
        await local_event_broker.publish_local(Event())
        release.set()

    with fail_after(3):
        while len(normal_calls) < 2:
            await anyio.sleep(0.01)

    assert len(one_shot_calls) == 1
    assert len(normal_calls) == 2


async def test_local_one_shot_exception_releases_subscription(
    local_event_broker: LocalEventBroker, caplog: LogCaptureFixture
) -> None:
    one_shot_calls: list[Event] = []
    normal_calls: list[Event] = []

    async def bad_callback(event: Event) -> None:
        one_shot_calls.append(event)
        raise RuntimeError("boom")

    subscription = local_event_broker.subscribe(bad_callback, one_shot=True)
    local_event_broker.subscribe(lambda event: normal_calls.append(event))
    await local_event_broker.publish_local(Event())
    with fail_after(3):
        while not normal_calls or not one_shot_calls:
            await anyio.sleep(0.01)

    assert len(one_shot_calls) == 1
    assert subscription.token not in local_event_broker._subscriptions
    assert "Error delivering Event" in caplog.text

    # The failed one_shot subscription must not receive any later events
    await local_event_broker.publish_local(Event())
    with fail_after(3):
        while len(normal_calls) < 2:
            await anyio.sleep(0.01)

    assert len(one_shot_calls) == 1


async def test_local_one_shot_delivery_cancelled_on_exit(
    logger: Logger,
) -> None:
    broker = LocalEventBroker()
    entered = anyio.Event()
    block = anyio.Event()
    calls: list[Event] = []

    async def blocking_callback(event: Event) -> None:
        calls.append(event)
        entered.set()
        await block.wait()

    # Cancelling at the exit boundary must cancel the in-flight delivery
    # without hanging or leaking tasks
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await broker.start(exit_stack, logger)
            subscription1 = broker.subscribe(blocking_callback, one_shot=True)
            await broker.publish_local(Event())
            with fail_after(3):
                await entered.wait()

            # Re-establish a same-callback one_shot subscription while the
            # first delivery is still blocked
            subscription2 = broker.subscribe(blocking_callback, one_shot=True)
            scope.cancel()

    assert len(calls) == 1
    assert subscription1.token not in broker._subscriptions
    assert subscription2.token in broker._subscriptions


def test_asyncpg_broker_from_async_engine() -> None:
    pytest.importorskip("asyncpg", reason="asyncpg is not installed")
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

    url = URL(
        "postgresql+asyncpg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, AsyncpgEventBroker)
    assert broker.dsn == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )


def test_psycopg_broker_from_async_engine() -> None:
    pytest.importorskip(
        "psycopg", exc_type=ImportError, reason="psycopg is not installed"
    )
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.psycopg import PsycopgEventBroker

    url = URL(
        "postgresql+psycopg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = PsycopgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, PsycopgEventBroker)
    assert broker.conninfo == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )
