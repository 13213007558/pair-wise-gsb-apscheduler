from __future__ import annotations

import sys
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger
from threading import Event as ThreadEvent

import anyio
import pytest
from _pytest.logging import LogCaptureFixture
from anyio import (
    CancelScope,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    get_cancelled_exc_class,
)

from apscheduler import Event, ScheduleAdded, TaskAdded
from apscheduler.abc import EventBroker
from apscheduler.eventbrokers.base import OneShotState
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


async def test_one_shot_concurrent_publishers(logger: Logger) -> None:
    async with AsyncExitStack() as exit_stack:
        broker = LocalEventBroker()
        await broker.start(exit_stack, logger)

        entered = anyio.Event()
        release = anyio.Event()
        publishers_started = anyio.Event()
        second_publisher_done = anyio.Event()
        one_shot_calls = 0
        delivered_tokens: list[object] = []
        token_holder: list[object] = []
        active_publishers = 0

        async def one_shot_callback(event: Event) -> None:
            nonlocal one_shot_calls
            one_shot_calls += 1
            delivered_tokens.append(token_holder[0])
            entered.set()
            await release.wait()

        subscription = broker.subscribe(
            one_shot_callback, {ScheduleAdded}, one_shot=True
        )
        token_holder.append(subscription.token)
        regular_send, regular_receive = create_memory_object_stream[Event](2)
        broker.subscribe(regular_send.send)
        with regular_send, regular_receive:
            async with create_task_group() as publishers:
                async def publish_first() -> None:
                    nonlocal active_publishers
                    active_publishers += 1
                    await broker.publish_local(first_event)
                    await release.wait()
                    active_publishers -= 1

                async def publish_second() -> None:
                    nonlocal active_publishers
                    active_publishers += 1
                    if active_publishers == 2:
                        publishers_started.set()
                    await publishers_started.wait()
                    await broker.publish_local(second_event)
                    second_publisher_done.set()
                    active_publishers -= 1

                first_event = ScheduleAdded(
                    schedule_id="1", task_id="task", next_fire_time=None
                )
                second_event = ScheduleAdded(
                    schedule_id="2", task_id="task", next_fire_time=None
                )
                publishers.start_soon(publish_first)
                with fail_after(1):
                    await entered.wait()
                publishers.start_soon(publish_second)
                with fail_after(1):
                    await second_publisher_done.wait()
                release.set()

                with fail_after(1):
                    received_first = await regular_receive.receive()
                    received_second = await regular_receive.receive()

        assert one_shot_calls == 1
        assert delivered_tokens == token_holder
        assert delivered_tokens.count(subscription.token) == 1
        assert received_first == first_event
        assert received_second == second_event
        assert all(
            subscriber.one_shot is False
            for subscriber in broker._subscriptions.values()
        )


async def test_one_shot_reentrant_publish(logger: Logger) -> None:
    async with AsyncExitStack() as exit_stack:
        broker = LocalEventBroker()
        await broker.start(exit_stack, logger)

        one_shot_calls = 0
        delivered_tokens: list[object] = []
        events_received = 0
        send, receive = create_memory_object_stream[Event](2)
        token_holder: list[object] = []

        async def one_shot_callback(event: Event) -> None:
            nonlocal one_shot_calls
            one_shot_calls += 1
            delivered_tokens.append(token_holder[0])
            await broker.publish_local(TaskAdded(task_id="nested"))

        async def regular_callback(event: Event) -> None:
            nonlocal events_received
            events_received += 1
            await send.send(event)

        subscription = broker.subscribe(one_shot_callback, one_shot=True)
        token_holder.append(subscription.token)
        broker.subscribe(regular_callback)
        with send, receive:
            await broker.publish_local(
                ScheduleAdded(schedule_id="outer", task_id="x", next_fire_time=None)
            )

            with fail_after(1):
                first_event = await receive.receive()
                second_event = await receive.receive()

        assert one_shot_calls == 1
        assert delivered_tokens == token_holder
        assert delivered_tokens.count(subscription.token) == 1
        assert events_received == 2
        assert {type(first_event), type(second_event)} == {
            ScheduleAdded,
            TaskAdded,
        }


async def test_one_shot_sync_async_mixed_delivery(logger: Logger) -> None:
    async with AsyncExitStack() as exit_stack:
        broker = LocalEventBroker()
        await broker.start(exit_stack, logger)

        send, receive = create_memory_object_stream[Event](2)
        sync_started = ThreadEvent()
        release_sync = ThreadEvent()
        sync_calls = 0

        def sync_one_shot(event: Event) -> None:
            nonlocal sync_calls
            sync_calls += 1
            sync_started.set()
            release_sync.wait(1)

        async def async_regular(event: Event) -> None:
            await send.send(event)

        broker.subscribe(sync_one_shot, {ScheduleAdded}, is_async=False, one_shot=True)
        broker.subscribe(async_regular)
        with send, receive:
            broker._task_group.start_soon(
                broker.publish_local,
                ScheduleAdded(schedule_id="1", task_id="task", next_fire_time=None),
            )
            with fail_after(1):
                await anyio.to_thread.run_sync(sync_started.wait)
            await broker.publish_local(TaskAdded(task_id="task"))
            with fail_after(1):
                first_event = await receive.receive()
                second_event = await receive.receive()

        release_sync.set()
        assert {type(first_event), type(second_event)} == {
            ScheduleAdded,
            TaskAdded,
        }
        assert sync_calls == 1


async def test_one_shot_event_type_filtering(logger: Logger) -> None:
    async with AsyncExitStack() as exit_stack:
        broker = LocalEventBroker()
        await broker.start(exit_stack, logger)

        one_shot_calls = 0
        send, receive = create_memory_object_stream[Event](3)

        async def one_shot_callback(event: Event) -> None:
            nonlocal one_shot_calls
            one_shot_calls += 1

        broker.subscribe(one_shot_callback, {ScheduleAdded}, one_shot=True)
        broker.subscribe(send.send, {TaskAdded, ScheduleAdded})
        with send, receive:
            events = [
                TaskAdded(task_id="task"),
                ScheduleAdded(schedule_id="1", task_id="task", next_fire_time=None),
                ScheduleAdded(schedule_id="2", task_id="task", next_fire_time=None),
            ]
            for event in events:
                await broker.publish_local(event)

            with fail_after(1):
                received_events = [await receive.receive() for _ in events]

        assert one_shot_calls == 1
        assert received_events == events


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


async def test_one_shot_exception_releases_subscription(
    caplog: LogCaptureFixture, logger: Logger
) -> None:
    async with AsyncExitStack() as exit_stack:
        broker = LocalEventBroker()
        await broker.start(exit_stack, logger)

        calls = 0
        send, receive = create_memory_object_stream[Event](3)

        async def bad_one_shot(event: Event) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("boom")

        broker.subscribe(bad_one_shot, {ScheduleAdded}, one_shot=True)
        broker.subscribe(send.send)
        with send, receive:
            await broker.publish_local(
                ScheduleAdded(schedule_id="1", task_id="t", next_fire_time=None)
            )
            await broker.publish_local(TaskAdded(task_id="t"))
            await broker.publish_local(
                ScheduleAdded(schedule_id="2", task_id="t", next_fire_time=None)
            )

            with fail_after(1):
                events = [await receive.receive() for _ in range(3)]

        assert calls == 1
        assert [type(event) for event in events] == [
            ScheduleAdded,
            TaskAdded,
            ScheduleAdded,
        ]
        assert "Error delivering ScheduleAdded event" in caplog.text
        assert all(
            not subscription.one_shot for subscription in broker._subscriptions.values()
        )


async def test_cancel_one_shot_delivery_keeps_replacement(logger: Logger) -> None:
    entered = anyio.Event()
    release = anyio.Event()
    replacement_entered = anyio.Event()
    replacement_created = anyio.Event()
    delivery_finished = anyio.Event()
    attempt = 0
    broker = LocalEventBroker()
    original_subscription = None
    cancelled_state = None

    async def callback(event: Event) -> None:
        nonlocal attempt, original_subscription, cancelled_state
        attempt += 1
        try:
            if attempt == 1:
                entered.set()
                await release.wait()
            else:
                replacement_entered.set()
                await release.wait()
                delivery_finished.set()
        except get_cancelled_exc_class():
            if attempt == 1:
                cancelled_state = original_subscription._claim
                broker.subscribe(callback, one_shot=True)
                replacement_created.set()
            raise

    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        original_subscription = broker.subscribe(callback, one_shot=True)
        await broker.publish_local(Event())
        with fail_after(1):
            await entered.wait()

        delivery_task = next(iter(broker._task_group._tasks))
        delivery_task.cancel()
        with fail_after(1):
            await replacement_created.wait()

        replacement = next(iter(broker._subscriptions.values()))
        assert attempt == 1
        assert len(broker._subscriptions) == 1
        assert replacement.token != original_subscription.token
        assert replacement._claim is not None
        assert replacement._claim.state is OneShotState.pending
        assert cancelled_state is not None
        assert cancelled_state.state is OneShotState.cancelled

        await broker.publish_local(Event())
        with fail_after(1):
            await replacement_entered.wait()
        release.set()
        with fail_after(1):
            await delivery_finished.wait()

    assert attempt == 2
    assert broker._subscriptions == {}
    assert broker._task_group._tasks == set()


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
