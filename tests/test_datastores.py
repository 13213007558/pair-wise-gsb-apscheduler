from __future__ import annotations

import platform
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from logging import Logger
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import anyio
import pytest
from anyio import CancelScope
from pytest_mock.plugin import MockerFixture

from apscheduler import (
    CoalescePolicy,
    ConflictingIdError,
    ConflictPolicy,
    DeserializationError,
    Event,
    Job,
    JobOutcome,
    JobResult,
    Schedule,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    Task,
    TaskAdded,
    TaskLookupError,
    TaskUpdated,
)
from apscheduler._structures import ScheduleResult
from apscheduler.abc import DataStore, EventBroker, Serializer
from apscheduler.datastores.base import BaseExternalDataStore
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.datastores.mongodb import MongoDBDataStore
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.triggers.date import DateTrigger

if TYPE_CHECKING:
    from time_machine import TimeMachineFixture

pytestmark = pytest.mark.anyio


@pytest.fixture
async def datastore(
    raw_datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> AsyncGenerator[DataStore, None]:
    async with AsyncExitStack() as exit_stack:
        await local_broker.start(exit_stack, logger)
        await raw_datastore.start(exit_stack, local_broker, logger)
        yield raw_datastore


@pytest.fixture
def schedules() -> list[Schedule]:
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule1 = Schedule(
        id="s1", task_id="task1", job_executor="async", trigger=trigger
    )
    schedule1.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 14, tzinfo=timezone.utc))
    schedule2 = Schedule(
        id="s2", task_id="task2", job_executor="async", trigger=trigger
    )
    schedule2.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 15, tzinfo=timezone.utc))
    schedule3 = Schedule(
        id="s3", task_id="task1", job_executor="async", trigger=trigger
    )
    schedule3.next_fire_time = trigger.next()

    return [schedule1, schedule2, schedule3]


@asynccontextmanager
async def capture_events(
    datastore: DataStore,
    limit: int,
    event_types: set[type[Event]] | None = None,
) -> AsyncGenerator[list[Event], None]:
    def listener(event: Event) -> None:
        events.append(event)
        if len(events) == limit:
            limit_event.set()
            subscription.unsubscribe()

    events: list[Event] = []
    limit_event = anyio.Event()
    subscription = datastore._event_broker.subscribe(listener, event_types)
    yield events
    if limit:
        with anyio.fail_after(3):
            await limit_event.wait()


async def test_add_replace_task(datastore: DataStore) -> None:
    event_types = {TaskAdded, TaskUpdated}
    async with capture_events(datastore, 3, event_types) as events:
        await datastore.add_task(
            Task(id="test_task", func="builtins:print", job_executor="async")
        )
        await datastore.add_task(
            Task(id="test_task2", func="math:ceil", job_executor="async")
        )
        await datastore.add_task(
            Task(id="test_task", func="builtins:repr", job_executor="async")
        )

        tasks = await datastore.get_tasks()
        assert len(tasks) == 2
        assert tasks[0].id == "test_task"
        assert tasks[0].func == "builtins:repr"
        assert tasks[1].id == "test_task2"
        assert tasks[1].func == "math:ceil"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskAdded)
    assert received_event.task_id == "test_task"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskAdded)
    assert received_event.task_id == "test_task2"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskUpdated)
    assert received_event.task_id == "test_task"

    assert not events


async def test_add_schedules(datastore: DataStore, schedules: list[Schedule]) -> None:
    async with capture_events(datastore, 3, {ScheduleAdded}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        assert await datastore.get_schedules() == schedules
        assert await datastore.get_schedules({"s1", "s2", "s3"}) == schedules
        assert await datastore.get_schedules({"s1"}) == [schedules[0]]
        assert await datastore.get_schedules({"s2"}) == [schedules[1]]
        assert await datastore.get_schedules({"s3"}) == [schedules[2]]

    for event, schedule in zip(events, schedules, strict=True):
        assert isinstance(event, ScheduleAdded)
        assert event.schedule_id == schedule.id
        assert event.task_id == schedule.task_id
        assert event.next_fire_time == schedule.next_fire_time


async def test_replace_schedules(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    async with capture_events(datastore, 1, {ScheduleUpdated}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        trigger = DateTrigger(datetime(2020, 9, 16, tzinfo=timezone.utc))
        next_fire_time = trigger.next()
        schedule = Schedule(
            id="s3",
            task_id="foo",
            trigger=trigger,
            args=(),
            kwargs={},
            job_executor="async",
            coalesce=CoalescePolicy.earliest,
            misfire_grace_time=None,
        )
        schedule.next_fire_time = next_fire_time
        await datastore.add_schedule(schedule, ConflictPolicy.replace)

        schedules = await datastore.get_schedules({schedule.id})
        assert schedules[0].task_id == "foo"
        assert schedules[0].next_fire_time == next_fire_time
        assert schedules[0].args == ()
        assert schedules[0].kwargs == {}
        assert schedules[0].coalesce is CoalescePolicy.earliest
        assert schedules[0].misfire_grace_time is None

    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleUpdated)
    assert received_event.schedule_id == "s3"
    assert received_event.task_id == "foo"
    assert received_event.next_fire_time == datetime(2020, 9, 16, tzinfo=timezone.utc)
    assert not events


async def test_add_schedules_batch(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    tasks = [
        Task(id="task1", func=None, job_executor="async"),
        Task(id="task2", func=None, job_executor="async"),
    ]
    event_types = {TaskAdded, ScheduleAdded}
    async with capture_events(datastore, 5, event_types) as events:
        await datastore.add_schedules(schedules, ConflictPolicy.exception, tasks=tasks)

        assert await datastore.get_schedules() == schedules
        stored_tasks = await datastore.get_tasks()
        assert [task.id for task in stored_tasks] == ["task1", "task2"]

    added_tasks = {event.task_id for event in events if isinstance(event, TaskAdded)}
    added_schedules = {
        event.schedule_id for event in events if isinstance(event, ScheduleAdded)
    }
    assert added_tasks == {"task1", "task2"}
    assert added_schedules == {"s1", "s2", "s3"}


async def test_add_schedules_batch_empty(datastore: DataStore) -> None:
    async with capture_events(datastore, 0) as events:
        await datastore.add_schedules([], ConflictPolicy.exception)

    assert not events
    assert await datastore.get_schedules() == []


async def test_add_schedules_batch_duplicate_ids(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    with pytest.raises(ValueError, match="duplicate schedule IDs"):
        await datastore.add_schedules(
            [schedules[0], schedules[0]], ConflictPolicy.exception
        )

    assert await datastore.get_schedules() == []


@pytest.mark.parametrize(
    "conflict_policy", [ConflictPolicy.replace, ConflictPolicy.do_nothing]
)
async def test_add_schedules_batch_conflict_keep_or_replace(
    datastore: DataStore, schedules: list[Schedule], conflict_policy: ConflictPolicy
) -> None:
    await datastore.add_schedules([schedules[0]], ConflictPolicy.exception)

    trigger = DateTrigger(datetime(2020, 10, 1, tzinfo=timezone.utc))
    replacement = Schedule(
        id="s1", task_id="task1", job_executor="async", trigger=trigger
    )
    replacement.next_fire_time = trigger.next()

    event_types = {ScheduleAdded, ScheduleUpdated}
    expected_events = 2 if conflict_policy is ConflictPolicy.replace else 1
    async with capture_events(datastore, expected_events, event_types) as events:
        await datastore.add_schedules([replacement, schedules[1]], conflict_policy)

    stored = {schedule.id: schedule for schedule in await datastore.get_schedules()}
    assert set(stored) == {"s1", "s2"}
    if conflict_policy is ConflictPolicy.replace:
        assert stored["s1"].next_fire_time == replacement.next_fire_time
        assert any(
            isinstance(event, ScheduleUpdated) and event.schedule_id == "s1"
            for event in events
        )
    else:
        assert stored["s1"].next_fire_time == schedules[0].next_fire_time
        assert not any(isinstance(event, ScheduleUpdated) for event in events)

    assert any(
        isinstance(event, ScheduleAdded) and event.schedule_id == "s2"
        for event in events
    )


async def test_add_schedules_batch_conflict_exception(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    await datastore.add_schedules(
        [schedules[0]],
        ConflictPolicy.exception,
        tasks=[Task(id="task1", func=None, job_executor="async")],
    )

    trigger = DateTrigger(datetime(2020, 10, 1, tzinfo=timezone.utc))
    replacement = Schedule(
        id="s1", task_id="task1", job_executor="async", trigger=trigger
    )
    replacement.next_fire_time = trigger.next()

    event_types = {TaskAdded, TaskUpdated, ScheduleAdded, ScheduleUpdated}
    async with capture_events(datastore, 0, event_types) as events:
        with pytest.raises(ConflictingIdError):
            await datastore.add_schedules(
                [replacement, schedules[1]],
                ConflictPolicy.exception,
                tasks=[Task(id="task2", func=None, job_executor="async")],
            )

    # The failed batch must not have added or modified anything, and no events
    # representing the rolled back changes may have been published
    assert not events
    stored = await datastore.get_schedules()
    assert [schedule.id for schedule in stored] == ["s1"]
    assert stored[0].next_fire_time == schedules[0].next_fire_time
    assert [task.id for task in await datastore.get_tasks()] == ["task1"]


@pytest.mark.parametrize("fail_at", [1, 2, 3])
async def test_add_schedules_batch_memory_write_failure(
    datastore: DataStore,
    schedules: list[Schedule],
    monkeypatch: pytest.MonkeyPatch,
    fail_at: int,
) -> None:
    if not isinstance(datastore, MemoryDataStore):
        pytest.skip("only applicable to the memory data store")

    import apscheduler.datastores.memory as memory_module

    original_insort = memory_module.insort_right
    call_count = 0

    def failing_insort(*args: Any, **kwargs: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == fail_at:
            raise RuntimeError("injected failure")

        original_insort(*args, **kwargs)

    monkeypatch.setattr(memory_module, "insort_right", failing_insort)
    with pytest.raises(RuntimeError, match="injected failure"):
        await datastore.add_schedules(
            schedules,
            ConflictPolicy.exception,
            tasks=[Task(id="task1", func=None, job_executor="async")],
        )

    # The failed batch must not have left any partial state behind
    assert await datastore.get_schedules() == []
    assert await datastore.get_tasks() == []


@pytest.mark.parametrize("fail_at", [1, 2, 3, 4, 5, 6])
async def test_add_schedules_batch_sqlalchemy_write_failure(
    tmp_path: Path,
    local_broker: EventBroker,
    logger: Logger,
    monkeypatch: pytest.MonkeyPatch,
    fail_at: int,
) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.ext.asyncio import create_async_engine

    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        datastore = SQLAlchemyDataStore(engine)
        async with AsyncExitStack() as exit_stack:
            await local_broker.start(exit_stack, logger)
            await datastore.start(exit_stack, local_broker, logger)

            # Pre-existing data that must survive the failed batch untouched
            await datastore.add_task(
                Task(id="existing_task", func=None, job_executor="async")
            )
            trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
            existing_schedule = Schedule(
                id="s_existing",
                task_id="existing_task",
                job_executor="async",
                trigger=trigger,
            )
            existing_schedule.next_fire_time = trigger.next()
            await datastore.add_schedule(existing_schedule, ConflictPolicy.exception)

            # The batch: one task update, one new task, one replaced schedule and
            # two new schedules (six statements in a single transaction)
            new_trigger = DateTrigger(datetime(2020, 10, 1, tzinfo=timezone.utc))
            replacement = Schedule(
                id="s_existing",
                task_id="existing_task",
                job_executor="async",
                trigger=new_trigger,
            )
            replacement.next_fire_time = new_trigger.next()
            new_schedules = [replacement]
            for i in (1, 2):
                schedule = Schedule(
                    id=f"s_new{i}",
                    task_id="new_task",
                    job_executor="async",
                    trigger=trigger,
                )
                schedule.next_fire_time = trigger.next()
                new_schedules.append(schedule)

            batch_tasks = [
                Task(
                    id="existing_task",
                    func=None,
                    job_executor="async",
                    max_running_jobs=5,
                ),
                Task(id="new_task", func=None, job_executor="async"),
            ]

            original_execute = SQLAlchemyDataStore._execute
            call_count = 0

            async def failing_execute(
                self: SQLAlchemyDataStore,
                conn: Any,
                statement: Any,
                parameters: Any = None,
            ) -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == fail_at:
                    raise RuntimeError("injected failure")

                return await original_execute(self, conn, statement, parameters)

            monkeypatch.setattr(SQLAlchemyDataStore, "_execute", failing_execute)
            with pytest.raises(RuntimeError, match="injected failure"):
                await datastore.add_schedules(
                    new_schedules, ConflictPolicy.replace, tasks=batch_tasks
                )

            # The failed batch must have been rolled back in its entirety
            stored_schedules = await datastore.get_schedules()
            assert [schedule.id for schedule in stored_schedules] == ["s_existing"]
            assert stored_schedules[0].next_fire_time == datetime(
                2020, 9, 13, tzinfo=timezone.utc
            )
            stored_tasks = await datastore.get_tasks()
            assert [task.id for task in stored_tasks] == ["existing_task"]
            assert stored_tasks[0].max_running_jobs is None
    finally:
        await engine.dispose()

    # Verify via an independent connection that nothing was committed
    sync_engine = create_engine(f"sqlite:///{db_path}")
    try:
        with sync_engine.connect() as conn:
            schedule_rows = conn.execute(text("SELECT id FROM schedules")).fetchall()
            task_rows = conn.execute(
                text("SELECT id, max_running_jobs FROM tasks")
            ).fetchall()

        assert [row[0] for row in schedule_rows] == ["s_existing"]
        assert task_rows == [("existing_task", None)]
    finally:
        sync_engine.dispose()


async def test_add_schedules_batch_sqlalchemy_commit_readback(
    tmp_path: Path, local_broker: EventBroker, logger: Logger
) -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.ext.asyncio import create_async_engine

    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        datastore = SQLAlchemyDataStore(engine)
        async with AsyncExitStack() as exit_stack:
            await local_broker.start(exit_stack, logger)
            await datastore.start(exit_stack, local_broker, logger)

            trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
            schedule = Schedule(
                id="s1", task_id="task1", job_executor="async", trigger=trigger
            )
            schedule.next_fire_time = trigger.next()
            await datastore.add_schedules(
                [schedule],
                ConflictPolicy.exception,
                tasks=[Task(id="task1", func=None, job_executor="async")],
            )
    finally:
        await engine.dispose()

    # The committed batch must be visible to an independent connection
    sync_engine = create_engine(f"sqlite:///{db_path}")
    try:
        with sync_engine.connect() as conn:
            schedule_rows = conn.execute(
                text("SELECT id, task_id FROM schedules")
            ).fetchall()
            task_rows = conn.execute(text("SELECT id FROM tasks")).fetchall()

        assert schedule_rows == [("s1", "task1")]
        assert task_rows == [("task1",)]
    finally:
        sync_engine.dispose()


async def test_add_schedules_batch_not_supported() -> None:
    pytest.importorskip("pymongo", reason="pymongo is not installed")

    datastore = MongoDBDataStore("mongodb://localhost:27017")
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule = Schedule(id="s1", task_id="task1", job_executor="async", trigger=trigger)
    schedule.next_fire_time = trigger.next()
    with pytest.raises(NotImplementedError, match="does not support atomically"):
        await datastore.add_schedules([schedule], ConflictPolicy.exception)


async def test_remove_schedules(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    async with capture_events(datastore, 2, {ScheduleRemoved}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        await datastore.remove_schedules(["s1", "s2"])
        assert await datastore.get_schedules() == [schedules[2]]

    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleRemoved)
    assert received_event.schedule_id == "s1"

    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleRemoved)
    assert received_event.schedule_id == "s2"

    assert not events


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_release_schedules(
    datastore: DataStore, schedules: list[Schedule], time_machine: TimeMachineFixture
) -> None:
    time_machine.move_to(datetime(2020, 9, 14, tzinfo=timezone.utc))

    event_types = {ScheduleRemoved, ScheduleUpdated}
    async with capture_events(datastore, 2, event_types) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        # The first scheduler gets the first due schedule
        schedules1 = await datastore.acquire_schedules(
            "dummy-id1", timedelta(seconds=30), 1
        )
        assert len(schedules1) == 1
        assert schedules1[0].id == "s1"

        # The second scheduler gets the second due schedule
        schedules2 = await datastore.acquire_schedules(
            "dummy-id2",
            timedelta(seconds=30),
            1,
        )
        assert len(schedules2) == 1
        assert schedules2[0].id == "s2"

        # The third scheduler gets nothing
        schedules3 = await datastore.acquire_schedules(
            "dummy-id3",
            timedelta(seconds=30),
            1,
        )
        assert not schedules3

        # Update the schedules and check that the job store actually deletes the
        # first one and updates the second one
        results1: list[ScheduleResult] = []
        results2: list[ScheduleResult] = []
        results1.append(
            ScheduleResult(
                schedule_id=schedules1[0].id,
                task_id=schedules1[0].task_id,
                trigger=schedules1[0].trigger,
                last_fire_time=datetime(2020, 9, 14, tzinfo=timezone.utc),
                next_fire_time=None,
            )
        )
        results2.append(
            ScheduleResult(
                schedule_id=schedules2[0].id,
                task_id=schedules2[0].task_id,
                trigger=schedules1[0].trigger,
                last_fire_time=datetime(2020, 9, 14, tzinfo=timezone.utc),
                next_fire_time=datetime(2020, 9, 15, tzinfo=timezone.utc),
            )
        )

        # Release all the schedules
        await datastore.release_schedules("dummy-id1", results1)
        await datastore.release_schedules("dummy-id2", results2)

        # Check that the first schedule has its next fire time nullified
        schedules = await datastore.get_schedules()
        assert len(schedules) == 3
        schedules.sort(key=lambda s: s.id)
        assert schedules[0].id == "s1"
        assert schedules[0].last_fire_time == datetime(2020, 9, 14, tzinfo=timezone.utc)
        assert schedules[0].next_fire_time is None
        assert schedules[1].id == "s2"
        assert schedules[1].last_fire_time == datetime(2020, 9, 14, tzinfo=timezone.utc)
        assert schedules[1].next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)
        assert schedules[2].id == "s3"
        assert schedules[2].last_fire_time is None
        assert schedules[2].next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)

    # Check for the appropriate update and delete events
    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleUpdated)
    assert received_event.schedule_id == "s1"
    assert received_event.next_fire_time is None

    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleUpdated)
    assert received_event.schedule_id == "s2"
    assert received_event.next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)

    assert not events


async def test_release_schedule_two_identical_fire_times(datastore: DataStore) -> None:
    """Regression test for #616."""
    for i in range(1, 3):
        trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
        schedule = Schedule(
            id=f"s{i}", task_id="task1", job_executor="async", trigger=trigger
        )
        schedule.next_fire_time = trigger.next()
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    schedules = await datastore.acquire_schedules(
        "foo",
        timedelta(seconds=30),
        3,
    )
    results = [
        ScheduleResult(
            schedule_id=schedules[0].id,
            task_id=schedules[0].task_id,
            trigger=schedules[0].trigger,
            last_fire_time=datetime(2020, 9, 10, tzinfo=timezone.utc),
            next_fire_time=None,
        ),
    ]
    await datastore.release_schedules("foo", results)

    remaining = await datastore.get_schedules({s.id for s in schedules})
    assert len(remaining) == 2
    remaining.sort(key=lambda s: s.id)
    assert remaining[0].id == schedules[0].id
    assert remaining[0].next_fire_time is None
    assert remaining[1].id == schedules[1].id
    assert remaining[1].next_fire_time


async def test_release_two_schedules_at_once(datastore: DataStore) -> None:
    """Regression test for #621."""
    for i in range(2):
        trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
        schedule = Schedule(
            id=f"s{i}", task_id="task1", job_executor="async", trigger=trigger
        )
        schedule.next_fire_time = trigger.next()
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    schedules = await datastore.acquire_schedules("foo", timedelta(seconds=30), 3)
    results = [
        ScheduleResult(
            schedule_id=schedules[0].id,
            task_id=schedules[0].task_id,
            trigger=schedules[0].trigger,
            last_fire_time=datetime(2020, 9, 10, tzinfo=timezone.utc),
            next_fire_time=None,
        ),
    ]
    await datastore.release_schedules("foo", results)

    remaining = await datastore.get_schedules({s.id for s in schedules})
    assert len(remaining) == 2


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_schedules_lock_timeout(
    datastore: DataStore,
    schedules: list[Schedule],
    time_machine: TimeMachineFixture,
) -> None:
    """
    Test that a scheduler can acquire schedules that were acquired by another
    scheduler but not released within the lock timeout period.

    """
    time_machine.move_to(datetime.now(timezone.utc), tick=False)
    await datastore.add_schedule(schedules[0], ConflictPolicy.exception)

    # First, one scheduler acquires the first available schedule
    acquired1 = await datastore.acquire_schedules(
        "dummy-id1",
        timedelta(seconds=30),
        1,
    )
    assert len(acquired1) == 1
    assert acquired1[0].id == "s1"

    # Try to acquire the schedule just at the threshold (now == acquired_until).
    # This should not yield any schedules.
    time_machine.shift(30)
    acquired2 = await datastore.acquire_schedules(
        "dummy-id2",
        timedelta(seconds=30),
        1,
    )
    assert not acquired2

    # Right after that, the schedule should be available
    time_machine.shift(1)
    acquired3 = await datastore.acquire_schedules(
        "dummy-id2",
        timedelta(seconds=30),
        1,
    )
    assert len(acquired3) == 1
    assert acquired3[0].id == "s1"


async def test_acquire_multiple_workers(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    jobs = [Job(task_id="task1", executor="async") for _ in range(2)]
    for job in jobs:
        await datastore.add_job(job)

    # The first worker gets the first job in the queue
    jobs1 = await datastore.acquire_jobs("worker1", timedelta(seconds=30), 1)
    assert len(jobs1) == 1
    assert jobs1[0].id == jobs[0].id

    # The second worker gets the second job
    jobs2 = await datastore.acquire_jobs("worker2", timedelta(seconds=30), 1)
    assert len(jobs2) == 1
    assert jobs2[0].id == jobs[1].id

    # The third worker gets nothing
    jobs3 = await datastore.acquire_jobs("worker3", timedelta(seconds=30), 1)
    assert not jobs3


async def test_job_release_success(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(minutes=1)
    )
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", timedelta(seconds=30), 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.success,
            return_value="foo",
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result
    assert result.outcome is JobOutcome.success
    assert result.exception is None
    assert result.return_value == "foo"

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_failure(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", job_executor="async", func="contextlib:asynccontextmanager")
    )
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(minutes=1)
    )
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", timedelta(seconds=30), 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.error,
            exception=ValueError("foo"),
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result
    assert result.outcome is JobOutcome.error
    assert isinstance(result.exception, ValueError)
    assert result.exception.args == ("foo",)
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_missed_deadline(datastore: DataStore):
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(minutes=1)
    )
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", timedelta(seconds=30), 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.missed_start_deadline,
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result
    assert result.outcome is JobOutcome.missed_start_deadline
    assert result.exception is None
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_cancelled(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(minutes=1)
    )
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker1", timedelta(seconds=30), 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker1",
        acquired[0],
        JobResult.from_job(acquired[0], JobOutcome.cancelled),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result
    assert result.outcome is JobOutcome.cancelled
    assert result.exception is None
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_jobs_lock_timeout(
    datastore: DataStore, time_machine: TimeMachineFixture
) -> None:
    """
    Test that a worker can acquire jobs that were acquired by another scheduler but
    not released within the lock timeout period.

    """
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(minutes=1)
    )
    await datastore.add_job(job)

    # First, one worker acquires the first available job
    time_machine.move_to(datetime.now(timezone.utc), tick=False)
    acquired = await datastore.acquire_jobs("worker1", timedelta(seconds=30), 1)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    # Try to acquire the job just at the threshold (now == acquired_until).
    # This should not yield any jobs.
    time_machine.shift(30)
    assert not await datastore.acquire_jobs("worker2", timedelta(seconds=30), 1)

    # Right after that, the job should be available
    time_machine.shift(1)
    acquired = await datastore.acquire_jobs("worker2", timedelta(seconds=30), 1)
    assert len(acquired) == 1
    assert acquired[0].id == job.id


async def test_acquire_jobs_max_number_exceeded(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(
            id="task1",
            func="contextlib:asynccontextmanager",
            job_executor="async",
            max_running_jobs=2,
        )
    )
    assert (await datastore.get_task("task1")).running_jobs == 0

    jobs = [
        Job(task_id="task1", executor="async"),
        Job(task_id="task1", executor="async"),
        Job(task_id="task1", executor="async"),
    ]
    for job in jobs:
        await datastore.add_job(job)

    # Check that only 2 jobs are returned from acquire_jobs() even though the limit
    # was 3
    acquired_jobs = await datastore.acquire_jobs("worker1", timedelta(seconds=30), 3)
    assert len(acquired_jobs) == 2
    assert [job.id for job in acquired_jobs] == [job.id for job in jobs[:2]]
    assert (await datastore.get_task("task1")).running_jobs == 2
    for job in acquired_jobs:
        assert job.acquired_by == "worker1"
        assert job.acquired_until

    # Check that no jobs are acquired now that the task is at capacity
    assert not await datastore.acquire_jobs("worker1", timedelta(seconds=30), 3)

    # Release one job, and the worker should be able to acquire the third job
    await datastore.release_job(
        "worker1",
        acquired_jobs[0],
        JobResult.from_job(
            acquired_jobs[0],
            JobOutcome.success,
            return_value=None,
        ),
    )
    assert (await datastore.get_task("task1")).running_jobs == 1
    remaining_jobs = await datastore.get_jobs()
    assert len(remaining_jobs) == 2

    acquired_jobs = await datastore.acquire_jobs("worker1", timedelta(seconds=30), 3)
    assert [job.id for job in acquired_jobs] == [jobs[2].id]
    assert (await datastore.get_task("task1")).running_jobs == 2


async def test_add_get_task(datastore: DataStore) -> None:
    with pytest.raises(TaskLookupError):
        await datastore.get_task("dummyid")

    await datastore.add_task(
        Task(id="dummyid", func="contextlib:asynccontextmanager", job_executor="async")
    )
    task = await datastore.get_task("dummyid")
    assert task.id == "dummyid"
    assert task.func == "contextlib:asynccontextmanager"


async def test_cancel_start(
    raw_datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> None:
    with CancelScope() as scope:
        scope.cancel()
        async with AsyncExitStack() as exit_stack:
            await raw_datastore.start(exit_stack, local_broker, logger)


async def test_cancel_stop(
    raw_datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> None:
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await raw_datastore.start(exit_stack, local_broker, logger)
            scope.cancel()


async def test_next_schedule_run_time(datastore: DataStore, schedules: list[Schedule]):
    next_schedule_run_time = await datastore.get_next_schedule_run_time()
    assert next_schedule_run_time is None

    for schedule in schedules:
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    next_schedule_run_time = await datastore.get_next_schedule_run_time()
    assert next_schedule_run_time == datetime(2020, 9, 13, tzinfo=timezone.utc)


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_cleanup_expired_schedule_leases(
    datastore: DataStore, schedules: list[Schedule], time_machine: TimeMachineFixture
) -> None:
    """
    Test that clean-up releases schedules whose leases have expired, and notifies
    schedulers about it.

    """
    time_machine.move_to(datetime(2020, 9, 14, tzinfo=timezone.utc))
    await datastore.add_schedule(schedules[0], ConflictPolicy.exception)

    # Acquire the schedule with a scheduler that then dies without releasing it
    acquired = await datastore.acquire_schedules("scheduler1", timedelta(seconds=30), 1)
    assert len(acquired) == 1

    # The lease has not expired yet, so clean-up must leave the schedule alone
    time_machine.shift(20)
    await datastore.cleanup()
    assert not await datastore.acquire_schedules("scheduler2", timedelta(seconds=30), 1)

    # The lease has expired now, so clean-up releases the schedule
    time_machine.shift(20)
    async with capture_events(datastore, 1, {ScheduleUpdated}) as events:
        await datastore.cleanup()

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, ScheduleUpdated)
    assert event.schedule_id == schedules[0].id
    assert event.task_id == schedules[0].task_id
    assert event.next_fire_time == schedules[0].next_fire_time

    acquired = await datastore.acquire_schedules("scheduler2", timedelta(seconds=30), 1)
    assert [schedule.id for schedule in acquired] == [schedules[0].id]


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_extend_acquired_schedule_leases(
    datastore: DataStore, time_machine: TimeMachineFixture, schedules: list[Schedule]
) -> None:
    """
    Test that the leases on acquired schedules are updated to prevent other schedulers
    from acquiring them.

    """
    time_machine.move_to(datetime(2020, 9, 14, tzinfo=timezone.utc))

    # Add a schedule to the data store
    await datastore.add_schedule(schedules[0], ConflictPolicy.exception)

    # Acquire the schedule
    schedules = await datastore.acquire_schedules(
        "scheduler_id",
        timedelta(seconds=30),
        1,
    )
    assert len(schedules) == 1

    # Move 20 seconds forward, then call extend_acquired_schedule_leases(). This should
    # set the acquired_until timestamp to 30 seconds from the new current time.
    time_machine.shift(20)
    await datastore.extend_acquired_schedule_leases(
        "scheduler_id", {schedules[0].id}, timedelta(seconds=30)
    )

    # The schedule was acquired by scheduler_id so scheduler2_id should not be able to
    # acquire it, as it's still within the original lock expiration delay
    assert not await datastore.acquire_schedules(
        "scheduler2_id",
        timedelta(seconds=30),
        1,
    )

    # Move 20 more seconds forward (beyond the initial lock expiration delay), then try
    # to have the second scheduler acquire the schedule again. This should also fail.
    time_machine.shift(20)
    assert not await datastore.acquire_schedules(
        "scheduler2_id",
        timedelta(seconds=30),
        1,
    )

    # Move 20 more seconds forward - this time the schedule should be available
    time_machine.shift(20)
    schedules = await datastore.acquire_schedules(
        "scheduler2_id",
        timedelta(seconds=30),
        1,
    )
    assert len(schedules) == 1


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_extend_acquired_job_leases(
    datastore: DataStore, time_machine: TimeMachineFixture
) -> None:
    """
    Test that the leases on acquired jobs are updated to prevent them from being cleaned
    up as if they had been abandoned.

    """
    time_machine.move_to(datetime(2020, 9, 14, tzinfo=timezone.utc))

    # Add a task to the data store
    task = Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    await datastore.add_task(task)

    # Add a job to the data store
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(seconds=30)
    )
    await datastore.add_job(job)

    # Acquire the job
    jobs = await datastore.acquire_jobs("scheduler_id", timedelta(seconds=30), 1)
    assert len(jobs) == 1

    # Move 20 seconds forward, then call extend_acquired_job_leases(). This should set
    # the acquired_until timestamp to 30 seconds from the new current time.
    time_machine.shift(20)
    await datastore.extend_acquired_job_leases(
        "scheduler_id", {job.id}, timedelta(seconds=30)
    )

    # The job was acquired by scheduler_id so scheduler2_id should not be able to
    # acquire it
    assert not await datastore.acquire_jobs("scheduler2_id", timedelta(seconds=30), 1)

    # Move 20 more seconds forward (beyond the initial lock expiration delay), then make
    # sure that the job is still within the data store.
    time_machine.shift(20)
    await datastore.cleanup()
    jobs = await datastore.get_jobs({job.id})
    assert len(jobs) == 1

    # Move 20 more seconds forward - this time the job's lease will have expired
    time_machine.shift(20)
    await datastore.cleanup()
    assert not await datastore.get_jobs({job.id})

    # Check that a result was recorded, with the "abandoned" outcome
    result = await datastore.get_job_result(job.id)
    assert result
    assert result.outcome is JobOutcome.abandoned


async def test_acquire_jobs_deserialization_failure(
    datastore: DataStore, mocker: MockerFixture
) -> None:
    if not isinstance(datastore, BaseExternalDataStore):
        pytest.skip("Only applicable to external data stores")

    # Add a task to the data store
    task = Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    await datastore.add_task(task)

    # Add a job to the data store
    job = Job(
        task_id="task1", executor="async", result_expiration_time=timedelta(seconds=30)
    )
    await datastore.add_job(job)

    # Make the serializer fail deserialization
    assert isinstance(datastore, BaseExternalDataStore)
    datastore.serializer = Mock(Serializer)
    datastore.serializer.deserialize.configure_mock(side_effect=DeserializationError)

    # This should not yield any jobs
    assert await datastore.acquire_jobs("scheduler_id", timedelta(seconds=30), 1) == []


async def test_reap_abandoned_jobs(
    datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> None:
    # Add a task, schedule and job and acquire the latter two
    task = Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    await datastore.add_task(task)

    job = Job(
        task_id="task1",
        executor="async",
        result_expiration_time=timedelta(seconds=30),
    )
    await datastore.add_job(job)
    await datastore.reap_abandoned_jobs("testscheduler")
    jobs = await datastore.acquire_jobs("testscheduler", timedelta(seconds=30), 1)
    assert len(jobs) == 1

    await datastore.reap_abandoned_jobs("testscheduler")
    assert not await datastore.get_jobs()
    abandoned_job_result = await datastore.get_job_result(jobs[0].id)
    assert abandoned_job_result.outcome is JobOutcome.abandoned

    task = await datastore.get_task("task1")
    assert task.running_jobs == 0


class TestRepr:
    async def test_memory(self, memory_store: MemoryDataStore) -> None:
        assert repr(memory_store) == "MemoryDataStore()"

    async def test_sqlite(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine

        expected_path = str(tmp_path).replace("\\", "\\\\")
        engine = create_engine(f"sqlite:///{tmp_path}")
        data_store = SQLAlchemyDataStore(engine)
        assert repr(data_store) == (
            f"SQLAlchemyDataStore(url='sqlite:///{expected_path}')"
        )

    async def test_psycopg(self) -> None:
        from sqlalchemy.ext.asyncio import create_async_engine

        pytest.importorskip("psycopg", reason="psycopg not available")
        engine = create_async_engine(
            "postgresql+psycopg://postgres:secret@localhost/testdb"
        )
        data_store = SQLAlchemyDataStore(engine, schema="myschema")
        assert repr(data_store) == (
            "SQLAlchemyDataStore(url='postgresql+psycopg://postgres:***@localhost/"
            "testdb', schema='myschema')"
        )

    async def test_mongodb(self) -> None:
        from pymongo.asynchronous.mongo_client import AsyncMongoClient

        async with AsyncMongoClient() as client:
            data_store = MongoDBDataStore(client)
            assert repr(data_store) == "MongoDBDataStore(host=[('localhost', 27017)])"
