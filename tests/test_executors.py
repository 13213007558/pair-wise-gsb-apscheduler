import asyncio
import concurrent.futures
import gc
import os
import signal
import time
from asyncio import CancelledError
from datetime import datetime
from threading import Event
from types import TracebackType
from unittest.mock import MagicMock, Mock, patch

import pytest
from pytz import UTC, utc

from apscheduler.events import (
    EVENT_JOB_CANCELLED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
)
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.base import (
    MaxInstancesReachedError,
    run_coroutine_job,
    run_job,
)
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.util import localize


@pytest.fixture
def mock_scheduler(timezone):
    scheduler_ = Mock(BaseScheduler, timezone=timezone)
    scheduler_._create_lock = MagicMock()
    return scheduler_


@pytest.fixture(params=["threadpool", "processpool"])
def executor(request, mock_scheduler):
    if request.param == "threadpool":
        from apscheduler.executors.pool import ThreadPoolExecutor

        executor_ = ThreadPoolExecutor()
    else:
        from apscheduler.executors.pool import ProcessPoolExecutor

        executor_ = ProcessPoolExecutor()

    executor_.start(mock_scheduler, "dummy")
    yield executor_
    executor_.shutdown()


@pytest.fixture
async def asyncio_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
async def asyncio_executor(asyncio_scheduler):
    executor = AsyncIOExecutor()
    executor.start(asyncio_scheduler, "default")
    yield executor
    executor.shutdown()


@pytest.fixture
def tornado_scheduler():
    scheduler = TornadoScheduler()
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
def tornado_executor(tornado_scheduler):
    executor = TornadoExecutor()
    executor.start(tornado_scheduler, "default")
    yield executor
    executor.shutdown()


async def waiter(sleep, exception):
    await sleep(0.1)
    if exception:
        raise Exception("dummy error")
    else:
        return True


def wait_event():
    time.sleep(0.2)
    return "test"


def failure():
    raise Exception("test failure")


def success():
    return 5


def test_max_instances(mock_scheduler, executor, create_job, timezone):
    """Tests that the maximum instance limit on a job is respected."""
    events = []
    mock_scheduler._dispatch_event = lambda event: events.append(event)
    now = datetime.now(timezone)
    job = create_job(func=wait_event, max_instances=2, next_run_time=None)
    executor.submit_job(job, [now])
    executor.submit_job(job, [now])

    pytest.raises(MaxInstancesReachedError, executor.submit_job, job, [now])
    executor.shutdown()
    assert len(events) == 2
    assert events[0].retval == "test"
    assert events[1].retval == "test"


@pytest.mark.parametrize(
    "event_code,func",
    [
        (EVENT_JOB_EXECUTED, success),
        (EVENT_JOB_MISSED, failure),
        (EVENT_JOB_ERROR, failure),
    ],
    ids=["executed", "missed", "error"],
)
def test_submit_job(mock_scheduler, executor, create_job, timezone, event_code, func):
    """
    Tests that an EVENT_JOB_EXECUTED event is delivered to the scheduler if the job was
    successfully executed.

    """
    mock_scheduler._dispatch_event = MagicMock()
    job = create_job(func=func, id="foo")
    job._jobstore_alias = "test_jobstore"
    now = datetime.now(timezone)
    run_time = (
        localize(datetime(1970, 1, 1), timezone)
        if event_code == EVENT_JOB_MISSED
        else now
    )
    executor.submit_job(job, [run_time])
    executor.shutdown()

    assert mock_scheduler._dispatch_event.call_count == 1
    event = mock_scheduler._dispatch_event.call_args[0][0]
    assert event.code == event_code
    assert event.job_id == "foo"
    assert event.jobstore == "test_jobstore"

    if event_code == EVENT_JOB_EXECUTED:
        assert event.retval == 5
    elif event_code == EVENT_JOB_ERROR:
        assert str(event.exception) == "test failure"
        assert isinstance(event.traceback, str)


class FauxJob:
    id = "abc"
    max_instances = 1
    _jobstore_alias = "foo"


def dummy_run_job(job, jobstore_alias, run_times, logger_name):
    raise Exception("dummy")


def test_run_job_error(monkeypatch, executor):
    """Tests that _run_job_error is properly called if an exception is raised in run_job()"""

    def finish_execution(execution, events=(), exception=None, traceback=None):
        assert execution.job_id == "abc"
        exc_traceback[:] = [exception, traceback]
        event.set()

    event = Event()
    exc_traceback = [None, None]
    monkeypatch.setattr("apscheduler.executors.base.run_job", dummy_run_job)
    monkeypatch.setattr("apscheduler.executors.pool.run_job", dummy_run_job)
    monkeypatch.setattr(executor, "_finish_execution", finish_execution)
    executor.submit_job(FauxJob(), [])

    event.wait(5)
    assert str(exc_traceback[0]) == "dummy"
    if exc_traceback[1] is not None:
        assert isinstance(exc_traceback[1], TracebackType)


def test_run_job_memory_leak():
    class FooBar:
        pass

    def func():
        foo = FooBar()  # noqa: F841
        raise Exception("dummy")

    fake_job = Mock(Job, func=func, args=(), kwargs={}, misfire_grace_time=1)
    with patch("logging.getLogger"):
        for _ in range(5):
            run_job(fake_job, "foo", [datetime.now(UTC)], __name__)

    foos = [x for x in gc.get_objects() if type(x) is FooBar]
    assert len(foos) == 0


@pytest.mark.anyio
async def test_run_async_job_memory_leak():
    class FooBar:
        pass

    async def func():
        foo = FooBar()  # noqa: F841
        raise Exception("dummy")

    fake_job = Mock(Job, func=func, args=(), kwargs={}, misfire_grace_time=1)
    with patch("logging.getLogger"):
        for _ in range(5):
            await run_coroutine_job(fake_job, "foo", [datetime.now(utc)], __name__)

    foos = [x for x in gc.get_objects() if type(x) is FooBar]
    assert len(foos) == 0


def test_broken_pool():
    def listener(evt):
        pid[0] = evt.retval
        event.set()

    pid = [None]
    event = Event()
    scheduler = BackgroundScheduler(executors={"default": ProcessPoolExecutor(1)})
    scheduler.add_listener(listener, EVENT_JOB_EXECUTED)
    scheduler.add_job(os.getpid, "date", run_date=datetime.now(UTC))
    scheduler.start()

    event.wait(3)
    killed_pid = pid[0]
    os.kill(pid[0], signal.SIGTERM)
    try:
        os.waitpid(pid[0], 0)
    except OSError:
        pass

    event.clear()
    scheduler.add_job(os.getpid, "date", run_date=datetime.now(UTC))
    event.wait(3)
    assert pid[0] != killed_pid
    scheduler.shutdown(True)


@pytest.mark.parametrize("exception", [False, True])
@pytest.mark.anyio
async def test_run_coroutine_job(asyncio_scheduler, asyncio_executor, exception):
    from asyncio import Future, sleep

    future = Future()
    job = asyncio_scheduler.add_job(
        waiter, "interval", seconds=1, args=[sleep, exception]
    )
    asyncio_executor._finish_execution = (
        lambda execution, events=(), exception=None, traceback=None: (
            future.set_exception(exception)
            if exception is not None
            else future.set_result(events)
        )
    )
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == "dummy error"
    else:
        assert events[0].retval is True


@pytest.mark.parametrize("exception", [False, True])
@pytest.mark.anyio
async def test_run_coroutine_job_tornado(
    tornado_scheduler, tornado_executor, exception
):
    from tornado.concurrent import Future
    from tornado.gen import sleep

    future = Future()
    job = tornado_scheduler.add_job(
        waiter, "interval", seconds=1, args=[sleep, exception]
    )
    tornado_executor._run_job_success = lambda job_id, events: future.set_result(events)
    tornado_executor._run_job_error = lambda job_id, exc, tb: future.set_exception(exc)
    tornado_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == "dummy error"
    else:
        assert events[0].retval is True


@pytest.mark.anyio
async def test_asyncio_executor_shutdown(asyncio_scheduler, asyncio_executor):
    """Test that the AsyncIO executor cancels its pending tasks on shutdown."""
    from asyncio import sleep

    events = []
    asyncio_scheduler.add_listener(
        events.append, EVENT_JOB_EXECUTED | EVENT_JOB_CANCELLED
    )
    job = asyncio_scheduler.add_job(waiter, "interval", seconds=1, args=[sleep, None])
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    futures = asyncio_executor._pending_futures.copy()
    assert len(futures) == 1

    asyncio_executor.shutdown()
    with pytest.raises(CancelledError):
        await futures.pop()
    assert not asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_CANCELLED


@pytest.mark.anyio
async def test_asyncio_executor_cancels_queued_coroutine(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocker():
        started.set()
        await release.wait()

    async def cancelled_job():
        await asyncio.Event().wait()

    first_job = asyncio_scheduler.add_job(blocker)
    first = asyncio_executor.submit_job(first_job, [datetime.now(utc)])
    await started.wait()

    second_job = asyncio_scheduler.add_job(cancelled_job)
    second = asyncio_executor.submit_job(second_job, [datetime.now(utc)])
    assert asyncio_executor._instances[second_job.id] == 1

    assert asyncio_executor.cancel_execution(second)
    with pytest.raises(CancelledError):
        await second.future

    assert second_job.id not in asyncio_executor._instances
    assert second.future.cancelled()
    assert events[-1].code == EVENT_JOB_CANCELLED

    assert asyncio_executor.cancel_execution(second)
    assert asyncio_executor._instances.get(second_job.id, 0) == 0

    release.set()
    await first.future
    assert not asyncio_executor._instances


@pytest.mark.anyio
async def test_asyncio_executor_cancels_running_coroutine(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)
    started = asyncio.Event()

    async def blocker():
        started.set()
        await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_scheduler._register_execution(
        asyncio_executor, job, [datetime.now(utc)]
    )
    await started.wait()

    assert asyncio_executor.cancel_execution(execution)
    with pytest.raises(CancelledError):
        await execution.future

    assert not asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_CANCELLED


@pytest.mark.anyio
async def test_asyncio_executor_cancel_completion_race(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)

    async def succeed():
        return "done"

    job = asyncio_scheduler.add_job(succeed)
    execution = asyncio_executor.submit_job(job, [datetime.now(utc)])
    await execution.future

    assert not asyncio_executor.cancel_execution(execution)
    assert not asyncio_executor._instances
    assert [event.code for event in events].count(EVENT_JOB_EXECUTED) == 1


@pytest.mark.anyio
async def test_asyncio_executor_coroutine_timeout(asyncio_scheduler, asyncio_executor):
    events = []
    asyncio_scheduler.add_listener(events.append)

    async def timeout_job():
        async with asyncio.timeout(0.01):
            await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(timeout_job)
    execution = asyncio_executor.submit_job(job, [datetime.now(utc)])
    await execution.future

    assert not asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_ERROR


@pytest.mark.anyio
async def test_asyncio_executor_uncancellable_thread(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)
    started = Event()
    release = Event()

    def blocker():
        started.set()
        release.wait(1)
        return "thread result"

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_executor.submit_job(job, [datetime.now(utc)])
    assert started.wait(1)

    assert asyncio_executor.cancel_execution(execution)
    await asyncio.sleep(0)
    assert asyncio_executor._instances[job.id] == 1

    release.set()
    await asyncio.sleep(0.2)
    assert not asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_EXECUTED
    assert events[-1].retval == "thread result"


@pytest.mark.anyio
async def test_asyncio_executor_cancels_queued_thread(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)
    asyncio_executor._eventloop._default_executor = (
        concurrent.futures.ThreadPoolExecutor(max_workers=1)
    )

    first_started = Event()
    first_release = Event()
    second_started = Event()

    def first():
        first_started.set()
        first_release.wait(1)

    def second():
        second_started.set()

    first_job = asyncio_scheduler.add_job(first)
    first_execution = asyncio_executor.submit_job(first_job, [datetime.now(utc)])
    assert first_started.wait(1)

    second_job = asyncio_scheduler.add_job(second)
    second_execution = asyncio_executor.submit_job(second_job, [datetime.now(utc)])
    assert asyncio_executor.cancel_execution(second_execution)
    await asyncio.sleep(0)
    assert not second_started.wait(0.05)
    assert second_job.id not in asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_CANCELLED

    first_release.set()
    await first_execution.future


@pytest.mark.anyio
async def test_asyncio_executor_does_not_release_new_generation(
    asyncio_scheduler, asyncio_executor
):
    started = Event()
    release = Event()

    def blocker():
        started.set()
        release.wait(1)

    release2 = Event()

    def immediate():
        release2.wait(1)
        return "new generation"

    old_job = asyncio_scheduler.add_job(blocker, id="same", max_instances=2)
    old_execution = asyncio_executor.submit_job(old_job, [datetime.now(utc)])
    assert started.wait(1)

    new_job = asyncio_scheduler.add_job(
        immediate,
        id="same",
        max_instances=2,
        replace_existing=True,
    )
    asyncio_executor.cancel_execution(old_execution)
    await asyncio.sleep(0)

    asyncio_executor.submit_job(new_job, [datetime.now(utc)])
    assert asyncio_executor._instances["same"] == 2

    release.set()
    await asyncio.sleep(0.2)
    assert asyncio_executor._instances["same"] == 1

    release2.set()
    await asyncio.sleep(0.2)
    assert "same" not in asyncio_executor._instances


@pytest.mark.anyio
async def test_asyncio_scheduler_restarts_after_cancelled_job(
    asyncio_scheduler, asyncio_executor
):
    started = asyncio.Event()

    async def blocker():
        started.set()
        await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_scheduler._register_execution(
        asyncio_executor, job, [datetime.now(utc)]
    )
    await started.wait()

    assert asyncio_scheduler.cancel_job(job.id)
    with pytest.raises(CancelledError):
        await execution.future

    assert not asyncio_executor._instances
    assert asyncio_scheduler._running_executions == {}

    async def accepted():
        return "accepted"

    follow_up = asyncio_scheduler.add_job(accepted)
    follow_up_execution = asyncio_scheduler._register_execution(
        asyncio_executor, follow_up, [datetime.now(utc)]
    )
    follow_up_events = await follow_up_execution.future
    assert follow_up_events[0].retval == "accepted"


@pytest.mark.anyio
async def test_asyncio_scheduler_shutdown_releases_execution(asyncio_scheduler):
    events = []
    asyncio_scheduler.add_listener(events.append)
    executor = asyncio_scheduler._executors["default"]

    started = asyncio.Event()

    async def blocker():
        started.set()
        await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_scheduler._register_execution(
        executor, job, [datetime.now(utc)]
    )
    await started.wait()

    asyncio_scheduler.shutdown(False)
    with pytest.raises(CancelledError):
        await execution.future

    assert not executor._instances
    assert events[-1].code == EVENT_JOB_CANCELLED
    asyncio_scheduler.start(paused=True)


@pytest.mark.anyio
async def test_asyncio_executor_cancel_before_submission(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append)
    job = asyncio_scheduler.add_job(success)
    execution = asyncio_executor._create_execution(job, [datetime.now(utc)])

    asyncio_executor.cancel_execution(execution)
    future = asyncio_executor._submit_execution(execution, job, [datetime.now(utc)])
    assert future is None
    await asyncio.sleep(0)

    assert execution.finished
    assert not asyncio_executor._instances
    assert events[-1].code == EVENT_JOB_CANCELLED


@pytest.mark.anyio
async def test_asyncio_executor_restart_keeps_new_generation_quota(
    asyncio_scheduler, asyncio_executor
):
    events = []
    asyncio_scheduler.add_listener(events.append, EVENT_JOB_EXECUTED)
    job = asyncio_scheduler.add_job(success)
    old_execution = asyncio_executor._create_execution(job, [datetime.now(utc)])

    asyncio_executor.start(asyncio_scheduler, "default")
    new_execution = asyncio_executor._create_execution(job, [datetime.now(utc)])
    assert asyncio_executor._instances[job.id] == 1

    asyncio_executor._finish_execution(old_execution, [object()])
    assert asyncio_executor._instances[job.id] == 1

    asyncio_executor._finish_execution(new_execution, [])
    assert job.id not in asyncio_executor._instances
    assert not events


@pytest.mark.anyio
async def test_asyncio_scheduler_remove_job_cancels_execution(
    asyncio_scheduler, asyncio_executor
):
    started = asyncio.Event()

    async def blocker():
        started.set()
        await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_scheduler._register_execution(
        asyncio_executor, job, [datetime.now(utc)]
    )
    await started.wait()

    asyncio_scheduler.remove_job(job.id)
    with pytest.raises(CancelledError):
        await execution.future

    assert not asyncio_executor._instances
    assert asyncio_scheduler._running_executions == {}


@pytest.mark.anyio
async def test_asyncio_scheduler_pause_job_cancels_execution(
    asyncio_scheduler, asyncio_executor
):
    started = asyncio.Event()

    async def blocker():
        started.set()
        await asyncio.Event().wait()

    job = asyncio_scheduler.add_job(blocker)
    execution = asyncio_scheduler._register_execution(
        asyncio_executor, job, [datetime.now(utc)]
    )
    await started.wait()

    asyncio_scheduler.pause_job(job.id)
    with pytest.raises(CancelledError):
        await execution.future

    assert not asyncio_executor._instances
    assert asyncio_scheduler._running_executions == {}
