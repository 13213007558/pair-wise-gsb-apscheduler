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

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.base import (
    BaseExecutor,
    MaxInstancesReachedError,
    run_coroutine_job,
    run_job,
)
from apscheduler.executors.debug import DebugExecutor
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


def test_max_instances_scoped_to_jobstore(
    mock_scheduler, executor, create_job, timezone
):
    """
    Tests that jobs with the same ID in different job stores do not share the max_instances quota,
    while the same job ID in the same job store still does.

    """
    now = datetime.now(timezone)
    job1 = create_job(func=wait_event, max_instances=1, next_run_time=None)
    job1._jobstore_alias = "store1"
    job2 = create_job(func=wait_event, max_instances=1, next_run_time=None)
    job2._jobstore_alias = "store2"
    assert job1.id == job2.id

    executor.submit_job(job1, [now])
    executor.submit_job(job2, [now])
    assert executor._instances == {("store1", job1.id): 1, ("store2", job2.id): 1}

    # Recreating the Job object must not bypass the limit within the same job store
    job3 = create_job(func=wait_event, max_instances=1, next_run_time=None)
    job3._jobstore_alias = "store1"
    pytest.raises(MaxInstancesReachedError, executor.submit_job, job3, [now])

    executor.shutdown()
    assert not executor._instances


def test_instance_counts_recover_after_failure(
    mock_scheduler, executor, create_job, timezone
):
    """
    Tests that repeatedly submitting failing jobs releases the instance slot every time, without
    leaking counts or driving them negative.

    """
    dispatched = Event()
    events = []

    def dispatch(event):
        events.append(event)
        dispatched.set()

    mock_scheduler._dispatch_event = dispatch
    now = datetime.now(timezone)
    job = create_job(func=failure, max_instances=1, next_run_time=None)
    job._jobstore_alias = "store1"

    for _ in range(5):
        executor.submit_job(job, [now])
        assert dispatched.wait(5)
        dispatched.clear()
        assert executor._instances.get(("store1", job.id), 0) == 0

    assert len(events) == 5
    assert all(event.code == EVENT_JOB_ERROR for event in events)
    assert not executor._instances


def test_synchronous_completion_releases_instance(mock_scheduler, create_job, timezone):
    """
    Tests that when a job completes synchronously during submission (as happens with the debug
    executor, or a future that is already done), the instance slot is properly released instead
    of leaking or going negative.

    """
    executor = DebugExecutor()
    executor.start(mock_scheduler, "debug")
    events = []
    mock_scheduler._dispatch_event = events.append
    now = datetime.now(timezone)
    job = create_job(func=success, max_instances=1, next_run_time=None)
    job._jobstore_alias = "store1"

    for _ in range(3):
        executor.submit_job(job, [now])
        assert not executor._instances

    assert len(events) == 3
    assert all(event.code == EVENT_JOB_EXECUTED for event in events)


def test_failed_submission_releases_instance(mock_scheduler, create_job, timezone):
    """
    Tests that if the job cannot be submitted to the executor, the reserved instance slot is
    released so that subsequent submissions are not blocked and no count is leaked.

    """

    class FailingExecutor(BaseExecutor):
        def _do_submit_job(self, job, run_times):
            raise RuntimeError("submission failed")

    executor = FailingExecutor()
    executor.start(mock_scheduler, "failing")
    now = datetime.now(timezone)
    job = create_job(func=success, max_instances=1, next_run_time=None)
    job._jobstore_alias = "store1"

    for _ in range(3):
        with pytest.raises(RuntimeError, match="submission failed"):
            executor.submit_job(job, [now])

        assert not executor._instances


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

    def run_job_error(jobstore_alias, job_id, exc, traceback):
        assert jobstore_alias == "foo"
        assert job_id == "abc"
        exc_traceback[:] = [exc, traceback]
        event.set()

    event = Event()
    exc_traceback = [None, None]
    monkeypatch.setattr("apscheduler.executors.base.run_job", dummy_run_job)
    monkeypatch.setattr("apscheduler.executors.pool.run_job", dummy_run_job)
    monkeypatch.setattr(executor, "_run_job_error", run_job_error)
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
    asyncio_executor._run_job_success = lambda jobstore_alias, job_id, events: (
        future.set_result(events)
    )
    asyncio_executor._run_job_error = lambda jobstore_alias, job_id, exc, tb: (
        future.set_exception(exc)
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
    tornado_executor._run_job_success = lambda jobstore_alias, job_id, events: (
        future.set_result(events)
    )
    tornado_executor._run_job_error = lambda jobstore_alias, job_id, exc, tb: (
        future.set_exception(exc)
    )
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

    job = asyncio_scheduler.add_job(waiter, "interval", seconds=1, args=[sleep, None])
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    futures = asyncio_executor._pending_futures.copy()
    assert len(futures) == 1

    asyncio_executor.shutdown()
    with pytest.raises(CancelledError):
        await futures.pop()


@pytest.mark.anyio
async def test_asyncio_max_instances_scoped_to_jobstore(
    asyncio_scheduler, asyncio_executor, create_job, timezone
):
    """
    Tests that the asyncio executor tracks max_instances separately for each job store, even for
    jobs sharing the same ID.

    """
    import asyncio

    started1, started2 = asyncio.Event(), asyncio.Event()
    release = asyncio.Event()

    async def blocking_job(started):
        started.set()
        await release.wait()

    now = datetime.now(timezone)
    job1 = create_job(
        func=blocking_job, args=(started1,), max_instances=1, next_run_time=None
    )
    job1._jobstore_alias = "store1"
    job2 = create_job(
        func=blocking_job, args=(started2,), max_instances=1, next_run_time=None
    )
    job2._jobstore_alias = "store2"
    assert job1.id == job2.id

    asyncio_executor.submit_job(job1, [now])
    asyncio_executor.submit_job(job2, [now])
    await asyncio.wait_for(started1.wait(), 5)
    await asyncio.wait_for(started2.wait(), 5)
    assert asyncio_executor._instances == {
        ("store1", job1.id): 1,
        ("store2", job2.id): 1,
    }

    # Recreating the Job object must not bypass the limit within the same job store
    job3 = create_job(
        func=blocking_job, args=(started1,), max_instances=1, next_run_time=None
    )
    job3._jobstore_alias = "store1"
    pytest.raises(MaxInstancesReachedError, asyncio_executor.submit_job, job3, [now])

    release.set()
    await asyncio.wait_for(asyncio.gather(*asyncio_executor._pending_futures), 5)
    await asyncio.sleep(0)
    assert not asyncio_executor._instances
