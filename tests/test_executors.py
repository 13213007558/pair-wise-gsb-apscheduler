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


def blocking_wait(started, release):
    """Blocks in a controlled manner until released (must be importable for pickling)."""
    started.set()
    release.wait(30)
    return "released"


def blocking_wait_files(started_path, release_path):
    """File-based variant of blocking_wait() usable across process boundaries."""
    with open(started_path, "w"):
        pass
    deadline = time.monotonic() + 30
    while not os.path.exists(release_path):
        if time.monotonic() > deadline:
            raise TimeoutError("release file never appeared")
        time.sleep(0.05)
    return "released"


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


def wait_for_instance_count(executor, expected, timeout=10):
    """Poll the executor's instance counts until they match the expected total."""
    deadline = time.monotonic() + timeout
    while True:
        with executor._lock:
            total = sum(executor._instances.values())
        if total == expected:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"instance count did not reach {expected}: {dict(executor._instances)}"
            )
        time.sleep(0.01)


class ThreadBlocker:
    """Controls a job running in a thread of the same process."""

    func = staticmethod(blocking_wait)

    def __init__(self, directory):
        self._started = Event()
        self._release = Event()
        self.args = [self._started, self._release]

    def wait_started(self, timeout=10):
        return self._started.wait(timeout)

    def release(self):
        self._release.set()


class FileBlocker:
    """Controls a job running in another process via files (synchronization
    primitives cannot be passed to a spawned process pool worker)."""

    func = staticmethod(blocking_wait_files)

    def __init__(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        self._started_path = str(directory / "started")
        self._release_path = str(directory / "release")
        self.args = [self._started_path, self._release_path]

    def wait_started(self, timeout=10):
        deadline = time.monotonic() + timeout
        while not os.path.exists(self._started_path):
            if time.monotonic() > deadline:
                return False
            time.sleep(0.05)
        return True

    def release(self):
        with open(self._release_path, "w"):
            pass


def make_blocker(executor, directory):
    if isinstance(executor, ProcessPoolExecutor):
        return FileBlocker(directory)
    return ThreadBlocker(directory)


def make_blocking_job(create_job, alias, blocker):
    job = create_job(
        func=blocker.func,
        args=blocker.args,
        id="refresh",
        max_instances=1,
        next_run_time=None,
    )
    job._jobstore_alias = alias
    return job


def test_max_instances_isolated_per_jobstore(
    mock_scheduler, executor, create_job, timezone, tmp_path
):
    """
    Jobs with the same ID coming from different job stores must not share the
    max_instances quota of a shared executor.
    """
    events = []
    mock_scheduler._dispatch_event = lambda event: events.append(event)
    blockers = [
        make_blocker(executor, tmp_path / "job1"),
        make_blocker(executor, tmp_path / "job2"),
    ]
    now = datetime.now(timezone)
    job1 = make_blocking_job(create_job, "store1", blockers[0])
    job2 = make_blocking_job(create_job, "store2", blockers[1])

    executor.submit_job(job1, [now])
    assert blockers[0].wait_started()
    # Must not raise MaxInstancesReachedError despite the identical job ID
    executor.submit_job(job2, [now])
    assert blockers[1].wait_started()

    for blocker in blockers:
        blocker.release()
    executor.shutdown()
    assert dict(executor._instances) == {}
    assert sorted(event.jobstore for event in events) == ["store1", "store2"]
    assert all(event.job_id == "refresh" for event in events)
    assert all(event.retval == "released" for event in events)


def test_max_instances_enforced_within_jobstore(
    mock_scheduler, executor, create_job, timezone, tmp_path
):
    """
    A brand-new Job instance with the same ID in the same job store (e.g. the job was
    removed and re-added while running) must not bypass the max_instances quota.
    """
    blocker = make_blocker(executor, tmp_path / "job")
    now = datetime.now(timezone)
    job1 = make_blocking_job(create_job, "store1", blocker)
    executor.submit_job(job1, [now])
    assert blocker.wait_started()

    job2 = make_blocking_job(create_job, "store1", blocker)
    pytest.raises(MaxInstancesReachedError, executor.submit_job, job2, [now])

    blocker.release()
    executor.shutdown()
    assert dict(executor._instances) == {}


def test_instance_counts_recover_after_completion(
    mock_scheduler, executor, create_job, timezone
):
    """Successful and failed jobs must both release their instance slots."""
    mock_scheduler._dispatch_event = MagicMock()
    now = datetime.now(timezone)
    ok_job = create_job(func=success, id="ok", next_run_time=None)
    err_job = create_job(func=failure, id="err", next_run_time=None)
    for _ in range(5):
        executor.submit_job(ok_job, [now])
        wait_for_instance_count(executor, 0)
        executor.submit_job(err_job, [now])
        wait_for_instance_count(executor, 0)

    executor.shutdown()
    assert dict(executor._instances) == {}


def test_instance_counts_recover_after_run_job_crash(
    monkeypatch, mock_scheduler, executor, create_job, timezone
):
    """If run_job() itself raises, the instance slot must still be released."""
    monkeypatch.setattr("apscheduler.executors.base.run_job", dummy_run_job)
    monkeypatch.setattr("apscheduler.executors.pool.run_job", dummy_run_job)
    job = create_job(func=success, id="boom", max_instances=1, next_run_time=None)
    now = datetime.now(timezone)
    for _ in range(3):
        executor.submit_job(job, [now])
        wait_for_instance_count(executor, 0)

    executor.shutdown()
    assert dict(executor._instances) == {}


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
    asyncio_executor._run_job_success = lambda alias, job_id, events: future.set_result(
        events
    )
    asyncio_executor._run_job_error = (
        lambda alias, job_id, exc, tb: future.set_exception(exc)
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
    tornado_executor._run_job_success = lambda alias, job_id, events: future.set_result(
        events
    )
    tornado_executor._run_job_error = (
        lambda alias, job_id, exc, tb: future.set_exception(exc)
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
async def test_asyncio_max_instances_isolated_per_jobstore(
    asyncio_scheduler, asyncio_executor
):
    """AsyncIOExecutor must scope the max_instances quota per job store."""
    from asyncio import Event, sleep, wait_for

    started = [Event(), Event()]
    release = Event()

    async def blocking(index):
        started[index].set()
        await release.wait()

    asyncio_scheduler.add_jobstore("memory", "store2")
    job1 = asyncio_scheduler.add_job(
        blocking, args=[0], id="refresh", jobstore="default", max_instances=1
    )
    job2 = asyncio_scheduler.add_job(
        blocking, args=[1], id="refresh", jobstore="store2", max_instances=1
    )
    now = datetime.now(utc)

    asyncio_executor.submit_job(job1, [now])
    await wait_for(started[0].wait(), 5)
    # Must not raise MaxInstancesReachedError despite the identical job ID
    asyncio_executor.submit_job(job2, [now])
    await wait_for(started[1].wait(), 5)
    # The same job in the same job store is still limited while running
    with pytest.raises(MaxInstancesReachedError):
        asyncio_executor.submit_job(job2, [now])

    release.set()
    # Awaiting the pending tasks also runs the executor's done callbacks (they were
    # registered first), which release the instance counts
    for f in list(asyncio_executor._pending_futures):
        await f
    # Let any already queued done callbacks run before checking the counts
    for _ in range(10):
        if not asyncio_executor._instances:
            break
        await sleep(0)
    assert dict(asyncio_executor._instances) == {}


@pytest.mark.anyio
async def test_asyncio_executor_shutdown_releases_instances(
    asyncio_scheduler, asyncio_executor
):
    """Cancelling a pending job on shutdown must release its instance slot."""
    from asyncio import Event, sleep, wait_for

    started = Event()
    never = Event()

    async def blocking():
        started.set()
        await never.wait()

    job = asyncio_scheduler.add_job(blocking, id="stuck", max_instances=1)
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    await wait_for(started.wait(), 5)
    assert asyncio_executor._instances[("default", "stuck")] == 1

    futures = list(asyncio_executor._pending_futures)
    asyncio_executor.shutdown()
    for f in futures:
        try:
            await f
        except CancelledError:
            pass
    # Let any already queued done callbacks run before checking the counts
    for _ in range(10):
        if not asyncio_executor._instances:
            break
        await sleep(0)
    assert dict(asyncio_executor._instances) == {}
