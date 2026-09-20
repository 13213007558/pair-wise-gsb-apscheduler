import asyncio
import threading
from datetime import datetime

import pytest
from pytz import utc

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.base import (
    MaxInstancesReachedError,
    run_coroutine_job,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler


@pytest.fixture
async def scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.start(paused=True)
    executor = AsyncIOExecutor()
    scheduler.add_executor(executor, "test")
    executor = scheduler._lookup_executor("test")
    yield scheduler, executor
    if scheduler.running:
        scheduler.shutdown(False)


async def pump(iterations=100):
    for _ in range(iterations):
        await asyncio.sleep(0)


@pytest.mark.anyio
async def test_cancelled_coroutine_releases_slot_and_emits_no_error(scheduler):
    """A cancelled coroutine task releases its slot once and is not an error event."""
    sched, executor = scheduler
    events = []
    sched.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    started = asyncio.Event()
    release = asyncio.Event()

    async def job_func():
        started.set()
        await release.wait()

    job = sched.add_job(job_func, id="c1")
    executor.submit_job(job, [datetime.now(utc)])
    await started.wait()
    assert executor._instances["c1"] == 1

    (task,) = executor._pending_futures
    task.cancel()
    task.cancel()  # repeated cancellation must be harmless
    with pytest.raises(asyncio.CancelledError):
        await task
    await pump()

    assert "c1" not in executor._instances
    assert executor._active == {}
    assert events == []

    # The slot is back: the same job can run again.
    done = asyncio.Event()

    async def quick():
        done.set()

    job2 = sched.add_job(quick, id="c1b")
    executor.submit_job(job2, [datetime.now(utc)])
    await done.wait()
    await pump()


@pytest.mark.anyio
async def test_exception_still_releases_slot(scheduler):
    sched, executor = scheduler
    events = []
    sched.add_listener(events.append, EVENT_JOB_ERROR)

    async def boom():
        raise RuntimeError("boom")

    job = sched.add_job(boom, id="err")
    executor.submit_job(job, [datetime.now(utc)])
    while executor._active:
        await asyncio.sleep(0)

    assert "err" not in executor._instances
    assert len(events) == 1
    assert events[0].code == EVENT_JOB_ERROR


@pytest.mark.anyio
async def test_cancel_complete_race_releases_once(scheduler):
    """Completion vs. cancellation must end in exactly one release."""
    sched, executor = scheduler

    ran = asyncio.Event()

    async def job_func():
        ran.set()

    job = sched.add_job(job_func, id="race")
    executor.submit_job(job, [datetime.now(utc)])
    (task,) = executor._pending_futures
    task.cancel()  # likely a no-op as the task finishes immediately
    await asyncio.gather(task, return_exceptions=True)
    await pump()

    assert executor._active == {}
    assert dict(executor._instances) == {}


@pytest.mark.anyio
async def test_stale_callback_after_restart_does_not_touch_new_generation(scheduler):
    """A late callback from an old generation must not release a new execution's slot."""
    sched, executor = scheduler
    started = asyncio.Event()
    release = asyncio.Event()

    async def job_func():
        started.set()
        await release.wait()

    job = sched.add_job(job_func, id="gen")
    executor.submit_job(job, [datetime.now(utc)])
    await started.wait()
    old_task = next(iter(executor._pending_futures))
    old_token = next(iter(executor._active))

    # Simulate a shutdown/restart while a task from the old generation is still alive.
    executor.shutdown()
    executor.start(sched, "test")
    assert executor._active == {}

    started2 = asyncio.Event()
    release2 = asyncio.Event()

    async def job_func2():
        started2.set()
        await release2.wait()

    job2 = sched.add_job(job_func2, id="gen", replace_existing=True)
    executor.submit_job(job2, [datetime.now(utc)])
    await started2.wait()
    assert executor._instances["gen"] == 1

    # The stale task from the old generation completes; it must not affect the new
    # generation's accounting (no decrement, no negative counter).
    release.set()
    await asyncio.gather(old_task, return_exceptions=True)
    await pump()
    assert old_token not in executor._active
    assert executor._instances["gen"] == 1

    with pytest.raises(MaxInstancesReachedError):
        executor.submit_job(job2, [datetime.now(utc)])

    release2.set()
    while executor._active:
        await asyncio.sleep(0)
    assert executor._active == {}


@pytest.mark.anyio
async def test_shutdown_cancels_coroutines_and_keeps_counter_nonnegative(scheduler):
    sched, executor = scheduler
    started = asyncio.Event()
    release = asyncio.Event()

    async def job_func():
        started.set()
        await release.wait()

    job = sched.add_job(job_func, id="down")
    executor.submit_job(job, [datetime.now(utc)])
    await started.wait()
    executor.shutdown()
    while "down" in executor._instances:
        await asyncio.sleep(0)

    assert executor._active == {}
    release.set()


@pytest.mark.anyio
async def test_uncancellable_thread_job_is_barrier(scheduler):
    """Cancellation while a thread function runs waits for the real thread."""
    sched, executor = scheduler
    events = []
    sched.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    entered = threading.Event()
    proceed = threading.Event()

    def thread_job():
        entered.set()
        proceed.wait(5)
        return "thread-result"

    job = sched.add_job(thread_job, id="thread", misfire_grace_time=None)
    executor.submit_job(job, [datetime.now(utc)])
    for _ in range(50):
        await asyncio.sleep(0)
        if entered.is_set():
            break
    assert entered.wait(5)
    (task,) = executor._pending_futures
    task.cancel()
    await asyncio.sleep(0.05)

    # The slot remains accounted for while the thread is still alive.
    assert executor._instances["thread"] == 1

    proceed.set()
    results = await asyncio.gather(task, return_exceptions=True)
    assert results[0][0].retval == "thread-result"
    while executor._active:
        await asyncio.sleep(0)

    assert executor._active == {}
    assert len(events) == 1
    assert events[0].code == EVENT_JOB_EXECUTED
    assert events[0].retval == "thread-result"


@pytest.mark.anyio
async def test_queued_future_cancelled_before_start_releases_slot(scheduler, monkeypatch):
    """Cancellation of a queued-but-not-started work item releases the slot."""
    sched, executor = scheduler

    class CancelledBeforeRun(asyncio.Future):
        """Mimics run_in_executor() for a work item cancelled while queued."""

        def cancel(self, msg=None):
            if not self.done():
                super().cancel(msg)
            return True

    def fake_run_in_executor(exc, func, *args):
        fut = CancelledBeforeRun(loop=sched._eventloop)
        # Never schedule func; the outer task's first cancel() removes the work item.
        sched._eventloop.call_soon(fut.cancel)
        return fut

    monkeypatch.setattr(
        sched._eventloop, "run_in_executor", fake_run_in_executor
    )

    started = threading.Event()
    job = sched.add_job(
        lambda: started.set(), id="queued", misfire_grace_time=None
    )
    executor.submit_job(job, [datetime.now(utc)])
    (task,) = executor._pending_futures
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert executor._active == {}
    assert "queued" not in executor._instances
    assert not started.is_set()


@pytest.mark.anyio
async def test_max_instances_permanently_recovered_after_cancel(scheduler):
    """The original symptom: after cancels the job keeps being schedulable."""
    sched, executor = scheduler
    started = asyncio.Event()
    release = asyncio.Event()

    async def job_func():
        started.set()
        await release.wait()

    job = sched.add_job(job_func, id="perm")
    for _ in range(3):
        executor.submit_job(job, [datetime.now(utc)])
        await started.wait()
        task = next(iter(executor._pending_futures))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        started.clear()

    assert executor._instances.get("perm", 0) == 0
    assert executor._active == {}


@pytest.mark.anyio
async def test_run_coroutine_job_propagates_cancellation(scheduler):
    """run_coroutine_job must not turn CancelledError into a JOB_ERROR event."""
    sched, executor = scheduler
    started = asyncio.Event()

    async def never_returns():
        started.set()
        await asyncio.Event().wait()

    job = sched.add_job(never_returns, id="prop")
    coro = run_coroutine_job(
        job, "test", [datetime.now(utc)], executor._logger.name
    )
    task = asyncio.ensure_future(coro)
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_submit_failure_rolls_back_reserved_slot(scheduler, monkeypatch):
    """If the backend refuses the job, the reserved slot is rolled back."""
    sched, executor = scheduler

    def boom(*args):
        raise RuntimeError("backend down")

    monkeypatch.setattr(executor, "_do_submit_job", boom)
    job = sched.add_job(lambda: None, id="nofuture")
    with pytest.raises(RuntimeError, match="backend down"):
        executor.submit_job(job, [datetime.now(utc)])

    assert executor._active == {}
    assert dict(executor._instances) == {}

    # The job can be submitted again once the backend recovers.
    monkeypatch.undo()
    ran = asyncio.Event()

    async def quick():
        ran.set()

    job2 = sched.add_job(quick, id="nofuture2")
    executor.submit_job(job2, [datetime.now(utc)])
    await ran.wait()
