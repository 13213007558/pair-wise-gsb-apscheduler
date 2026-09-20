import asyncio
from datetime import datetime

import pytest
from pytz import utc

from apscheduler.schedulers.asyncio import AsyncIOScheduler


@pytest.mark.anyio
async def test_scheduler_keeps_accepting_jobs_after_cancel_and_restart():
    """End-to-end: cancel, restart cycle never wedges the job at max_instances."""
    scheduler = AsyncIOScheduler()
    scheduler.start(paused=True)
    try:
        for cycle in range(3):
            started = asyncio.Event()
            release = asyncio.Event()

            async def stuck():
                started.set()
                await release.wait()

            job = scheduler.add_job(
                stuck, id="stuck", replace_existing=True, max_instances=1
            )
            executor = scheduler._lookup_executor(job.executor)
            executor.submit_job(job, [datetime.now(utc)])
            await started.wait()
            assert executor._instances["stuck"] == 1

            (task,) = executor._pending_futures
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            assert "stuck" not in executor._instances
            assert executor._active == {}

            # A new execution of the same job id must be accepted every cycle.
            done = asyncio.Event()

            async def quick():
                done.set()

            job2 = scheduler.add_job(quick, id=f"quick-{cycle}")
            executor.submit_job(job2, [datetime.now(utc)])
            await asyncio.wait_for(done.wait(), timeout=2)
            release.set()
    finally:
        scheduler.shutdown(False)


@pytest.mark.anyio
async def test_scheduler_shutdown_with_running_coroutine_releases_slot():
    scheduler = AsyncIOScheduler()
    scheduler.start(paused=True)
    started = asyncio.Event()
    release = asyncio.Event()

    async def stuck():
        started.set()
        await release.wait()

    job = scheduler.add_job(stuck, id="shutdown-stuck")
    executor = scheduler._lookup_executor(job.executor)
    executor.submit_job(job, [datetime.now(utc)])
    await started.wait()

    scheduler.shutdown(False)
    release.set()
    for _ in range(50):
        await asyncio.sleep(0)
        if "shutdown-stuck" not in executor._instances:
            break
    assert executor._active == {}
    assert dict(executor._instances) == {}


@pytest.mark.anyio
async def test_repeated_scheduler_start_restart_counter_resets():
    """After a restart, a reused executor accepts jobs without leaked counts."""
    scheduler = AsyncIOScheduler()
    scheduler.start(paused=True)
    executor = scheduler._lookup_executor("default")

    started = asyncio.Event()
    release = asyncio.Event()

    async def stuck():
        started.set()
        await release.wait()

    job = scheduler.add_job(stuck, id="reuse")
    executor.submit_job(job, [datetime.now(utc)])
    await started.wait()
    scheduler.shutdown(False)
    release.set()
    for _ in range(50):
        await asyncio.sleep(0)
        if not scheduler.running:
            break
    assert not scheduler.running

    scheduler.start(paused=True)
    executor = scheduler._lookup_executor("default")
    assert dict(executor._instances) == {}
    assert executor._active == {}

    completed = asyncio.Event()

    async def instant():
        completed.set()

    job2 = scheduler.add_job(instant, id="reuse2")
    executor.submit_job(job2, [datetime.now(utc)])
    await asyncio.wait_for(completed.wait(), timeout=2)
    scheduler.shutdown(False)
    for _ in range(50):
        await asyncio.sleep(0)
        if not scheduler.running:
            break
