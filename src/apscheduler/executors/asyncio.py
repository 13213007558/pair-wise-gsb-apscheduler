import asyncio
import sys
from threading import Event

from apscheduler.executors.base import BaseExecutor, run_coroutine_job, run_job
from apscheduler.util import iscoroutinefunction_partial


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

    If the job function is a native coroutine function, it is scheduled to be run directly in the
    event loop as soon as possible. All other functions are run in the event loop's default
    executor which is usually a thread pool.

    Plugin alias: ``asyncio``
    """

    def start(self, scheduler, alias):
        super().start(scheduler, alias)
        self._eventloop = scheduler._eventloop
        self._pending_futures = set()
        self._accepting_submissions = True

    def shutdown(self, wait=True):
        # There is no way to honor wait=True without converting this method into a coroutine method
        self._accepting_submissions = False
        for f in self._pending_futures:
            if not f.done():
                f.cancel()

        self._pending_futures.clear()

    async def async_shutdown(self, timeout=None):
        """
        Asynchronously shut down this executor, waiting for submitted jobs to finish.

        New job submissions are rejected immediately. Already submitted jobs are
        awaited without blocking the event loop, and their success/error events are
        dispatched normally. If the timeout expires before all jobs have finished,
        coroutine jobs (which run as asyncio tasks) are cancelled and awaited so
        that their finally blocks run to completion. Jobs running in worker threads
        cannot be cancelled once started; any such jobs are reported to the caller
        instead of being silently abandoned.

        :param float|None timeout: maximum number of seconds to wait for pending
            jobs to finish gracefully (None means wait indefinitely); the
            cancellation cleanup of coroutine jobs is always awaited afterwards,
            regardless of the timeout
        :return: a list of futures whose jobs are still running in worker threads
            and could not be cancelled; an empty list means that all jobs either
            finished or were successfully cancelled

        """
        self._accepting_submissions = False

        pending = [f for f in self._pending_futures if not f.done()]
        if pending:
            _, pending = await asyncio.wait(pending, timeout=timeout)

        still_running = []
        if pending:
            # The timeout expired: cancel coroutine tasks and wait for their
            # cleanup (finally blocks) to complete
            tasks = [f for f in pending if isinstance(f, asyncio.Task)]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.wait(tasks)

            for f in [f for f in pending if not isinstance(f, asyncio.Task)]:
                if f.done():
                    continue

                started = getattr(f, "_apscheduler_started", None)
                if started is not None and not started.is_set():
                    # Still queued in the thread pool: try to cancel it before
                    # its thread starts running it
                    f.cancel()
                    if not started.is_set():
                        continue

                # A job already running in a worker thread cannot be killed;
                # leave its future alone so that its events are still dispatched
                # when it eventually finishes, and report it to the caller
                still_running.append(f)
                self._logger.warning(
                    'Job "%s" is still running in a worker thread and cannot be '
                    "cancelled; it will keep running in the background",
                    getattr(f, "_apscheduler_job_id", "<unknown>"),
                )

        self._pending_futures.clear()
        return still_running

    def submit_job(self, job, run_times):
        if not self._accepting_submissions:
            raise RuntimeError(
                "This executor is shutting down and no longer accepts new jobs"
            )

        super().submit_job(job, run_times)

    def _do_submit_job(self, job, run_times):
        def callback(f):
            self._pending_futures.discard(f)
            try:
                events = f.result()
            except BaseException:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        if iscoroutinefunction_partial(job.func):
            coro = run_coroutine_job(
                job, job._jobstore_alias, run_times, self._logger.name
            )
            f = self._eventloop.create_task(coro)
        else:
            started = Event()

            def run_thread_job():
                # Lets async_shutdown() tell apart queued jobs from ones already
                # running in a worker thread (which cannot be cancelled)
                started.set()
                return run_job(job, job._jobstore_alias, run_times, self._logger.name)

            f = self._eventloop.run_in_executor(None, run_thread_job)
            f._apscheduler_started = started

        f.add_done_callback(callback)
        f._apscheduler_job_id = job.id
        self._pending_futures.add(f)
