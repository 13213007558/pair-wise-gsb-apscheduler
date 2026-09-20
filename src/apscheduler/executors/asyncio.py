import asyncio
import sys
import threading

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
        self._thread_job_started = {}
        self._accepting_jobs = True
        self._shutdown_task = None

    def shutdown(self, wait=True):
        # There is no way to honor wait=True without converting this method into a coroutine method
        self._accepting_jobs = False
        for f in self._pending_futures:
            if not f.done():
                f.cancel()

        self._pending_futures.clear()

    async def shutdown_async(self, timeout=None):
        """
        Asynchronously shut down this executor, waiting for submitted jobs to finish.

        New job submissions are rejected immediately. Jobs already submitted are then
        awaited without blocking the event loop:

        * Jobs that finish within ``timeout`` dispatch their success/error events as
          usual.
        * If ``timeout`` expires, pending native coroutine jobs are cancelled and
          their cleanup (e.g. ``finally`` blocks) is awaited before this method
          returns.
        * Jobs running in worker threads cannot be cancelled or killed. They are
          reported to the caller (and logged) as still running; this method does
          **not** wait for them and never claims they were cancelled.

        Concurrent and repeated calls share the same shutdown process and all receive
        the same result. The ``timeout`` of the first call wins.

        :param float|None timeout: maximum number of seconds to wait for pending jobs
            to finish on their own before cancelling the cancellable ones (``None``
            means wait indefinitely)
        :return: list of futures that were still running in worker threads and could
            not be cancelled (empty if all jobs finished or were cancelled)
        """
        if self._shutdown_task is None:
            self._shutdown_task = self._eventloop.create_task(
                self._shutdown_async(timeout)
            )

        # Shield the shared task so that one caller being cancelled does not
        # interrupt the shutdown process for the others.
        return await asyncio.shield(self._shutdown_task)

    async def _shutdown_async(self, timeout):
        # Stop accepting new job submissions first
        self._accepting_jobs = False

        pending = [f for f in self._pending_futures if not f.done()]
        if pending:
            _, pending = await asyncio.wait(pending, timeout=timeout)

        still_running = []
        if pending:
            cancelled = []
            for f in pending:
                if isinstance(f, asyncio.Task):
                    # Native coroutine job: cancel it and wait for its cleanup
                    # (e.g. finally blocks) to finish below.
                    if f.cancel():
                        cancelled.append(f)
                else:
                    # Thread pool job: cancellation only has an effect if the job
                    # is still queued in the pool; a running thread cannot be
                    # killed. Note that cancelling the wrapper future alone would
                    # report success even when the underlying thread keeps
                    # running, so the started flag is the source of truth here.
                    started = self._thread_job_started.get(f)
                    if started is not None and started.is_set():
                        # Leave the future pending so that the job's result (and
                        # its success/error event) is still delivered when the
                        # thread eventually finishes.
                        still_running.append(f)
                    else:
                        f.cancel()
                        cancelled.append(f)

            if cancelled:
                # Let the cancelled coroutines run their finally blocks to
                # completion before resources are torn down.
                await asyncio.wait(cancelled)

            if still_running:
                self._logger.warning(
                    "Executor shutdown timed out with %d job(s) still running in "
                    "worker threads; these cannot be cancelled and were left running",
                    len(still_running),
                )

        return still_running

    def _do_submit_job(self, job, run_times):
        if not self._accepting_jobs:
            raise RuntimeError(
                "This executor is shutting down and no longer accepts new jobs"
            )

        def callback(f):
            self._pending_futures.discard(f)
            self._thread_job_started.pop(f, None)
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
            started = threading.Event()

            def func():
                started.set()
                return run_job(job, job._jobstore_alias, run_times, self._logger.name)

            f = self._eventloop.run_in_executor(None, func)
            self._thread_job_started[f] = started

        f.add_done_callback(callback)
        self._pending_futures.add(f)
