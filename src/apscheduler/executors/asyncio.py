import asyncio
import sys

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
        # The set persists across generations: barrier tasks that survive a
        # shutdown/restart cycle keep a strong reference here until their own done
        # callback runs, but a new shutdown() must not cancel them again, so shutdown()
        # removes the still-pending tasks from the set before cancelling.
        if not hasattr(self, "_pending_futures"):
            self._pending_futures = set()

    def shutdown(self, wait=True):
        # Coroutine tasks are cancelled; they release their instance slot through their done
        # callback. Thread-pool tasks cannot be killed, so their wrapper tasks ignore further
        # cancellation requests and wait for the thread to finish (see _run_thread_job).
        # There is no way to honor wait=True without converting this method into a coroutine
        # method.
        pending = self._pending_futures
        self._pending_futures = set()
        for f in pending:
            if not f.done():
                f.cancel()

    async def _run_thread_job(self, job, jobstore_alias, run_times, logger_name):
        """
        Runs a synchronous job function in the event loop's default thread pool.

        Threads cannot be forcibly killed, so a cancellation request while the future is
        queued or the function is running acts as a barrier: it only marks the task as
        cancelling. Every cancellation is swallowed while this coroutine waits for the
        thread to complete, guaranteeing the instance slot is released exactly once
        when the thread is actually done.

        """
        future = self._eventloop.run_in_executor(
            None, run_job, job, jobstore_alias, run_times, logger_name
        )
        while True:
            try:
                # shield() prevents the cancellation propagated to this task from also
                # cancelling the wrapped thread future; a fresh shield is needed for
                # every wait because the previous one is itself cancelled.
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                if future.done():
                    return future.result()

    def _do_submit_job(self, job, run_times, token):
        def callback(f):
            # Runs in the event loop thread; idempotent release is enforced by the token
            # machinery in the base class.
            try:
                events = f.result()
            except asyncio.CancelledError:
                self._job_cancelled(token)
            except BaseException:
                self._job_failed(token, *sys.exc_info()[1:])
            else:
                self._job_succeeded(token, events)
            finally:
                self._pending_futures.discard(f)

        if iscoroutinefunction_partial(job.func):
            coro = run_coroutine_job(
                job, job._jobstore_alias, run_times, self._logger.name
            )
            f = self._eventloop.create_task(coro)
        else:
            coro = self._run_thread_job(
                job, job._jobstore_alias, run_times, self._logger.name
            )
            f = self._eventloop.create_task(coro)

        f.add_done_callback(callback)
        self._pending_futures.add(f)
