import asyncio
from functools import partial, wraps

from apscheduler.events import EVENT_SCHEDULER_SHUTDOWN, SchedulerEvent
from apscheduler.schedulers import (
    SchedulerAlreadyRunningError,
    SchedulerNotRunningError,
)
from apscheduler.schedulers.base import STATE_STOPPED, BaseScheduler
from apscheduler.util import maybe_ref


def run_in_event_loop(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        wrapped = partial(func, self, *args, **kwargs)
        self._eventloop.call_soon_threadsafe(wrapped)

    return wrapper


class AsyncIOScheduler(BaseScheduler):
    """
    A scheduler that runs on an asyncio (:pep:`3156`) event loop.

    The default executor can run jobs based on native coroutines (``async def``).

    Extra options:

    ============== =============================================================
    ``event_loop`` AsyncIO event loop to use (defaults to the global event loop)
    ============== =============================================================
    """

    _eventloop = None
    _timeout = None
    _shutdown_future = None

    def start(self, paused=False):
        if self._shutdown_future is not None:
            if not self._shutdown_future.done():
                raise SchedulerAlreadyRunningError(
                    "Cannot start the scheduler while an asynchronous shutdown "
                    "is still in progress"
                )

            # A previous shutdown has completed; allow the scheduler to be
            # started again with a fresh shutdown state
            self._shutdown_future = None

        if not self._eventloop:
            self._eventloop = asyncio.get_running_loop()

        super().start(paused)

    @run_in_event_loop
    def shutdown(self, wait=True):
        super().shutdown(wait)
        self._stop_timer()

    async def async_shutdown(self, timeout=None):
        """
        Asynchronously shut down the scheduler, waiting for running jobs to finish.

        This is the awaitable counterpart of :meth:`shutdown`, intended for use in
        asynchronous application lifecycles (for example an ASGI lifespan handler).
        It must be awaited on the same event loop the scheduler is running on.

        The shutdown proceeds as follows:

        1. Job processing is stopped immediately, so no new jobs are submitted.
        2. Already submitted jobs are awaited, without blocking the event loop,
           until they finish or the timeout expires. Success and error events of
           jobs that finish within the timeout are dispatched normally.
        3. If the timeout expires first, coroutine jobs are cancelled and awaited
           so that their finally blocks run to completion. Jobs running in worker
           threads cannot be cancelled; they are reported to the caller and keep
           running in the background.
        4. Executors and job stores are shut down and the scheduler moves to the
           stopped state.

        Concurrent calls share the same shutdown process and all of them receive
        its result. Once the shutdown has completed, further calls return the same
        result immediately without doing anything. Starting the scheduler again
        resets this state.

        :param float|None timeout: maximum number of seconds to wait for running
            jobs to finish gracefully (None means wait indefinitely)
        :return: a list of futures whose jobs are still running in worker threads
            and could not be cancelled; an empty list means a fully clean shutdown
        :raises SchedulerNotRunningError: if the scheduler has not been started

        """
        shutdown_future = self._shutdown_future
        if shutdown_future is None:
            if self.state == STATE_STOPPED:
                raise SchedulerNotRunningError

            if not self._eventloop:
                self._eventloop = asyncio.get_running_loop()

            shutdown_future = self._shutdown_future = self._eventloop.create_task(
                self._async_shutdown(timeout)
            )

        return await shutdown_future

    async def _async_shutdown(self, timeout):
        # Stop the wakeup timer first so that no new jobs get submitted
        self._stop_timer()

        deadline = None if timeout is None else self._eventloop.time() + timeout
        still_running = []
        with self._executors_lock, self._jobstores_lock:
            for executor in self._executors.values():
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - self._eventloop.time())
                )
                executor_async_shutdown = getattr(executor, "async_shutdown", None)
                if executor_async_shutdown is not None:
                    still_running.extend(await executor_async_shutdown(remaining))
                else:
                    # Executors without an asynchronous shutdown path are shut
                    # down synchronously, as before
                    executor.shutdown(wait=True)

            # Job stores are only shut down after the executors have drained, so
            # that finishing jobs can still record their outcomes
            for jobstore in self._jobstores.values():
                jobstore.shutdown()

        self.state = STATE_STOPPED
        self._logger.info("Scheduler has been shut down")
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))

        if still_running:
            self._logger.warning(
                "Shutdown completed with %d job(s) still running in worker "
                "threads; they could not be cancelled and were not waited on",
                len(still_running),
            )

        return still_running

    def _configure(self, config):
        self._eventloop = maybe_ref(config.pop("event_loop", None))
        super()._configure(config)

    def _start_timer(self, wait_seconds):
        self._stop_timer()
        if wait_seconds is not None:
            self._timeout = self._eventloop.call_later(wait_seconds, self.wakeup)

    def _stop_timer(self):
        if self._timeout:
            self._timeout.cancel()
            del self._timeout

    @run_in_event_loop
    def wakeup(self):
        self._stop_timer()
        wait_seconds = self._process_jobs()
        self._start_timer(wait_seconds)

    def _create_default_executor(self):
        from apscheduler.executors.asyncio import AsyncIOExecutor

        return AsyncIOExecutor()
