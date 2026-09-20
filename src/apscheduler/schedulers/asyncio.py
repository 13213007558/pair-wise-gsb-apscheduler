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
    _shutdown_task = None

    def start(self, paused=False):
        if self._shutdown_task is not None:
            if not self._shutdown_task.done():
                raise SchedulerAlreadyRunningError(
                    "Cannot start the scheduler while it is being shut down"
                )

            # A previous asynchronous shutdown has completed, so starting again
            # is allowed; reset the shutdown state for the next run.
            self._shutdown_task = None

        if not self._eventloop:
            self._eventloop = asyncio.get_running_loop()

        super().start(paused)

    @run_in_event_loop
    def shutdown(self, wait=True):
        super().shutdown(wait)
        self._stop_timer()

    async def shutdown_async(self, timeout=None):
        """
        Asynchronously shut down the scheduler, waiting for running jobs to finish.

        This is the awaitable counterpart of :meth:`shutdown`, intended for use in
        asynchronous application lifecycles (e.g. an ASGI lifespan handler). It works
        as follows:

        #. The scheduler's wakeup timer is stopped, so no new job processing rounds
           are started.
        #. Each executor stops accepting new job submissions and is awaited (see
           :meth:`apscheduler.executors.asyncio.AsyncIOExecutor.shutdown_async`).
           Jobs that finish within ``timeout`` dispatch their success/error events
           as usual. If the timeout expires, pending coroutine jobs are cancelled and
           their cleanup is awaited; jobs still running in worker threads cannot be
           cancelled and are reported in the return value instead.
        #. Only after that are the job stores shut down and the
           ``EVENT_SCHEDULER_SHUTDOWN`` event dispatched, so events from finished
           jobs are still delivered to listeners.

        The event loop is never blocked while waiting. Concurrent and repeated calls
        share the same shutdown process and all receive the same result; once the
        shutdown has completed, further calls return the same result immediately
        (unless the scheduler is started again, which resets the shutdown state).
        Starting the scheduler while a shutdown is in progress raises
        :class:`~apscheduler.schedulers.SchedulerAlreadyRunningError`.

        :param float|None timeout: maximum total number of seconds to wait for
            pending jobs to finish before cancelling the cancellable ones
            (``None`` means wait indefinitely)
        :return: dict of executor alias -> list of futures still running in worker
            threads that could not be cancelled (empty lists if the shutdown was
            clean)
        :raises SchedulerNotRunningError: if the scheduler has not been started
        """
        if self._shutdown_task is None:
            if self.state == STATE_STOPPED:
                raise SchedulerNotRunningError

            self._shutdown_task = self._eventloop.create_task(
                self._shutdown_async(timeout)
            )

        # Shield the shared task so that one caller being cancelled does not
        # interrupt the shutdown process for the others.
        return await asyncio.shield(self._shutdown_task)

    async def _shutdown_async(self, timeout):
        # Stop the wakeup timer first so no new job processing rounds are started
        self._stop_timer()

        # Wait for the executors to drain, honoring the total timeout across all of
        # them, then shut down the job stores
        loop = self._eventloop
        deadline = None if timeout is None else loop.time() + timeout
        with self._executors_lock:
            executors = list(self._executors.items())
        with self._jobstores_lock:
            jobstores = list(self._jobstores.values())

        still_running = {}
        for alias, executor in executors:
            remaining = timeout
            if deadline is not None:
                remaining = max(0.0, deadline - loop.time())

            shutdown_async = getattr(executor, "shutdown_async", None)
            if shutdown_async is not None:
                still_running[alias] = await shutdown_async(remaining)
            else:
                executor.shutdown(True)
                still_running[alias] = []

        for jobstore in jobstores:
            jobstore.shutdown()

        self.state = STATE_STOPPED
        self._logger.info("Scheduler has been shut down")
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))
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
