import concurrent.futures
from asyncio import CancelledError, wrap_future

from apscheduler.executors.base import (
    BaseExecutor,
    run_coroutine_job,
    run_job,
)
from apscheduler.util import iscoroutinefunction_partial


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

    Native coroutine functions run directly in the event loop. All other functions run in the
    event loop's default executor, which is usually a thread pool.

    Plugin alias: ``asyncio``
    """

    def start(self, scheduler, alias):
        super().start(scheduler, alias)
        self._eventloop = scheduler._eventloop
        self._pending_futures = set()

    def shutdown(self, wait=True):
        self._shutdown = True
        with self._lock:
            futures = tuple(self._pending_futures)

        for future in futures:
            if not future.done():
                future.cancel()

    def cancel_execution(self, execution):
        with self._lock:
            future = execution.future
            execution.cancelled = True
            execution.cancelled_by_scheduler = True

        if future is None:
            return True
        if future.done():
            return future.cancelled()

        return future.cancel()

    def _get_default_executor(self):
        executor = self._eventloop._default_executor
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(
                thread_name_prefix="asyncio"
            )
            self._eventloop._default_executor = executor

        return executor

    def _do_submit_job(self, job, run_times, execution):
        if execution.cancelled:
            self._eventloop.call_soon(
                self._finish_execution,
                execution,
                self._cancelled_events(execution),
            )
            return None

        if iscoroutinefunction_partial(job.func):
            coro = run_coroutine_job(
                job,
                job._jobstore_alias,
                run_times,
                self._logger.name,
            )
            future = self._eventloop.create_task(coro)
            future.add_done_callback(lambda f: self._coroutine_done(f, execution))
        else:
            underlying_future = self._get_default_executor().submit(
                run_job,
                job,
                job._jobstore_alias,
                run_times,
                self._logger.name,
            )
            execution.underlying_future = underlying_future
            future = wrap_future(underlying_future, loop=self._eventloop)
            future.add_done_callback(lambda f: self._thread_job_done(f, execution))

        self._pending_futures.add(future)
        return future

    def _coroutine_done(self, future, execution):
        self._pending_futures.discard(future)

        if future.cancelled():
            self._finish_execution(execution, self._cancelled_events(execution))
            return

        exception = future.exception()
        if exception is not None:
            if execution.cancelled_by_scheduler and isinstance(
                exception, CancelledError
            ):
                events = self._cancelled_events(execution)
                self._finish_execution(execution, events)
            else:
                self._finish_execution(
                    execution,
                    (),
                    exception=exception,
                    traceback=exception.__traceback__,
                )
        else:
            self._finish_execution(execution, future.result())

    def _thread_job_done(self, future, execution):
        self._pending_futures.discard(future)

        if future.cancelled():
            underlying_future = execution.underlying_future
            if underlying_future is not None and underlying_future.running():
                self._wait_for_uncancellable_thread(execution, underlying_future)
                return

            self._finish_execution(execution, self._cancelled_events(execution))
            return

        exception = future.exception()
        if exception is not None:
            self._finish_execution(
                execution,
                (),
                exception=exception,
                traceback=exception.__traceback__,
            )
        else:
            self._finish_execution(execution, future.result())

    def _wait_for_uncancellable_thread(self, execution, future):
        def release():
            if future.cancelled():
                events = self._cancelled_events(execution) if future.cancelled() else ()
                exception = None
                traceback = None
            else:
                events = future.result()
                exception = future.exception()
                traceback = exception.__traceback__ if exception is not None else None

            self._finish_execution(
                execution,
                events,
                exception=exception,
                traceback=traceback,
            )

        future.add_done_callback(lambda _: release())
