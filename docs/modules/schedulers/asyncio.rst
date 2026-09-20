:mod:`apscheduler.schedulers.asyncio`
=====================================

.. automodule:: apscheduler.schedulers.asyncio

API
---

.. autoclass:: AsyncIOScheduler
    :show-inheritance:


Introduction
------------

AsyncIOScheduler was meant to be used with the `AsyncIO <https://docs.python.org/3/library/asyncio.html>`_ event loop.
By default, it will run jobs in the event loop's thread pool.

If you have an application that runs on an AsyncIO event loop, you will want to use this scheduler.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.asyncio.AsyncIOExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/asyncio_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/schedulers/asyncio_.py>`_).


Graceful shutdown
-----------------

The synchronous :meth:`~apscheduler.schedulers.base.BaseScheduler.shutdown`
method cannot wait for the default executor's pending jobs, because
:class:`~apscheduler.executors.asyncio.AsyncIOExecutor` tracks them as event
loop futures that can only be awaited asynchronously. For applications that
need to wait for running jobs to wind down -- for example an ASGI lifespan
handler -- :class:`AsyncIOScheduler` provides an awaitable counterpart::

    still_running = await scheduler.async_shutdown(timeout=10)

The shutdown proceeds as follows:

#. Job processing is stopped immediately, so no new jobs are submitted.
#. Already submitted jobs are awaited *without blocking the event loop*, so
   the rest of the application (heartbeats, request handlers, ...) keeps
   running. Jobs that finish within ``timeout`` dispatch their success/error
   events as usual.
#. If ``timeout`` expires first, coroutine jobs are cancelled and awaited, so
   their ``finally`` blocks run to completion before resources are closed.
   Jobs running in worker threads **cannot be cancelled**; they are left
   running, reported in the return value, and logged as warnings. The method
   never claims that such jobs were cancelled.
#. Executors and job stores are shut down and the scheduler moves to the
   stopped state.

The return value is a list of futures whose jobs are still running in worker
threads; an empty list means a fully clean shutdown. Concurrent calls share
the same shutdown process, and once it has completed, further calls return
the same result immediately. Calling
:meth:`~apscheduler.schedulers.base.BaseScheduler.start` while a shutdown is
in progress raises
:class:`~apscheduler.schedulers.SchedulerAlreadyRunningError`.

ASGI lifespan example
---------------------

See ``examples/schedulers/asgi_lifespan.py`` for a complete, framework
agnostic example. The essential part::

    from contextlib import asynccontextmanager

    from apscheduler.schedulers.asyncio import AsyncIOScheduler


    @asynccontextmanager
    async def lifespan(app):
        scheduler = AsyncIOScheduler()
        scheduler.add_job(my_job, "interval", minutes=5)
        scheduler.start()
        try:
            yield {"scheduler": scheduler}
        finally:
            # Wait up to 10 seconds for running jobs to finish; coroutine
            # jobs are cancelled after the deadline while thread pool jobs
            # that outlive it are reported in the return value.
            still_running = await scheduler.async_shutdown(timeout=10)
            if still_running:
                print(f"{len(still_running)} thread job(s) still running")

This lifespan handler can be passed directly to FastAPI, Starlette, Litestar
or any other framework supporting the ASGI lifespan protocol.
