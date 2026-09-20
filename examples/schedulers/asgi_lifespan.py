"""
Demonstrates how to shut down AsyncIOScheduler gracefully from an ASGI
application lifespan, waiting for running jobs to finish within a deadline.

The lifespan function below works with any ASGI framework that supports
lifespan handlers (Starlette, FastAPI, Quart, Litestar, plain ASGI apps, ...).
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)


def tick():
    print(f"Tick! The time is: {datetime.now()}")


async def slow_cleanup_job():
    # Simulates a job that needs a moment to finish; its finally block is
    # guaranteed to run even if the shutdown deadline is exceeded
    try:
        await asyncio.sleep(30)
    finally:
        print("slow_cleanup_job: cleaning up resources")


@asynccontextmanager
async def lifespan(app):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(tick, "interval", seconds=3)
    scheduler.add_job(slow_cleanup_job, "date")
    scheduler.start()
    try:
        yield {"scheduler": scheduler}
    finally:
        # Wait up to 10 seconds for running jobs to finish. If the deadline
        # expires, coroutine jobs are cancelled (their finally blocks still
        # run to completion) while jobs stuck in worker threads are reported
        # in the return value and keep running in the background.
        still_running = await scheduler.async_shutdown(timeout=10)
        if still_running:
            logging.warning(
                "Shutdown finished with %d job(s) still running in worker " "threads",
                len(still_running),
            )


# Wiring it up with common frameworks:
#
#   FastAPI:      app = FastAPI(lifespan=lifespan)
#   Starlette:    app = Starlette(lifespan=lifespan)
#   Litestar:     app = Litestar(lifespan=[lifespan])
#
# Plain ASGI demo (run with: python examples/schedulers/asgi_lifespan.py):


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                lifespan_context = lifespan(None)
                await lifespan_context.__aenter__()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await lifespan_context.__aexit__(None, None, None)
                await send({"type": "lifespan.shutdown.complete"})
                return
    else:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"OK"})


async def main():
    # Simulates an ASGI server driving the lifespan protocol
    sent = []

    async def receive():
        await asyncio.sleep(5 if not sent else 0.2)
        return {"type": "lifespan.startup" if not sent else "lifespan.shutdown"}

    async def send(message):
        sent.append(message)
        print("ASGI:", message["type"])

    await app({"type": "lifespan"}, receive, send)


if __name__ == "__main__":
    asyncio.run(main())
