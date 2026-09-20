"""
Demonstrates how to shut down an AsyncIOScheduler gracefully from an ASGI
application's lifespan, waiting for running jobs to finish without blocking the
event loop.

Run with: uvicorn asyncio_asgi_lifespan:app
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def tick():
    print(f"Tick! The time is: {datetime.now()}")


async def slow_job():
    # A coroutine job with cleanup that must be allowed to finish on shutdown
    try:
        await asyncio.sleep(30)
    finally:
        logger.info("slow_job cleanup done")


@asynccontextmanager
async def lifespan(app):
    scheduler.add_job(tick, "interval", seconds=3)
    scheduler.add_job(slow_job, "date")
    scheduler.start()
    yield
    # Wait up to 10 seconds for running jobs to finish. Coroutine jobs that
    # outlive the timeout are cancelled and their cleanup (finally blocks) is
    # awaited; jobs still running in worker threads cannot be cancelled and are
    # reported in the result instead of being claimed as cancelled.
    still_running = await scheduler.shutdown_async(timeout=10)
    for executor_alias, futures in still_running.items():
        for future in futures:
            logger.warning(
                "Job %r in executor %r is still running in a worker thread",
                future,
                executor_alias,
            )


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return {"scheduler_running": scheduler.running}
