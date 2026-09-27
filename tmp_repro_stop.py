import anyio

from apscheduler import AsyncScheduler, JobReleased


async def dummy() -> None:
    return None


async def main():
    send, receive = anyio.create_memory_object_stream(2)
    with send, receive:
        async with AsyncScheduler() as scheduler:
            subscription = scheduler.subscribe(send.send)
            await scheduler.configure_task("dummy", func=dummy)
            job_id = await scheduler.add_job("dummy")
            await scheduler.start_in_background()
            with anyio.fail_after(3):
                async for event in receive:
                    print(type(event).__name__)
                    if isinstance(event, JobReleased):
                        subscription.unsubscribe()
                        break
            print("job done", job_id)
    print("exited")


anyio.run(main)
