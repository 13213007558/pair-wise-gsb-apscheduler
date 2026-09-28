import threading
import time

import anyio
import pytest

from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_SUBMITTED,
    EVENT_SCHEDULER_SHUTDOWN,
    JobEvent,
    JobExecutionEvent,
    JobSubmissionEvent,
)
from apscheduler.eventstream import (
    AsyncEventStream,
    EventStreamCancelled,
    OverflowPolicy,
)
from apscheduler.schedulers.background import BackgroundScheduler


def make_job_event(code, job_id="job1", jobstore="default"):
    return JobEvent(code, job_id, jobstore)


def drain_sync(stream):
    """Synchronously pull everything currently buffered (test helper)."""
    events = []
    while True:
        with stream._lock:
            if not stream._buffer:
                break
            events.append(stream._buffer.popleft())
    return events


class TestOverflowPolicies:
    def test_drop_newest(self):
        stream = AsyncEventStream(capacity=2, overflow=OverflowPolicy.DROP_NEWEST)
        events = [make_job_event(EVENT_JOB_ADDED, "job%d" % i) for i in range(5)]
        for event in events:
            stream.publish(event)

        buffered = drain_sync(stream)
        assert [e.job_id for e in buffered] == ["job0", "job1"]
        assert stream.stats.enqueued == 2
        assert stream.stats.dropped == 3
        assert stream.stats.overflows == 3
        assert stream.stats.overflow_log == [
            ("dropped_newest", ("job2", "default")),
            ("dropped_newest", ("job3", "default")),
            ("dropped_newest", ("job4", "default")),
        ]

    def test_drop_oldest(self):
        stream = AsyncEventStream(capacity=2, overflow=OverflowPolicy.DROP_OLDEST)
        for i in range(5):
            stream.publish(make_job_event(EVENT_JOB_ADDED, "job%d" % i))

        buffered = drain_sync(stream)
        assert [e.job_id for e in buffered] == ["job3", "job4"]
        assert stream.stats.enqueued == 5
        assert stream.stats.dropped == 3
        assert stream.stats.overflows == 3
        assert stream.stats.overflow_log[-1] == ("evicted_oldest", ("job2", "default"))

    def test_coalesce_same_job(self):
        stream = AsyncEventStream(capacity=2, overflow=OverflowPolicy.COALESCE)
        stream.publish(make_job_event(EVENT_JOB_SUBMITTED, "job1"))
        stream.publish(make_job_event(EVENT_JOB_SUBMITTED, "job1"))
        # buffer full: two events for job1; a third event for job1 coalesces
        stream.publish(make_job_event(EVENT_JOB_EXECUTED, "job1"))

        buffered = drain_sync(stream)
        assert len(buffered) == 2
        # the tail keeps its queue position but carries the newest event
        assert buffered[0].code == EVENT_JOB_SUBMITTED
        assert buffered[1].code == EVENT_JOB_EXECUTED
        assert stream.stats.coalesced == 1
        assert stream.stats.dropped == 0

    def test_coalesce_different_job_falls_back_to_evict_oldest(self):
        stream = AsyncEventStream(capacity=2, overflow=OverflowPolicy.COALESCE)
        stream.publish(make_job_event(EVENT_JOB_SUBMITTED, "job1"))
        stream.publish(make_job_event(EVENT_JOB_SUBMITTED, "job2"))
        stream.publish(make_job_event(EVENT_JOB_EXECUTED, "job3"))

        buffered = drain_sync(stream)
        assert [e.job_id for e in buffered] == ["job2", "job3"]
        assert stream.stats.coalesced == 0
        assert stream.stats.dropped == 1

    def test_capacity_validation(self):
        with pytest.raises(ValueError):
            AsyncEventStream(capacity=0)

    def test_mask_filtering(self):
        stream = AsyncEventStream(mask=EVENT_JOB_EXECUTED, capacity=10)
        stream.publish(make_job_event(EVENT_JOB_SUBMITTED))
        assert stream.stats.enqueued == 0
        stream.publish(make_job_event(EVENT_JOB_EXECUTED))
        assert stream.stats.enqueued == 1


class TestEventSnapshot:
    def test_published_event_is_snapshotted(self):
        stream = AsyncEventStream(capacity=10)
        event = JobSubmissionEvent(EVENT_JOB_SUBMITTED, "job1", "default", [1, 2])
        stream.publish(event)

        # the publisher mutates the event afterwards
        event.job_id = "tampered"
        event.scheduled_run_times.append(999)

        (snapshot,) = drain_sync(stream)
        assert snapshot.job_id == "job1"
        assert snapshot.scheduled_run_times == [1, 2]
        assert snapshot is not event

    def test_execution_event_snapshot(self):
        stream = AsyncEventStream(capacity=10)
        event = JobExecutionEvent(EVENT_JOB_EXECUTED, "job1", "default", None, retval=1)
        stream.publish(event)
        event.retval = "tampered"

        (snapshot,) = drain_sync(stream)
        assert snapshot.retval == 1


@pytest.mark.anyio
class TestAsyncReceive:
    async def test_receive_waits_and_delivers_in_order(self):
        stream = AsyncEventStream(capacity=10)
        received = []

        async def consumer():
            async for event in stream:
                received.append(event)

        async with anyio.create_task_group() as tg:
            tg.start_soon(consumer)
            await anyio.sleep(0.05)
            # publish from a foreign thread while the consumer is waiting
            thread = threading.Thread(
                target=lambda: [
                    stream.publish(make_job_event(EVENT_JOB_ADDED, "job%d" % i))
                    for i in range(3)
                ]
            )
            thread.start()
            thread.join()
            await anyio.sleep(0.1)
            stream.close()
            await anyio.sleep(0.1)

        assert [e.job_id for e in received] == ["job0", "job1", "job2"]
        assert stream.stats.delivered == 3

    async def test_close_drain_then_end_of_stream(self):
        stream = AsyncEventStream(capacity=10)
        stream.publish(make_job_event(EVENT_JOB_ADDED, "job0"))
        stream.publish(make_job_event(EVENT_JOB_ADDED, "job1"))
        stream.close()

        assert (await stream.receive()).job_id == "job0"
        assert (await stream.receive()).job_id == "job1"
        with pytest.raises(anyio.EndOfStream):
            await stream.receive()
        assert stream.stats.closed
        assert stream.stats.drained_on_close == 2

    async def test_close_without_drain(self):
        stream = AsyncEventStream(capacity=10)
        stream.publish(make_job_event(EVENT_JOB_ADDED, "job0"))
        stream.close(drain=False)
        with pytest.raises(anyio.EndOfStream):
            await stream.receive()
        assert stream.stats.discarded_on_close == 1

    async def test_pending_receiver_woken_on_close(self):
        stream = AsyncEventStream(capacity=10)
        outcome = []

        async def consumer():
            try:
                await stream.receive()
            except anyio.EndOfStream:
                outcome.append("end")

        async with anyio.create_task_group() as tg:
            tg.start_soon(consumer)
            await anyio.sleep(0.05)
            stream.close()
            await anyio.sleep(0.1)

        assert outcome == ["end"]

    async def test_cancel_raises_and_discards(self):
        stream = AsyncEventStream(capacity=10)
        stream.publish(make_job_event(EVENT_JOB_ADDED, "job0"))
        stream.cancel()
        with pytest.raises(EventStreamCancelled):
            await stream.receive()
        assert stream.stats.cancelled
        assert stream.stats.discarded_on_close == 1

    async def test_pending_receiver_woken_on_cancel(self):
        stream = AsyncEventStream(capacity=10)
        outcome = []

        async def consumer():
            try:
                await stream.receive()
            except EventStreamCancelled:
                outcome.append("cancelled")

        async with anyio.create_task_group() as tg:
            tg.start_soon(consumer)
            await anyio.sleep(0.05)
            stream.cancel()
            await anyio.sleep(0.1)

        assert outcome == ["cancelled"]

    async def test_publish_after_close_is_counted(self):
        stream = AsyncEventStream(capacity=10)
        stream.close()
        stream.publish(make_job_event(EVENT_JOB_ADDED, "job0"))
        assert stream.stats.dropped_after_close == 1
        with pytest.raises(anyio.EndOfStream):
            await stream.receive()


class TestSchedulerIntegration:
    def test_publish_never_blocks_on_full_buffer(self):
        """A slow subscriber with a full buffer must not stall dispatch."""
        scheduler = BackgroundScheduler()
        stream = scheduler.add_event_stream(capacity=4)
        for i in range(4):
            stream.publish(make_job_event(EVENT_JOB_ADDED, "job%d" % i))

        start = time.monotonic()
        for _ in range(2000):
            scheduler._dispatch_event(make_job_event(EVENT_JOB_ADDED, "jobX"))
        elapsed = time.monotonic() - start

        assert elapsed < 2.0
        assert stream.stats.dropped == 2000
        assert stream.stats.overflows == 2000

    def test_failing_stream_only_removes_itself(self):
        scheduler = BackgroundScheduler()
        good_events = []
        scheduler.add_listener(lambda event: good_events.append(event), EVENT_JOB_ADDED)
        broken = scheduler.add_event_stream()
        good = scheduler.add_event_stream()

        def explode(event):
            raise RuntimeError("boom")

        broken._publish = explode
        scheduler._dispatch_event(make_job_event(EVENT_JOB_ADDED, "job0"))

        assert broken.stats.errored
        assert broken.stats.publish_errors == 1
        assert broken not in scheduler._event_streams
        # the sync listener and the other stream were unaffected
        assert len(good_events) == 1
        assert good.stats.enqueued == 1
        # and the scheduler keeps dispatching to the survivors
        scheduler._dispatch_event(make_job_event(EVENT_JOB_ADDED, "job1"))
        assert len(good_events) == 2
        assert good.stats.enqueued == 2

    def test_sync_listener_exception_does_not_affect_stream(self):
        scheduler = BackgroundScheduler()

        def bad_listener(event):
            raise RuntimeError("listener boom")

        scheduler.add_listener(bad_listener)
        stream = scheduler.add_event_stream()
        scheduler._dispatch_event(make_job_event(EVENT_JOB_ADDED, "job0"))
        assert stream.stats.enqueued == 1

    def test_remove_event_stream(self):
        scheduler = BackgroundScheduler()
        stream = scheduler.add_event_stream()
        scheduler.remove_event_stream(stream)
        assert stream.closed
        assert stream.stats.closed
        scheduler._dispatch_event(make_job_event(EVENT_JOB_ADDED, "job0"))
        assert stream.stats.enqueued == 0

    def test_shutdown_defines_publication_boundary(self):
        scheduler = BackgroundScheduler()
        stream = scheduler.add_event_stream()
        scheduler.start()
        scheduler.shutdown()

        assert stream.closed
        buffered = drain_sync(stream)
        codes = [e.code for e in buffered]
        # the shutdown event was the last one published to the stream
        assert codes[-1] == EVENT_SCHEDULER_SHUTDOWN
        # the stream was unregistered, so no further events are published
        assert scheduler._event_streams == []
        # anything still offered to the closed stream is observably refused
        stream.publish(make_job_event(EVENT_JOB_ADDED, "late"))
        assert stream.stats.dropped_after_close == 1

    def test_pause_resume_keep_stream_open(self):
        scheduler = BackgroundScheduler()
        stream = scheduler.add_event_stream()
        scheduler.start(paused=True)
        scheduler.resume()
        scheduler.pause()
        scheduler.shutdown()

        assert not stream.stats.cancelled
        buffered = drain_sync(stream)
        codes = [e.code for e in buffered]
        assert 2**3 in codes  # EVENT_SCHEDULER_RESUMED
        assert 2**2 in codes  # EVENT_SCHEDULER_PAUSED

    @pytest.mark.anyio
    async def test_slow_subscriber_does_not_affect_fast_one(self):
        """
        End-to-end: a real scheduler with a blocked slow subscriber and a
        concurrently consuming fast subscriber. The fast subscriber must
        receive every event, in per-job order, while the slow one overflows
        with observable statistics.
        """
        scheduler = BackgroundScheduler()
        slow = scheduler.add_event_stream(capacity=2)
        fast = scheduler.add_event_stream(capacity=1000)
        run_counter = {"count": 0}
        published = []
        scheduler.add_listener(
            lambda event: published.append((event.code, getattr(event, "job_id", None)))
        )

        def job():
            run_counter["count"] += 1

        scheduler.add_job(job, "interval", seconds=0.05, id="fastjob", max_instances=3)
        scheduler.start()

        received = []
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and run_counter["count"] < 3:
                with anyio.fail_after(1.0):
                    event = await fast.receive()
                received.append(event)
        finally:
            scheduler.shutdown()

        # drain whatever the shutdown boundary left queued
        while True:
            try:
                with anyio.fail_after(1.0):
                    received.append(await fast.receive())
            except anyio.EndOfStream:
                break

        # the fast subscriber got everything, in order
        assert fast.stats.dropped == 0
        assert fast.stats.delivered == fast.stats.enqueued
        streamed = [(e.code, getattr(e, "job_id", None)) for e in received]
        # the stream delivered the exact publication sequence seen by listeners
        assert streamed == published
        job_events = [e for e in received if getattr(e, "job_id", None) == "fastjob"]
        assert len(job_events) >= 3
        # per-job ordering: submitted always precedes the matching executed
        submitted = [
            i for i, e in enumerate(job_events) if e.code == EVENT_JOB_SUBMITTED
        ]
        executed = [i for i, e in enumerate(job_events) if e.code == EVENT_JOB_EXECUTED]
        assert submitted and executed
        assert submitted[0] < executed[-1]

        # the slow subscriber overflowed with clear statistics
        assert slow.stats.overflows > 0
        assert slow.stats.dropped > 0
        assert slow.stats.overflow_log
        # and the scheduler kept ticking despite the blocked subscriber
        assert run_counter["count"] >= 3
