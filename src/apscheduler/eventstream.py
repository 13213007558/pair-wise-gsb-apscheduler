"""
Bounded, non-blocking asynchronous event streams for scheduler events.

This module implements an *optional* event delivery channel that complements the
classic synchronous listeners (see :meth:`BaseScheduler.add_listener`). Each
subscriber gets its own bounded buffer so that a slow or blocked subscriber can
never slow down the scheduler's main loop or accumulate unbounded memory.

Guarantees and semantics:

* **Never blocks the scheduler.** :meth:`AsyncEventStream.publish` is a
  synchronous, O(1), non-blocking operation. It is safe to call from any
  thread. Slow subscribers are never on the scheduling tick's wait chain.
* **Per-job ordering.** Events concerning the same job are delivered to each
  subscriber in the exact order they were published. Different subscribers are
  fully isolated from each other.
* **Bounded memory.** Each stream holds at most `capacity` events. On
  overflow the configured :class:`OverflowPolicy` decides whether the incoming
  event is dropped, the oldest queued event is evicted, or consecutive events
  of the same job are coalesced. Every outcome is recorded in
  :attr:`AsyncEventStream.stats`.
* **Immutability.** Events are snapshotted before being queued, so a publisher
  mutating an event object afterwards can never alter already queued events.
* **Terminal states.** Closing or cancelling a stream is permanent. Closing
  with `drain=True` (the default) lets the subscriber consume the remaining
  buffered events before :exc:`anyio.EndOfStream` is raised; cancelling
  discards the buffer and raises :exc:`EventStreamCancelled` in the receiver.
* **Fault isolation.** A subscriber that fails or is cancelled only removes
  itself; other subscribers and synchronous listeners are unaffected.

.. note::
   Receiving is only supported on the asyncio backend of AnyIO (the only
   backend the bundled schedulers use natively). Publishing works from any
   thread.
"""

__all__ = (
    "OverflowPolicy",
    "EventStreamStats",
    "EventStreamCancelled",
    "AsyncEventStream",
)

import copy
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from logging import getLogger

import anyio
import sniffio

from apscheduler.events import EVENT_ALL, JobEvent, JobSubmissionEvent

logger = getLogger(__name__)


class OverflowPolicy(Enum):
    """Defines what happens when a subscriber's buffer is full."""

    #: The incoming event is discarded; queued events are kept.
    DROP_NEWEST = "drop_newest"

    #: The oldest queued event is evicted to make room for the incoming one.
    DROP_OLDEST = "drop_oldest"

    #: If the newest queued event concerns the same job as the incoming event,
    #: it is replaced by the incoming event (preserving its queue position).
    #: Otherwise the policy falls back to evicting the oldest queued event.
    COALESCE = "coalesce"


@dataclass
class EventStreamStats:
    """
    Observable statistics and terminal-state flags of an
    :class:`AsyncEventStream`.

    All counters only ever increase. `closed`/`cancelled`/`errored` are
    the terminal-state flags; exactly one of them is set when the stream
    reaches its terminal state (`errored` may accompany `closed`).
    """

    #: events accepted into the buffer
    enqueued: int = 0
    #: events handed out via :meth:`AsyncEventStream.receive`
    delivered: int = 0
    #: events discarded because the buffer was full (or replaced by coalescing)
    dropped: int = 0
    #: events merged into a queued event by :attr:`OverflowPolicy.COALESCE`
    coalesced: int = 0
    #: events that arrived after the stream was closed/cancelled
    dropped_after_close: int = 0
    #: buffered events discarded by `close(drain=False)` or `cancel()`
    discarded_on_close: int = 0
    #: events delivered from the buffer after the stream was closed (drain)
    drained_on_close: int = 0
    #: number of times the buffer was found full on publish
    overflows: int = 0
    #: unexpected errors while publishing to this stream
    publish_errors: int = 0
    #: unexpected errors while receiving from this stream
    receive_errors: int = 0
    #: terminal flag: closed via :meth:`AsyncEventStream.close`
    closed: bool = False
    #: terminal flag: cancelled via :meth:`AsyncEventStream.cancel`
    cancelled: bool = False
    #: terminal flag: removed after an unexpected publish error
    errored: bool = False
    #: bounded log of the most recent overflow outcomes
    overflow_log: list = field(default_factory=list)


class EventStreamCancelled(Exception):
    """Raised by :meth:`AsyncEventStream.receive` when the stream was cancelled."""


def _snapshot_event(event):
    """
    Return a copy of `event` that is immune to later in-place mutation.

    A shallow copy protects against attribute reassignment on the event
    object itself; mutable event payloads owned by the event (currently only
    `JobSubmissionEvent.scheduled_run_times`) are copied as well.
    """
    snapshot = copy.copy(event)
    if isinstance(event, JobSubmissionEvent):
        snapshot.scheduled_run_times = list(event.scheduled_run_times)
    return snapshot


def _event_job_key(event):
    """Return a hashable key identifying the job an event concerns, or None."""
    if isinstance(event, JobEvent):
        return (event.job_id, event.jobstore)
    return None


class AsyncEventStream:
    """
    A bounded, single-consumer asynchronous stream of scheduler events.

    Instances are created via :meth:`BaseScheduler.add_event_stream
    <apscheduler.schedulers.base.BaseScheduler.add_event_stream>`.

    :param mask: bitmask of event types to receive (see :mod:`apscheduler.events`)
    :param capacity: maximum number of buffered events
    :param overflow: the :class:`OverflowPolicy` applied when the buffer is full

    The stream must be consumed from a single task at a time. Iterate with
    `async for event in stream:` or call :meth:`receive` directly.
    """

    def __init__(
        self, mask=EVENT_ALL, capacity=100, overflow=OverflowPolicy.DROP_NEWEST
    ):
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if not isinstance(overflow, OverflowPolicy):
            overflow = OverflowPolicy(overflow)
        self.mask = mask
        self.capacity = capacity
        self.overflow = overflow
        self.stats = EventStreamStats()
        self._buffer = deque()
        self._lock = threading.Lock()
        self._closed = False
        self._cancelled = False
        self._waiter = None  # (asyncio loop, anyio.Event) of the pending receiver

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} (capacity={self.capacity}, "
            f"overflow={self.overflow.value}, buffered={len(self._buffer)}, "
            f"closed={self._closed}, cancelled={self._cancelled})>"
        )

    #
    # Publishing side (called by the scheduler; must never block or raise)
    #

    def matches(self, code):
        """Return True if events with the given type code pass the mask."""
        return bool(code & self.mask)

    @property
    def closed(self):
        """True if the stream has been closed or cancelled (terminal)."""
        return self._closed or self._cancelled

    def publish(self, event):
        """
        Enqueue a snapshot of `event` without blocking, applying the overflow
        policy if the buffer is full.

        This method is safe to call from any thread and never raises. Any
        unexpected error is recorded in :attr:`stats` and logged, and the
        stream transitions to the errored terminal state so the scheduler can
        drop just this subscriber.
        """
        if not self.matches(event.code):
            return
        try:
            self._publish(event)
        except BaseException:
            with self._lock:
                self.stats.publish_errors += 1
                self.stats.errored = True
                self._closed = True
            logger.exception("Error publishing event to %r; stream removed", self)
            raise

    def _publish(self, event):
        snapshot = _snapshot_event(event)
        with self._lock:
            if self._closed or self._cancelled:
                self.stats.dropped_after_close += 1
                return

            waiter = self._waiter
            if len(self._buffer) >= self.capacity:
                self.stats.overflows += 1
                self._apply_overflow(snapshot)
            else:
                self._buffer.append(snapshot)
                self.stats.enqueued += 1

        self._wake(waiter)

    def _apply_overflow(self, snapshot):
        """Apply the overflow policy. Called with the lock held, buffer full."""
        if self.overflow is OverflowPolicy.COALESCE:
            key = _event_job_key(snapshot)
            if (
                key is not None
                and self._buffer
                and _event_job_key(self._buffer[-1]) == key
            ):
                self._buffer[-1] = snapshot
                self.stats.coalesced += 1
                outcome = "coalesced"
            else:
                evicted = self._buffer.popleft()
                self._buffer.append(snapshot)
                self.stats.dropped += 1
                self.stats.enqueued += 1
                outcome = ("evicted_oldest", _event_job_key(evicted))
        elif self.overflow is OverflowPolicy.DROP_OLDEST:
            evicted = self._buffer.popleft()
            self._buffer.append(snapshot)
            self.stats.dropped += 1
            self.stats.enqueued += 1
            outcome = ("evicted_oldest", _event_job_key(evicted))
        else:  # DROP_NEWEST
            self.stats.dropped += 1
            outcome = ("dropped_newest", _event_job_key(snapshot))

        # Keep a bounded, observable trail of the most recent overflow outcomes.
        if len(self.stats.overflow_log) >= 100:
            del self.stats.overflow_log[:50]
        self.stats.overflow_log.append(outcome)

    def _wake(self, waiter):
        if waiter is None:
            return
        loop, event = waiter
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            # The receiver's event loop is gone; nothing to wake.
            with self._lock:
                self.stats.publish_errors += 1
            logger.warning("Could not wake receiver of %r: event loop closed", self)

    #
    # Receiving side (async, single consumer)
    #

    async def receive(self):
        """
        Return the next buffered event, waiting if necessary.

        :raises EventStreamCancelled: if the stream was cancelled
        :raises anyio.EndOfStream: if the stream was closed and the buffer has
            been drained
        """
        try:
            return await self._receive()
        except (EventStreamCancelled, anyio.EndOfStream):
            raise
        except BaseException:
            with self._lock:
                self.stats.receive_errors += 1
            logger.exception("Error receiving event from %r", self)
            raise

    async def _receive(self):
        while True:
            with self._lock:
                if self._buffer:
                    event = self._buffer.popleft()
                    self.stats.delivered += 1
                    if self._closed:
                        self.stats.drained_on_close += 1
                    return event
                if self._cancelled:
                    raise EventStreamCancelled(
                        "the event stream was cancelled; buffered events were discarded"
                    )
                if self._closed:
                    raise anyio.EndOfStream
                if self._waiter is None:
                    if sniffio.current_async_library() != "asyncio":
                        raise RuntimeError(
                            "AsyncEventStream receiving requires the asyncio backend"
                        )
                    import asyncio

                    self._waiter = (asyncio.get_running_loop(), anyio.Event())
                waiter_event = self._waiter[1]

            await waiter_event.wait()
            with self._lock:
                self._waiter = None

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        try:
            while True:
                yield await self.receive()
        except (EventStreamCancelled, anyio.EndOfStream):
            return

    #
    # Terminal state management
    #

    def close(self, drain=True):
        """
        Close the stream (terminal). No further events will be accepted.

        :param drain: if True (default), buffered events remain available
            via :meth:`receive` until the buffer is empty, after which
            :exc:`anyio.EndOfStream` is raised; if False, buffered events
            are discarded immediately

        This method is synchronous and safe to call from any thread.
        """
        with self._lock:
            if self._closed or self._cancelled:
                return
            self._closed = True
            self.stats.closed = True
            if not drain:
                self.stats.discarded_on_close += len(self._buffer)
                self._buffer.clear()
            waiter = self._waiter
        self._wake(waiter)

    async def aclose(self, drain=True):
        """Async counterpart of :meth:`close`, for symmetry with AnyIO streams."""
        self.close(drain=drain)

    def cancel(self):
        """
        Cancel the stream (terminal). Buffered events are discarded and the
        pending/next :meth:`receive` call raises :exc:`EventStreamCancelled`.

        Only this subscriber is affected; the scheduler and all other
        subscribers keep working.
        """
        with self._lock:
            if self._closed or self._cancelled:
                return
            self._cancelled = True
            self.stats.cancelled = True
            self.stats.discarded_on_close += len(self._buffer)
            self._buffer.clear()
            waiter = self._waiter
        self._wake(waiter)
