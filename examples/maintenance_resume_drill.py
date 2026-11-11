"""
Maintenance pause/resume drill for short-lived credential issuance (APScheduler 4.x).

Proves, against the in-repo scheduler with a MemoryDataStore and a recording fake
issuer:

1. BAD config (coalesce=all, misfire_grace_time=None, resume_from=None) replays the
   maintenance backlog in one burst ("catch-up storm").
2. GOOD config (coalesce=latest, misfire_grace_time set, resume_from="now") skips the
   backlog; pausing stops *job generation* but never withdraws jobs already created;
   those are still executed, gated only by their start_deadline.
3. The scheduler's start deadline is NOT the business authorization deadline: jobs
   that expire mid-run are only caught by checks inside the job function (shown with
   a controlled clock).
4. max_jitter shifts the fire time and the start_deadline is computed from the
   *jittered* fire time, so jitter eats into the grace window.

Run:  .venv/bin/python examples/maintenance_resume_drill.py
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import anyio

from apscheduler import (
    AsyncScheduler,
    CoalescePolicy,
    JobAdded,
    JobOutcome,
    JobReleased,
)
from apscheduler.triggers.interval import IntervalTrigger

FAR_FUTURE = datetime(9999, 1, 1, tzinfo=UTC)  # business window effectively open
INTERVAL = 0.4  # seconds between scheduled fires


class ControlledClock:
    """Injectable business clock; the drill can advance it at will."""

    def __init__(self) -> None:
        self._offset = timedelta(0)

    def now(self) -> datetime:
        return datetime.now(UTC) + self._offset

    def advance(self, seconds: float) -> None:
        self._offset += timedelta(seconds=seconds)


class FakeIssuer:
    """Fake short-lived-credential issuer that records every decision."""

    def __init__(self, clock: ControlledClock, *, enforce_business_window: bool):
        self.clock = clock
        self.enforce_business_window = enforce_business_window
        self.records: list[tuple[str, str, datetime]] = []
        self.work_hook = None  # optional async callable run mid-"signing"

    async def __call__(self, label: str, valid_until: datetime) -> None:
        # App-level check 1: refuse to start if the authorization window has closed
        if self.enforce_business_window and self.clock.now() >= valid_until:
            self.records.append(("rejected_expired_at_start", label, self.clock.now()))
            return

        if self.work_hook is not None:
            await self.work_hook()  # simulated signing work (HSM round-trip etc.)

        # App-level check 2: re-validate right before emitting the credential
        if self.enforce_business_window and self.clock.now() >= valid_until:
            self.records.append(("aborted_mid_run", label, self.clock.now()))
            return

        self.records.append(("issued", label, self.clock.now()))

    def count(self, decision: str) -> int:
        return sum(1 for r in self.records if r[0] == decision)


class DrillContext:
    issuer: FakeIssuer
    gate: anyio.Event | None = None
    entered_first_call: anyio.Event | None = None


_current = DrillContext()


async def issue_task(label: str, valid_until: datetime) -> None:
    """Scheduled task body; delegates to the scenario's fake issuer."""
    if _current.gate is not None and not _current.gate.is_set():
        _current.entered_first_call.set()
        await _current.gate.wait()  # simulate a busy executor / slow HSM
    await _current.issuer(label, valid_until)


class EventLog:
    def __init__(self) -> None:
        self.events: list[tuple[datetime, object]] = []

    async def __call__(self, event) -> None:
        self.events.append((datetime.now(UTC), event))

    def of(self, *types):
        return [(t, e) for t, e in self.events if isinstance(e, types)]


async def scenario_bad_config() -> None:
    """coalesce=all + no grace time + resume_from=None -> backlog storm."""
    print("\n=== Scenario 1: BAD config (catch-up storm) ===")
    clock = ControlledClock()
    # Naive issuer: no business-window enforcement, like a bare signing function
    _current.issuer = FakeIssuer(clock, enforce_business_window=False)
    _current.gate = None
    log = EventLog()

    async with AsyncScheduler() as scheduler:
        scheduler.subscribe(log, {JobAdded, JobReleased})
        await scheduler.add_schedule(
            issue_task,
            IntervalTrigger(seconds=INTERVAL),
            id="sign",
            coalesce=CoalescePolicy.all,  # one job per missed fire time
            # misfire_grace_time left at default None -> jobs never expire
            args=["bad-config", FAR_FUTURE],
        )
        await scheduler.start_in_background()
        await anyio.sleep(0.9)  # ~2 normal fires
        issued_before_pause = _current.issuer.count("issued")

        await scheduler.pause_schedule("sign")
        added_at_pause = len(log.of(JobAdded))
        await anyio.sleep(1.3)  # maintenance window: ~3 fire times pass
        added_during_pause = len(log.of(JobAdded)) - added_at_pause

        await scheduler.unpause_schedule("sign")  # resume_from=None (default!)
        await anyio.sleep(0.6)  # let the backlog storm execute
        storm_issued = _current.issuer.count("issued") - issued_before_pause
        storm_releases = [
            e
            for t, e in log.of(JobReleased)
            if t > datetime.now(UTC) - timedelta(seconds=1.0)
            and e.outcome is JobOutcome.success
        ]

    print(f"  issued before pause:            {issued_before_pause}")
    print(f"  jobs generated during pause:    {added_during_pause} (pause stops generation)")
    print(f"  issued right after resume:      {storm_issued} (backlog storm)")
    for e in storm_releases[:3]:
        lag = (e.started_at - e.scheduled_start).total_seconds()
        print(f"    stale job: scheduled {lag:.2f}s before it ran -> still issued")
    assert added_during_pause == 0, "paused schedule must not generate jobs"
    assert storm_issued >= 3, "expected the missed fire times to replay as a burst"
    print("  -> BAD config replays the maintenance backlog. As expected.")


async def scenario_good_config() -> None:
    """coalesce=latest + grace time + resume_from='now' -> no backlog replay."""
    print("\n=== Scenario 2: GOOD config (maintenance-safe resume) ===")
    clock = ControlledClock()
    _current.issuer = FakeIssuer(clock, enforce_business_window=True)
    _current.gate = anyio.Event()
    _current.entered_first_call = anyio.Event()
    log = EventLog()

    async with AsyncScheduler(max_concurrent_jobs=1) as scheduler:
        scheduler.subscribe(log, {JobAdded, JobReleased})
        await scheduler.add_schedule(
            issue_task,
            IntervalTrigger(seconds=INTERVAL),
            id="sign",
            coalesce=CoalescePolicy.latest,
            misfire_grace_time=timedelta(seconds=0.3),  # -> job.start_deadline
            args=["good-config", FAR_FUTURE],
        )
        await scheduler.start_in_background()
        with anyio.fail_after(5):
            await _current.entered_first_call.wait()  # job 1 is running (blocked)

        await anyio.sleep(1.0)  # jobs 2..3 are created and queue behind job 1
        await scheduler.pause_schedule("sign")
        added_at_pause = len(log.of(JobAdded))
        await anyio.sleep(0.9)  # maintenance; paused -> no new jobs generated
        added_during_pause = len(log.of(JobAdded)) - added_at_pause

        # Existing jobs are NOT withdrawn by the pause: job 1 finishes and the
        # queued jobs are still processed (their start deadlines have passed).
        _current.gate.set()
        await anyio.sleep(0.6)
        released_while_paused = [
            e
            for t, e in log.of(JobReleased)
            if e.outcome in {JobOutcome.success, JobOutcome.missed_start_deadline}
        ]
        missed = [e for e in released_while_paused if e.outcome is JobOutcome.missed_start_deadline]

        await scheduler.unpause_schedule("sign", resume_from="now")
        added_at_resume = len(log.of(JobAdded))
        await anyio.sleep(0.45)  # only future fire times may generate jobs now
        burst_after_resume = len(log.of(JobAdded)) - added_at_resume
        issued_total = _current.issuer.count("issued")

    print(f"  jobs generated during pause:    {added_during_pause}")
    print(f"  pre-pause jobs processed anyway: {len(released_while_paused)} "
          f"(pause does not withdraw existing jobs)")
    print(f"  of which missed start deadline:  {len(missed)} (issuer NOT called)")
    print(f"  jobs created in burst at resume: {burst_after_resume} (backlog skipped)")
    print(f"  total credentials issued:        {issued_total} (job 1 + first post-resume fire)")
    assert added_during_pause == 0
    assert len(missed) >= 2, "queued jobs must miss their start deadline, not run"
    assert all(e.started_at is None for e in missed)
    assert burst_after_resume <= 1, "resume_from='now' must skip the backlog"
    assert issued_total == 2, "only job 1 and the first post-maintenance fire may issue"
    print("  -> GOOD config: no catch-up storm, stale jobs expire instead of running.")


async def scenario_business_window() -> None:
    """start_deadline != business authorization deadline; checks live in the job."""
    print("\n=== Scenario 3: business authorization window (controlled clock) ===")
    clock = ControlledClock()
    issuer = FakeIssuer(clock, enforce_business_window=True)

    # (a) window already closed when the job starts
    await issuer("expired-at-start", valid_until=clock.now() - timedelta(seconds=1))

    # (b) window closes *while* the job is running: the scheduler's start deadline
    # was satisfied at start, so only the in-job re-check can stop the issuance
    async def slow_signing() -> None:
        clock.advance(3600)  # controlled clock: window closes mid-run

    issuer.work_hook = slow_signing
    await issuer("mid-run-expiry", valid_until=clock.now() + timedelta(seconds=600))

    # (c) healthy issuance inside the window
    issuer.work_hook = None
    await issuer("healthy", valid_until=clock.now() + timedelta(seconds=600))

    decisions = [r[0] for r in issuer.records]
    print(f"  decisions: {decisions}")
    assert decisions == ["rejected_expired_at_start", "aborted_mid_run", "issued"]
    print("  -> expired-at-start and mid-run-expiry are distinct cases; both need")
    print("     app-level checks against the business clock, not the scheduler.")


async def scenario_jitter() -> None:
    """max_jitter shifts fire times; start_deadline is computed after jitter."""
    print("\n=== Scenario 4: jitter vs start deadline ===")
    clock = ControlledClock()
    _current.issuer = FakeIssuer(clock, enforce_business_window=True)
    _current.gate = anyio.Event()  # hold the first job inside the executor
    _current.entered_first_call = anyio.Event()
    grace = timedelta(seconds=0.3)
    max_jitter = timedelta(seconds=0.15)

    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(
            issue_task,
            IntervalTrigger(seconds=INTERVAL),
            id="sign",
            misfire_grace_time=grace,
            max_jitter=max_jitter,
            args=["jitter", FAR_FUTURE],
        )
        await scheduler.start_in_background()
        with anyio.fail_after(5):
            await _current.entered_first_call.wait()
        # The first job is now acquired and blocked inside the issuer, so it is
        # guaranteed to still be present in the store for inspection
        jobs = await scheduler.get_jobs()
        job = next(j for j in jobs if j.schedule_id == "sign")
        _current.gate.set()

    print(f"  jitter applied:                 {job.jitter.total_seconds():.3f}s")
    print(f"  start_deadline - fire_time:     "
          f"{(job.start_deadline - job.scheduled_fire_time).total_seconds():.3f}s")
    assert timedelta(0) <= job.jitter <= max_jitter
    assert job.jitter < timedelta(seconds=INTERVAL), "jitter is capped below the interval"
    assert job.start_deadline == job.scheduled_fire_time + grace
    print("  -> start_deadline = jittered fire time + grace; jitter consumes part of")
    print("     the effective grace window, so size grace = jitter headroom + tolerance.")


async def main() -> None:
    await scenario_bad_config()
    await scenario_good_config()
    await scenario_business_window()
    await scenario_jitter()
    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    anyio.run(main)
