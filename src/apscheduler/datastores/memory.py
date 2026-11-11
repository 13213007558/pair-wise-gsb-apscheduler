from __future__ import annotations

from bisect import bisect_left, bisect_right, insort_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import MAXYEAR, UTC, datetime, timedelta
from functools import partial
from typing import TypeVar
from uuid import UUID

import attrs

from .. import JobOutcome
from .._enums import ConflictPolicy
from .._events import (
    JobAcquired,
    JobAdded,
    JobReleased,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import ConflictingIdError, TaskLookupError
from .._structures import Job, JobResult, Schedule, ScheduleResult, Task
from .._utils import create_repr
from .base import BaseDataStore

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)

_T = TypeVar("_T")


def _detach(obj: _T) -> _T:
    """
    Return a shallow, independent copy of a stored structure.

    Only the structure itself and its own mutable containers (``args``, ``kwargs``
    and ``metadata``) are copied; referenced values such as task arguments or
    triggers are shared deliberately. This avoids both accidental mutation of the
    store's own objects and expensive or impossible deep copies of non-picklable
    runtime values (e.g. synchronization primitives passed as job arguments).

    """
    detached = attrs.evolve(  # type: ignore[misc]
        obj,
        args=tuple(getattr(obj, "args", ())),
        kwargs=dict(getattr(obj, "kwargs", {})),
        metadata=dict(getattr(obj, "metadata", {})),
    )
    return detached


@attrs.define(eq=False, repr=False)
class MemoryDataStore(BaseDataStore):
    """
    Stores scheduler data in memory, without serializing it.

    Can be shared between multiple schedulers within the same event loop.
    """

    _tasks: dict[str, Task] = attrs.Factory(dict)
    _schedules: list[Schedule] = attrs.Factory(list)
    _schedules_by_id: dict[str, Schedule] = attrs.Factory(dict)
    _schedules_by_task_id: dict[str, set[Schedule]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _jobs_by_id: dict[UUID, Job] = attrs.Factory(dict)
    _jobs_by_task_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _jobs_by_schedule_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _job_results: dict[UUID, JobResult] = attrs.Factory(dict)

    def __repr__(self) -> str:
        return create_repr(self)

    def _find_schedule_index(self, schedule: Schedule) -> int:
        left_index = bisect_left(self._schedules, schedule)
        right_index = bisect_right(self._schedules, schedule)
        return self._schedules.index(schedule, left_index, right_index + 1)

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        if ids is None:
            return [_detach(schedule) for schedule in self._schedules]

        return [
            _detach(schedule)
            for schedule in self._schedules
            if ids is None or schedule.id in ids
        ]

    async def add_task(self, task: Task) -> None:
        if task.id in self._tasks:
            task.running_jobs = self._tasks[task.id].running_jobs
            self._tasks[task.id] = task
            await self._event_broker.publish(TaskUpdated(task_id=task.id))
        else:
            self._tasks[task.id] = task
            await self._event_broker.publish(TaskAdded(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        try:
            del self._tasks[task_id]
        except KeyError:
            raise TaskLookupError(task_id) from None

        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise TaskLookupError(task_id) from None

    async def get_tasks(self) -> list[Task]:
        return sorted(self._tasks.values())

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        old_schedule = self._schedules_by_id.get(schedule.id)
        if old_schedule is not None:
            if conflict_policy is ConflictPolicy.do_nothing:
                return
            elif conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id)

            index = self._find_schedule_index(old_schedule)
            del self._schedules[index]
            self._schedules_by_task_id[old_schedule.task_id].remove(old_schedule)

        self._schedules_by_id[schedule.id] = schedule
        self._schedules_by_task_id[schedule.task_id].add(schedule)
        insort_right(self._schedules, schedule)

        event: ScheduleUpdated | ScheduleAdded
        if old_schedule is not None:
            event = ScheduleUpdated(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )

        await self._event_broker.publish(event)

    async def remove_schedules(
        self, ids: Iterable[str], *, finished: bool = False
    ) -> None:
        for schedule_id in ids:
            schedule = self._schedules_by_id.pop(schedule_id, None)
            if schedule:
                self._schedules.remove(schedule)
                event = ScheduleRemoved(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    finished=finished,
                )
                await self._event_broker.publish(event)

    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        now = datetime.now(UTC)
        acquired_until = now + lease_duration
        schedules: list[Schedule] = []
        for schedule in self._schedules:
            if schedule.next_fire_time is None or schedule.next_fire_time > now:
                # The schedule is either exhausted or not yet due. There will be no
                # schedules that are due after this one, so we can stop here.
                break
            elif schedule.paused:
                # The schedule is paused
                continue
            elif schedule.acquired_until is not None:
                if (
                    schedule.acquired_by != scheduler_id
                    and now <= schedule.acquired_until
                ):
                    # The schedule has been acquired by another scheduler and the
                    # timeout has not expired yet
                    continue

            schedules.append(schedule)
            schedule.acquired_by = scheduler_id
            schedule.acquired_until = acquired_until
            schedule.acquisition_token = (schedule.acquisition_token or 0) + 1
            if len(schedules) == limit:
                break

        # Hand back detached copies so callers can never mutate the store's own
        # objects (and never observe another holder's later re-acquisition)
        return [_detach(schedule) for schedule in schedules]

    async def release_schedules(
        self,
        scheduler_id: str,
        results: Sequence[ScheduleResult],
        acquisition_tokens: dict[str, int] | None = None,
    ) -> None:
        # Send update events for schedules
        for result in results:
            schedule = self._schedules_by_id[result.schedule_id]

            # Fence off stale holders: a schedule re-acquired by another scheduler (or
            # re-acquired by this same scheduler under a new token) must not be
            # overwritten by a late release.
            if schedule.acquired_by != scheduler_id or (
                acquisition_tokens is not None
                and schedule.acquisition_token
                != acquisition_tokens.get(result.schedule_id)
            ):
                self._logger.warning(
                    "Ignoring release of schedule %s from scheduler %r with a stale "
                    "acquisition token",
                    result.schedule_id,
                    scheduler_id,
                )
                continue

            # Remove the schedule
            index = self._find_schedule_index(schedule)
            del self._schedules[index]

            # Re-add the schedule to its new position
            schedule.last_fire_time = result.last_fire_time
            schedule.next_fire_time = result.next_fire_time
            schedule.acquired_by = None
            schedule.acquired_until = None
            schedule.acquisition_token = None
            insort_right(self._schedules, schedule)
            event = ScheduleUpdated(
                schedule_id=result.schedule_id,
                task_id=schedule.task_id,
                next_fire_time=result.next_fire_time,
            )
            await self._event_broker.publish(event)

    async def get_next_schedule_run_time(self) -> datetime | None:
        for schedule in self._schedules:
            if schedule.next_fire_time is None:
                # Exhausted schedules sort last, so no eligible schedule can follow
                break
            elif schedule.paused or schedule.acquired_by is not None:
                # Paused and already acquired schedules are not active; they will be
                # announced via an event when they become relevant again
                continue

            return schedule.next_fire_time

        return None

    async def add_job(self, job: Job) -> None:
        self._jobs_by_id[job.id] = job
        self._jobs_by_task_id[job.task_id].add(job)
        if job.schedule_id is not None:
            self._jobs_by_schedule_id[job.schedule_id].add(job)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        if ids is not None:
            ids = frozenset(ids)

        if ids is None:
            return [_detach(job) for job in self._jobs_by_id.values()]

        return [
            _detach(job)
            for job in self._jobs_by_id.values()
            if ids is None or job.id in ids
        ]

    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        now = datetime.now(UTC)
        acquired_until = now + lease_duration
        jobs: list[Job] = []
        job_results: dict[Job, JobResult] = {}
        for job in self._jobs_by_id.values():
            task = self._tasks[job.task_id]

            # Skip already acquired jobs (unless the acquisition lock has expired)
            if job.acquired_until is not None:
                if job.acquired_until >= now:
                    continue
                else:
                    task.running_jobs -= 1

            # Discard the job if its start deadline has passed
            if job.start_deadline and job.start_deadline < now:
                # Claim the job for this scheduler first (with a fresh token) so the
                # fenced release below is recognized as coming from its current holder
                job.acquired_by = scheduler_id
                job.acquired_until = acquired_until
                job.acquisition_token = (job.acquisition_token or 0) + 1
                job_results[job] = JobResult(
                    job_id=job.id,
                    outcome=JobOutcome.missed_start_deadline,
                    finished_at=now,
                    expires_at=now + job.result_expiration_time,
                )
                continue

            # Skip the job if no more slots are available
            if (
                task.max_running_jobs is not None
                and task.running_jobs >= task.max_running_jobs
            ):
                self._logger.debug(
                    "Skipping job %s because task %r has the maximum number of %d jobs "
                    "already running",
                    job.id,
                    job.task_id,
                    task.running_jobs,
                )
                continue

            # Mark the job as acquired by this worker, issuing a new fencing token that
            # invalidates any token held by a previous holder
            jobs.append(job)
            job.acquired_by = scheduler_id
            job.acquired_until = acquired_until
            job.acquisition_token = (job.acquisition_token or 0) + 1

            # Increment the number of running jobs for this task
            task.running_jobs += 1

            # Exit the loop if enough jobs have been acquired
            if len(jobs) == limit:
                break

        # Publish the appropriate events
        for job in jobs:
            await self._event_broker.publish(
                JobAcquired.from_job(job, scheduler_id=scheduler_id)
            )

        # Discard the jobs that could not start
        for job, result in job_results.items():
            event = await self._release_job(
                scheduler_id,
                job,
                result,
                job.acquisition_token,
                decrement_running_job_count=False,
            )
            if event is not None:
                await self._event_broker.publish(event)

        # Hand back detached copies so callers can never mutate the store's own
        # objects (and never observe another holder's later re-acquisition)
        return [_detach(job) for job in jobs]

    async def _release_job(
        self,
        scheduler_id: str,
        job: Job,
        result: JobResult,
        acquisition_token: int | None,
        *,
        decrement_running_job_count: bool = True,
    ) -> JobReleased | None:
        stored_job = self._jobs_by_id.get(result.job_id)

        # Fence off stale holders: if the job has been (re)acquired since the caller
        # got its token, the caller no longer owns this lease and must not touch the
        # job, its result or the task's running job counter.
        if (
            stored_job is None
            or stored_job.acquired_by != scheduler_id
            or stored_job.acquisition_token != acquisition_token
        ):
            self._logger.warning(
                "Ignoring release of job %s from scheduler %r with a stale acquisition "
                "token (%r); the job is now held under token %r by %r",
                result.job_id,
                scheduler_id,
                acquisition_token,
                stored_job.acquisition_token if stored_job is not None else None,
                stored_job.acquired_by if stored_job is not None else None,
            )
            return None

        # Record the job result
        if result.expires_at > result.finished_at:
            self._job_results[result.job_id] = result

        # Decrement the number of running jobs for this task
        if decrement_running_job_count and stored_job.acquired_by:
            self._tasks[stored_job.task_id].running_jobs -= 1

        # Delete the job
        job = self._jobs_by_id.pop(result.job_id)

        # Remove the job from the jobs belonging to its task
        task_jobs = self._jobs_by_task_id[job.task_id]
        task_jobs.remove(job)
        if not task_jobs:
            del self._jobs_by_task_id[job.task_id]

        # If this was a scheduled job, remove the job from the set of jobs belonging to
        # this schedule
        if job.schedule_id:
            schedule_jobs = self._jobs_by_schedule_id[job.schedule_id]
            schedule_jobs.remove(job)
            if not schedule_jobs:
                del self._jobs_by_schedule_id[job.schedule_id]

        # Notify other schedulers
        return JobReleased.from_result(
            result,
            scheduler_id,
            job.task_id,
            job.schedule_id,
            job.scheduled_fire_time,
        )

    async def release_job(
        self,
        scheduler_id: str,
        job: Job,
        result: JobResult,
        acquisition_token: int | None = None,
    ) -> None:
        # Capture the token at call time so a later (re)acquisition mutating the same
        # in-memory object cannot make this call pass the fence retroactively.
        token = (
            acquisition_token
            if acquisition_token is not None
            else job.acquisition_token
        )
        event = await self._release_job(scheduler_id, job, result, token)
        if event is not None:
            await self._event_broker.publish(event)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        return self._job_results.pop(job_id, None)

    async def extend_acquired_schedule_leases(
        self,
        scheduler_id: str,
        schedule_ids: set[str],
        duration: timedelta,
        acquisition_tokens: dict[str, int] | None = None,
    ) -> None:
        acquired_until = datetime.now(UTC) + duration
        for schedule in self._schedules:
            if schedule.acquired_by != scheduler_id or schedule.id not in schedule_ids:
                continue

            # Only extend the lease if the caller still holds this acquisition
            if (
                acquisition_tokens is not None
                and schedule.acquisition_token != acquisition_tokens.get(schedule.id)
            ):
                continue

            schedule.acquired_until = acquired_until

    async def extend_acquired_job_leases(
        self,
        scheduler_id: str,
        job_ids: set[UUID],
        duration: timedelta,
        acquisition_tokens: dict[UUID, int] | None = None,
    ) -> None:
        acquired_until = datetime.now(UTC) + duration
        for job in self._jobs_by_id.values():
            if job.acquired_by != scheduler_id or job.id not in job_ids:
                continue

            # Only extend the lease if the caller still holds this acquisition
            if (
                acquisition_tokens is not None
                and job.acquisition_token != acquisition_tokens.get(job.id)
            ):
                continue

            job.acquired_until = acquired_until

    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        now = datetime.now(UTC)
        for job in list(self._jobs_by_id.values()):
            if job.acquired_by == scheduler_id:
                result = JobResult.from_job(
                    job=job, outcome=JobOutcome.abandoned, finished_at=now
                )
                event = await self._release_job(
                    job.acquired_by,
                    job,
                    result,
                    job.acquisition_token,
                )
                if event is not None:
                    await self._event_broker.publish(event)

    async def cleanup(self) -> None:
        # Clean up expired job results
        now = datetime.now(UTC)
        expired_job_ids = [
            result.job_id
            for result in self._job_results.values()
            if result.expires_at <= now
        ]
        for job_id in expired_job_ids:
            del self._job_results[job_id]

        # Finish any jobs whose leases have expired
        expired_jobs = [
            job
            for job in self._jobs_by_id.values()
            if job.acquired_until is not None and job.acquired_until < now
        ]
        for job in expired_jobs:
            result = JobResult.from_job(
                job=job, outcome=JobOutcome.abandoned, finished_at=now
            )
            assert job.acquired_by is not None
            event = await self._release_job(
                job.acquired_by,
                job,
                result,
                job.acquisition_token,
            )
            if event is not None:
                await self._event_broker.publish(event)

        # Release any schedules whose leases have expired
        for schedule in self._schedules:
            if schedule.acquired_until is not None and schedule.acquired_until < now:
                schedule.acquired_by = None
                schedule.acquired_until = None
                schedule.acquisition_token = None
                schedule_event = ScheduleUpdated(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    next_fire_time=schedule.next_fire_time,
                )
                await self._event_broker.publish(schedule_event)

        # Clean up finished schedules that have no running jobs
        finished_schedule_ids = [
            schedule_id
            for schedule_id, schedule in self._schedules_by_id.items()
            if schedule.next_fire_time is None
            and schedule_id not in self._jobs_by_schedule_id
        ]
        await self.remove_schedules(finished_schedule_ids, finished=True)
