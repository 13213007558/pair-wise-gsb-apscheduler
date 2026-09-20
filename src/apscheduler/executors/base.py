import asyncio
import logging
import sys
import traceback
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from traceback import format_tb

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)


class MaxInstancesReachedError(Exception):
    def __init__(self, job):
        super().__init__(
            'Job "%s" has already reached its maximum number of instances (%d)'
            % (job.id, job.max_instances)
        )


class BaseExecutor(metaclass=ABCMeta):
    """Abstract base class that defines the interface that every executor must implement."""

    _scheduler = None
    _lock = None
    _logger = logging.getLogger("apscheduler.executors")

    def __init__(self):
        super().__init__()
        self._next_token = 0
        self._generation = 0
        # Maps generation ids to their own accounting state. A generation represents one
        # start()/shutdown() lifecycle. Each generation keeps:
        #   instances: per-job-id running instance counts used to enforce max_instances
        #   active:    execution token -> job id, used to make slot release idempotent
        # Jobs that survive a shutdown (e.g. an uncancellable worker thread) keep their
        # slot in the previous generation, so a late completion callback can never
        # decrement or otherwise disturb a newer execution of the same job id.
        self._generations = {}

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the executor is being
        added to an already running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting
            this executor
        :param str|unicode alias: alias of this executor as it was assigned to the scheduler

        """
        self._scheduler = scheduler
        self._lock = scheduler._create_lock()
        self._logger = logging.getLogger(f"apscheduler.executors.{alias}")
        # Begin a new accounting generation. Slots held by executions that survive the
        # shutdown (e.g. uncancellable worker threads) stay behind in old generations
        # and are reconciled when those executions finish.
        with self._lock:
            generation = self._generation + 1
            self._generation = generation
            self._generations[generation] = {
                "instances": defaultdict(lambda: 0),
                "active": {},
            }

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param bool wait: ``True`` to wait until all submitted jobs
            have been executed
        """

    @property
    def _instances(self):
        """Per-job running instance counts of the current (active) generation."""
        state = self._generations.get(self._generation)
        if state is None:
            return defaultdict(lambda: 0)
        return state["instances"]

    @property
    def _active(self):
        """Execution token -> (job id, generation) map of the active generation."""
        state = self._generations.get(self._generation)
        if state is None:
            return {}
        return state["active"]

    def submit_job(self, job, run_times):
        """
        Submits job for execution.

        :param Job job: job to execute
        :param list[datetime] run_times: list of datetimes specifying
            when the job should have been run
        :raises MaxInstancesReachedError: if the maximum number of
            allowed instances for this job has been reached

        """
        assert self._lock is not None, "This executor has not been started yet"
        with self._lock:
            state = self._generations[self._generation]
            instances = state["instances"]
            active = state["active"]
            if instances[job.id] >= job.max_instances:
                raise MaxInstancesReachedError(job)

            # Reserve the slot and mint an execution token before handing the job to
            # the backend. If submission itself fails, the reservation is rolled back
            # so the job is not left permanently at max_instances.
            token = self._next_token
            self._next_token += 1
            generation = self._generation
            active[token] = (job.id, generation)
            instances[job.id] += 1
            try:
                self._do_submit_job(job, run_times, token)
            except BaseException:
                active.pop(token, None)
                instances[job.id] -= 1
                if instances[job.id] <= 0:
                    del instances[job.id]
                raise

    @abstractmethod
    def _do_submit_job(self, job, run_times, token):
        """Performs the actual task of scheduling `run_job` to be called."""

    def _release_instance_locked(self, token):
        """
        Releases the running instance slot associated with ``token``.

        :return: ``(job_id, generation, from_previous_generation)`` or ``None`` if the
            token was unknown (duplicate completion callback)
        """
        for generation, state in self._generations.items():
            entry = state["active"].pop(token, None)
            if entry is not None:
                job_id, _ = entry
                break
        else:
            return None

        instances = state["instances"]
        instances[job_id] -= 1
        if instances[job_id] <= 0:
            del instances[job_id]

        previous_generation = generation < self._generation
        if previous_generation and not state["active"] and not instances:
            # The last carry-over execution of a superseded generation is done.
            del self._generations[generation]

        return job_id, generation, previous_generation

    def _finalize_execution(self, token, outcome, *outcome_args):
        """
        Common terminal transition for every submitted execution.

        ``outcome`` is one of the ``self._run_job_*`` callbacks; it is only invoked if
        the release happened in the current generation. Results arriving from a
        superseded generation only reconcile the old accounting and never reach the
        scheduler.

        """
        with self._lock:
            result = self._release_instance_locked(token)

        if result is None:
            self._logger.debug(
                "Discarding duplicate %s result of execution token %d",
                outcome.__name__,
                token,
            )
            return

        job_id, generation, previous_generation = result
        if previous_generation:
            self._logger.debug(
                "Discarding %s result of job %r from superseded executor generation %d",
                outcome.__name__,
                job_id,
                generation,
            )
            return

        outcome(job_id, *outcome_args)

    def _job_succeeded(self, token, events):
        """Terminal transition for an execution that finished normally."""
        self._finalize_execution(token, self._run_job_success, events)

    def _job_failed(self, token, exc, traceback=None):
        """Terminal transition for an execution that raised an exception."""
        self._finalize_execution(token, self._run_job_error, exc, traceback)

    def _job_cancelled(self, token):
        """Terminal transition for an execution that was cancelled."""
        self._finalize_execution(token, self._run_job_cancelled)

    def _run_job_success(self, job_id, events):
        """
        Called by the executor with the list of generated events when :func:`run_job` has been
        successfully called.

        """
        for event in events:
            self._scheduler._dispatch_event(event)

    def _run_job_error(self, job_id, exc, traceback=None):
        """Called by the executor with the exception if there is an error  calling `run_job`."""
        exc_info = (exc.__class__, exc, traceback)
        self._logger.error("Error running job %s", job_id, exc_info=exc_info)

    def _run_job_cancelled(self, job_id):
        """Called when a submitted execution is cancelled before it can finish."""
        self._logger.info('Execution of job "%s" was cancelled', job_id)


def run_job(job, jobstore_alias, run_times, logger_name):
    """
    Called by executors to run the job. Returns a list of scheduler events to be dispatched by the
    scheduler.

    """
    events = []
    logger = logging.getLogger(logger_name)
    for run_time in run_times:
        # See if the job missed its run time window, and handle
        # possible misfires accordingly
        if job.misfire_grace_time is not None:
            difference = datetime.now(timezone.utc) - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                events.append(
                    JobExecutionEvent(
                        EVENT_JOB_MISSED, job.id, jobstore_alias, run_time
                    )
                )
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
                continue

        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        try:
            retval = job.func(*job.args, **job.kwargs)
        except BaseException:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = "".join(format_tb(tb))
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_ERROR,
                    job.id,
                    jobstore_alias,
                    run_time,
                    exception=exc,
                    traceback=formatted_tb,
                )
            )
            logger.exception('Job "%s" raised an exception', job)

            # This is to prevent cyclic references that would lead to memory leaks
            traceback.clear_frames(tb)
            del tb
        else:
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_EXECUTED, job.id, jobstore_alias, run_time, retval=retval
                )
            )
            logger.info('Job "%s" executed successfully', job)

    return events


async def run_coroutine_job(job, jobstore_alias, run_times, logger_name):
    """Coroutine version of run_job()."""
    events = []
    logger = logging.getLogger(logger_name)
    for run_time in run_times:
        # See if the job missed its run time window, and handle possible misfires accordingly
        if job.misfire_grace_time is not None:
            difference = datetime.now(timezone.utc) - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                events.append(
                    JobExecutionEvent(
                        EVENT_JOB_MISSED, job.id, jobstore_alias, run_time
                    )
                )
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
                continue

        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        try:
            retval = await job.func(*job.args, **job.kwargs)
        except asyncio.CancelledError:
            logger.info('Execution of job "%s" was cancelled', job)
            raise
        except Exception:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = "".join(format_tb(tb))
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_ERROR,
                    job.id,
                    jobstore_alias,
                    run_time,
                    exception=exc,
                    traceback=formatted_tb,
                )
            )
            logger.exception('Job "%s" raised an exception', job)
            traceback.clear_frames(tb)
        else:
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_EXECUTED, job.id, jobstore_alias, run_time, retval=retval
                )
            )
            logger.info('Job "%s" executed successfully', job)

    return events
