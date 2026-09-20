import logging
import sys
import traceback
from abc import ABCMeta, abstractmethod
from asyncio import CancelledError
from datetime import datetime, timedelta, timezone
from itertools import count
from traceback import format_tb

from apscheduler.events import (
    EVENT_JOB_CANCELLED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)


class _JobExecution:
    __slots__ = (
        "id",
        "job_id",
        "jobstore_alias",
        "run_times",
        "future",
        "underlying_future",
        "executor_alias",
        "cancelled",
        "cancelled_by_scheduler",
        "finished",
    )

    def __init__(self, execution_id, job, run_times):
        self.id = execution_id
        self.job_id = job.id
        self.jobstore_alias = job._jobstore_alias
        self.run_times = run_times
        self.future = None
        self.underlying_future = None
        self.executor_alias = None
        self.cancelled = False
        self.cancelled_by_scheduler = False
        self.finished = False


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
        self._instances = {}
        self._active_executions = {}
        self._execution_id_counter = count(1)

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
        self._alias = alias
        self._logger = logging.getLogger(f"apscheduler.executors.{alias}")
        self._instances = {}
        self._active_executions = {}
        self._execution_id_counter = count(1)
        self._shutdown = False

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param bool wait: ``True`` to wait until all submitted jobs
            have been executed
        """

    def cancel_execution(self, execution):
        return False

    def _create_execution(self, job, run_times):
        assert self._lock is not None, "This executor has not been started yet"
        with self._lock:
            if self._instances.get(job.id, 0) >= job.max_instances:
                raise MaxInstancesReachedError(job)

            execution = _JobExecution(next(self._execution_id_counter), job, run_times)
            execution.executor_alias = self._alias
            self._active_executions[execution.id] = execution
            self._instances[job.id] = self._instances.get(job.id, 0) + 1
            return execution

    def _submit_execution(self, execution, job, run_times):
        try:
            return self._do_submit_job(job, run_times, execution)
        except BaseException:
            with self._lock:
                self._release_execution(execution)
            raise

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
        execution = self._create_execution(job, run_times)
        future = self._submit_execution(execution, job, run_times)
        if future is not None:
            execution.future = future
        return execution

    @abstractmethod
    def _do_submit_job(self, job, run_times, execution):
        """Performs the actual task of scheduling run_job to be called."""

    def _release_execution(self, execution):
        if execution.finished:
            return False
        if self._active_executions.get(execution.id) is not execution:
            return False

        execution.finished = True
        self._active_executions.pop(execution.id)
        count = self._instances.get(execution.job_id, 0)
        if count:
            count -= 1
            if count:
                self._instances[execution.job_id] = count
            else:
                self._instances.pop(execution.job_id, None)

        return True

    def _finish_execution(self, execution, events=(), exception=None, traceback=None):
        scheduler = self._scheduler
        unregister = getattr(scheduler, "_job_execution_finished", None)
        if unregister is not None:
            unregister(execution)

        with self._lock:
            released = self._release_execution(execution)

        if not released:
            return

        for event in events:
            scheduler._dispatch_event(event)

        if exception is not None:
            exc_info = (exception.__class__, exception, traceback)
            self._logger.error(
                "Error running job %s", execution.job_id, exc_info=exc_info
            )

    def _cancelled_events(self, execution, run_time=None):
        if run_time is None:
            run_time = execution.run_times[0] if execution.run_times else None
        if run_time is None:
            return ()

        return (
            JobExecutionEvent(
                EVENT_JOB_CANCELLED,
                execution.job_id,
                execution.jobstore_alias,
                run_time,
                exception=CancelledError(),
            ),
        )

    def _run_job_success(self, job_id, events, execution=None):
        """
        Called by the executor with the list of generated events when :func:`run_job` has been
        successfully called.

        """
        if execution is None:
            with self._lock:
                active = [
                    item
                    for item in self._active_executions.values()
                    if item.job_id == job_id
                ]
            execution = active[-1] if active else None

        if execution is not None:
            self._finish_execution(execution, events)
        else:
            for event in events:
                self._scheduler._dispatch_event(event)

    def _run_job_error(self, job_id, exc, traceback=None, execution=None):
        """Called by the executor with the exception if there is an error calling run_job."""
        if execution is None:
            with self._lock:
                active = [
                    item
                    for item in self._active_executions.values()
                    if item.job_id == job_id
                ]
            execution = active[-1] if active else None

        if execution is not None:
            self._finish_execution(execution, (), exception=exc, traceback=traceback)
        else:
            exc_info = (exc.__class__, exc, traceback)
            self._logger.error("Error running job %s", job_id, exc_info=exc_info)


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
        except CancelledError:
            raise
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
            traceback.clear_frames(tb)
        else:
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_EXECUTED, job.id, jobstore_alias, run_time, retval=retval
                )
            )
            logger.info('Job "%s" executed successfully', job)

    return events
