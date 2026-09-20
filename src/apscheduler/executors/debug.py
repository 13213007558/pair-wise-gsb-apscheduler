import sys

from apscheduler.executors.base import BaseExecutor, run_job


class DebugExecutor(BaseExecutor):
    """
    A special executor that executes the target callable directly instead of deferring it to a
    thread or process.

    Plugin alias: ``debug``
    """

    def _do_submit_job(self, job, run_times, token):
        try:
            events = run_job(job, job._jobstore_alias, run_times, self._logger.name)
        except BaseException:
            self._job_failed(token, *sys.exc_info()[1:])
        else:
            self._job_succeeded(token, events)
