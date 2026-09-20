import sys

from apscheduler.executors.base import BaseExecutor, run_job

try:
    import gevent
except ImportError as exc:  # pragma: nocover
    raise ImportError("GeventExecutor requires gevent installed") from exc


class GeventExecutor(BaseExecutor):
    """
    Runs jobs as greenlets.

    Plugin alias: ``gevent``
    """

    def _do_submit_job(self, job, run_times, token):
        def callback(greenlet):
            try:
                events = greenlet.get()
            except BaseException:
                self._job_failed(token, *sys.exc_info()[1:])
            else:
                self._job_succeeded(token, events)

        gevent.spawn(
            run_job, job, job._jobstore_alias, run_times, self._logger.name
        ).link(callback)
