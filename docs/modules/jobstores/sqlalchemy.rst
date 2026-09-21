:mod:`apscheduler.jobstores.sqlalchemy`
=======================================

.. automodule:: apscheduler.jobstores.sqlalchemy

API
---

.. autoclass:: SQLAlchemyJobStore(url=None, engine=None, tablename='apscheduler_jobs', metadata=None, pickle_protocol=pickle.HIGHEST_PROTOCOL, optimistic_versioning=False)
    :show-inheritance:


Introduction
------------

SQLAlchemyJobStore stores jobs in any relational database management system supported by
`SQLAlchemy <http://www.sqlalchemy.org/>`_. It can use either a preconfigured
`Engine <http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_ or you can pass it a
connection URL.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `SQLAlchemy <https://pypi.python.org/pypi/SQLAlchemy/>`_ (+ the backend specific driver package)
   * - Example
     - ``examples/jobstores/sqlalchemy_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/jobstores/sqlalchemy_.py>`_).


Sharing the job store between multiple scheduler instances
----------------------------------------------------------

By default, the last writer wins when two scheduler instances update the same job
concurrently, so a stale snapshot can silently overwrite a newer ``next_run_time``,
paused state or trigger. Setting ``optimistic_versioning=True`` enables optimistic
concurrency control: a ``version`` column is maintained on the jobs table and every
update is guarded by the version the job was read with (the check and the write happen
in a single atomic ``UPDATE``, so no process-level locking is required). A conflicting
update from a stale snapshot raises
:class:`~apscheduler.jobstores.base.ConcurrentModificationError` instead of silently
overwriting the newer state, and an update to a job that was removed in the meantime
raises :class:`~apscheduler.jobstores.base.JobLookupError`.

Scheduler operations react to conflicts as follows:

* :meth:`~apscheduler.schedulers.base.BaseScheduler.modify_job` (and by extension
  ``reschedule_job()``, ``pause_job()`` and ``resume_job()``) re-reads the job and
  re-applies the changes, giving up with
  :class:`~apscheduler.jobstores.base.ConcurrentModificationError` after a few attempts
* the scheduler's job processing loop skips the update when the job was concurrently
  modified or removed, leaving the newer state in the store untouched

Migration notes:

* tables created before this option was enabled are migrated automatically when the
  job store starts (``ALTER TABLE ... ADD COLUMN version INTEGER NOT NULL DEFAULT 0``);
  if the database does not support that, a clear error is raised and the column can be
  added manually, or the option can be left disabled
* job stores running with ``optimistic_versioning=False`` (the default) work unchanged
  against both old and new table layouts, so the option can be rolled back at any time
* the pickled job state format is unchanged, so jobs written by older releases load
  transparently (and vice versa)
