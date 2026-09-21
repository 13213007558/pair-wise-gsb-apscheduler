:mod:`apscheduler.jobstores.sqlalchemy`
=======================================

.. automodule:: apscheduler.jobstores.sqlalchemy

API
---

.. autoclass:: SQLAlchemyJobStore(url=None, engine=None, tablename='apscheduler_jobs', metadata=None, pickle_protocol=pickle.HIGHEST_PROTOCOL, optimistic=False, lease_duration=15.0, conflict_policy='error', conflict_retries=3, auto_migrate=True)
    :show-inheritance:


Introduction
------------

SQLAlchemyJobStore stores jobs in any relational database management system supported by
`SQLAlchemy <http://www.sqlalchemy.org/>`_. It can use either a preconfigured
`Engine <http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_ or you can pass it a
connection URL.

Multiple schedulers
~~~~~~~~~~~~~~~~~~~

The default schema and behavior remain compatible with older APScheduler 3.x releases. When more
than one scheduler process shares the same table, construct every store with
``optimistic=True``::

    SQLAlchemyJobStore(url="postgresql+psycopg2://...", optimistic=True)

In this mode the table uses three nullable columns: ``version``, ``lease_expires`` and
``lease_owner``. Reading due jobs is a claim transaction: eligible rows are atomically stamped
with an opaque lease token, read back and returned to that scheduler. Administrative modify,
pause and resume operations condition on the version read from the store and clear an active
lease. A scheduler advancing or removing a claimed job also conditions on both the version and
the lease token obtained during the claim. Lookup and listing operations use read transactions
and do not acquire leases; add, modify, remove and lease release each commit independently.
Scheduler shutdown sets the scheduler state before stopping executors and disposing engines, and
a processing loop that finds the stopped state returns without claiming another batch.

A stale administrative snapshot raises :class:`~apscheduler.jobstores.base.JobConflictError`
when ``conflict_policy="error"`` (the default). Set ``conflict_policy="retry"`` to reread the
job and reapply the requested administrative change up to ``conflict_retries`` times. A stale
claimed job is never retried by writing its old trigger or ``next_run_time``; its callback is
discarded. Unconditional administrative removal still removes the current row, while a claimed
callback that arrives after that removal receives
:class:`~apscheduler.jobstores.base.JobLookupError` and does not recreate the job. Event
``job_id`` and ``jobstore`` values are unchanged.

Time zone information continues to be carried in the existing pickled trigger and
``next_run_time`` values; the relational columns remain UTC floating point timestamps.

Migration and rollback
~~~~~~~~~~~~~~~~~~~~~~

With ``auto_migrate=True`` (the default), starting an optimistic store against the old table adds
the nullable columns and initializes existing versions to ``1`` in a migration transaction.
Existing rows remain readable by their original pickle format after a restart. Setting
``auto_migrate=False`` with ``optimistic=True`` fails fast with ``RuntimeError`` if an upgrade
is required. Leaving ``optimistic=False`` keeps the old read/write behavior and works against
either old tables or upgraded tables. The added columns are nullable, so rolling the application
code back to older APScheduler does not require changing the table. To remove the columns as
well, take the database offline and use the database-supported ALTER TABLE DROP operation, or
recreate and repopulate the table on databases without drop-column support. All schedulers
sharing a table must use the new optimistic mode; a mix of old and new processes cannot provide
the conflict guarantee because old processes do not write versions or honor leases.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `SQLAlchemy <https://pypi.python.org/pypi/SQLAlchemy/>`_ (+ the backend specific driver package)
   * - Example
     - ``examples/jobstores/sqlalchemy_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/jobstores/sqlalchemy_.py>`_).
