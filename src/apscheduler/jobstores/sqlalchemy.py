import pickle

from apscheduler.job import Job
from apscheduler.jobstores.base import (
    BaseJobStore,
    ConcurrentModificationError,
    ConflictingIdError,
    JobLookupError,
)
from apscheduler.util import (
    datetime_to_utc_timestamp,
    maybe_ref,
    utc_timestamp_to_datetime,
)

try:
    from sqlalchemy import (
        Column,
        Float,
        Integer,
        LargeBinary,
        MetaData,
        Table,
        Unicode,
        and_,
        create_engine,
        inspect,
        select,
        text,
    )
    from sqlalchemy.exc import IntegrityError, SQLAlchemyError
    from sqlalchemy.sql.expression import null
except ImportError as exc:  # pragma: nocover
    raise ImportError("SQLAlchemyJobStore requires SQLAlchemy installed") from exc


class SQLAlchemyJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLAlchemy.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``sqlalchemy``

    :param str url: connection string (see
        :ref:`SQLAlchemy documentation <sqlalchemy:database_urls>` on this)
    :param engine: an SQLAlchemy :class:`~sqlalchemy.engine.Engine` to use instead of creating a
        new one based on ``url``
    :param str tablename: name of the table to store jobs in
    :param metadata: a :class:`~sqlalchemy.schema.MetaData` instance to use instead of creating a
        new one
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    :param str tableschema: name of the (existing) schema in the target database where the table
        should be
    :param dict engine_options: keyword arguments to :func:`~sqlalchemy.create_engine`
        (ignored if ``engine`` is given)
    :param bool optimistic_versioning: when ``True``, a ``version`` column is maintained on the
        jobs table and every update is guarded by the version the job was read with, so that a
        stale snapshot can never silently overwrite a newer state written by another scheduler
        instance sharing this job store (a
        :class:`~apscheduler.jobstores.base.ConcurrentModificationError` is raised instead).
        If the table already exists without the ``version`` column, it is added automatically
        when the job store is started. Defaults to ``False`` (the historical behavior, no schema
        changes).
    """

    def __init__(
        self,
        url=None,
        engine=None,
        tablename="apscheduler_jobs",
        metadata=None,
        pickle_protocol=pickle.HIGHEST_PROTOCOL,
        tableschema=None,
        engine_options=None,
        optimistic_versioning=False,
    ):
        super().__init__()
        self.pickle_protocol = pickle_protocol
        self.optimistic_versioning = optimistic_versioning
        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url, **(engine_options or {}))
        else:
            raise ValueError('Need either "engine" or "url" defined')

        # 191 = max key length in MySQL for InnoDB/utf8mb4 tables,
        # 25 = precision that translates to an 8-byte float
        columns = [
            Column("id", Unicode(191), primary_key=True),
            Column("next_run_time", Float(25), index=True),
            Column("job_state", LargeBinary, nullable=False),
        ]
        if optimistic_versioning:
            columns.append(
                Column("version", Integer, nullable=False, server_default="0")
            )

        self.jobs_t = Table(tablename, metadata, *columns, schema=tableschema)

    def start(self, scheduler, alias):
        super().start(scheduler, alias)
        self.jobs_t.create(self.engine, True)
        if self.optimistic_versioning:
            self._ensure_version_column()

    def _ensure_version_column(self):
        """
        Adds the ``version`` column to a pre-existing jobs table if necessary.

        This is the migration path for tables created before optimistic versioning was enabled.
        If the column cannot be added automatically (e.g. the database dialect does not support
        ``ALTER TABLE ... ADD COLUMN``), a clear error is raised; the operator can then either
        add the column manually or keep ``optimistic_versioning`` disabled.
        """
        with self.engine.begin() as connection:
            columns = inspect(connection).get_columns(
                self.jobs_t.name, schema=self.jobs_t.schema
            )
            if any(column["name"] == "version" for column in columns):
                return

            table_name = connection.dialect.identifier_preparer.format_table(
                self.jobs_t
            )
            try:
                connection.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        "ADD COLUMN version INTEGER NOT NULL DEFAULT 0"
                    )
                )
            except SQLAlchemyError as exc:
                raise RuntimeError(
                    f"The jobs table {self.jobs_t.name!r} does not have the 'version' column "
                    "required by optimistic_versioning and it could not be added automatically. "
                    "Either add the column manually "
                    "(ALTER TABLE ... ADD COLUMN version INTEGER NOT NULL DEFAULT 0) or disable "
                    "optimistic_versioning."
                ) from exc

    def lookup_job(self, job_id):
        columns = [self.jobs_t.c.job_state]
        if self.optimistic_versioning:
            columns.append(self.jobs_t.c.version)

        selectable = select(*columns).where(self.jobs_t.c.id == job_id)
        with self.engine.begin() as connection:
            row = connection.execute(selectable).first()
            if not row:
                return None

            version = row.version if self.optimistic_versioning else None
            return self._reconstitute_job(row.job_state, version)

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.jobs_t.c.next_run_time <= timestamp)

    def get_next_run_time(self):
        selectable = (
            select(self.jobs_t.c.next_run_time)
            .where(self.jobs_t.c.next_run_time != null())
            .order_by(self.jobs_t.c.next_run_time)
            .limit(1)
        )
        with self.engine.begin() as connection:
            next_run_time = connection.execute(selectable).scalar()
            return utc_timestamp_to_datetime(next_run_time)

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        values = {
            "id": job.id,
            "next_run_time": datetime_to_utc_timestamp(job.next_run_time),
            "job_state": pickle.dumps(job.__getstate__(), self.pickle_protocol),
        }
        if self.optimistic_versioning:
            values["version"] = 0

        insert = self.jobs_t.insert().values(**values)
        with self.engine.begin() as connection:
            try:
                connection.execute(insert)
            except IntegrityError:
                raise ConflictingIdError(job.id)

    def update_job(self, job):
        values = {
            "next_run_time": datetime_to_utc_timestamp(job.next_run_time),
            "job_state": pickle.dumps(job.__getstate__(), self.pickle_protocol),
        }
        conditions = [self.jobs_t.c.id == job.id]
        if self.optimistic_versioning:
            # Optimistic concurrency control: the update only succeeds if the row still
            # carries the version the job was read with. The version check and the write
            # happen in a single atomic UPDATE, so no process-level locking is involved.
            expected_version = job._version or 0
            values["version"] = expected_version + 1
            conditions.append(self.jobs_t.c.version == expected_version)

        update = self.jobs_t.update().values(**values).where(and_(*conditions))
        with self.engine.begin() as connection:
            result = connection.execute(update)
            if result.rowcount == 0:
                if self.optimistic_versioning:
                    # Distinguish a removed job from a version conflict
                    selectable = select(self.jobs_t.c.id).where(
                        self.jobs_t.c.id == job.id
                    )
                    if connection.execute(selectable).scalar() is not None:
                        raise ConcurrentModificationError(job.id)

                raise JobLookupError(job.id)

        if self.optimistic_versioning:
            job._version = expected_version + 1

    def remove_job(self, job_id):
        delete = self.jobs_t.delete().where(self.jobs_t.c.id == job_id)
        with self.engine.begin() as connection:
            result = connection.execute(delete)
            if result.rowcount == 0:
                raise JobLookupError(job_id)

    def remove_all_jobs(self):
        delete = self.jobs_t.delete()
        with self.engine.begin() as connection:
            connection.execute(delete)

    def shutdown(self):
        self.engine.dispose()

    def _reconstitute_job(self, job_state, version=None):
        job_state = pickle.loads(job_state)
        job_state["jobstore"] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        job._version = version
        return job

    def _get_jobs(self, *conditions):
        jobs = []
        columns = [self.jobs_t.c.id, self.jobs_t.c.job_state]
        if self.optimistic_versioning:
            columns.append(self.jobs_t.c.version)

        selectable = select(*columns).order_by(self.jobs_t.c.next_run_time)
        selectable = selectable.where(and_(*conditions)) if conditions else selectable
        failed_job_ids = set()
        with self.engine.begin() as connection:
            for row in connection.execute(selectable):
                try:
                    version = row.version if self.optimistic_versioning else None
                    jobs.append(self._reconstitute_job(row.job_state, version))
                except BaseException:
                    self._logger.exception(
                        'Unable to restore job "%s" -- removing it', row.id
                    )
                    failed_job_ids.add(row.id)

            # Remove all the jobs we failed to restore
            if failed_job_ids:
                delete = self.jobs_t.delete().where(
                    self.jobs_t.c.id.in_(failed_job_ids)
                )
                connection.execute(delete)

        return jobs

    def __repr__(self):
        return f"<{self.__class__.__name__} (url={self.engine.url})>"
