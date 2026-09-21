import pickle
import uuid
from datetime import datetime, timezone

from apscheduler.job import Job
from apscheduler.jobstores.base import (
    BaseJobStore,
    ConflictingIdError,
    JobConflictError,
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
        func,
        or_,
        select,
    )
    from sqlalchemy import inspect as sqla_inspect
    from sqlalchemy.exc import IntegrityError
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
    :param bool optimistic: enable row versions and lease-based claiming for multiple schedulers
    :param float lease_duration: seconds a due job is reserved for the claiming scheduler
    :param str conflict_policy: ``error`` raises :class:`JobConflictError`; ``retry`` rereads
        administrative updates that lose an optimistic concurrency conflict
    :param int conflict_retries: reread attempts used by the retry policy
    :param bool auto_migrate: add nullable optimistic-concurrency columns to an existing table
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
        optimistic=False,
        lease_duration=15.0,
        conflict_policy="error",
        conflict_retries=3,
        auto_migrate=True,
    ):
        super().__init__()
        self.pickle_protocol = pickle_protocol
        self.optimistic = optimistic
        self.lease_duration = float(lease_duration)
        self.conflict_policy = conflict_policy
        self.conflict_retries = conflict_retries
        self.auto_migrate = auto_migrate
        self.supports_optimistic_jobs = False

        if self.optimistic and self.lease_duration <= 0:
            raise ValueError("lease_duration must be a positive number")
        if conflict_policy not in ("error", "retry"):
            raise ValueError('conflict_policy must be either "error" or "retry"')
        if not isinstance(conflict_retries, int) or conflict_retries < 0:
            raise ValueError("conflict_retries must be a non-negative integer")

        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url, **(engine_options or {}))
        else:
            raise ValueError('Need either "engine" or "url" defined')

        self.tablename = tablename
        self.tableschema = tableschema
        self.jobs_t = self._define_table(metadata, optimistic)

    def start(self, scheduler, alias):
        super().start(scheduler, alias)
        self.jobs_t.create(self.engine, checkfirst=True)

        with self.engine.begin() as connection:
            inspector = sqla_inspect(connection)
            columns = {
                column["name"]
                for column in inspector.get_columns(
                    self.tablename, schema=self.tableschema
                )
            }
            required_columns = {"version", "lease_expires", "lease_owner"}
            missing_columns = required_columns - columns

            if missing_columns and self.optimistic:
                if not self.auto_migrate:
                    raise RuntimeError(
                        "Optimistic job stores require the version, lease_expires and "
                        "lease_owner columns, but auto_migrate is disabled"
                    )

                self._add_optimistic_columns(connection, missing_columns)
                connection.execute(
                    self.jobs_t.update()
                    .where(self.jobs_t.c.version.is_(None))
                    .values(version=1)
                )
                columns |= required_columns

        self.supports_optimistic_jobs = self.optimistic and required_columns <= columns
        if not self.optimistic and missing_columns:
            self.jobs_t = self._define_table(MetaData(), False)

    def lookup_job(self, job_id):
        selectable = select(*self._select_columns()).where(self.jobs_t.c.id == job_id)
        with self.engine.begin() as connection:
            row = connection.execute(selectable).first()
            return self._reconstitute_job_row(row) if row else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        if self.supports_optimistic_jobs:
            return self._claim_due_jobs(timestamp)
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
        if self.supports_optimistic_jobs:
            values["version"] = 1
        insert = self.jobs_t.insert().values(**values)
        with self.engine.begin() as connection:
            try:
                connection.execute(insert)
            except IntegrityError:
                raise ConflictingIdError(job.id)

    def update_job(self, job, lease_token=None, preserve_lease=False):
        values = {
            "next_run_time": datetime_to_utc_timestamp(job.next_run_time),
            "job_state": pickle.dumps(job.__getstate__(), self.pickle_protocol),
        }
        conditions = [self.jobs_t.c.id == job.id]
        conditional_update = False

        if self.supports_optimistic_jobs:
            expected_version = getattr(job, "_persistent_version", None)
            conditional_update = expected_version is not None
            if conditional_update:
                conditions.append(self.jobs_t.c.version == expected_version)
            if lease_token is not None:
                conditions.append(self.jobs_t.c.lease_owner == lease_token)

            values["version"] = self.jobs_t.c.version + 1
            if not preserve_lease:
                values["lease_expires"] = None
                values["lease_owner"] = None

        update = self.jobs_t.update().values(**values).where(and_(*conditions))
        with self.engine.begin() as connection:
            result = connection.execute(update)
            if result.rowcount == 0:
                self._raise_write_error(connection, job.id, conditional_update)

    def remove_job(self, job_id, expected_version=None, lease_token=None):
        conditions = [self.jobs_t.c.id == job_id]
        conditional_remove = (
            self.supports_optimistic_jobs and expected_version is not None
        )
        if expected_version is not None:
            conditions.append(self.jobs_t.c.version == expected_version)
        if lease_token is not None:
            conditions.append(self.jobs_t.c.lease_owner == lease_token)

        delete = self.jobs_t.delete().where(and_(*conditions))
        with self.engine.begin() as connection:
            result = connection.execute(delete)
            if result.rowcount == 0:
                self._raise_write_error(connection, job_id, conditional_remove)

    def remove_all_jobs(self):
        delete = self.jobs_t.delete()
        with self.engine.begin() as connection:
            connection.execute(delete)

    def shutdown(self):
        self.engine.dispose()

    def release_job_lease(self, job):
        expected_version = getattr(job, "_persistent_version", None)
        lease_token = getattr(job, "_lease_token", None)
        if expected_version is None or lease_token is None:
            raise JobConflictError(job.id, expected_version)

        conditions = [self.jobs_t.c.id == job.id]
        conditions.append(self.jobs_t.c.version == expected_version)
        conditions.append(self.jobs_t.c.lease_owner == lease_token)

        update = (
            self.jobs_t.update()
            .values(
                version=self.jobs_t.c.version + 1,
                lease_expires=None,
                lease_owner=None,
            )
            .where(and_(*conditions))
        )
        with self.engine.begin() as connection:
            result = connection.execute(update)
            if result.rowcount == 0:
                self._raise_write_error(
                    connection, job.id, expected_version is not None
                )

    def _reconstitute_job(
        self,
        job_state,
        version=None,
        lease_expires=None,
        lease_owner=None,
        current_lease_token=None,
    ):
        job_state = pickle.loads(job_state)
        job_state["jobstore"] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        job._persistent_version = version
        job._lease_expires_at = (
            utc_timestamp_to_datetime(lease_expires) if lease_expires else None
        )
        job._lease_token = lease_owner if lease_owner == current_lease_token else None
        return job

    def _reconstitute_job_row(self, row, current_lease_token=None):
        if hasattr(row, "version"):
            return self._reconstitute_job(
                row.job_state,
                row.version,
                row.lease_expires,
                row.lease_owner,
                current_lease_token,
            )
        return self._reconstitute_job(row.job_state)

    def _select_columns(self):
        columns = [self.jobs_t.c.job_state]
        if self.supports_optimistic_jobs:
            columns.extend(
                [
                    self.jobs_t.c.version,
                    self.jobs_t.c.lease_expires,
                    self.jobs_t.c.lease_owner,
                ]
            )
        return columns

    def _define_table(self, metadata, optimistic):
        columns = [
            Column("id", Unicode(191), primary_key=True),
            # 25 = precision that translates to an 8-byte float
            Column("next_run_time", Float(25), index=True),
            Column("job_state", LargeBinary, nullable=False),
        ]
        if optimistic:
            columns.extend(
                [
                    Column("version", Integer),
                    Column("lease_expires", Float(25)),
                    Column("lease_owner", Unicode(32)),
                ]
            )

        return Table(self.tablename, metadata, *columns, schema=self.tableschema)

    def _add_optimistic_columns(self, connection, missing_columns):
        column_types = {
            "version": self.jobs_t.c.version.type,
            "lease_expires": self.jobs_t.c.lease_expires.type,
            "lease_owner": self.jobs_t.c.lease_owner.type,
        }
        preparer = connection.dialect.identifier_preparer
        table_name = preparer.quote(self.jobs_t.name)
        if self.tableschema:
            table_name = f"{preparer.quote_schema(self.tableschema)}.{table_name}"

        for column_name in sorted(missing_columns):
            column_type = column_types[column_name].compile(dialect=connection.dialect)
            quoted_name = preparer.quote(column_name)
            if connection.dialect.name == "oracle":
                statement = (
                    f"ALTER TABLE {table_name} ADD ({quoted_name} {column_type})"
                )
            else:
                statement = (
                    f"ALTER TABLE {table_name} ADD COLUMN {quoted_name} {column_type}"
                )
            connection.exec_driver_sql(statement)

    def _raise_write_error(self, connection, job_id, conditional):
        if not conditional:
            raise JobLookupError(job_id)

        actual_version = connection.execute(
            select(self.jobs_t.c.version).where(self.jobs_t.c.id == job_id)
        ).scalar()
        if actual_version is None:
            raise JobLookupError(job_id)
        raise JobConflictError(job_id)

    def _claim_due_jobs(self, timestamp):
        jobs = []
        now = self._utc_timestamp()
        lease_expires = now + self.lease_duration
        lease_token = uuid.uuid4().hex
        available = or_(
            self.jobs_t.c.lease_expires.is_(None),
            self.jobs_t.c.lease_expires < now,
        )
        claim = (
            self.jobs_t.update()
            .where(
                and_(
                    self.jobs_t.c.next_run_time <= timestamp,
                    self.jobs_t.c.next_run_time.is_not(None),
                    available,
                )
            )
            .values(
                version=func.coalesce(self.jobs_t.c.version, 0) + 1,
                lease_expires=lease_expires,
                lease_owner=lease_token,
            )
        )
        selectable = (
            select(
                self.jobs_t.c.id,
                self.jobs_t.c.job_state,
                self.jobs_t.c.version,
                self.jobs_t.c.lease_expires,
                self.jobs_t.c.lease_owner,
            )
            .where(
                and_(self.jobs_t.c.lease_owner == lease_token)
            )
            .order_by(self.jobs_t.c.next_run_time)
        )
        failed_job_ids = set()

        with self.engine.begin() as connection:
            connection.execute(claim)
            for row in connection.execute(selectable):
                try:
                    jobs.append(self._reconstitute_job_row(row, lease_token))
                except BaseException:
                    self._logger.exception(
                        'Unable to restore job "%s" -- removing it', row.id
                    )
                    failed_job_ids.add(row.id)

            if failed_job_ids:
                connection.execute(
                    self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
                )

        return jobs

    def _utc_timestamp(self):
        return datetime_to_utc_timestamp(datetime.now(timezone.utc))

    def _get_jobs(self, *conditions):
        jobs = []
        columns = [self.jobs_t.c.id, *self._select_columns()]
        selectable = select(*columns).order_by(self.jobs_t.c.next_run_time)
        selectable = selectable.where(and_(*conditions)) if conditions else selectable
        failed_job_ids = set()
        with self.engine.begin() as connection:
            for row in connection.execute(selectable):
                try:
                    jobs.append(self._reconstitute_job_row(row))
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
