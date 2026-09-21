"""
Tests for the optional optimistic version/lease updates of persistent jobs, guarding
against lost updates when multiple scheduler instances share one SQLAlchemyJobStore.

The two stores used in each test are fully independent SQLAlchemy engines/connections
against the same database file; no process-level locking is involved anywhere.
"""

import pickle
from datetime import datetime

import pytest

sqlalchemy_module = pytest.importorskip("apscheduler.jobstores.sqlalchemy")

from sqlalchemy import (  # noqa: E402
    Column,
    Float,
    LargeBinary,
    MetaData,
    Table,
    Unicode,
    create_engine,
    inspect,
)

from apscheduler.jobstores.base import (  # noqa: E402
    ConcurrentModificationError,
    JobLookupError,
)
from apscheduler.util import datetime_to_utc_timestamp, localize  # noqa: E402


def dummy_job():
    pass


@pytest.fixture
def db_path(tmpdir):
    return str(tmpdir.join("apscheduler_concurrency.sqlite"))


def make_store(db_path, alias, versioning=True):
    store = sqlalchemy_module.SQLAlchemyJobStore(
        url=f"sqlite:///{db_path}", optimistic_versioning=versioning
    )
    store.start(None, alias)
    return store


@pytest.fixture
def store_a(db_path):
    store = make_store(db_path, "store_a")
    yield store
    store.shutdown()


@pytest.fixture
def store_b(db_path):
    store = make_store(db_path, "store_b")
    yield store
    store.shutdown()


@pytest.fixture
def add_job(create_job, timezone):
    def add(store, run_date=datetime(2020, 5, 4), paused=False):
        run_date = localize(run_date, timezone)
        job = create_job(
            func=dummy_job, trigger="date", trigger_args={"run_date": run_date}
        )
        job.next_run_time = None if paused else run_date
        if store:
            store.add_job(job)
        return job

    return add


def create_old_schema_table(db_path, job):
    """Creates a jobs table in the historical (pre-versioning) format."""
    engine = create_engine(f"sqlite:///{db_path}")
    jobs_t = Table(
        "apscheduler_jobs",
        MetaData(),
        Column("id", Unicode(191), primary_key=True),
        Column("next_run_time", Float(25), index=True),
        Column("job_state", LargeBinary, nullable=False),
    )
    jobs_t.create(engine)
    with engine.begin() as connection:
        connection.execute(
            jobs_t.insert().values(
                id=job.id,
                next_run_time=datetime_to_utc_timestamp(job.next_run_time),
                job_state=pickle.dumps(job.__getstate__()),
            )
        )

    engine.dispose()


class TestInterleavedCommits:
    """Two SQLite connections committing interleaved updates to the same job."""

    def test_stale_snapshot_conflicts(self, store_a, store_b, add_job, timezone):
        job = add_job(store_a)
        new_run_time = localize(datetime(2020, 6, 6), timezone)
        other_run_time = localize(datetime(2020, 7, 7), timezone)

        # Both "schedulers" read (claim) the same due job
        snapshot_a = store_a.lookup_job(job.id)
        snapshot_b = store_b.lookup_job(job.id)
        assert snapshot_a._version == snapshot_b._version == 0

        # B commits first -- this must succeed
        snapshot_b.next_run_time = new_run_time
        store_b.update_job(snapshot_b)
        assert snapshot_b._version == 1

        # A commits its stale snapshot -- this must conflict, not overwrite
        snapshot_a.next_run_time = other_run_time
        pytest.raises(ConcurrentModificationError, store_a.update_job, snapshot_a)

        # The stored state is still B's version
        stored = store_a.lookup_job(job.id)
        assert stored.next_run_time == new_run_time
        assert stored._version == 1

        # After re-reading, A's update goes through
        stored.next_run_time = other_run_time
        store_a.update_job(stored)
        assert store_b.lookup_job(job.id).next_run_time == other_run_time

    def test_stale_snapshot_does_not_unpause(self, store_a, store_b, add_job):
        job = add_job(store_a)
        snapshot_a = store_a.lookup_job(job.id)
        snapshot_b = store_b.lookup_job(job.id)

        # B pauses the job
        snapshot_b.next_run_time = None
        store_b.update_job(snapshot_b)

        # A's stale snapshot (still holding the old run time) must not unpause it
        pytest.raises(ConcurrentModificationError, store_a.update_job, snapshot_a)
        assert store_b.lookup_job(job.id).next_run_time is None

    def test_late_update_after_remove_does_not_persist(
        self, store_a, store_b, add_job, timezone
    ):
        job = add_job(store_a)
        snapshot_a = store_a.lookup_job(job.id)

        # B removes the job; A's late post-execution update must fail and
        # must not resurrect the row
        store_b.remove_job(job.id)
        snapshot_a.next_run_time = localize(datetime(2020, 6, 6), timezone)
        pytest.raises(JobLookupError, store_a.update_job, snapshot_a)
        assert store_a.lookup_job(job.id) is None
        assert store_b.get_all_jobs() == []

    def test_repeated_updates_from_same_snapshot(self, store_a, add_job, timezone):
        """A successfully updated job object carries its new version along."""
        job = add_job(store_a)
        snapshot = store_a.lookup_job(job.id)
        for day in (6, 7, 8):
            snapshot.next_run_time = localize(datetime(2020, 6, day), timezone)
            store_a.update_job(snapshot)

        assert snapshot._version == 3
        stored = store_a.lookup_job(job.id)
        assert stored.next_run_time == localize(datetime(2020, 6, 8), timezone)


class TestSchemaMigration:
    """Old tables and old pickled states must keep working across restarts."""

    def test_old_table_migrated_on_start(self, db_path, create_job, add_job, timezone):
        job = add_job(None)
        create_old_schema_table(db_path, job)

        store = make_store(db_path, "default")
        try:
            columns = inspect(store.engine).get_columns("apscheduler_jobs")
            assert "version" in [column["name"] for column in columns]

            # The old row is readable and carries the initial version
            loaded = store.lookup_job(job.id)
            assert loaded is not None
            assert loaded.next_run_time == job.next_run_time
            assert loaded._version == 0

            # ... and updatable under version control
            loaded.next_run_time = localize(datetime(2020, 6, 6), timezone)
            store.update_job(loaded)
            assert store.lookup_job(job.id)._version == 1
        finally:
            store.shutdown()

    def test_old_table_without_versioning_behaves_as_before(
        self, db_path, add_job, timezone
    ):
        job = add_job(None)
        create_old_schema_table(db_path, job)

        store = make_store(db_path, "default", versioning=False)
        try:
            # No version column is added in the default (single instance) mode
            columns = inspect(store.engine).get_columns("apscheduler_jobs")
            assert "version" not in [column["name"] for column in columns]

            loaded = store.lookup_job(job.id)
            assert loaded is not None
            loaded.next_run_time = localize(datetime(2020, 6, 6), timezone)
            store.update_job(loaded)
            assert store.lookup_job(job.id).next_run_time == loaded.next_run_time
        finally:
            store.shutdown()

    def test_versioned_table_opened_without_versioning(
        self, db_path, add_job, timezone
    ):
        """Rollback path: a versioned table stays usable with versioning off."""
        store = make_store(db_path, "default")
        job = add_job(store)
        store.shutdown()

        store = make_store(db_path, "default", versioning=False)
        try:
            loaded = store.lookup_job(job.id)
            assert loaded is not None
            loaded.next_run_time = localize(datetime(2020, 6, 6), timezone)
            store.update_job(loaded)
            assert store.lookup_job(job.id).next_run_time == loaded.next_run_time
        finally:
            store.shutdown()

    def test_old_pickled_state_loads_with_versioning(self, db_path, store_a, add_job):
        """Pickled job states written before this feature remain readable."""
        job = add_job(None)
        old_style_state = job.__getstate__()
        assert "_version" not in old_style_state

        store_a.add_job(job)
        # Simulate a row written by an old release (pickle without any version info)
        with store_a.engine.begin() as connection:
            connection.execute(
                store_a.jobs_t.update()
                .values(job_state=pickle.dumps(old_style_state))
                .where(store_a.jobs_t.c.id == job.id)
            )

        loaded = store_a.lookup_job(job.id)
        assert loaded is not None
        assert loaded._version == 0
