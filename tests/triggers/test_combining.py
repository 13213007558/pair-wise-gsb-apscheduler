import pickle
from datetime import datetime, timedelta

import pytest

from apscheduler.triggers.combining import AndTrigger, BaseCombiningTrigger, OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import localize, obj_to_ref


class TestAndTrigger:
    @pytest.fixture
    def trigger(self, timezone):
        return AndTrigger(
            [
                CronTrigger(
                    month="5-8",
                    day="6-15",
                    end_date=localize(datetime(2017, 8, 10), timezone),
                ),
                CronTrigger(
                    month="6-9",
                    day="*/3",
                    end_date=localize(datetime(2017, 9, 7), timezone),
                ),
            ]
        )

    @pytest.mark.parametrize(
        "start_time, expected",
        [
            (datetime(2017, 8, 6), datetime(2017, 8, 7)),
            (datetime(2017, 8, 10, 1), None),
        ],
        ids=["firstmatch", "end"],
    )
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = localize(expected, timezone) if expected else None
        assert (
            trigger.get_next_fire_time(None, localize(start_time, timezone)) == expected
        )

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = localize(datetime(2017, 8, 6), timezone)
        expected = localize(datetime(2017, 8, 7), timezone)
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = f", jitter={jitter}" if jitter else ""
        assert repr(trigger) == (
            "<AndTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            f"timezone='Europe/Berlin')>]{jitter_part})>"
        )

    def test_str(self, trigger):
        assert (
            str(trigger)
            == "and[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"
        )

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))


class TestOrTriggerIndependentCursors:
    @pytest.fixture
    def start_time(self, timezone):
        return localize(datetime(2017, 8, 6), timezone)

    @pytest.fixture
    def trigger(self, timezone, start_time):
        return OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone),
                IntervalTrigger(minutes=7, start_date=start_time, timezone=timezone),
            ],
            independent_cursors=True,
        )

    @staticmethod
    def get_fire_times(trigger, now, count, previous_fire_time=None):
        """Emulates how the scheduler drives the trigger, using the same instance."""
        fire_times = []
        for _ in range(count):
            next_fire_time = trigger.get_next_fire_time(previous_fire_time, now)
            if next_fire_time is None:
                break

            fire_times.append(next_fire_time)
            previous_fire_time = next_fire_time

        return fire_times

    @staticmethod
    def expected_times(start_time, offsets):
        return [start_time + timedelta(minutes=offset) for offset in offsets]

    def test_interleaved_intervals(self, trigger, start_time):
        """Each interval trigger keeps its own phase when combined."""
        fire_times = self.get_fire_times(trigger, start_time, 12)
        assert fire_times == self.expected_times(
            start_time, [0, 5, 7, 10, 14, 15, 20, 21, 25, 28, 30, 35]
        )

    def test_colliding_fire_times(self, timezone, start_time):
        """Fire times produced by several triggers at the same instant are emitted once."""
        trigger = OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone),
                IntervalTrigger(minutes=10, start_date=start_time, timezone=timezone),
            ],
            independent_cursors=True,
        )
        fire_times = self.get_fire_times(trigger, start_time, 7)
        assert fire_times == self.expected_times(start_time, [0, 5, 10, 15, 20, 25, 30])

    def test_sub_trigger_exhausted(self, timezone, start_time):
        """When one trigger finishes, the other one keeps its schedule."""
        trigger = OrTrigger(
            [
                IntervalTrigger(
                    minutes=5,
                    start_date=start_time,
                    end_date=start_time + timedelta(minutes=20),
                    timezone=timezone,
                ),
                IntervalTrigger(minutes=7, start_date=start_time, timezone=timezone),
            ],
            independent_cursors=True,
        )
        fire_times = self.get_fire_times(trigger, start_time, 10)
        assert fire_times == self.expected_times(
            start_time, [0, 5, 7, 10, 14, 15, 20, 21, 28, 35]
        )

    def test_all_sub_triggers_exhausted(self, timezone, start_time):
        end_date = start_time + timedelta(minutes=10)
        trigger = OrTrigger(
            [
                IntervalTrigger(
                    minutes=5,
                    start_date=start_time,
                    end_date=end_date,
                    timezone=timezone,
                ),
                IntervalTrigger(
                    minutes=7,
                    start_date=start_time,
                    end_date=end_date,
                    timezone=timezone,
                ),
            ],
            independent_cursors=True,
        )
        fire_times = self.get_fire_times(trigger, start_time, 10)
        assert fire_times == self.expected_times(start_time, [0, 5, 7, 10])

    def test_repeated_calls_same_arguments(self, trigger, start_time):
        """Repeating a call with the same arguments must not consume candidates."""
        first = trigger.get_next_fire_time(None, start_time)
        assert first == start_time
        for _ in range(10):
            assert trigger.get_next_fire_time(None, start_time) == first

        second = trigger.get_next_fire_time(first, start_time)
        assert second == start_time + timedelta(minutes=5)
        for _ in range(10):
            assert trigger.get_next_fire_time(first, start_time) == second

        # A call with the same arguments as an already confirmed call must not
        # skip ahead either
        third = trigger.get_next_fire_time(second, start_time)
        assert third == start_time + timedelta(minutes=7)
        assert trigger.get_next_fire_time(first, start_time) == third

    def test_missed_fire_times_not_lost(self, trigger, start_time):
        """Catch-up after downtime emits every missed fire time, in order."""
        first = trigger.get_next_fire_time(None, start_time)
        assert first == start_time

        # Simulate the scheduler being unable to run the job for 36 minutes
        now = start_time + timedelta(minutes=36)
        fire_times = [first] + self.get_fire_times(trigger, now, 11, first)
        assert fire_times == self.expected_times(
            start_time, [0, 5, 7, 10, 14, 15, 20, 21, 25, 28, 30, 35]
        )

    def test_pickle_mid_sequence(self, trigger, start_time):
        """A trigger restored from a pickle continues exactly where it left off."""
        reference = self.get_fire_times(trigger, start_time, 3)

        restored = pickle.loads(pickle.dumps(trigger))
        assert restored.independent_cursors is True

        previous_fire_time = reference[-1]
        for expected_offset in (10, 14, 15, 20, 21, 25, 28, 30):
            expected = start_time + timedelta(minutes=expected_offset)
            next_fire_time = restored.get_next_fire_time(previous_fire_time, start_time)
            assert next_fire_time == expected
            assert (
                trigger.get_next_fire_time(previous_fire_time, start_time) == expected
            )
            previous_fire_time = next_fire_time

    def test_pickle_preserves_unemitted_candidates(self, trigger, start_time):
        """Candidates computed but not yet emitted survive serialization."""
        first = trigger.get_next_fire_time(None, start_time)
        second = trigger.get_next_fire_time(first, start_time)
        state = trigger.__getstate__()
        assert state["version"] == 2
        assert state["independent_cursors"] is True
        # The next candidates of both interval triggers are pending; the 7 minute one
        # has not been emitted yet and must not be lost
        assert sorted(state["pendings"]) == self.expected_times(start_time, [5, 7])

        restored = OrTrigger.__new__(OrTrigger)
        restored.__setstate__(state)
        assert restored.get_next_fire_time(second, start_time) == (
            start_time + timedelta(minutes=7)
        )

    def test_nested_or_trigger(self, timezone, start_time):
        inner = OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone),
                IntervalTrigger(minutes=7, start_date=start_time, timezone=timezone),
            ],
            independent_cursors=True,
        )
        outer = OrTrigger(
            [
                inner,
                IntervalTrigger(minutes=11, start_date=start_time, timezone=timezone),
            ],
            independent_cursors=True,
        )
        fire_times = self.get_fire_times(outer, start_time, 10)
        assert fire_times == self.expected_times(
            start_time, [0, 5, 7, 10, 11, 14, 15, 20, 21, 22]
        )

    def test_jitter(self, timezone, start_time):
        """Jittered values are stable across repeated calls and still confirmable."""
        trigger = OrTrigger(
            [IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone)],
            independent_cursors=True,
            jitter=30,
        )
        first = trigger.get_next_fire_time(None, start_time)
        assert start_time <= first <= start_time + timedelta(seconds=30)
        for _ in range(100):
            assert trigger.get_next_fire_time(None, start_time) == first

        second = trigger.get_next_fire_time(first, start_time)
        assert (
            start_time + timedelta(minutes=5)
            <= second
            <= (start_time + timedelta(minutes=5, seconds=30))
        )
        for _ in range(100):
            assert trigger.get_next_fire_time(first, start_time) == second

    def test_legacy_state_compatible(self, timezone, start_time):
        """State serialized by the previous version (without cursor data) still loads."""
        triggers = [
            IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone),
            IntervalTrigger(minutes=7, start_date=start_time, timezone=timezone),
        ]
        legacy_state = {
            "version": 1,
            "triggers": [
                (obj_to_ref(trigger.__class__), trigger.__getstate__())
                for trigger in triggers
            ],
            "jitter": None,
        }
        trigger = OrTrigger.__new__(OrTrigger)
        trigger.__setstate__(legacy_state)
        assert trigger.independent_cursors is False
        # Legacy mode passes the combined previous fire time to every sub-trigger
        assert trigger.get_next_fire_time(None, start_time) == start_time
        assert trigger.get_next_fire_time(start_time, start_time) == (
            start_time + timedelta(minutes=5)
        )

    def test_default_mode_is_legacy(self, trigger, timezone, start_time):
        legacy = OrTrigger(
            [IntervalTrigger(minutes=5, start_date=start_time, timezone=timezone)]
        )
        assert legacy.independent_cursors is False
        state = legacy.__getstate__()
        assert state["version"] == 1
        assert "pendings" not in state

    def test_repr(self, trigger):
        assert repr(trigger).endswith(", independent_cursors=True)>")


class TestOrTrigger:
    @pytest.fixture
    def trigger(self, timezone):
        return OrTrigger(
            [
                CronTrigger(
                    month="5-8",
                    day="6-15",
                    end_date=localize(datetime(2017, 8, 10), timezone),
                ),
                CronTrigger(
                    month="6-9",
                    day="*/3",
                    end_date=localize(datetime(2017, 9, 7), timezone),
                ),
            ]
        )

    @pytest.mark.parametrize(
        "start_time, expected",
        [(datetime(2017, 8, 6), datetime(2017, 8, 6)), (datetime(2017, 9, 7, 1), None)],
        ids=["earliest", "end"],
    )
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = localize(expected, timezone) if expected else None
        assert (
            trigger.get_next_fire_time(None, localize(start_time, timezone)) == expected
        )

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = expected = localize(datetime(2017, 8, 6), timezone)
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = f", jitter={jitter}" if jitter else ""
        assert repr(trigger) == (
            "<OrTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            f"timezone='Europe/Berlin')>]{jitter_part})>"
        )

    def test_str(self, trigger):
        assert (
            str(trigger)
            == "or[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"
        )

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))
