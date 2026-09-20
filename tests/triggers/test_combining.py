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


class TestOrTriggerIndependent:
    @pytest.fixture
    def start_date(self, timezone):
        return localize(datetime(2017, 5, 31, 12, 0, 0), timezone)

    @pytest.fixture
    def trigger(self, start_date, timezone):
        return OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_date, timezone=timezone),
                IntervalTrigger(minutes=7, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )

    @staticmethod
    def get_sequence(trigger, count, now, previous_fire_time=None):
        """
        Advance the trigger like the scheduler would, passing each produced fire time back
        as the previous fire time on the next call. The same trigger instance is reused
        for the whole sequence.
        """
        fire_times = []
        for _ in range(count):
            previous_fire_time = trigger.get_next_fire_time(previous_fire_time, now)
            if previous_fire_time is None:
                break
            fire_times.append(previous_fire_time)
            now = max(now, previous_fire_time)
        return fire_times

    def test_independent_defaults_off(self):
        assert OrTrigger([]).independent is False

    def test_interleaved_intervals(self, trigger, start_date):
        expected_offsets = [0, 5, 7, 10, 14, 15, 20, 21, 25, 28, 30, 35]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger, len(expected), start_date) == expected

    def test_colliding_fire_times_emitted_once(self, start_date, timezone):
        trigger = OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_date, timezone=timezone),
                IntervalTrigger(minutes=10, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )
        expected_offsets = [0, 5, 10, 15, 20, 25, 30]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger, len(expected), start_date) == expected

    def test_subtrigger_exhausted(self, start_date, timezone):
        trigger = OrTrigger(
            [
                IntervalTrigger(
                    minutes=5,
                    start_date=start_date,
                    end_date=start_date + timedelta(minutes=10),
                    timezone=timezone,
                ),
                IntervalTrigger(minutes=7, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )
        # The 5-minute trigger stops after 0:10 but the 7-minute one keeps its own phase
        expected_offsets = [0, 5, 7, 10, 14, 21, 28]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger, len(expected), start_date) == expected

    def test_all_subtriggers_exhausted(self, start_date, timezone):
        trigger = OrTrigger(
            [
                IntervalTrigger(
                    minutes=5,
                    start_date=start_date,
                    end_date=start_date + timedelta(minutes=5),
                    timezone=timezone,
                ),
                IntervalTrigger(
                    minutes=7,
                    start_date=start_date,
                    end_date=start_date + timedelta(minutes=7),
                    timezone=timezone,
                ),
            ],
            independent=True,
        )
        expected_offsets = [0, 5, 7]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger, len(expected) + 5, start_date) == expected

    def test_repeated_calls_do_not_consume_fire_times(self, trigger, start_date):
        first = trigger.get_next_fire_time(None, start_date)
        assert first == start_date
        # A repeated call with the same arguments must return the same fire time
        assert trigger.get_next_fire_time(None, start_date) == first

        second = trigger.get_next_fire_time(first, start_date)
        assert second == start_date + timedelta(minutes=5)
        # Repeating the previous call must not consume the next fire time either
        assert trigger.get_next_fire_time(first, start_date) == second
        assert trigger.get_next_fire_time(first, start_date) == second

    def test_missed_fire_times_not_skipped(self, trigger, start_date):
        """Simulate the scheduler being down for 16 minutes and then catching up."""
        first = trigger.get_next_fire_time(None, start_date)
        now = start_date + timedelta(minutes=16)
        fire_times = [first]
        while fire_times[-1] <= now:
            fire_times.append(trigger.get_next_fire_time(fire_times[-1], now))

        expected_offsets = [0, 5, 7, 10, 14, 15, 20]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert fire_times == expected

    def test_nested_or_trigger(self, start_date, timezone):
        inner = OrTrigger(
            [
                IntervalTrigger(minutes=5, start_date=start_date, timezone=timezone),
                IntervalTrigger(minutes=7, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )
        trigger = OrTrigger(
            [
                inner,
                IntervalTrigger(minutes=3, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )
        expected_offsets = [0, 3, 5, 6, 7, 9, 10, 12, 14, 15, 18, 20, 21]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger, len(expected), start_date) == expected

    def test_pickle_mid_sequence(self, trigger, start_date):
        first = trigger.get_next_fire_time(None, start_date)
        second = trigger.get_next_fire_time(first, start_date)

        trigger2 = pickle.loads(pickle.dumps(trigger, 2))
        assert trigger2.independent is True
        assert trigger2._next_fire_times == trigger._next_fire_times

        # The restored trigger must continue the sequence where it left off
        expected_offsets = [7, 10, 14, 15, 20]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert (
            self.get_sequence(
                trigger2, len(expected), second, previous_fire_time=second
            )
            == expected
        )

    def test_pickle_before_first_fire(self, trigger, start_date):
        trigger2 = pickle.loads(pickle.dumps(trigger, 2))
        expected_offsets = [0, 5, 7]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert self.get_sequence(trigger2, len(expected), start_date) == expected

    def test_pickle_nested(self, start_date, timezone):
        trigger = OrTrigger(
            [
                OrTrigger(
                    [
                        IntervalTrigger(
                            minutes=5, start_date=start_date, timezone=timezone
                        ),
                        IntervalTrigger(
                            minutes=7, start_date=start_date, timezone=timezone
                        ),
                    ],
                    independent=True,
                ),
                IntervalTrigger(minutes=3, start_date=start_date, timezone=timezone),
            ],
            independent=True,
        )
        previous = self.get_sequence(trigger, 3, start_date)[-1]
        trigger2 = pickle.loads(pickle.dumps(trigger, 2))
        expected_offsets = [6, 7, 9, 10]
        expected = [
            start_date + timedelta(minutes=offset) for offset in expected_offsets
        ]
        assert (
            self.get_sequence(
                trigger2, len(expected), previous, previous_fire_time=previous
            )
            == expected
        )

    def test_setstate_legacy_version_1(self, start_date, timezone):
        """States serialized before the independent option existed must still load."""
        triggers = [
            IntervalTrigger(minutes=5, start_date=start_date, timezone=timezone),
            IntervalTrigger(minutes=7, start_date=start_date, timezone=timezone),
        ]
        legacy_state = {
            "version": 1,
            "triggers": [
                (obj_to_ref(subtrigger.__class__), subtrigger.__getstate__())
                for subtrigger in triggers
            ],
            "jitter": None,
        }
        trigger = OrTrigger.__new__(OrTrigger)
        trigger.__setstate__(legacy_state)
        assert trigger.independent is False
        assert trigger._next_fire_times is None
        # The default (legacy) behavior passes the combined previous fire time onwards
        assert trigger.get_next_fire_time(None, start_date) == start_date

    def test_setstate_version_too_new(self, trigger):
        state = trigger.__getstate__()
        state["version"] = 3
        exc = pytest.raises(
            ValueError, OrTrigger.__new__(OrTrigger).__setstate__, state
        )
        assert "only versions up to 2 can be handled" in str(exc.value)

    def test_jitter(self, trigger, start_date):
        trigger.jitter = 5
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_date)
            assert timedelta() <= next_fire_time - start_date <= timedelta(seconds=5)

    def test_repr(self, trigger):
        assert repr(trigger).startswith("<OrTrigger([<IntervalTrigger")
        assert repr(trigger).endswith(", independent=True)>")

    def test_str(self, trigger):
        assert str(trigger) == "or[interval[0:05:00], interval[0:07:00]]"


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
