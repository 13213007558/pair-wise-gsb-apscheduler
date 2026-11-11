"""Regression tests for timezone-aware triggers across DST and process restarts.

These tests pin down the behaviour of :class:`CronTrigger`,
:class:`CalendarIntervalTrigger`, :class:`AndTrigger` and :class:`OrTrigger`
around daylight saving time (DST) transitions and after a state save/restore
cycle (the in-process equivalent of a scheduler restart).

The expected values below are derived by hand from the rules of the involved
time zones, never by calling the trigger under test. Every entry is expressed
as a *UTC* instant plus a ``fold`` flag (PEP 495). The tests only use fixed
dates in 2024, so they never depend on "today".

Time zone facts used (from the IANA tz database, Europe/Berlin)
---------------------------------------------------------------
* Spring forward on **2024-03-31 at 02:00 UTC+01:00**: the clock jumps to
  03:00 UTC+02:00. The local wall times 02:00:00-02:59:59 **do not exist**
  (a "gap"). Before the jump the offset is +01:00 (CET), afterwards it is
  +02:00 (CEST).
* Fall back on **2024-10-27 at 03:00 UTC+02:00**: the clock jumps back to
  02:00 UTC+01:00. The local wall times 02:00:00-02:59:59 therefore
  **occur twice** (a "fold"). The first pass is CEST (+02:00, ``fold=0``),
  the second pass is CET (+01:00, ``fold=1``).

Conventions under test
----------------------
* :class:`CronTrigger` works on wall-clock fields. A wall time inside the gap
  is never emitted (the sequence jumps from 01:30 +01:00 straight to
  03:00 +02:00), whereas every pass of a repeated wall time is emitted,
  distinguished by ``fold``. Two identical wall readings are therefore *not*
  the same instant.
* :class:`CalendarIntervalTrigger` keeps a fixed wall time of day. A day whose
  wall time lands in the gap is **discarded and the whole date is skipped**;
  on a repeated wall time only the **earlier** occurrence (``fold=0``) is
  used. Hence its skip rule differs from ``CronTrigger``.
"""

from __future__ import annotations

import copy
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from apscheduler import MaxIterationsReached
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.combining import AndTrigger, OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

BERLIN = ZoneInfo("Europe/Berlin")
NAIROBI = ZoneInfo("Africa/Nairobi")

# Each expectation is a (UTC wall reading, fold) pair. Comparing the converted
# UTC instant *and* the fold makes the two passes of a repeated hour distinct,
# while still rejecting phantom instants inside a gap.
ExpectedFire = tuple[datetime, int]


def _utc(
    year: int, month: int, day: int, hour: int, minute: int, fold: int = 0
) -> ExpectedFire:
    return datetime(year, month, day, hour, minute, tzinfo=UTC), fold


def _assert_matches(actual: datetime, expected: ExpectedFire) -> None:
    expected_utc, expected_fold = expected
    assert actual.astimezone(UTC) == expected_utc
    assert actual.fold == expected_fold


def _restore(trigger):
    """Simulate a process restart using only the public state hooks."""
    restored = trigger.__class__.__new__(trigger.__class__)
    restored.__setstate__(copy.deepcopy(trigger.__getstate__()))
    return restored


def _drain(trigger, count: int) -> list[datetime]:
    return [trigger.next() for _ in range(count)]


def _drain_until_none(trigger) -> list[datetime | None]:
    results: list[datetime | None] = []
    while True:
        fire_time = trigger.next()
        results.append(fire_time)
        if fire_time is None:
            return results


def _drain_fires(trigger) -> list[datetime]:
    """Drain until the trigger is exhausted, asserting every result is a fire."""
    results = _drain_until_none(trigger)
    assert results[-1] is None
    return results[:-1]  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# CronTrigger
# ---------------------------------------------------------------------------

# minute="*/30" starting just before the 2024-03-31 gap. The 02:00/02:30 wall
# readings do not exist, so after 01:30 +01:00 (00:30 UTC) the next fire is
# 03:00 +02:00 (01:00 UTC): a real one-hour hole in the UTC sequence.
CRON_SPRING_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 3, 31, 0, 0),  # local 01:00 +01:00
    _utc(2024, 3, 31, 0, 30),  # local 01:30 +01:00
    _utc(2024, 3, 31, 1, 0),  # local 03:00 +02:00 (02:00 local skipped)
    _utc(2024, 3, 31, 1, 30),  # local 03:30 +02:00
    _utc(2024, 3, 31, 2, 0),  # local 04:00 +02:00
    _utc(2024, 3, 31, 2, 30),  # local 04:30 +02:00
]


def _cron_spring():
    return CronTrigger(
        minute="*/30",
        start_time=datetime(2024, 3, 31, 1, 0, tzinfo=BERLIN),
        timezone=BERLIN,
    )


# minute="*/30" across the 2024-10-27 repeated hour. The 02:00/02:30 wall
# readings fire twice: first as CEST (+02:00, fold=0) then as CET (+01:00,
# fold=1). The UTC instants are one hour apart and must both be retained.
CRON_AUTUMN_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 10, 26, 23, 30),  # local 01:30 +02:00
    _utc(2024, 10, 27, 0, 0),  # local 02:00 +02:00, first pass
    _utc(2024, 10, 27, 0, 30),  # local 02:30 +02:00, first pass
    _utc(2024, 10, 27, 1, 0, fold=1),  # local 02:00 +01:00, second pass
    _utc(2024, 10, 27, 1, 30, fold=1),  # local 02:30 +01:00, second pass
    _utc(2024, 10, 27, 2, 0),  # local 03:00 +01:00
    _utc(2024, 10, 27, 2, 30),  # local 03:30 +01:00
    _utc(2024, 10, 27, 3, 0),  # local 04:00 +01:00
]


def _cron_autumn():
    return CronTrigger(
        minute="*/30",
        start_time=datetime(2024, 10, 27, 1, 30, tzinfo=BERLIN),
        timezone=BERLIN,
    )


@pytest.mark.parametrize(
    "factory, expected",
    [
        pytest.param(_cron_spring, CRON_SPRING_EXPECTED, id="spring_gap"),
        pytest.param(_cron_autumn, CRON_AUTUMN_EXPECTED, id="autumn_fold"),
    ],
)
def test_cron_utc_sequence(factory, expected) -> None:
    trigger = factory()
    for actual, want in zip(_drain(trigger, len(expected)), expected, strict=True):
        _assert_matches(actual, want)


@pytest.mark.parametrize(
    "factory, expected, checkpoints",
    [
        # Before the transition, right after the (non-existent) gap would have
        # been, and late in the captured window.
        pytest.param(_cron_spring, CRON_SPRING_EXPECTED, (0, 2, 4), id="spring_gap"),
        # Before the transition, immediately after the first *repeated* instant
        # (checkpoint 4 = the 02:00 fold=1 fire has just been emitted), and
        # near the end of the window.
        pytest.param(_cron_autumn, CRON_AUTUMN_EXPECTED, (0, 4, 6), id="autumn_fold"),
    ],
)
def test_cron_state_restore_matches_uninterrupted(
    factory, expected, checkpoints
) -> None:
    reference = _drain(factory(), len(expected))

    for checkpoint in checkpoints:
        trigger = factory()
        for _ in range(checkpoint):
            trigger.next()

        trigger = _restore(trigger)

        resumed = _drain(trigger, len(expected) - checkpoint)
        for actual, reference_time in zip(resumed, reference[checkpoint:], strict=True):
            assert actual == reference_time
            assert actual.fold == reference_time.fold


def test_cron_state_restore_through_serializer(serializer) -> None:
    expected = CRON_AUTUMN_EXPECTED
    reference = _drain(_cron_autumn(), len(expected))

    # Restart after the first pass of the repeated hour.
    trigger = _cron_autumn()
    trigger.next()
    trigger.next()
    trigger.next()
    trigger = serializer.deserialize(serializer.serialize(trigger))

    resumed = _drain(trigger, len(expected) - 3)
    for actual, reference_time in zip(resumed, reference[3:], strict=True):
        assert actual == reference_time
        assert actual.fold == reference_time.fold


# ---------------------------------------------------------------------------
# CalendarIntervalTrigger
# ---------------------------------------------------------------------------

# Daily at 02:30 local across the 2024-03-31 gap. 2024-03-31 is discarded
# because 02:30 does not exist that day, so the sequence jumps from
# 2024-03-30 (01:30 UTC) straight to 2024-04-01 (00:30 UTC).
CAL_SPRING_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 3, 29, 1, 30),  # local Fri 02:30 +01:00
    _utc(2024, 3, 30, 1, 30),  # local Sat 02:30 +01:00
    _utc(2024, 4, 1, 0, 30),  # local Mon 02:30 +02:00 (Sun skipped)
    _utc(2024, 4, 2, 0, 30),  # local Tue 02:30 +02:00
]


def _cal_spring():
    return CalendarIntervalTrigger(
        days=1,
        hour=2,
        minute=30,
        start_date=date(2024, 3, 29),
        end_date=date(2024, 4, 2),
        timezone=BERLIN,
    )


# Daily at 02:30 local across the 2024-10-27 fold. Only the earlier (CEST,
# fold=0) occurrence is used; the trigger never emits the 01:30 UTC reading.
CAL_AUTUMN_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 10, 26, 0, 30),  # local Sat 02:30 +02:00
    _utc(2024, 10, 27, 0, 30),  # local Sun 02:30 +02:00, earlier occurrence
    _utc(2024, 10, 28, 1, 30),  # local Mon 02:30 +01:00
]


def _cal_autumn():
    return CalendarIntervalTrigger(
        days=1,
        hour=2,
        minute=30,
        start_date=date(2024, 10, 26),
        end_date=date(2024, 10, 28),
        timezone=BERLIN,
    )


@pytest.mark.parametrize(
    "factory, expected",
    [
        pytest.param(_cal_spring, CAL_SPRING_EXPECTED, id="spring_gap_skips_day"),
        pytest.param(_cal_autumn, CAL_AUTUMN_EXPECTED, id="autumn_earlier_occurrence"),
    ],
)
def test_calendar_interval_utc_sequence(factory, expected) -> None:
    for actual, want in zip(_drain_fires(factory()), expected, strict=True):
        _assert_matches(actual, want)


def test_calendar_interval_exhausts_at_end_date() -> None:
    # The final next() must return None exactly once end_date is passed, and
    # that terminating state must survive a restart.
    trigger = _cal_autumn()
    assert len(_drain_until_none(trigger)) == len(CAL_AUTUMN_EXPECTED) + 1

    trigger = _cal_autumn()
    _drain(trigger, len(CAL_AUTUMN_EXPECTED))
    trigger = _restore(trigger)
    assert trigger.next() is None


@pytest.mark.parametrize(
    "factory, expected, checkpoints",
    [
        # Before the gap, after the skipped gap day, and just before end_date.
        pytest.param(_cal_spring, CAL_SPRING_EXPECTED, (0, 2, 3), id="spring_gap"),
        # Before the fold, after the earlier repeated occurrence, and at the
        # last valid date (rule about to be exhausted).
        pytest.param(_cal_autumn, CAL_AUTUMN_EXPECTED, (0, 1, 2), id="autumn_fold"),
    ],
)
def test_calendar_interval_restore_matches_uninterrupted(
    factory, expected, checkpoints
) -> None:
    reference = _drain_until_none(factory())

    for checkpoint in checkpoints:
        trigger = factory()
        for _ in range(checkpoint):
            trigger.next()

        trigger = _restore(trigger)

        resumed = _drain_until_none(trigger)
        assert len(resumed) == len(reference) - checkpoint
        for actual, reference_time in zip(resumed, reference[checkpoint:], strict=True):
            if reference_time is None:
                assert actual is None
            else:
                assert actual == reference_time
                assert actual.fold == reference_time.fold


# ---------------------------------------------------------------------------
# AndTrigger / OrTrigger combinations
# ---------------------------------------------------------------------------

# OrTrigger over two identical */30 crons. Simultaneous instants from the two
# triggers collapse into one fire, but the two passes of the repeated hour are
# different UTC instants and are both kept.
OR_FOLD_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 10, 27, 0, 0),  # local 02:00 +02:00, first pass
    _utc(2024, 10, 27, 0, 30),  # local 02:30 +02:00, first pass
    _utc(2024, 10, 27, 1, 0, fold=1),  # local 02:00 +01:00, second pass
    _utc(2024, 10, 27, 1, 30, fold=1),  # local 02:30 +01:00, second pass
    _utc(2024, 10, 27, 2, 0),  # local 03:00 +01:00
]


def _or_fold():
    inner = CronTrigger(
        minute="*/30",
        start_time=datetime(2024, 10, 27, 2, 0, tzinfo=BERLIN),
        timezone=BERLIN,
    )
    return OrTrigger([copy.deepcopy(inner), copy.deepcopy(inner)])


def test_or_trigger_dedups_but_keeps_both_folds() -> None:
    trigger = _or_fold()
    for actual, want in zip(
        _drain(trigger, len(OR_FOLD_EXPECTED)), OR_FOLD_EXPECTED, strict=True
    ):
        _assert_matches(actual, want)


def test_or_trigger_restore_matches_uninterrupted() -> None:
    reference = _drain(_or_fold(), len(OR_FOLD_EXPECTED))

    # Restart right after the first pass of the repeated hour (index 1).
    trigger = _or_fold()
    trigger.next()
    trigger.next()
    trigger = _restore(trigger)

    resumed = _drain(trigger, len(OR_FOLD_EXPECTED) - 2)
    for actual, reference_time in zip(resumed, reference[2:], strict=True):
        assert actual == reference_time
        assert actual.fold == reference_time.fold


# AndTrigger over two identical hourly crons (threshold 1s). They always agree
# on the same instant, so the combined trigger fires hourly but still honours
# the gap: 01:00 +01:00 is followed by 03:00 +02:00, never 02:00.
AND_GAP_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 3, 30, 23, 0),  # local 00:00 +01:00
    _utc(2024, 3, 31, 0, 0),  # local 01:00 +01:00
    _utc(2024, 3, 31, 1, 0),  # local 03:00 +02:00 (02:00 local skipped)
    _utc(2024, 3, 31, 2, 0),  # local 04:00 +02:00
    _utc(2024, 3, 31, 3, 0),  # local 05:00 +02:00
]


def _and_gap():
    inner = CronTrigger(
        minute=0,
        start_time=datetime(2024, 3, 31, 0, 0, tzinfo=BERLIN),
        timezone=BERLIN,
    )
    return AndTrigger(
        [copy.deepcopy(inner), copy.deepcopy(inner)], threshold=timedelta(seconds=1)
    )


def test_and_trigger_advances_through_gap() -> None:
    trigger = _and_gap()
    for actual, want in zip(
        _drain(trigger, len(AND_GAP_EXPECTED)), AND_GAP_EXPECTED, strict=True
    ):
        _assert_matches(actual, want)


def test_and_trigger_restore_matches_uninterrupted() -> None:
    reference = _drain(_and_gap(), len(AND_GAP_EXPECTED))

    # Restart after the last pre-gap fire (index 1).
    trigger = _and_gap()
    trigger.next()
    trigger.next()
    trigger = _restore(trigger)

    resumed = _drain(trigger, len(AND_GAP_EXPECTED) - 2)
    for actual, reference_time in zip(resumed, reference[2:], strict=True):
        assert actual == reference_time
        assert actual.fold == reference_time.fold


def test_and_trigger_iteration_cap() -> None:
    # Two 4-second interval triggers permanently offset by 2 seconds can never
    # agree within the threshold; AndTrigger must give up at max_iterations
    # instead of looping forever.
    start = datetime(2024, 1, 1, tzinfo=BERLIN)
    trigger = AndTrigger(
        [
            IntervalTrigger(seconds=4, start_time=start),
            IntervalTrigger(seconds=4, start_time=start + timedelta(seconds=2)),
        ],
        max_iterations=50,
    )
    with pytest.raises(MaxIterationsReached):
        trigger.next()


# ---------------------------------------------------------------------------
# No-DST controls: a fixed-offset zone and UTC must show uniform spacing.
# ---------------------------------------------------------------------------

# Africa/Nairobi is a fixed +03:00 zone all year. The same */30 schedule that
# hits a gap/fold in Berlin must here produce perfectly uniform 30-minute UTC
# spacing with fold always 0.
NAIROBI_CRON_EXPECTED: list[ExpectedFire] = [
    _utc(2024, 3, 30, 22, 0),
    _utc(2024, 3, 30, 22, 30),
    _utc(2024, 3, 30, 23, 0),
    _utc(2024, 3, 30, 23, 30),
    _utc(2024, 3, 31, 0, 0),
]


def test_cron_no_dst_zone_uniform() -> None:
    trigger = CronTrigger(
        minute="*/30",
        start_time=datetime(2024, 3, 31, 1, 0, tzinfo=NAIROBI),
        timezone=NAIROBI,
    )
    fires = _drain(trigger, len(NAIROBI_CRON_EXPECTED))
    for actual, want in zip(fires, NAIROBI_CRON_EXPECTED, strict=True):
        _assert_matches(actual, want)

    # No gap and no fold: strictly 30 minutes between every UTC instant.
    instants = [fire.astimezone(UTC) for fire in fires]
    deltas = {instants[i] - instants[i - 1] for i in range(1, len(instants))}
    assert deltas == {timedelta(minutes=30)}
    assert all(fire.fold == 0 for fire in fires)
    assert all(fire.utcoffset() == timedelta(hours=3) for fire in fires)


def test_calendar_interval_no_dst_and_utc_controls() -> None:
    # Fixed-offset zone: every calendar date fires, nothing skipped.
    nairobi = CalendarIntervalTrigger(
        days=1,
        hour=2,
        minute=30,
        start_date=date(2024, 3, 30),
        end_date=date(2024, 4, 1),
        timezone=NAIROBI,
    )
    nairobi_want = [
        _utc(2024, 3, 29, 23, 30),
        _utc(2024, 3, 30, 23, 30),
        _utc(2024, 3, 31, 23, 30),
    ]
    for actual, want in zip(_drain_fires(nairobi), nairobi_want, strict=True):
        _assert_matches(actual, want)

    # UTC control across the Berlin fall-back date: offset is always zero and
    # the wall time is unchanged.
    utc_trigger = CalendarIntervalTrigger(
        days=1,
        hour=2,
        minute=30,
        start_date=date(2024, 10, 26),
        end_date=date(2024, 10, 28),
        timezone=UTC,
    )
    utc_want = [
        _utc(2024, 10, 26, 2, 30),
        _utc(2024, 10, 27, 2, 30),
        _utc(2024, 10, 28, 2, 30),
    ]
    utc_fires = _drain_fires(utc_trigger)
    for actual, want in zip(utc_fires, utc_want, strict=True):
        _assert_matches(actual, want)
        assert actual.utcoffset() == timedelta(0)
