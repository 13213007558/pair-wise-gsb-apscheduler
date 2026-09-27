from datetime import datetime, timedelta

from tzlocal import get_localzone

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron.fields import (
    DEFAULT_VALUES,
    BaseField,
    DayOfMonthField,
    DayOfWeekField,
    MonthField,
    WeekField,
)
from apscheduler.util import (
    astimezone,
    convert_to_datetime,
    datetime_ceil,
    datetime_repr,
    timezone_repr,
)


class CronTrigger(BaseTrigger):
    """
    Triggers when current time matches all specified time constraints,
    similarly to how the UNIX cron scheduler works.

    :param int|str year: 4-digit year
    :param int|str month: month (1-12)
    :param int|str day: day of month (1-31)
    :param int|str week: ISO week (1-53)
    :param int|str day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
    :param int|str hour: hour (0-23)
    :param int|str minute: minute (0-59)
    :param int|str second: second (0-59)
    :param datetime|str start_date: earliest possible date/time to trigger on (inclusive)
    :param datetime|str end_date: latest possible date/time to trigger on (inclusive)
    :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations (defaults
        to scheduler timezone)
    :param int|None jitter: delay the job execution by ``jitter`` seconds at most
    :param str dst_policy: controls how wall-clock times that are skipped (``gap``) or repeated
        (``fold``) by daylight saving time transitions are handled. ``skip`` skips the nominal
        time, ``first`` uses the earlier concrete time, ``next_valid`` uses the later concrete
        time and ``both`` returns both concrete times. The default is ``first``.

    .. note:: The first weekday is always **monday**.
    """

    FIELD_NAMES = (
        "year",
        "month",
        "day",
        "week",
        "day_of_week",
        "hour",
        "minute",
        "second",
    )
    FIELDS_MAP = {
        "year": BaseField,
        "month": MonthField,
        "week": WeekField,
        "day": DayOfMonthField,
        "day_of_week": DayOfWeekField,
        "hour": BaseField,
        "minute": BaseField,
        "second": BaseField,
    }

    __slots__ = (
        "timezone",
        "start_date",
        "end_date",
        "fields",
        "jitter",
        "dst_policy",
    )

    DST_POLICIES = ("skip", "first", "both", "next_valid")

    def __init__(
        self,
        year=None,
        month=None,
        day=None,
        week=None,
        day_of_week=None,
        hour=None,
        minute=None,
        second=None,
        start_date=None,
        end_date=None,
        timezone=None,
        jitter=None,
        dst_policy="first",
    ):
        if dst_policy not in self.DST_POLICIES:
            raise ValueError(
                "dst_policy must be one of: " + ", ".join(self.DST_POLICIES)
            )

        if timezone:
            self.timezone = astimezone(timezone)
        elif isinstance(start_date, datetime) and start_date.tzinfo:
            self.timezone = astimezone(start_date.tzinfo)
        elif isinstance(end_date, datetime) and end_date.tzinfo:
            self.timezone = astimezone(end_date.tzinfo)
        else:
            self.timezone = get_localzone()

        self.start_date = self._convert_datetime(
            convert_to_datetime(start_date, self.timezone, "start_date")
        )
        self.end_date = self._convert_datetime(
            convert_to_datetime(end_date, self.timezone, "end_date")
        )
        if (
            self.start_date
            and self.end_date
            and self.end_date.timestamp() < self.start_date.timestamp()
        ):
            raise ValueError("end_date cannot be earlier than start_date")

        self.jitter = jitter
        self.dst_policy = dst_policy

        values = dict(
            (key, value)
            for (key, value) in locals().items()
            if key in self.FIELD_NAMES and value is not None
        )
        self.fields = []
        assign_defaults = False
        for field_name in self.FIELD_NAMES:
            if field_name in values:
                exprs = values.pop(field_name)
                is_default = False
                assign_defaults = not values
            elif assign_defaults:
                exprs = DEFAULT_VALUES[field_name]
                is_default = True
            else:
                exprs = "*"
                is_default = True

            field_class = self.FIELDS_MAP[field_name]
            field = field_class(field_name, exprs, is_default)
            self.fields.append(field)

    @classmethod
    def from_crontab(cls, expr, timezone=None, dst_policy="first"):
        """
        Create a :class:`~CronTrigger` from a standard crontab expression.

        See https://en.wikipedia.org/wiki/Cron for more information on the format accepted here.

        :param expr: minute, hour, day of month, month, day of week
        :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations
            (defaults to scheduler timezone)
        :param str dst_policy: controls how daylight saving time gaps and folds are handled
        :return: a :class:`~CronTrigger` instance

        """
        values = expr.split()
        if len(values) != 5:
            raise ValueError(f"Wrong number of fields; got {len(values)}, expected 5")

        return cls(
            minute=values[0],
            hour=values[1],
            day=values[2],
            month=values[3],
            day_of_week=values[4],
            timezone=timezone,
            dst_policy=dst_policy,
        )

    def _convert_datetime(self, value):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=self.timezone, fold=0)
        return value.astimezone(self.timezone)

    def _increment_field_value(self, dateval, fieldnum):
        """
        Increments the designated field and resets all less significant fields to their minimum
        values.

        :type dateval: datetime
        :type fieldnum: int
        :return: a tuple containing the new date, and the number of the field that was actually
            incremented
        :rtype: tuple
        """

        values = {}
        i = 0
        while i < len(self.fields):
            field = self.fields[i]
            if not field.REAL:
                if i == fieldnum:
                    fieldnum -= 1
                    i -= 1
                else:
                    i += 1
                continue

            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
                i += 1
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
                i += 1
            else:
                value = field.get_value(dateval)
                maxval = field.get_max(dateval)
                if value == maxval:
                    fieldnum -= 1
                    i -= 1
                else:
                    values[field.name] = value + 1
                    i += 1

        difference = datetime(**values) - dateval.replace(tzinfo=None)
        timestamp = dateval.timestamp() + difference.total_seconds()
        dateval = datetime.fromtimestamp(timestamp, self.timezone)
        wall_dateval = datetime(**values, tzinfo=self.timezone, fold=dateval.fold)
        target_dateval = datetime(**values)
        if fieldnum >= 0 and dateval.replace(tzinfo=None) < target_dateval:
            dateval = wall_dateval
        return dateval, fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        values = {}
        for i, field in enumerate(self.fields):
            if field.REAL:
                if i < fieldnum:
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        return datetime(**values, tzinfo=self.timezone, fold=dateval.fold)

    def _calculate_next_wall_time(self, start_date):
        if start_date.tzinfo is None:
            next_date = datetime_ceil(start_date).replace(
                tzinfo=self.timezone, fold=0
            )
        else:
            next_date = datetime_ceil(start_date.astimezone(self.timezone))
        fieldnum = 0
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            curr_value = field.get_value(next_date)
            next_value = field.get_next_value(next_date)

            if next_value is None:
                # No valid value was found
                next_date, fieldnum = self._increment_field_value(
                    next_date, fieldnum - 1
                )
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    next_date = self._set_field_value(next_date, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_date, fieldnum = self._increment_field_value(
                        next_date, fieldnum
                    )
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

            if (
                self.end_date
                and next_date.timestamp() > self.end_date.timestamp()
            ):
                return None

        if fieldnum < 0:
            return None

        return next_date

    def _resolve_dst(self, dateval):
        earliest_raw = dateval.replace(fold=0)
        latest_raw = dateval.replace(fold=1)
        earliest = datetime.fromtimestamp(earliest_raw.timestamp(), self.timezone)
        latest = datetime.fromtimestamp(latest_raw.timestamp(), self.timezone)

        if earliest.timestamp() == latest.timestamp():
            return [(earliest, False)], earliest

        wall_time = dateval.replace(tzinfo=None)
        is_gap = (
            earliest.replace(tzinfo=None) != wall_time
            or latest.replace(tzinfo=None) != wall_time
        )
        concrete_times = sorted((earliest, latest), key=lambda dt: dt.timestamp())
        concrete_times = [(fire_time, is_gap) for fire_time in concrete_times]

        boundary = max(concrete_times, key=lambda value: value[0].timestamp())
        if self.dst_policy == "skip":
            return [], boundary[0]
        elif self.dst_policy == "first":
            return [concrete_times[0]], boundary[0]
        elif self.dst_policy == "next_valid":
            return [concrete_times[-1]], boundary[0]
        else:
            return concrete_times, boundary[0]

    def _get_fold_partner(self, previous_fire_time, start_date):
        if self.dst_policy != "both":
            return None

        raw_date = datetime(
            previous_fire_time.year,
            previous_fire_time.month,
            previous_fire_time.day,
            previous_fire_time.hour,
            previous_fire_time.minute,
            previous_fire_time.second,
            previous_fire_time.microsecond,
            tzinfo=self.timezone,
            fold=0,
        )
        candidates, _ = self._resolve_dst(raw_date)
        if len(candidates) != 2:
            return None

        wall_time = raw_date.replace(tzinfo=None)
        if any(
            fire_time.replace(tzinfo=None) != wall_time for fire_time, _ in candidates
        ):
            return None

        previous_timestamp = previous_fire_time.timestamp()
        try:
            partner = next(
                fire_time
                for fire_time, _ in candidates
                if fire_time.timestamp() != previous_timestamp
            )
        except StopIteration:
            return None

        if partner.timestamp() < start_date.timestamp():
            return None
        if self.end_date and partner.timestamp() > self.end_date.timestamp():
            return None
        return partner

    def get_next_fire_time(self, previous_fire_time, now):
        previous_fire_time = self._convert_datetime(previous_fire_time)
        now = self._convert_datetime(now)
        if previous_fire_time:
            start_date = min(now, previous_fire_time + timedelta(microseconds=1))
            if start_date == previous_fire_time:
                start_date += timedelta(microseconds=1)
        else:
            start_date = (
                datetime.fromtimestamp(
                    max(now.timestamp(), self.start_date.timestamp()), self.timezone
                )
                if self.start_date
                else now
            )

        if previous_fire_time:
            partner = self._get_fold_partner(previous_fire_time, start_date)
            if partner is not None:
                return self._apply_jitter(partner, self.jitter, now)

        wall_start = start_date
        threshold_timestamp = start_date.timestamp()
        pending_fire_times = {}
        while True:
            next_date = self._calculate_next_wall_time(wall_start)
            if next_date is None:
                return None
            fire_times, _ = self._resolve_dst(next_date)
            for fire_time, is_gap_adjusted in fire_times:
                if fire_time.timestamp() >= threshold_timestamp and (
                    not self.end_date
                    or fire_time.timestamp() <= self.end_date.timestamp()
                ):
                    timestamp = fire_time.timestamp()
                    if timestamp not in pending_fire_times or not is_gap_adjusted:
                        pending_fire_times[timestamp] = fire_time

            wall_start = next_date.replace(tzinfo=None) + timedelta(microseconds=1)
            following_date = self._calculate_next_wall_time(wall_start)
            if pending_fire_times and (
                following_date is None
                or following_date.timestamp() >= min(pending_fire_times)
            ):
                earliest_timestamp = min(pending_fire_times)
                fire_time = datetime.fromtimestamp(
                    earliest_timestamp, self.timezone
                )
                fire_time = self._apply_jitter(fire_time, self.jitter, now)
                return min(fire_time, self.end_date) if self.end_date else fire_time

    def __getstate__(self):
        return {
            "version": 3,
            "timezone": timezone_repr(self.timezone)
            if hasattr(self.timezone, "key")
            else self.timezone,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "fields": self.fields,
            "jitter": self.jitter,
            "dst_policy": self.dst_policy,
        }

    def __setstate__(self, state):
        # This is for compatibility with APScheduler 3.0.x
        if isinstance(state, tuple):
            state = state[1]

        if state.get("version", 1) > 3:
            raise ValueError(
                f"Got serialized data for version {state['version']} of "
                f"{self.__class__.__name__}, but only versions up to 3 can be handled"
            )

        self.timezone = astimezone(state["timezone"])
        self.start_date = state["start_date"]
        self.end_date = state["end_date"]
        self.fields = state["fields"]
        self.jitter = state.get("jitter")
        self.dst_policy = state.get("dst_policy", "first")

    def __str__(self):
        options = [f"{f.name}='{f}'" for f in self.fields if not f.is_default]
        return "cron[{}]".format(", ".join(options))

    def __repr__(self):
        options = [f"{f.name}='{f}'" for f in self.fields if not f.is_default]
        if self.start_date:
            options.append(f"start_date={datetime_repr(self.start_date)!r}")
        if self.end_date:
            options.append(f"end_date={datetime_repr(self.end_date)!r}")
        if self.jitter:
            options.append(f"jitter={self.jitter}")
        if self.dst_policy != "first":
            options.append(f"dst_policy={self.dst_policy!r}")

        return "<{} ({}, timezone='{}')>".format(
            self.__class__.__name__,
            ", ".join(options),
            self.timezone,
        )
