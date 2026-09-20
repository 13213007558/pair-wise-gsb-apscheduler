from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import obj_to_ref, ref_to_obj


class BaseCombiningTrigger(BaseTrigger):
    __slots__ = ("triggers", "jitter")

    #: maximum serialized state version this class can handle
    _max_state_version = 1

    def __init__(self, triggers, jitter=None):
        self.triggers = triggers
        self.jitter = jitter

    def __getstate__(self):
        return {
            "version": 1,
            "triggers": [
                (obj_to_ref(trigger.__class__), trigger.__getstate__())
                for trigger in self.triggers
            ],
            "jitter": self.jitter,
        }

    def __setstate__(self, state):
        if state.get("version", 1) > self._max_state_version:
            raise ValueError(
                f"Got serialized data for version {state['version']} of "
                f"{self.__class__.__name__}, but only versions up to "
                f"{self._max_state_version} can be handled"
            )

        self.jitter = state["jitter"]
        self.triggers = []
        for clsref, state in state["triggers"]:
            cls = ref_to_obj(clsref)
            trigger = cls.__new__(cls)
            trigger.__setstate__(state)
            self.triggers.append(trigger)

    def __repr__(self):
        return "<{}({}{})>".format(
            self.__class__.__name__,
            self.triggers,
            f", jitter={self.jitter}" if self.jitter else "",
        )


class AndTrigger(BaseCombiningTrigger):
    """
    Always returns the earliest next fire time that all the given triggers can agree on.
    The trigger is considered to be finished when any of the given triggers has finished its
    schedule.

    Trigger alias: ``and``

    .. warning:: This trigger should only be used to combine triggers that fire on
        specific times of day, such as
        :class:`~apscheduler.triggers.cron.CronTrigger` and
        class:`~apscheduler.triggers.calendarinterval.CalendarIntervalTrigger`.
        Attempting to use it with
        :class:`~apscheduler.triggers.interval.IntervalTrigger` will likely result in
        the scheduler hanging as it tries to find a fire time that matches exactly
        between fire times produced by all the given triggers.

    :param list triggers: triggers to combine
    :param int|None jitter: delay the job execution by ``jitter`` seconds at most
    """

    __slots__ = ()

    def get_next_fire_time(self, previous_fire_time, now):
        while True:
            fire_times = [
                trigger.get_next_fire_time(previous_fire_time, now)
                for trigger in self.triggers
            ]
            if None in fire_times:
                return None
            elif min(fire_times) == max(fire_times):
                return self._apply_jitter(fire_times[0], self.jitter, now)
            else:
                now = max(fire_times)

    def __str__(self):
        return "and[{}]".format(", ".join(str(trigger) for trigger in self.triggers))


class OrTrigger(BaseCombiningTrigger):
    """
    Always returns the earliest next fire time produced by any of the given triggers.
    The trigger is considered finished when all the given triggers have finished their schedules.

    Trigger alias: ``or``

    :param list triggers: triggers to combine
    :param int|None jitter: delay the job execution by ``jitter`` seconds at most
    :param bool independent_cursors: if ``True``, each trigger is advanced based only on
        its own previously produced fire time instead of the fire time last produced by
        this trigger as a whole. This keeps triggers that depend on their previous fire
        time (such as :class:`~apscheduler.triggers.interval.IntervalTrigger`) on their
        own schedule when combined, at the cost of making this trigger stateful (the
        extra state is included when the trigger is serialized).

    .. note:: Triggers that depends on the previous fire time, such as the interval trigger, may
        seem to behave strangely since they are always passed the previous fire time produced by
        any of the given triggers. Pass ``independent_cursors=True`` to give each trigger its
        own cursor instead.
    """

    __slots__ = (
        "independent_cursors",
        "_cursors",
        "_pendings",
        "_last_returned",
        "_last_emitted",
    )

    _max_state_version = 2

    def __init__(self, triggers, jitter=None, independent_cursors=False):
        super().__init__(triggers, jitter)
        self.independent_cursors = independent_cursors
        self._cursors = [None] * len(self.triggers)
        self._pendings = [None] * len(self.triggers)
        self._last_returned = None
        self._last_emitted = None

    def get_next_fire_time(self, previous_fire_time, now):
        if not self.independent_cursors:
            fire_times = [
                trigger.get_next_fire_time(previous_fire_time, now)
                for trigger in self.triggers
            ]
            fire_times = [
                fire_time for fire_time in fire_times if fire_time is not None
            ]
            if fire_times:
                return self._apply_jitter(min(fire_times), self.jitter, now)
            return None

        # If the caller passes back the exact value this trigger returned last time, that
        # value is considered emitted, so advance the cursors of the triggers that
        # produced it. Any other value (including None) leaves the cursors untouched,
        # which keeps repeated calls with the same arguments from consuming candidates.
        if (
            previous_fire_time is not None
            and self._last_returned is not None
            and previous_fire_time == self._last_returned
        ):
            for index, pending in enumerate(self._pendings):
                if pending is not None and pending == self._last_emitted:
                    self._cursors[index] = pending
                    self._pendings[index] = None

            self._last_returned = None
            self._last_emitted = None

        # Compute the next un-emitted candidate for every trigger that doesn't have one.
        # Candidates that have already been computed but not yet emitted are kept as is,
        # so that restarting from serialized state never loses them.
        for index, trigger in enumerate(self.triggers):
            if self._pendings[index] is None:
                self._pendings[index] = trigger.get_next_fire_time(
                    self._cursors[index], now
                )

        candidates = [pending for pending in self._pendings if pending is not None]
        if not candidates:
            return None

        # Fire times produced by several triggers at the same instant are only emitted
        # once, since all of them are consumed together on the next confirmation.
        # If the last returned value was never confirmed (fed back as
        # ``previous_fire_time``), the candidates are unchanged, so the same value is
        # returned again instead of consuming or re-randomizing anything.
        if self._last_returned is None:
            self._last_emitted = min(candidates)
            self._last_returned = self._apply_jitter(
                self._last_emitted, self.jitter, now
            )

        return self._last_returned

    def __getstate__(self):
        state = super().__getstate__()
        if self.independent_cursors:
            state.update(
                {
                    "version": 2,
                    "independent_cursors": True,
                    "cursors": self._cursors,
                    "pendings": self._pendings,
                    "last_returned": self._last_returned,
                    "last_emitted": self._last_emitted,
                }
            )

        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.independent_cursors = state.get("independent_cursors", False)
        self._cursors = state.get("cursors") or [None] * len(self.triggers)
        self._pendings = state.get("pendings") or [None] * len(self.triggers)
        self._last_returned = state.get("last_returned")
        self._last_emitted = state.get("last_emitted")

    def __str__(self):
        return "or[{}]".format(", ".join(str(trigger) for trigger in self.triggers))

    def __repr__(self):
        return "<{}({}{}{})>".format(
            self.__class__.__name__,
            self.triggers,
            f", jitter={self.jitter}" if self.jitter else "",
            ", independent_cursors=True" if self.independent_cursors else "",
        )
