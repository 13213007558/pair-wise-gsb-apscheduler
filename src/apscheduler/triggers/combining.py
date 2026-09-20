from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import obj_to_ref, ref_to_obj


class BaseCombiningTrigger(BaseTrigger):
    __slots__ = ("triggers", "jitter")

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
        if state.get("version", 1) > 1:
            raise ValueError(
                f"Got serialized data for version {state['version']} of "
                f"{self.__class__.__name__}, but only versions up to 1 can be handled"
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
    :param bool independent: if ``True``, each of the combined triggers is advanced based
        only on its own previous fire time instead of the previous fire time produced by this
        trigger as a whole. This keeps triggers that depend on the previous fire time (such as
        :class:`~apscheduler.triggers.interval.IntervalTrigger`) on their own schedules,
        unaffected by the fire times of the other combined triggers. Fire times produced by
        more than one of the combined triggers at the same moment are only returned once, and
        the not-yet-emitted fire times of the combined triggers are included in the serialized
        state of this trigger so that they survive scheduler restarts without being skipped.

    .. note:: Unless ``independent`` is enabled, triggers that depend on the previous fire
        time, such as the interval trigger, may seem to behave strangely since they are always
        passed the previous fire time produced by any of the given triggers.
    """

    __slots__ = ("independent", "_next_fire_times")

    def __init__(self, triggers, jitter=None, independent=False):
        super().__init__(triggers, jitter)
        self.independent = independent
        self._next_fire_times = None

    def get_next_fire_time(self, previous_fire_time, now):
        if self.independent:
            return self._get_next_fire_time_independent(previous_fire_time, now)

        fire_times = [
            trigger.get_next_fire_time(previous_fire_time, now)
            for trigger in self.triggers
        ]
        fire_times = [fire_time for fire_time in fire_times if fire_time is not None]
        if fire_times:
            return self._apply_jitter(min(fire_times), self.jitter, now)
        else:
            return None

    def _get_next_fire_time_independent(self, previous_fire_time, now):
        if previous_fire_time is None:
            # The schedule is (re)started from scratch, so (re)initialize the per-trigger
            # cursors with the first fire time of each trigger
            self._next_fire_times = [
                trigger.get_next_fire_time(None, now) for trigger in self.triggers
            ]
        elif self._next_fire_times is None:
            # The trigger was unserialized from a state that did not contain the per-trigger
            # cursors, so seed them the same way the default mode would
            self._next_fire_times = [
                trigger.get_next_fire_time(previous_fire_time, now)
                for trigger in self.triggers
            ]
        else:
            # Advance the cursor of each trigger past the fire time that was previously
            # returned (every trigger whose pending fire time was emitted moves on to its
            # own next fire time, so equal fire times are only emitted once). This is
            # idempotent: calling this method again with the same arguments yields the same
            # result without consuming any further fire times.
            next_fire_times = []
            for trigger, next_fire_time in zip(self.triggers, self._next_fire_times):
                while (
                    next_fire_time is not None and next_fire_time <= previous_fire_time
                ):
                    next_fire_time = trigger.get_next_fire_time(next_fire_time, now)
                next_fire_times.append(next_fire_time)
            self._next_fire_times = next_fire_times

        fire_times = [
            fire_time for fire_time in self._next_fire_times if fire_time is not None
        ]
        if fire_times:
            return self._apply_jitter(min(fire_times), self.jitter, now)
        else:
            return None

    def __getstate__(self):
        state = super().__getstate__()
        if self.independent:
            state.update(
                {
                    "version": 2,
                    "independent": self.independent,
                    "next_fire_times": self._next_fire_times,
                }
            )
        return state

    def __setstate__(self, state):
        version = state.get("version", 1)
        if version > 2:
            raise ValueError(
                f"Got serialized data for version {version} of "
                f"{self.__class__.__name__}, but only versions up to 2 can be handled"
            )

        # The base class only accepts version 1 states, so downgrade the state before
        # passing it on (version 2 only adds the keys handled below)
        super().__setstate__(dict(state, version=1))
        self.independent = state.get("independent", False)
        self._next_fire_times = state.get("next_fire_times")

    def __repr__(self):
        options = f", jitter={self.jitter}" if self.jitter else ""
        if self.independent:
            options += ", independent=True"
        return f"<{self.__class__.__name__}({self.triggers}{options})>"

    def __str__(self):
        return "or[{}]".format(", ".join(str(trigger) for trigger in self.triggers))
