:mod:`apscheduler.triggers.combining`
=====================================

These triggers combine the behavior of other triggers in different ways to produce schedules more
complex than would be possible with any single built-in trigger.

.. automodule:: apscheduler.triggers.combining

API
---

.. autoclass:: AndTrigger

.. autoclass:: OrTrigger


Examples
--------

Run ``job_function`` every 3 days (at midnight), but only when that date lands on on a
Saturday or Sunday::

    from apscheduler.triggers.combining import AndTrigger
    from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
    from apscheduler.triggers.cron import CronTrigger


    trigger = AndTrigger([CalendarIntervalTrigger(days=3),
                          CronTrigger(day_of_week='sat,sun')])
    scheduler.add_job(job_function, trigger)

Run ``job_function`` every Monday at 2 am and every Tuesday at 3 pm::

    trigger = OrTrigger([CronTrigger(day_of_week='mon', hour=2),
                         CronTrigger(day_of_week='tue', hour=15)])
    scheduler.add_job(job_function, trigger)

Run ``job_function`` every 5 minutes and every 7 minutes, keeping each interval trigger on
its own schedule (0, 5, 7, 10, 14, 15, ... minutes from the start) by giving each combined
trigger an independent cursor::

    from apscheduler.triggers.interval import IntervalTrigger

    trigger = OrTrigger([IntervalTrigger(minutes=5), IntervalTrigger(minutes=7)],
                        independent=True)
    scheduler.add_job(job_function, trigger)
