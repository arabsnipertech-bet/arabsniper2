"""Fast slots are defined in the Italian calendar, including on DST dates."""

from datetime import datetime, time, timedelta

from night_scan_guard import TZ, timestamp

ON_HOUR = '0 6-21/3 * * *'
HALF_HOUR = '30 7-22/3 * * *'
AFTERNOON = '30 15 * * *'
ORDINARY, EXTENDED, FORCED = 'ordinary', 'extended', 'forced'
DAY_COUNT = {ORDINARY: 1, EXTENDED: 3, FORCED: 5}
INTERVAL = timedelta(minutes=90)


def slots(day, scope):
    minutes = (15 * 60 + 30,) if scope == EXTENDED else range(6 * 60, 22 * 60 + 31, 90)
    return [datetime.combine(day, time(m // 60, m % 60), TZ) for m in minutes]


def latest_slot(at, scope, schedule=None):
    local = at.astimezone(TZ)
    minute = {ON_HOUR: 0, HALF_HOUR: 30}.get(schedule)
    return next((slot for slot in reversed(slots(local.date(), scope))
                 if slot <= local and (minute is None or slot.minute == minute)), None)


def explicit_slot(value, scope, now):
    slot = timestamp(value)
    local = now.astimezone(TZ)
    if scope == FORCED or slot is None or slot > now or slot not in slots(local.date(), scope):
        raise ValueError('Invalid or expired Fast slot for Europe/Rome')
    return slot.astimezone(TZ)
