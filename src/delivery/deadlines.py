from datetime import datetime, time, timedelta, timezone
EST = timezone(timedelta(hours=-5))
def deadline(monday):
    if monday.weekday() != 0:
        raise ValueError('explicit Monday business date required')
    return datetime.combine(monday, time(9), tzinfo=EST)
def status(completed_at, due_at, review_minutes=60):
    if completed_at.tzinfo is None or due_at.tzinfo is None:
        raise ValueError('timezone-aware timestamps required')
    return {'sla_met': completed_at <= due_at,
            'review_buffer_met': completed_at <= due_at-timedelta(minutes=review_minutes),
            'seconds_remaining': (due_at-completed_at).total_seconds()}
