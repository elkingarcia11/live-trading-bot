"""GEX level-refresh schedule helpers.

Responsibility: Decide when structural GEX levels (walls / flip) should be
recomputed. Does not fetch chains or evaluate strategy rules.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def parse_local_hhmm_times(values: tuple[str, ...] | list[str]) -> tuple[time, ...]:
    """Parse ``HH:MM`` local wall-clock times, sorted ascending."""
    parsed: list[time] = []
    for raw in values:
        hour_s, minute_s = str(raw).strip().split(":", 1)
        parsed.append(time(int(hour_s), int(minute_s)))
    return tuple(sorted(parsed))


def next_refresh_at(
    now: datetime,
    *,
    refresh_times_local: tuple[time, ...],
    timezone_name: str,
    already_fired: set[datetime] | None = None,
) -> datetime:
    """Return the next local refresh datetime after ``now``.

    Skips slots already present in ``already_fired`` (timezone-aware datetimes).
    """
    if not refresh_times_local:
        raise ValueError("refresh_times_local must not be empty")

    tz = ZoneInfo(timezone_name)
    local_now = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    fired = already_fired or set()

    for day_offset in range(0, 8):
        day = local_now.date() + timedelta(days=day_offset)
        for slot in refresh_times_local:
            candidate = datetime.combine(day, slot, tzinfo=tz)
            if candidate <= local_now:
                continue
            if candidate in fired:
                continue
            return candidate

    # Fallback: first slot a week out (should be unreachable with day_offset range).
    day = local_now.date() + timedelta(days=7)
    return datetime.combine(day, refresh_times_local[0], tzinfo=tz)


def due_refresh_slots(
    now: datetime,
    *,
    refresh_times_local: tuple[time, ...],
    timezone_name: str,
    already_fired: set[datetime],
) -> list[datetime]:
    """Return today's refresh slots that are due and not yet fired."""
    if not refresh_times_local:
        return []

    tz = ZoneInfo(timezone_name)
    local_now = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    due: list[datetime] = []
    for slot in refresh_times_local:
        candidate = datetime.combine(local_now.date(), slot, tzinfo=tz)
        if candidate <= local_now and candidate not in already_fired:
            due.append(candidate)
    return due


def prune_fired_slots(already_fired: set[datetime], *, keep_on_or_after: date) -> set[datetime]:
    """Drop fired markers from previous calendar days."""
    return {
        stamp
        for stamp in already_fired
        if stamp.astimezone(stamp.tzinfo).date() >= keep_on_or_after
    }
