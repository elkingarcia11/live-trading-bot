"""UTC end-of-day scheduling helpers for regular-session shutdown."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from gap_detector import session_bounds_for_day


@dataclass(frozen=True)
class EodSchedule:
    """Times (UTC) to flatten positions and shut down after regular hours."""

    enabled: bool = True
    flatten_time_utc: time = time(19, 59)
    shutdown_time_utc: time = time(20, 0)
    trading_days_only: bool = True


def parse_utc_hhmm(value: str) -> time:
    """Parse ``HH:MM`` into a UTC wall-clock time."""
    return parse_hhmm(value)


def parse_hhmm(value: str) -> time:
    """Parse ``HH:MM`` into a wall-clock time."""
    hour, minute = value.split(":", 1)
    return time(int(hour), int(minute))


def is_trading_day(day: date) -> bool:
    """Return True for Monday–Friday."""
    return day.weekday() < 5


def is_regular_hours_timestamp_local(
    timestamp: datetime,
    *,
    session_start_local: time,
    session_end_local: time,
    market_timezone: str,
    trading_days_only: bool = True,
) -> bool:
    """Return True when ``timestamp`` falls inside the regular session in market local time."""
    ts = _to_utc(timestamp).replace(second=0, microsecond=0)
    local = ts.astimezone(ZoneInfo(market_timezone))
    if trading_days_only and not is_trading_day(local.date()):
        return False
    local_time = local.timetz().replace(tzinfo=None, second=0, microsecond=0)
    return session_start_local <= local_time < session_end_local


def is_regular_hours_timestamp(
    timestamp: datetime,
    *,
    session_start_utc: time,
    session_end_utc: time,
    trading_days_only: bool = True,
) -> bool:
    """Return True when ``timestamp`` falls inside the regular session (UTC)."""
    ts = _to_utc(timestamp).replace(second=0, microsecond=0)
    if trading_days_only and not is_trading_day(ts.date()):
        return False
    session_open, session_close = session_bounds_for_day(
        ts.date(),
        session_start_utc,
        session_end_utc,
    )
    return session_open <= ts < session_close


def should_flatten_positions(
    now: datetime,
    *,
    schedule: EodSchedule,
    flattened_on: date | None,
) -> bool:
    """Return True when open positions should be flattened once for ``now``'s date."""
    if not schedule.enabled:
        return False
    now = _to_utc(now)
    if schedule.trading_days_only and not is_trading_day(now.date()):
        return False
    if flattened_on == now.date():
        return False
    return is_at_or_past_flatten_time(now, schedule=schedule)


def is_at_or_past_flatten_time(now: datetime, *, schedule: EodSchedule) -> bool:
    """Return True when ``now`` is at or after the configured flatten time (UTC)."""
    if not schedule.enabled:
        return False
    now = _to_utc(now)
    if schedule.trading_days_only and not is_trading_day(now.date()):
        return False
    return now.time() >= schedule.flatten_time_utc


def flatten_deadline_utc(day: date, *, schedule: EodSchedule) -> datetime:
    """Return the UTC datetime when same-day positions must be flattened."""
    return datetime.combine(day, schedule.flatten_time_utc, tzinfo=timezone.utc)


def should_shutdown(
    now: datetime,
    *,
    schedule: EodSchedule,
    shutdown_on: date | None,
) -> bool:
    """Return True when the process should exit once for ``now``'s date."""
    if not schedule.enabled:
        return False
    now = _to_utc(now)
    if schedule.trading_days_only and not is_trading_day(now.date()):
        return False
    if shutdown_on == now.date():
        return False
    return now.time() >= schedule.shutdown_time_utc


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
