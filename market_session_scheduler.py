"""Market-session and end-of-day scheduling helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from gap_detector import session_bounds_for_day


@dataclass(frozen=True)
class EodSchedule:
    """Times to flatten positions and shut down after the trading day.

    Prefer local market-timezone wall clocks when ``flatten_time_local`` /
    ``shutdown_time_local`` are set (DST-safe). Otherwise fall back to UTC.
    """

    enabled: bool = True
    flatten_time_utc: time = time(19, 59)
    shutdown_time_utc: time = time(20, 0)
    trading_days_only: bool = True
    market_timezone: str = "America/New_York"
    flatten_time_local: Optional[time] = None
    shutdown_time_local: Optional[time] = None


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
    return is_local_session_timestamp(
        timestamp,
        session_start_local=session_start_local,
        session_end_local=session_end_local,
        market_timezone=market_timezone,
        trading_days_only=trading_days_only,
    )


def is_local_session_timestamp(
    timestamp: datetime,
    *,
    session_start_local: time,
    session_end_local: time,
    market_timezone: str,
    trading_days_only: bool = True,
) -> bool:
    """Return True when ``timestamp`` is inside ``[start, end)`` in market local time.

    Supports sessions that wrap midnight (``end <= start``).
    """
    ts = _to_utc(timestamp)
    local = ts.astimezone(ZoneInfo(market_timezone))
    if trading_days_only and not is_trading_day(local.date()):
        return False
    local_time = local.timetz().replace(tzinfo=None, microsecond=0)
    if session_end_local <= session_start_local:
        # e.g. 20:00 -> 04:00 next day (not used for US equities extended).
        return local_time >= session_start_local or local_time < session_end_local
    return session_start_local <= local_time < session_end_local


def is_equity_streaming_session(
    timestamp: datetime,
    *,
    session_start_local: time = time(4, 0),
    session_end_local: time = time(20, 0),
    market_timezone: str = "America/New_York",
    trading_days_only: bool = True,
) -> bool:
    """US equities Databento window: pre-market through post-market (default 4am-8pm ET)."""
    return is_local_session_timestamp(
        timestamp,
        session_start_local=session_start_local,
        session_end_local=session_end_local,
        market_timezone=market_timezone,
        trading_days_only=trading_days_only,
    )


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
    local_day = _schedule_local_day(now, schedule)
    if schedule.trading_days_only and not is_trading_day(local_day):
        return False
    if flattened_on == local_day:
        return False
    return is_at_or_past_flatten_time(now, schedule=schedule)


def is_at_or_past_flatten_time(now: datetime, *, schedule: EodSchedule) -> bool:
    """Return True when ``now`` is at or after the configured flatten time."""
    if not schedule.enabled:
        return False
    now = _to_utc(now)
    local_day = _schedule_local_day(now, schedule)
    if schedule.trading_days_only and not is_trading_day(local_day):
        return False
    return _is_at_or_past_local_or_utc(
        now,
        local_time=schedule.flatten_time_local,
        utc_time=schedule.flatten_time_utc,
        market_timezone=schedule.market_timezone,
    )


def flatten_deadline_utc(day: date, *, schedule: EodSchedule) -> datetime:
    """Return the UTC datetime when same-day positions must be flattened."""
    if schedule.flatten_time_local is not None:
        local = datetime.combine(
            day,
            schedule.flatten_time_local,
            tzinfo=ZoneInfo(schedule.market_timezone),
        )
        return local.astimezone(timezone.utc)
    return datetime.combine(day, schedule.flatten_time_utc, tzinfo=timezone.utc)


def should_shutdown(
    now: datetime,
    *,
    schedule: EodSchedule,
    shutdown_on: date | None,
) -> bool:
    """Return True when the process should exit once for ``now``'s local market date."""
    if not schedule.enabled:
        return False
    now = _to_utc(now)
    local_day = _schedule_local_day(now, schedule)
    if schedule.trading_days_only and not is_trading_day(local_day):
        return False
    if shutdown_on == local_day:
        return False
    return _is_at_or_past_local_or_utc(
        now,
        local_time=schedule.shutdown_time_local,
        utc_time=schedule.shutdown_time_utc,
        market_timezone=schedule.market_timezone,
    )


def _schedule_local_day(now: datetime, schedule: EodSchedule) -> date:
    if schedule.flatten_time_local is not None or schedule.shutdown_time_local is not None:
        return now.astimezone(ZoneInfo(schedule.market_timezone)).date()
    return now.date()


def _is_at_or_past_local_or_utc(
    now: datetime,
    *,
    local_time: Optional[time],
    utc_time: time,
    market_timezone: str,
) -> bool:
    if local_time is not None:
        local = now.astimezone(ZoneInfo(market_timezone))
        return local.timetz().replace(tzinfo=None, microsecond=0) >= local_time
    return now.timetz().replace(tzinfo=None, microsecond=0) >= utc_time


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
