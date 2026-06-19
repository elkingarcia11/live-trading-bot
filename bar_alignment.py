"""Shared UTC bar-bucket alignment for live aggregation and historical rollup.

All minute-based buckets floor timestamps to UTC epoch boundaries so live
1m->3m aggregation, Schwab backfill rollup, gap detection, and warmup windows
use identical candle timestamps.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Optional


def to_utc(timestamp: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def timeframe_minutes(timeframe: str) -> int:
    """Convert a timeframe label such as ``3m`` into whole minutes."""
    if len(timeframe) < 2:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    unit = timeframe[-1]
    amount = int(timeframe[:-1])
    if unit == "m":
        return amount
    if unit == "h":
        return amount * 60
    if unit == "d":
        return amount * 24 * 60
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def timeframe_timedelta(timeframe: str) -> timedelta:
    """Convert a timeframe label into a timedelta."""
    return timedelta(minutes=timeframe_minutes(timeframe))


def align_bucket_start(timestamp: datetime, timeframe: str) -> datetime:
    """Floor a timestamp to the inclusive left edge of its bar bucket."""
    timestamp = to_utc(timestamp)

    if timeframe == "1d":
        return datetime.combine(timestamp.date(), time.min, tzinfo=timezone.utc)

    interval_seconds = int(timeframe_timedelta(timeframe).total_seconds())
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = int((timestamp - epoch).total_seconds())
    aligned = elapsed - (elapsed % interval_seconds)
    return epoch + timedelta(seconds=aligned)


def is_aligned_timestamp(timestamp: datetime, timeframe: str) -> bool:
    """Return whether ``timestamp`` is already on a bucket left edge."""
    return align_bucket_start(timestamp, timeframe) == to_utc(timestamp).replace(
        second=0,
        microsecond=0,
    )


def last_completed_bar_timestamp(
    timeframe: str,
    now: Optional[datetime] = None,
) -> datetime:
    """Return the left-edge timestamp of the most recently completed bar."""
    current = to_utc(now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    current_bucket = align_bucket_start(current, timeframe)
    return current_bucket - timeframe_timedelta(timeframe)


def next_bucket_start(timestamp: datetime, timeframe: str) -> datetime:
    """Return the left edge of the bar bucket immediately after ``timestamp``."""
    bucket = align_bucket_start(timestamp, timeframe)
    return bucket + timeframe_timedelta(timeframe)


def session_first_bucket(
    session_open: datetime,
    interval: timedelta,
) -> datetime:
    """Return the first bar left-edge at or after extended/session open."""
    session_open = to_utc(session_open).replace(second=0, microsecond=0)
    interval_seconds = int(interval.total_seconds())
    if interval_seconds <= 0:
        raise ValueError("interval must be positive")

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = int((session_open - epoch).total_seconds())
    aligned = elapsed - (elapsed % interval_seconds)
    bucket = epoch + timedelta(seconds=aligned)
    if bucket < session_open:
        bucket += timedelta(seconds=interval_seconds)
    return bucket


def last_completed_minute(now: Optional[datetime] = None) -> datetime:
    """Return the most recently completed 1-minute bar timestamp."""
    return to_utc(now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)


def aggregation_checkpoint(
    last_saved_timestamp: datetime,
    *,
    timeframe: str,
    now: Optional[datetime] = None,
) -> tuple[datetime, datetime]:
    """Return (completed_through, first_minute_to_seed) for live aggregation.

    ``completed_through`` is the newest fully-finished 3m bar that must not be
    emitted again. ``first_minute_to_seed`` is the first 1m timestamp to feed
    into the open bucket after the last saved candle.
    """
    last_saved = align_bucket_start(last_saved_timestamp, timeframe)
    current = to_utc(now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    current_bucket = align_bucket_start(current, timeframe)
    interval = timeframe_timedelta(timeframe)

    if last_saved >= current_bucket:
        return last_saved - interval, last_saved

    return last_saved, last_saved + interval


def bucket_members(
    bucket_start: datetime,
    timeframe: str,
) -> tuple[datetime, datetime]:
    """Return the inclusive/exclusive UTC bounds for one bar bucket.

    A 3m bucket starting at 14:30 contains 1m bars at 14:30, 14:31, and 14:32.
    It should finalize when the 14:32 1m bar is complete.
    """
    start = align_bucket_start(bucket_start, timeframe)
    end = start + timeframe_timedelta(timeframe)
    return start, end


def is_bucket_closing_minute(timestamp: datetime, timeframe: str) -> bool:
    """Return whether a 1m timestamp is the final minute in its bar bucket."""
    if timeframe == "1d":
        return False

    bucket = align_bucket_start(timestamp, timeframe)
    last_minute = bucket + timeframe_timedelta(timeframe) - timedelta(minutes=1)
    minute = to_utc(timestamp).replace(second=0, microsecond=0)
    return minute == last_minute
