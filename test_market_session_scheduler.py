"""Tests for end-of-day UTC scheduling."""

from __future__ import annotations

from datetime import date, datetime, time, timezone

from market_session_scheduler import (
    EodSchedule,
    flatten_deadline_utc,
    is_at_or_past_flatten_time,
    is_equity_streaming_session,
    is_regular_hours_timestamp,
    is_regular_hours_timestamp_local,
    should_flatten_positions,
    should_shutdown,
)


def test_should_flatten_once_after_flatten_time_on_weekday() -> None:
    schedule = EodSchedule(
        flatten_time_utc=time(19, 59),
        shutdown_time_utc=time(20, 0),
    )
    now = datetime(2026, 6, 16, 19, 59, 30, tzinfo=timezone.utc)

    assert should_flatten_positions(now, schedule=schedule, flattened_on=None) is True
    assert (
        should_flatten_positions(now, schedule=schedule, flattened_on=date(2026, 6, 16))
        is False
    )


def test_should_shutdown_once_after_shutdown_time() -> None:
    schedule = EodSchedule(
        flatten_time_utc=time(19, 59),
        shutdown_time_utc=time(20, 0),
    )
    now = datetime(2026, 6, 16, 20, 0, 5, tzinfo=timezone.utc)

    assert should_shutdown(now, schedule=schedule, shutdown_on=None) is True
    assert should_shutdown(now, schedule=schedule, shutdown_on=date(2026, 6, 16)) is False


def test_should_shutdown_skipped_when_shutdown_disabled() -> None:
    schedule = EodSchedule(
        shutdown_time_local=time(20, 0),
        shutdown_enabled=False,
        market_timezone="America/New_York",
    )
    now = datetime(2026, 6, 16, 20, 0, 5, tzinfo=timezone.utc)

    assert should_shutdown(now, schedule=schedule, shutdown_on=None) is False


def test_eod_actions_skip_weekends() -> None:
    schedule = EodSchedule(trading_days_only=True)
    saturday = datetime(2026, 6, 20, 20, 5, tzinfo=timezone.utc)

    assert should_flatten_positions(saturday, schedule=schedule, flattened_on=None) is False
    assert should_shutdown(saturday, schedule=schedule, shutdown_on=None) is False


def test_is_at_or_past_flatten_time_on_weekday() -> None:
    schedule = EodSchedule(flatten_time_utc=time(19, 59))
    before = datetime(2026, 6, 16, 19, 58, tzinfo=timezone.utc)
    at = datetime(2026, 6, 16, 19, 59, tzinfo=timezone.utc)

    assert is_at_or_past_flatten_time(before, schedule=schedule) is False
    assert is_at_or_past_flatten_time(at, schedule=schedule) is True


def test_flatten_deadline_utc_combines_day_and_time() -> None:
    schedule = EodSchedule(flatten_time_utc=time(19, 59))
    deadline = flatten_deadline_utc(date(2026, 6, 16), schedule=schedule)
    assert deadline == datetime(2026, 6, 16, 19, 59, tzinfo=timezone.utc)


def test_is_regular_hours_timestamp_inside_session() -> None:
    session_start = time(14, 30)
    session_end = time(21, 0)
    open_bar = datetime(2026, 6, 16, 14, 30, tzinfo=timezone.utc)
    premarket = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)

    assert (
        is_regular_hours_timestamp(
            open_bar,
            session_start_utc=session_start,
            session_end_utc=session_end,
        )
        is True
    )
    assert (
        is_regular_hours_timestamp(
            premarket,
            session_start_utc=session_start,
            session_end_utc=session_end,
        )
        is False
    )


def test_equity_streaming_session_covers_pre_rth_and_post() -> None:
    # 2026-06-17 is a Wednesday. Times below are UTC during EDT (UTC-4).
    premarket = datetime(2026, 6, 17, 8, 5, tzinfo=timezone.utc)  # 04:05 ET
    rth = datetime(2026, 6, 17, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
    post = datetime(2026, 6, 17, 23, 30, tzinfo=timezone.utc)  # 19:30 ET
    after_close = datetime(2026, 6, 18, 0, 5, tzinfo=timezone.utc)  # 20:05 ET
    before_open = datetime(2026, 6, 17, 7, 59, tzinfo=timezone.utc)  # 03:59 ET

    assert is_equity_streaming_session(premarket) is True
    assert is_equity_streaming_session(rth) is True
    assert is_equity_streaming_session(post) is True
    assert is_equity_streaming_session(after_close) is False
    assert is_equity_streaming_session(before_open) is False


def test_eod_local_times_use_market_timezone() -> None:
    schedule = EodSchedule(
        flatten_time_local=time(16, 0),
        shutdown_time_local=time(20, 0),
        market_timezone="America/New_York",
    )
    # 16:00 ET on 2026-06-16 == 20:00 UTC (EDT)
    before = datetime(2026, 6, 16, 19, 59, tzinfo=timezone.utc)
    at_flatten = datetime(2026, 6, 16, 20, 0, tzinfo=timezone.utc)
    at_shutdown = datetime(2026, 6, 17, 0, 0, tzinfo=timezone.utc)

    assert should_flatten_positions(before, schedule=schedule, flattened_on=None) is False
    assert should_flatten_positions(at_flatten, schedule=schedule, flattened_on=None) is True
    assert should_shutdown(at_flatten, schedule=schedule, shutdown_on=None) is False
    assert should_shutdown(at_shutdown, schedule=schedule, shutdown_on=None) is True


def test_is_regular_hours_timestamp_local_honors_dst() -> None:
    session_start = time(9, 30)
    session_end = time(16, 0)
    summer_open = datetime(2026, 6, 17, 13, 42, tzinfo=timezone.utc)
    summer_premarket = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    winter_open = datetime(2026, 1, 15, 14, 42, tzinfo=timezone.utc)

    assert (
        is_regular_hours_timestamp_local(
            summer_open,
            session_start_local=session_start,
            session_end_local=session_end,
            market_timezone="America/New_York",
        )
        is True
    )
    assert (
        is_regular_hours_timestamp_local(
            summer_premarket,
            session_start_local=session_start,
            session_end_local=session_end,
            market_timezone="America/New_York",
        )
        is False
    )
    assert (
        is_regular_hours_timestamp_local(
            winter_open,
            session_start_local=session_start,
            session_end_local=session_end,
            market_timezone="America/New_York",
        )
        is True
    )
