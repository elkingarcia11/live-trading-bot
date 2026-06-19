"""Tests for shared bar bucket alignment."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

from bar_alignment import (
    aggregation_checkpoint,
    align_bucket_start,
    bucket_members,
    is_aligned_timestamp,
    last_completed_bar_timestamp,
    session_first_bucket,
)
from data_aggregator import AggregatedBar, DataAggregator
from schwab_market_data_client import _aggregate_minute_candles
from stream_data_processor import CleanBarEvent


def test_session_first_bucket_starts_at_extended_open() -> None:
    session_open = datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    assert session_first_bucket(session_open, timedelta(minutes=3)) == session_open


def test_session_first_bucket_skips_bucket_before_open() -> None:
    session_open = datetime(2026, 6, 15, 8, 1, tzinfo=timezone.utc)
    assert session_first_bucket(session_open, timedelta(minutes=3)) == datetime(
        2026, 6, 15, 8, 3, tzinfo=timezone.utc
    )


def test_expected_intervals_cap_at_last_completed_bar() -> None:
    from gap_detector import GapDetector

    detector = GapDetector()
    last_bar = datetime(2026, 6, 15, 17, 30, tzinfo=timezone.utc)
    intervals = detector.build_expected_intervals(
        date(2026, 6, 15),
        session_start=time(8, 0),
        session_end=time(1, 0),
        interval=timedelta(minutes=3),
        last_bar_inclusive=last_bar,
    )
    assert intervals[0] == datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    assert intervals[-1] == last_bar
    assert datetime(2026, 6, 15, 17, 33, tzinfo=timezone.utc) not in intervals


def test_aggregation_checkpoint_after_complete_saved_bar() -> None:
    last_saved = datetime(2026, 6, 15, 17, 30, tzinfo=timezone.utc)
    now = datetime(2026, 6, 15, 17, 35, tzinfo=timezone.utc)
    through, seed_start = aggregation_checkpoint(last_saved, timeframe="3m", now=now)
    assert through == last_saved
    assert seed_start == datetime(2026, 6, 15, 17, 33, tzinfo=timezone.utc)


def test_aggregation_checkpoint_after_flushed_open_bucket() -> None:
    last_saved = datetime(2026, 6, 15, 17, 33, tzinfo=timezone.utc)
    now = datetime(2026, 6, 15, 17, 35, tzinfo=timezone.utc)
    through, seed_start = aggregation_checkpoint(last_saved, timeframe="3m", now=now)
    assert through == datetime(2026, 6, 15, 17, 30, tzinfo=timezone.utc)
    assert seed_start == last_saved


def test_data_aggregator_suppresses_already_saved_complete_bar() -> None:
    aggregator = DataAggregator(target_timeframes=("3m",))
    base = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    aggregator.set_completed_through("SPY", "3m", datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc))

    completed: list[AggregatedBar] = []
    for offset in range(4):
        for aggregated in aggregator.on_bar(
            CleanBarEvent(
                symbol="SPY",
                timeframe="1m",
                timestamp=base + timedelta(minutes=offset),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=100.0,
            )
        ):
            if aggregated.is_complete:
                completed.append(aggregated)

    assert completed == []
    assert aggregator.completed_through("SPY", "3m") == datetime(
        2026, 6, 15, 14, 30, tzinfo=timezone.utc
    )


def test_align_bucket_start_for_3m_utc_epoch() -> None:
    ts = datetime(2026, 6, 15, 17, 35, tzinfo=timezone.utc)
    assert align_bucket_start(ts, "3m") == datetime(2026, 6, 15, 17, 33, tzinfo=timezone.utc)


def test_session_open_aligns_to_3m_bucket() -> None:
    session_open = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    assert is_aligned_timestamp(session_open, "3m")
    assert align_bucket_start(session_open, "3m") == session_open


def test_last_completed_bar_timestamp_during_forming_bucket() -> None:
    now = datetime(2026, 6, 15, 17, 35, tzinfo=timezone.utc)
    last_bar = last_completed_bar_timestamp("3m", now)
    assert last_bar == datetime(2026, 6, 15, 17, 30, tzinfo=timezone.utc)


def test_bucket_members_for_3m() -> None:
    start = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    left, right = bucket_members(start, "3m")
    assert left == start
    assert right == datetime(2026, 6, 15, 14, 33, tzinfo=timezone.utc)


def test_data_aggregator_matches_backfill_rollup() -> None:
    candles = [
        {
            "datetime": (datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc) + timedelta(minutes=offset)).isoformat(),
            "open": 100.0 + offset,
            "high": 101.0 + offset,
            "low": 99.0 + offset,
            "close": 100.5 + offset,
            "volume": 100 + offset,
        }
        for offset in range(6)
    ]
    backfill = _aggregate_minute_candles(candles, 3)

    aggregator = DataAggregator(target_timeframes=("3m",))
    live_completed: list[AggregatedBar] = []
    for offset in range(6):
        ts = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc) + timedelta(minutes=offset)
        for aggregated in aggregator.on_bar(
            CleanBarEvent(
                symbol="SPY",
                timeframe="1m",
                timestamp=ts,
                open=100.0 + offset,
                high=101.0 + offset,
                low=99.0 + offset,
                close=100.5 + offset,
                volume=100 + offset,
            )
        ):
            if aggregated.is_complete:
                live_completed.append(aggregated)

    assert len(backfill) == len(live_completed) == 2
    for rolled, live in zip(backfill, live_completed):
        assert align_bucket_start(
            pd.to_datetime(rolled["datetime"], utc=True).to_pydatetime(),
            "3m",
        ) == live.timestamp
        assert rolled["open"] == live.open
        assert rolled["close"] == live.close
        assert rolled["volume"] == live.volume


def test_bucket_closes_on_last_minute_not_next_bucket() -> None:
    aggregator = DataAggregator(target_timeframes=("3m",))
    base = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    completion_times: list[datetime] = []

    for offset in range(3):
        for aggregated in aggregator.on_bar(
            CleanBarEvent(
                symbol="SPY",
                timeframe="1m",
                timestamp=base + timedelta(minutes=offset),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0 + offset,
                volume=100.0,
            )
        ):
            if aggregated.is_complete:
                completion_times.append(aggregated.timestamp)

    assert completion_times == [base]


def test_data_aggregator_only_emits_one_complete_bar_per_bucket() -> None:
    aggregator = DataAggregator(target_timeframes=("3m",))
    base = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    completed: list[AggregatedBar] = []

    for offset in range(6):
        for aggregated in aggregator.on_bar(
            CleanBarEvent(
                symbol="SPY",
                timeframe="1m",
                timestamp=base + timedelta(minutes=offset),
                open=100.0 + offset,
                high=101.0 + offset,
                low=99.0 + offset,
                close=100.5 + offset * 0.1,
                volume=100.0,
            )
        ):
            if aggregated.is_complete:
                completed.append(aggregated)

    assert len(completed) == 2
    assert completed[0].timestamp == base
    assert completed[1].timestamp == datetime(2026, 6, 15, 14, 33, tzinfo=timezone.utc)
