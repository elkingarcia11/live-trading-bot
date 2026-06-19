"""Tests for workflow startup warmup helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd

from config import AppConfig
from workflow_warmup import (
    backfill_timeframe,
    bootstrap_sync_start,
    configured_sync_start,
    last_completed_bar_timestamp,
    load_recent_stored_bars,
    load_stored_bars,
    startup_sync_window,
    warmup_lookback_duration,
    warmup_required_bar_count,
)


def test_last_completed_bar_timestamp_for_3m() -> None:
    now = datetime(2026, 6, 15, 17, 7, 45, tzinfo=timezone.utc)
    last_bar = last_completed_bar_timestamp("3m", now)
    assert last_bar.minute % 3 == 0
    assert last_bar < now.replace(second=0, microsecond=0)


def test_backfill_timeframe_uses_historical_setting() -> None:
    app = AppConfig()
    assert backfill_timeframe(app) == "3m"


def test_warmup_required_bar_count_for_supertrend_only() -> None:
    app = AppConfig()
    object.__setattr__(app.indicators, "max_bars", 100)
    object.__setattr__(app.indicators, "dema", None)
    object.__setattr__(
        app.indicators,
        "supertrend",
        app.indicators.supertrend.__class__(
            atr_period=11,
            source="hl2",
            multiplier=1.0,
            change_atr=True,
        ),
    )
    assert warmup_required_bar_count(app) == 100


def test_warmup_lookback_duration_for_3m() -> None:
    app = AppConfig()
    object.__setattr__(app.indicators, "max_bars", 100)
    object.__setattr__(app.indicators, "dema", None)
    object.__setattr__(
        app.indicators,
        "supertrend",
        app.indicators.supertrend.__class__(
            atr_period=11,
            source="hl2",
            multiplier=1.0,
            change_atr=True,
        ),
    )
    assert warmup_lookback_duration(app, "3m") == timedelta(minutes=303)


def test_bootstrap_sync_start_uses_extended_open_when_enabled() -> None:
    app = AppConfig()
    object.__setattr__(app.historical, "need_extended_hours", True)
    object.__setattr__(app.historical, "extended_session_start_utc", "08:00")
    end = datetime(2026, 6, 15, 17, 6, tzinfo=timezone.utc)
    assert bootstrap_sync_start(app, end) == datetime(
        2026, 4, 15, 8, 0, tzinfo=timezone.utc
    )


def test_bootstrap_sync_start_uses_two_month_lookback() -> None:
    app = AppConfig()
    end = datetime(2026, 6, 15, 17, 6, tzinfo=timezone.utc)
    assert bootstrap_sync_start(app, end) == datetime(
        2026, 4, 15, 14, 30, tzinfo=timezone.utc
    )


def test_startup_sync_window_uses_bootstrap_range_for_backfill() -> None:
    app = AppConfig()
    end = datetime(2026, 6, 15, 17, 6, tzinfo=timezone.utc)
    sync_start, warmup_start, bootstrap_start = startup_sync_window(
        app,
        end=end,
        timeframe="3m",
    )
    assert sync_start == bootstrap_start
    assert sync_start == datetime(2026, 4, 15, 14, 30, tzinfo=timezone.utc)
    assert warmup_start <= end
    assert warmup_start >= sync_start


def test_configured_sync_start_uses_historical_setting() -> None:
    app = AppConfig()
    assert configured_sync_start(app) == datetime(2024, 1, 1, 14, 30, tzinfo=timezone.utc)


def test_load_stored_bars_reads_daily_partitions() -> None:
    storage = MagicMock()
    day = date(2026, 6, 15)
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-06-15 17:00:00", "2026-06-15 17:03:00"],
                utc=True,
            ),
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.05, 2.05],
            "volume": [100.0, 200.0],
        }
    )

    def _exists(symbol: str, timeframe: str, *, partition_date: date | None = None) -> bool:
        return partition_date == day

    def _read(
        symbol: str,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        partition_date: date | None = None,
    ) -> pd.DataFrame:
        return frame

    storage.exists.side_effect = _exists
    storage.read.side_effect = _read

    start = datetime(2026, 6, 15, 17, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 17, 3, tzinfo=timezone.utc)
    loaded = load_stored_bars(
        storage,
        "SPY",
        "3m",
        start,
        end,
        use_daily_partitions=True,
    )
    assert len(loaded) == 2
    assert float(loaded.iloc[-1]["close"]) == 2.05


def test_load_recent_stored_bars_walks_back_across_partitions() -> None:
    storage = MagicMock()
    day_old = date(2026, 6, 14)
    day_new = date(2026, 6, 15)
    old_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [f"2026-06-14 {hour:02d}:00:00" for hour in range(14, 20, 3)],
                utc=True,
            ),
            "open": [1.0] * 2,
            "high": [1.1] * 2,
            "low": [0.9] * 2,
            "close": [1.05] * 2,
            "volume": [100.0] * 2,
        }
    )
    new_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-06-15 17:00:00", "2026-06-15 17:03:00"],
                utc=True,
            ),
            "open": [2.0, 3.0],
            "high": [2.1, 3.1],
            "low": [1.9, 2.9],
            "close": [2.05, 3.05],
            "volume": [200.0, 300.0],
        }
    )

    def _exists(symbol: str, timeframe: str, *, partition_date: date | None = None) -> bool:
        return partition_date in {day_old, day_new}

    def _read(
        symbol: str,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        partition_date: date | None = None,
    ) -> pd.DataFrame:
        if partition_date == day_old:
            return old_frame
        if partition_date == day_new:
            return new_frame
        return pd.DataFrame()

    storage.exists.side_effect = _exists
    storage.read.side_effect = _read

    end = datetime(2026, 6, 15, 17, 3, tzinfo=timezone.utc)
    floor = datetime(2026, 6, 1, tzinfo=timezone.utc)
    loaded = load_recent_stored_bars(
        storage,
        "SPY",
        "3m",
        end=end,
        required_bars=3,
        floor=floor,
        use_daily_partitions=True,
    )
    assert len(loaded) == 3
    assert loaded.iloc[0]["timestamp"] == pd.Timestamp("2026-06-14 17:00:00", tz="UTC")
    assert float(loaded.iloc[-1]["close"]) == 3.05
