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
    fetch_recent_1m_volumes,
    indicator_warmup_needed,
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


def test_indicator_warmup_not_needed_for_gex_only_without_indicators() -> None:
    app = AppConfig()
    object.__setattr__(app.indicators, "dema", None)
    object.__setattr__(app.indicators, "supertrend", None)
    object.__setattr__(app.indicators, "gaussian_bands", None)
    assert indicator_warmup_needed(app, ("gex_scalp",)) is False


def test_indicator_warmup_needed_when_supertrend_enabled() -> None:
    app = AppConfig()
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
    assert indicator_warmup_needed(app, ("gex_scalp",)) is True


def test_fetch_recent_1m_volumes_from_storage() -> None:
    app = AppConfig()
    end = datetime(2026, 6, 15, 17, 20, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                end - timedelta(minutes=19),
                end,
                freq="1min",
                tz="UTC",
            ),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": [float(i) for i in range(20)],
        }
    )
    storage = MagicMock()
    storage.exists.return_value = True
    storage.read.return_value = frame

    volumes, source = fetch_recent_1m_volumes(
        app,
        "SPY",
        lookback_bars=20,
        end=end,
        storage=storage,
    )
    assert source == "storage"
    assert len(volumes) == 20
    assert volumes[0] == 0.0
    assert volumes[-1] == 19.0


def test_seed_gex_volume_history_caps_at_lookback() -> None:
    from collections import deque

    from workflow import TradingWorkflow

    config = MagicMock()
    config.app.gex.volume_lookback_bars = 5
    config.app.gex.enabled = True
    config.strategies = ("gex_scalp",)
    config.symbols = ("SPY",)
    config.warmup_from_storage = False
    config.persist_session_bars = False
    config.market_config.stream_timeframe = "1m"
    config.market_config.strategy_timeframe = "1m"
    config.market_config.aggregation_timeframes = ("1m",)
    config.indicator_config.build_jobs.return_value = ()
    config.sync_broker_positions_on_start = False
    config.eod_schedule.enabled = False
    config.managed_exits = False
    config.app.options.enabled = False
    config.app.broker.provider = "schwab"
    config.stream_provider = "schwab"
    config.run_schwab_stream = False
    config.websocket_url = ""

    workflow = object.__new__(TradingWorkflow)
    workflow._config = config
    workflow._symbols = ("SPY",)
    workflow._volume_history = {"SPY": deque(maxlen=5)}

    seeded = workflow.seed_gex_volume_history("SPY", list(range(10)))
    assert seeded == 5
    assert list(workflow._volume_history["SPY"]) == [5.0, 6.0, 7.0, 8.0, 9.0]
