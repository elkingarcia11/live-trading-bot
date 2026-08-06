"""Tests for cumulative GCS history merge and storage-only warmup."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from config import AppConfig
from session_ohlcv_recorder import SessionOhlcvRecorder
from stream_data_processor import CleanBarEvent
from workflow_warmup import (
    fetch_recent_1m_volumes,
    prepare_trading_day_from_storage,
    warmup_lookback_duration_for_bars,
    warm_start_gex,
)


class _FakeStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str, pd.DataFrame, object]] = []
        self._objects: dict[tuple[str, str, object], pd.DataFrame] = {}

    def exists(self, symbol: str, timeframe: str, *, partition_date=None) -> bool:
        return (symbol, timeframe, partition_date) in self._objects

    def read(
        self,
        symbol: str,
        timeframe: str,
        *,
        partition_date=None,
        start=None,
        end=None,
    ) -> pd.DataFrame:
        frame = self._objects[(symbol, timeframe, partition_date)].copy()
        if start is not None:
            frame = frame[frame["timestamp"] >= pd.Timestamp(start)]
        if end is not None:
            frame = frame[frame["timestamp"] <= pd.Timestamp(end)]
        return frame.reset_index(drop=True)

    def write(
        self,
        symbol: str,
        timeframe: str,
        data: pd.DataFrame,
        *,
        partition_date=None,
    ) -> str:
        self.writes.append((symbol, timeframe, data.copy(), partition_date))
        self._objects[(symbol, timeframe, partition_date)] = data.copy()
        return f"gs://bucket/ohlcv/{symbol}/{timeframe}/{partition_date}.parquet"


def _ohlcv_frame(timestamps: list[datetime], volumes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps, utc=True),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": volumes,
        }
    )


def test_session_recorder_merges_into_existing_partition() -> None:
    storage = _FakeStorage()
    day = datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc).date()
    prior = _ohlcv_frame(
        [datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc)],
        [10.0],
    )
    storage._objects[("SPY", "50t", day)] = prior

    recorder = SessionOhlcvRecorder(storage, timeframes=("50t",))
    recorder.record_clean_bar(
        CleanBarEvent(
            symbol="SPY",
            timeframe="50t",
            timestamp=datetime(2026, 6, 17, 15, 0, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=20.0,
        )
    )
    summary = recorder.flush()

    assert summary.rows_written == 1
    assert summary.partitions_written == 1
    merged = storage._objects[("SPY", "50t", day)]
    assert len(merged) == 2
    assert list(merged["volume"]) == [10.0, 20.0]


def test_session_recorder_dedupes_same_timestamp_keep_last() -> None:
    storage = _FakeStorage()
    day = datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc).date()
    ts = datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc)
    storage._objects[("SPY", "50t", day)] = _ohlcv_frame([ts], [10.0])

    recorder = SessionOhlcvRecorder(storage, timeframes=("50t",))
    recorder.record_clean_bar(
        CleanBarEvent(
            symbol="SPY",
            timeframe="50t",
            timestamp=ts,
            open=100,
            high=102,
            low=99,
            close=101.0,
            volume=99.0,
        )
    )
    recorder.flush()
    merged = storage._objects[("SPY", "50t", day)]
    assert len(merged) == 1
    assert float(merged.iloc[0]["volume"]) == 99.0
    assert float(merged.iloc[0]["close"]) == 101.0


def test_fetch_recent_volumes_storage_only_never_calls_executor() -> None:
    app = AppConfig.from_dict(
        {
            "market": {"symbols": ["SPY"], "stream_timeframe": "50t"},
            "gcs": {"use_daily_partitions": True},
        }
    )
    end = datetime(2026, 6, 17, 20, 0, tzinfo=timezone.utc)
    timestamps = [end - timedelta(minutes=i) for i in range(19, -1, -1)]
    frame = _ohlcv_frame(timestamps, [float(i) for i in range(20)])

    storage = MagicMock()
    storage.exists.return_value = True
    storage.read.return_value = frame
    executor = MagicMock()

    volumes, source = fetch_recent_1m_volumes(
        app,
        "SPY",
        lookback_bars=20,
        end=end,
        storage=storage,
        executor=executor,
    )

    assert source == "storage"
    assert len(volumes) == 20
    executor.execute.assert_not_called()


def test_fetch_recent_volumes_returns_none_when_storage_empty() -> None:
    app = AppConfig.from_dict(
        {"market": {"symbols": ["SPY"], "stream_timeframe": "50t"}}
    )
    storage = MagicMock()
    storage.exists.return_value = False

    volumes, source = fetch_recent_1m_volumes(
        app,
        "SPY",
        lookback_bars=20,
        end=datetime(2026, 6, 17, 20, 0, tzinfo=timezone.utc),
        storage=storage,
        executor=MagicMock(),
    )
    assert volumes == []
    assert source == "none"


def test_warmup_lookback_for_tick_timeframe_uses_calendar_days() -> None:
    span = warmup_lookback_duration_for_bars("50t", 20)
    assert span.days >= 3


def test_prepare_trading_day_does_not_run_vendor_backfill_pipeline() -> None:
    workflow = MagicMock()
    workflow.config.app = AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_timeframe": "50t",
                "strategy_timeframe": "50t",
            },
            "strategies": ["gex_scalp"],
            "gex": {"enabled": True, "seed_volume_history": True},
            "indicators": {
                "dema": {"enabled": False},
                "supertrend": {"enabled": False},
                "gaussian_bands": {"enabled": False},
            },
            "gcs": {"bucket_name": "live-trading-bot", "ohlcv_prefix": "ohlcv"},
        }
    )
    workflow.config.strategies = ("gex_scalp",)
    workflow.symbols = ("SPY",)
    workflow.seed_gex_volume_history.return_value = 0

    with patch(
        "workflow_warmup.warm_start_pipeline"
    ) as pipeline, patch(
        "workflow_warmup.build_storage_repository",
        side_effect=RuntimeError("no gcs"),
    ):
        prepare_trading_day_from_storage(workflow)
        pipeline.assert_not_called()


def test_warm_start_gex_does_not_build_backfill_executor() -> None:
    workflow = MagicMock()
    workflow.config.app = AppConfig.from_dict(
        {
            "market": {"symbols": ["SPY"], "stream_timeframe": "50t"},
            "strategies": ["gex_scalp"],
            "gex": {
                "enabled": True,
                "seed_volume_history": True,
                "volume_lookback_bars": 5,
            },
        }
    )
    workflow.config.strategies = ("gex_scalp",)
    workflow.symbols = ("SPY",)
    workflow.seed_gex_volume_history.return_value = 0

    with patch(
        "workflow_warmup.build_storage_repository",
        return_value=MagicMock(exists=MagicMock(return_value=False)),
    ), patch("workflow_warmup.build_backfill_executor") as build_exec:
        warm_start_gex(workflow)
        build_exec.assert_not_called()
