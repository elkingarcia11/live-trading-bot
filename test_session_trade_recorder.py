"""Tests for session raw-trade recorder buffering and flush."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from session_trade_recorder import SessionTradeRecorder
from tick_bar_builder import TickBarBuilder


def test_session_trade_recorder_flushes_to_local_partition(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        use_daily_partitions=True,
        remote_enabled=False,
    )
    ts = datetime(2026, 8, 13, 14, 30, tzinfo=timezone.utc)
    recorder.record_trade(symbol="spy", timestamp=ts, price=500.1, size=10, side="A")
    recorder.record_trade(
        symbol="SPY",
        timestamp=ts.replace(second=1),
        price=500.2,
        size=5,
        side="B",
    )

    summary = recorder.flush()
    assert summary.rows_buffered == 2
    assert summary.rows_written == 2
    assert summary.partitions_written == 1

    path = tmp_path / "trades" / "SPY" / "2026-08-13.parquet"
    assert path.exists()
    frame = pd.read_parquet(path)
    assert len(frame) == 2
    assert list(frame["price"]) == [500.1, 500.2]


def test_session_trade_recorder_merges_existing_partition(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    day = datetime(2026, 8, 13, 14, 0, tzinfo=timezone.utc)
    recorder.record_trade(symbol="SPY", timestamp=day, price=100.0, size=1)
    recorder.flush()

    recorder.record_trade(
        symbol="SPY",
        timestamp=day.replace(minute=1),
        price=101.0,
        size=2,
    )
    summary = recorder.flush()
    assert summary.rows_written == 1

    frame = pd.read_parquet(tmp_path / "trades" / "SPY" / "2026-08-13.parquet")
    assert len(frame) == 2
    assert list(frame["price"]) == [100.0, 101.0]


def test_tick_bar_builder_flush_emits_partial_bar() -> None:
    builder = TickBarBuilder(ticks_per_bar=5)
    base = datetime(2026, 8, 13, 14, 30, tzinfo=timezone.utc)
    for index in range(3):
        assert (
            builder.update(
                symbol="SPY",
                price=500.0 + index,
                size=1.0,
                timestamp=base.replace(second=index),
            )
            is None
        )

    payloads = builder.flush()
    assert len(payloads) == 1
    bar = payloads[0]["bar"]
    assert payloads[0]["timeframe"] == "5t"
    assert bar["partial"] is True
    assert bar["ticks"] == 3
    assert bar["open"] == 500.0
    assert bar["close"] == 502.0
    assert builder.forming_ticks("SPY") is None
