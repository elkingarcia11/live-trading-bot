"""Focused tests for raw collector buffering and ATM seed extraction."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from data_collector_workflow import (
    DataCollectorWorkflow,
    TICK_TIMEFRAMES,
    DailyCsvBuffer,
    _chain_underlying_price,
)
from tick_bar_builder import TickBarBuilder


def test_daily_csv_buffer_flushes_and_deduplicates(tmp_path: Path) -> None:
    buffer = DailyCsvBuffer(
        local_root=tmp_path,
        bucket_name="unused",
        remote_enabled=False,
        filename="call.csv",
        columns=("timestamp", "description", "mark_price", "underlying_price"),
        key_columns=("timestamp", "description",
                     "mark_price", "underlying_price"),
    )
    row = {
        "timestamp": datetime(2026, 8, 19, 14, 30, tzinfo=timezone.utc),
        "description": "SPY call",
        "mark_price": 5.0,
        "underlying_price": 500.0,
    }
    buffer.add(row)
    assert buffer.flush() == 1
    buffer.add(row)
    assert buffer.flush() == 0
    assert (tmp_path / "call.csv").exists()


def test_tick_timeframes_cover_requested_es_outputs() -> None:
    assert TICK_TIMEFRAMES == (
        "25t", "50t", "100t", "200t", "400t", "800t", "1200t"
    )


def test_es_tick_bar_is_routed_to_matching_buffer() -> None:
    class CaptureBuffer:
        def __init__(self) -> None:
            self.rows: list[dict[str, object]] = []

        def add(self, row: dict[str, object]) -> None:
            self.rows.append(row)

    workflow = DataCollectorWorkflow.__new__(DataCollectorWorkflow)
    workflow._es_tick_builders = {
        timeframe: TickBarBuilder(ticks_per_bar=int(timeframe[:-1]))
        for timeframe in TICK_TIMEFRAMES
    }
    buffers = {
        timeframe: CaptureBuffer() for timeframe in TICK_TIMEFRAMES
    }
    workflow._es_tick_buffers = buffers

    for index in range(25):
        workflow._add_es_tick_bars({
            "symbol": "ES",
            "timestamp": datetime(2026, 8, 19, 14, 30, index,
                                   tzinfo=timezone.utc),
            "price": 5000.0 + index,
            "size": 1.0,
        })

    assert len(buffers["25t"].rows) == 1
    assert buffers["25t"].rows[0]["open"] == 5000.0
    assert buffers["25t"].rows[0]["close"] == 5024.0
    assert buffers["50t"].rows == []


def test_chain_underlying_price_uses_current_quote() -> None:
    assert _chain_underlying_price({"underlying": {"last": 501.25}}) == 501.25
