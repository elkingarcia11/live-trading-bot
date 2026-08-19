"""Focused tests for raw collector buffering and ATM seed extraction."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from data_collector_workflow import DailyCsvBuffer, _chain_underlying_price


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
    assert (tmp_path / "2026-08-19" / "call.csv").exists()


def test_chain_underlying_price_uses_current_quote() -> None:
    assert _chain_underlying_price({"underlying": {"last": 501.25}}) == 501.25
