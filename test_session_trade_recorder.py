"""Tests for session raw-trade recorder buffering and flush."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from session_trade_recorder import TRADE_COLUMNS, SessionTradeRecorder
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


def test_session_trade_recorder_stores_full_databento_fields(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    ts = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    recorder.record_trade(
        symbol="ES.n.0",
        timestamp=ts,
        price=7792.25,
        size=1,
        side="B",
        raw_symbol="ESU6",
        price_raw=7792250000000,
        action="T",
        depth=0,
        flags=0,
        sequence=12345,
        instrument_id=42140870,
        publisher_id=1,
        rtype=0,
        ts_event=1_786_982_268_000_000_000,
        ts_recv=1_786_982_268_000_020_000,
        ts_in_delta=17523,
        ts_index=1_786_982_268_000_020_000,
    )
    recorder.flush()
    frame = pd.read_parquet(tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet")
    assert list(frame.columns) == list(TRADE_COLUMNS)
    row = frame.iloc[0]
    assert row["symbol"] == "ES.n.0"
    assert row["raw_symbol"] == "ESU6"
    assert row["price"] == 7792.25
    assert int(row["sequence"]) == 12345
    assert int(row["instrument_id"]) == 42140870
    assert int(row["ts_in_delta"]) == 17523


def test_session_trade_recorder_dedupes_on_sequence(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    ts = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    common = dict(
        symbol="ES.n.0",
        timestamp=ts,
        price=7792.0,
        size=1,
        side="A",
        sequence=99,
        instrument_id=1,
        ts_event=1_786_982_268_000_000_000,
    )
    recorder.record_trade(**common)
    recorder.flush()
    recorder.record_trade(**common)  # exact duplicate
    summary = recorder.flush()
    assert summary.rows_buffered == 1
    frame = pd.read_parquet(tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet")
    assert len(frame) == 1


def test_session_trade_recorder_records_during_in_flight_flush(tmp_path: Path) -> None:
    """New ticks buffer while a flush write runs; they are not lost or interleaved."""
    import threading
    import time

    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    day = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    for index in range(3):
        recorder.record_trade(
            symbol="ES.n.0",
            timestamp=day.replace(second=index),
            price=100.0 + index,
            size=1,
            sequence=index,
            instrument_id=1,
            ts_event=1_000_000 + index,
        )

    started = threading.Event()
    release = threading.Event()
    original_write = recorder._write

    def slow_write(symbol, frame, *, partition_date=None):
        started.set()
        release.wait(timeout=2.0)
        return original_write(symbol, frame, partition_date=partition_date)

    recorder._write = slow_write  # type: ignore[method-assign]

    summary_holder: list = []

    def _flush() -> None:
        summary_holder.append(recorder.flush(block=True))

    writer = threading.Thread(target=_flush)
    writer.start()
    assert started.wait(timeout=2.0)

    # Concurrent tick while write is in progress.
    recorder.record_trade(
        symbol="ES.n.0",
        timestamp=day.replace(second=30),
        price=200.0,
        size=1,
        sequence=99,
        instrument_id=1,
        ts_event=2_000_000,
    )
    assert recorder.buffered_row_count == 1
    assert recorder.flush_in_progress is True
    assert recorder.flush(block=False) is None  # does not interrupt

    release.set()
    writer.join(timeout=5.0)
    assert summary_holder and summary_holder[0] is not None
    assert summary_holder[0].rows_written == 3

    # Leftover tick flushes on its own.
    second = recorder.flush(block=True)
    assert second is not None
    assert second.rows_written == 1
    frame = pd.read_parquet(tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet")
    assert len(frame) == 4
    assert sorted(frame["price"].tolist()) == [100.0, 101.0, 102.0, 200.0]

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


def test_append_rows_does_not_merge_until_compact(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    ts = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    recorder.append_rows(
        [
            {
                "timestamp": ts,
                "symbol": "ES.n.0",
                "price": 7792.0,
                "size": 1,
                "sequence": 1,
                "instrument_id": 1,
                "ts_event": 1,
            }
        ]
    )
    daily = tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet"
    assert not daily.exists()
    assert recorder.part_file_count() == 1

    summary = recorder.compact()
    assert summary.rows_written == 1
    assert daily.exists()
    assert recorder.part_file_count() == 0
    frame = pd.read_parquet(daily)
    assert list(frame["price"]) == [7792.0]


def test_compact_merges_parts_into_existing_daily(tmp_path: Path) -> None:
    recorder = SessionTradeRecorder(
        local_root=tmp_path,
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
    )
    ts = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    recorder.record_trade(
        symbol="ES.n.0",
        timestamp=ts,
        price=100.0,
        size=1,
        sequence=1,
        instrument_id=1,
        ts_event=1,
    )
    recorder.flush()
    recorder.append_rows(
        [
            {
                "timestamp": ts.replace(minute=1),
                "symbol": "ES.n.0",
                "price": 101.0,
                "size": 2,
                "sequence": 2,
                "instrument_id": 1,
                "ts_event": 2,
            }
        ]
    )
    recorder.compact()
    frame = pd.read_parquet(tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet")
    assert list(frame["price"]) == [100.0, 101.0]
