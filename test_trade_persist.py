"""Tests for non-blocking trade persist queue and background writer."""

from __future__ import annotations

import queue
import threading
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from session_trade_recorder import build_trade_row
from trade_persist import (
    TradePersistClient,
    TradePersistWriterConfig,
    run_trade_persist_writer,
)


def _row(*, price: float, sequence: int) -> dict[str, object]:
    row = build_trade_row(
        symbol="ES.n.0",
        timestamp=datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc),
        price=price,
        size=1,
        sequence=sequence,
        instrument_id=1,
        ts_event=sequence,
    )
    assert row is not None
    return row


def test_try_put_drops_when_queue_is_full() -> None:
    client = TradePersistClient(
        queue.Queue(maxsize=1),
        threading.Event(),
        queue.Queue(),
    )
    assert client.try_put(_row(price=100.0, sequence=1)) is True
    assert client.try_put(_row(price=101.0, sequence=2)) is False


def test_persist_writer_appends_and_compacts_on_shutdown(tmp_path: Path) -> None:
    work = queue.Queue()
    stop = threading.Event()
    stats = queue.Queue()
    config = TradePersistWriterConfig(
        local_root=str(tmp_path),
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
        every_rows=5000,
        interval_seconds=0.0,
        compact_interval_seconds=0.0,
    )
    thread = threading.Thread(
        target=run_trade_persist_writer,
        args=(work, stop, stats, config),
        daemon=True,
    )
    thread.start()
    work.put(_row(price=100.0, sequence=1))
    work.put(_row(price=101.0, sequence=2))
    stop.set()
    thread.join(timeout=20)
    assert not thread.is_alive()

    path = tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet"
    assert path.exists()
    frame = pd.read_parquet(path)
    assert list(frame["price"]) == [100.0, 101.0]
    events = []
    while True:
        try:
            events.append(stats.get_nowait())
        except queue.Empty:
            break
    kinds = {event["kind"] for event in events}
    assert "flush" in kinds
    assert "compact" in kinds


def test_persist_client_process_round_trip(tmp_path: Path) -> None:
    config = TradePersistWriterConfig(
        local_root=str(tmp_path),
        bucket_name="unused-bucket",
        prefix="trades",
        remote_enabled=False,
        every_rows=1,
        interval_seconds=0.0,
        compact_interval_seconds=0.0,
    )
    client = TradePersistClient.start(config, queue_maxsize=8)
    try:
        assert client.try_put(_row(price=100.0, sequence=1)) is True
        assert client.try_put(_row(price=101.0, sequence=2)) is True
    finally:
        client.shutdown(timeout=30)

    path = tmp_path / "trades" / "ES.n.0" / "2026-08-17.parquet"
    assert path.exists()
    frame = pd.read_parquet(path)
    assert list(frame["price"]) == [100.0, 101.0]
