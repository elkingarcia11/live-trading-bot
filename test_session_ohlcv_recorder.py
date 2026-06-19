"""Tests for session OHLCV recorder buffering."""

from __future__ import annotations

from datetime import datetime, timezone

from data_aggregator import AggregatedBar
from session_ohlcv_recorder import SessionOhlcvRecorder
from stream_data_processor import CleanBarEvent


class _FakeStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str, object, object]] = []
        self._objects: dict[tuple[str, str, object], object] = {}

    def exists(self, symbol: str, timeframe: str, *, partition_date=None) -> bool:
        return (symbol, timeframe, partition_date) in self._objects

    def read(self, symbol: str, timeframe: str, *, partition_date=None, start=None, end=None):
        return self._objects[(symbol, timeframe, partition_date)].copy()

    def write(self, symbol: str, timeframe: str, data, *, partition_date=None) -> str:
        self.writes.append((symbol, timeframe, data, partition_date))
        self._objects[(symbol, timeframe, partition_date)] = data
        return f"gs://bucket/ohlcv/{symbol}/{timeframe}/{partition_date}.parquet"


def test_session_recorder_flushes_clean_and_aggregated_bars() -> None:
    storage = _FakeStorage()
    recorder = SessionOhlcvRecorder(storage, timeframes=("1m", "3m"))
    ts = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)

    recorder.record_clean_bar(
        CleanBarEvent(
            symbol="SPY",
            timeframe="1m",
            timestamp=ts,
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=1000,
        )
    )
    recorder.record_aggregated_bar(
        AggregatedBar(
            symbol="SPY",
            timeframe="3m",
            timestamp=ts,
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=1000,
            is_complete=True,
        )
    )

    summary = recorder.flush()
    assert summary.rows_buffered == 2
    assert summary.rows_written == 2
    assert summary.partitions_written == 2
    assert len(storage.writes) == 2
