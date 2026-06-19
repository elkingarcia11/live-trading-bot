"""Buffer live session OHLCV bars and flush them to GCS on shutdown.

Responsibility: Persist streamed 1m bars and completed aggregated bars from a
live session so future startups can load them from storage. Does not fetch
historical data or evaluate strategies.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd

from cloud_storage_repository import CloudStorageRepository
from data_aggregator import AggregatedBar
from ohlcv_schema import OHLCV_COLUMNS
from stream_data_processor import CleanBarEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionFlushSummary:
    """Outcome of flushing buffered session bars to GCS."""

    rows_buffered: int
    rows_written: int
    partitions_written: int
    storage_uris: tuple[str, ...] = ()


@dataclass
class _BufferedRows:
    rows: list[dict[str, object]] = field(default_factory=list)


class SessionOhlcvRecorder:
    """Accumulate live bars in memory and merge-write them on shutdown."""

    def __init__(
        self,
        storage: CloudStorageRepository,
        *,
        timeframes: tuple[str, ...],
        use_daily_partitions: bool = True,
    ) -> None:
        if not timeframes:
            raise ValueError("At least one timeframe is required")
        self._storage = storage
        self._timeframes = frozenset(timeframes)
        self._use_daily_partitions = use_daily_partitions
        self._buffers: dict[tuple[str, str], _BufferedRows] = {}
        self._lock = threading.Lock()

    @property
    def buffered_row_count(self) -> int:
        with self._lock:
            return sum(len(bucket.rows) for bucket in self._buffers.values())

    def record_clean_bar(self, bar: CleanBarEvent) -> None:
        """Buffer one validated stream bar when its timeframe is tracked."""
        if bar.timeframe not in self._timeframes:
            return
        self._append(
            bar.symbol,
            bar.timeframe,
            timestamp=bar.timestamp,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )

    def record_aggregated_bar(self, bar: AggregatedBar) -> None:
        """Buffer one completed aggregated bar when its timeframe is tracked."""
        if not bar.is_complete or bar.timeframe not in self._timeframes:
            return
        self._append(
            bar.symbol,
            bar.timeframe,
            timestamp=bar.timestamp,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )

    def flush(self) -> SessionFlushSummary:
        """Merge buffered rows into existing GCS partitions and upload."""
        with self._lock:
            pending = {
                key: list(bucket.rows)
                for key, bucket in self._buffers.items()
                if bucket.rows
            }
            self._buffers.clear()

        if not pending:
            logger.info("Session OHLCV flush: no buffered bars to save")
            return SessionFlushSummary(
                rows_buffered=0,
                rows_written=0,
                partitions_written=0,
            )

        rows_buffered = sum(len(rows) for rows in pending.values())
        logger.info(
            "Flushing %d buffered session bar(s) across %d symbol/timeframe bucket(s) to GCS",
            rows_buffered,
            len(pending),
        )

        rows_written = 0
        partitions_written = 0
        storage_uris: list[str] = []

        for (symbol, timeframe), rows in sorted(pending.items()):
            frame = _rows_to_frame(rows)
            if frame.empty:
                continue

            if self._use_daily_partitions:
                for partition_date, partition_frame in _split_by_partition_date(frame):
                    uri = self._merge_and_write(
                        symbol,
                        timeframe,
                        partition_frame,
                        partition_date=partition_date,
                    )
                    partitions_written += 1
                    rows_written += len(partition_frame)
                    storage_uris.append(uri)
                    logger.info(
                        "Saved %d %s %s bar(s) to %s",
                        len(partition_frame),
                        symbol,
                        timeframe,
                        uri,
                    )
            else:
                uri = self._merge_and_write(symbol, timeframe, frame)
                partitions_written = 1
                rows_written += len(frame)
                storage_uris.append(uri)
                logger.info(
                    "Saved %d %s %s bar(s) to %s",
                    len(frame),
                    symbol,
                    timeframe,
                    uri,
                )

        summary = SessionFlushSummary(
            rows_buffered=rows_buffered,
            rows_written=rows_written,
            partitions_written=partitions_written,
            storage_uris=tuple(storage_uris),
        )
        logger.info(
            "Session OHLCV flush complete: %d row(s) written to %d partition(s)",
            summary.rows_written,
            summary.partitions_written,
        )
        return summary

    def _append(
        self,
        symbol: str,
        timeframe: str,
        *,
        timestamp: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        key = (symbol.upper(), timeframe)
        row = {
            "timestamp": _to_utc(timestamp),
            "open": float(open),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
        }
        with self._lock:
            bucket = self._buffers.get(key)
            if bucket is None:
                bucket = _BufferedRows()
                self._buffers[key] = bucket
            bucket.rows.append(row)

    def _merge_and_write(
        self,
        symbol: str,
        timeframe: str,
        incoming: pd.DataFrame,
        *,
        partition_date: Optional[date] = None,
    ) -> str:
        exists = (
            self._storage.exists(symbol, timeframe, partition_date=partition_date)
            if self._use_daily_partitions
            else self._storage.exists(symbol, timeframe)
        )
        if exists:
            try:
                existing = self._storage.read(
                    symbol,
                    timeframe,
                    partition_date=partition_date,
                )
            except FileNotFoundError:
                existing = pd.DataFrame(columns=list(OHLCV_COLUMNS))
            merged = pd.concat([existing, incoming], ignore_index=True)
        else:
            merged = incoming

        merged = (
            merged.drop_duplicates(subset=["timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        return self._storage.write(
            symbol,
            timeframe,
            merged,
            partition_date=partition_date,
        )


def _rows_to_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(OHLCV_COLUMNS))
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.sort_values("timestamp").reset_index(drop=True)


def _split_by_partition_date(frame: pd.DataFrame) -> list[tuple[date, pd.DataFrame]]:
    partitions: list[tuple[date, pd.DataFrame]] = []
    partition_dates = frame["timestamp"].dt.date.unique()
    for partition_date in sorted(partition_dates):
        day_frame = frame[frame["timestamp"].dt.date == partition_date].reset_index(
            drop=True
        )
        if not day_frame.empty:
            partitions.append((partition_date, day_frame))
    return partitions


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
