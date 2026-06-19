"""Data Aggregator.

Responsibility: Higher-timeframe bar aggregation.

Consumes 1-minute bars and rolls them up into higher timeframes (5m, 1h, 1d)
using a localized buffer to track incomplete bars. Does not calculate
indicators, evaluate strategies, or persist data.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from bar_alignment import align_bucket_start, is_bucket_closing_minute, timeframe_timedelta
from ohlc_sanity import repair_ohlc_bar
from stream_data_processor import CleanBarEvent


@dataclass(frozen=True)
class AggregatedBar:
    """OHLCV bar produced from 1-minute rollup logic."""

    symbol: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_complete: bool


@dataclass
class _PartialBar:
    """In-progress higher-timeframe bar state."""

    symbol: str
    timeframe: str
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class DataAggregator:
    """Rolls 1-minute bars into higher timeframe bars."""

    DEFAULT_TARGETS = ("5m", "1h", "1d")

    def __init__(
        self,
        *,
        target_timeframes: tuple[str, ...] = DEFAULT_TARGETS,
    ) -> None:
        """Initialize the aggregator.

        Args:
            target_timeframes: Higher timeframes to build from 1-minute input.
        """
        self._target_timeframes = target_timeframes
        self._partials: dict[tuple[str, str], _PartialBar] = {}
        self._completed_through: dict[tuple[str, str], datetime] = {}
        self._lock = threading.Lock()

    def set_completed_through(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
    ) -> None:
        """Mark stored 3m bars through ``timestamp`` as already finalized."""
        key = (symbol.upper(), timeframe)
        with self._lock:
            self._completed_through[key] = align_bucket_start(timestamp, timeframe)

    def completed_through(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[datetime]:
        """Return the newest 3m bucket already persisted and replayed."""
        with self._lock:
            return self._completed_through.get((symbol.upper(), timeframe))

    def on_bar(self, bar: CleanBarEvent) -> list[AggregatedBar]:
        """Consume one 1-minute bar and emit higher-timeframe updates.

        Args:
            bar: Validated 1-minute bar from the stream processor.

        Returns:
            Aggregated bars for configured targets. Completed bars are emitted
            when a bucket closes; the current bucket is returned with
            `is_complete=False`.
        """
        if bar.timeframe != "1m":
            raise ValueError(f"DataAggregator expects 1m bars, got {bar.timeframe}")

        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        )
        if (open_price, high_price, low_price, close_price) != (
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        ):
            bar = CleanBarEvent(
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                timestamp=bar.timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=bar.volume,
            )

        emitted: list[AggregatedBar] = []
        symbol = bar.symbol.upper()

        with self._lock:
            for timeframe in self._target_timeframes:
                emitted.extend(self._process_timeframe(symbol, timeframe, bar))

        return emitted

    def _process_timeframe(
        self,
        symbol: str,
        timeframe: str,
        bar: CleanBarEvent,
    ) -> list[AggregatedBar]:
        """Update one target timeframe buffer for an incoming 1-minute bar."""
        bucket_start = self._bucket_start(bar.timestamp, timeframe)
        key = (symbol, timeframe)
        current = self._partials.get(key)
        emitted: list[AggregatedBar] = []

        if current is not None and current.bucket_start != bucket_start:
            completed = self._to_aggregated_bar(current, is_complete=True)
            if not self._should_suppress_complete(symbol, timeframe, completed.timestamp):
                emitted.append(completed)
            current = None

        if current is None:
            current = _PartialBar(
                symbol=symbol,
                timeframe=timeframe,
                bucket_start=bucket_start,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
        else:
            current.high = max(current.high, bar.high)
            current.low = min(current.low, bar.low)
            current.close = bar.close
            current.volume += bar.volume

        if is_bucket_closing_minute(bar.timestamp, timeframe):
            completed = self._to_aggregated_bar(current, is_complete=True)
            if not self._should_suppress_complete(symbol, timeframe, completed.timestamp):
                emitted.append(completed)
            self._partials.pop(key, None)
            return emitted

        self._partials[key] = current
        emitted.append(self._to_aggregated_bar(current, is_complete=False))
        return emitted

    def flush(self) -> list[AggregatedBar]:
        """Emit in-progress buckets as completed bars (for shutdown persistence)."""
        emitted: list[AggregatedBar] = []
        with self._lock:
            for partial in self._partials.values():
                emitted.append(self._to_aggregated_bar(partial, is_complete=True))
            self._partials.clear()
        return emitted

    def _to_aggregated_bar(self, partial: _PartialBar, *, is_complete: bool) -> AggregatedBar:
        """Convert internal partial state into an emitted aggregated bar."""
        return AggregatedBar(
            symbol=partial.symbol,
            timeframe=partial.timeframe,
            timestamp=partial.bucket_start,
            open=partial.open,
            high=partial.high,
            low=partial.low,
            close=partial.close,
            volume=partial.volume,
            is_complete=is_complete,
        )

    def _should_suppress_complete(
        self,
        symbol: str,
        timeframe: str,
        bucket_start: datetime,
    ) -> bool:
        """Return whether a closing bucket was already saved and replayed."""
        through = self._completed_through.get((symbol, timeframe))
        if through is None:
            return False
        return bucket_start <= through

    def _bucket_start(self, timestamp: datetime, timeframe: str) -> datetime:
        """Floor a timestamp to the start of its higher-timeframe bucket."""
        return align_bucket_start(timestamp, timeframe)

    def _parse_timeframe(self, timeframe: str) -> timedelta:
        """Convert a timeframe label into a timedelta."""
        return timeframe_timedelta(timeframe)


if __name__ == "__main__":
    from datetime import timezone as tz

    aggregator = DataAggregator(target_timeframes=("5m", "1h"))

    base = datetime(2024, 1, 15, 9, 30, tzinfo=tz.utc)
    for offset in range(6):
        bar = CleanBarEvent(
            symbol="AAPL",
            timeframe="1m",
            timestamp=base + timedelta(minutes=offset),
            open=185.0 + offset,
            high=185.5 + offset,
            low=184.8 + offset,
            close=185.2 + offset,
            volume=1000 + offset,
        )
        results = aggregator.on_bar(bar)
        completed = [item for item in results if item.is_complete]
        if completed:
            print(f"Completed bars: {[(r.timeframe, r.timestamp) for r in completed]}")
