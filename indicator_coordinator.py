"""Indicator Coordinator.

Responsibility: Indicator configuration and job dispatch.

Manages which symbols require which indicators and parameters, maintains local
bar buffers, and dispatches calculation jobs to the indicator calculator. Does
not define strategy rules or evaluate trading signals.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from data_aggregator import AggregatedBar
from ohlc_sanity import repair_ohlc_bar
from indicator_calculator import (
    DEFAULT_DEMA_PERIOD,
    DEFAULT_DEMA_SOURCE,
    DEFAULT_SUPERTREND_ATR_PERIOD,
    DEFAULT_SUPERTREND_CHANGE_ATR,
    DEFAULT_SUPERTREND_MULTIPLIER,
    DEFAULT_SUPERTREND_SOURCE,
    IndicatorCalculator,
)
from ohlcv_schema import OHLCV_COLUMNS
from stream_data_processor import CleanBarEvent


@dataclass(frozen=True)
class IndicatorJob:
    """One indicator calculation request for a symbol and timeframe."""

    name: str
    timeframe: str
    params: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class SymbolIndicatorConfig:
    """Indicator jobs configured for one symbol."""

    symbol: str
    jobs: tuple[IndicatorJob, ...]


@dataclass
class IndicatorSnapshot:
    """Latest indicator values produced for one symbol and timeframe."""

    symbol: str
    timeframe: str
    values: dict[str, Any] = field(default_factory=dict)


def build_dema_job(
    timeframe: str,
    *,
    period: int = DEFAULT_DEMA_PERIOD,
    source: str = DEFAULT_DEMA_SOURCE,
) -> IndicatorJob:
    """Build a DEMA indicator job with configurable period and source column."""
    return IndicatorJob(
        name="dema",
        timeframe=timeframe,
        params=(("period", period), ("source", source)),
    )


def build_supertrend_job(
    timeframe: str,
    *,
    atr_period: int = DEFAULT_SUPERTREND_ATR_PERIOD,
    source: str = DEFAULT_SUPERTREND_SOURCE,
    multiplier: float = DEFAULT_SUPERTREND_MULTIPLIER,
    change_atr: bool = DEFAULT_SUPERTREND_CHANGE_ATR,
) -> IndicatorJob:
    """Build a Supertrend indicator job with configurable ATR and source."""
    return IndicatorJob(
        name="supertrend",
        timeframe=timeframe,
        params=(
            ("atr_period", atr_period),
            ("source", source),
            ("multiplier", multiplier),
            ("change_atr", change_atr),
        ),
    )


class IndicatorCoordinator:
    """Coordinates indicator configuration, buffering, and calculation jobs."""

    def __init__(
        self,
        *,
        calculator: Optional[IndicatorCalculator] = None,
        max_bars: int = 500,
    ) -> None:
        """Initialize the coordinator.

        Args:
            calculator: Stateless indicator calculator used for all jobs.
            max_bars: Maximum buffered bars kept per symbol and timeframe.
        """
        self._calculator = calculator or IndicatorCalculator()
        self._max_bars = max_bars
        self._configs: dict[str, SymbolIndicatorConfig] = {}
        self._buffers: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
        self._latest: dict[tuple[str, str], IndicatorSnapshot] = {}
        self._lock = threading.Lock()

    def register(self, config: SymbolIndicatorConfig) -> None:
        """Register indicator jobs for a symbol.

        Args:
            config: Symbol-specific indicator job configuration.
        """
        with self._lock:
            self._configs[config.symbol.upper()] = config

    def on_minute_bar(self, bar: CleanBarEvent) -> Optional[IndicatorSnapshot]:
        """Buffer a 1-minute bar and dispatch configured 1-minute indicator jobs.

        Args:
            bar: Validated 1-minute bar.

        Returns:
            Latest indicator snapshot when jobs exist for the symbol/timeframe.
        """
        return self._dispatch(
            symbol=bar.symbol,
            timeframe="1m",
            row=self._row_from_clean_bar(bar),
        )

    def on_aggregated_bar(self, bar: AggregatedBar) -> Optional[IndicatorSnapshot]:
        """Buffer an aggregated bar and dispatch matching indicator jobs.

        Args:
            bar: Aggregated higher-timeframe bar.

        Returns:
            Latest indicator snapshot when jobs exist for the symbol/timeframe.
        """
        symbol = bar.symbol.upper()
        config = self._configs.get(symbol)
        if config is None:
            return None

        matching_jobs = [job for job in config.jobs if job.timeframe == bar.timeframe]
        if not matching_jobs:
            return None

        if not bar.is_complete:
            with self._lock:
                return self._latest.get((symbol, bar.timeframe))

        return self._dispatch(
            symbol=symbol,
            timeframe=bar.timeframe,
            row=self._row_from_aggregated_bar(bar),
        )

    def get_latest(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[IndicatorSnapshot]:
        """Return the latest indicator snapshot for a symbol and timeframe."""
        with self._lock:
            return self._latest.get((symbol.upper(), timeframe))

    def latest_close(self, symbol: str, timeframe: str) -> float | None:
        """Return the close of the newest buffered bar for a symbol/timeframe."""
        key = (symbol.upper(), timeframe)
        with self._lock:
            buffer = self._buffers.get(key)
            if not buffer:
                return None
            return float(buffer[-1]["close"])

    def _dispatch(
        self,
        *,
        symbol: str,
        timeframe: str,
        row: dict[str, Any],
    ) -> Optional[IndicatorSnapshot]:
        """Append a bar, run matching jobs, and store the latest snapshot."""
        symbol = symbol.upper()
        config = self._configs.get(symbol)
        if config is None:
            return None

        matching_jobs = [job for job in config.jobs if job.timeframe == timeframe]
        if not matching_jobs:
            return None

        key = (symbol, timeframe)
        with self._lock:
            buffer = self._buffers[key]
            if buffer and buffer[-1]["timestamp"] == row["timestamp"]:
                buffer[-1] = row
            else:
                buffer.append(row)
            while len(buffer) > self._max_bars:
                buffer.popleft()
            bars = pd.DataFrame(list(buffer), columns=list(OHLCV_COLUMNS))
            bars = (
                bars.drop_duplicates(subset=["timestamp"], keep="last")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )

        values: dict[str, Any] = {}
        for job in matching_jobs:
            values.update(self._run_job(job, bars))

        snapshot = IndicatorSnapshot(symbol=symbol, timeframe=timeframe, values=values)
        with self._lock:
            self._latest[key] = snapshot
        return snapshot

    def _run_job(self, job: IndicatorJob, bars: pd.DataFrame) -> dict[str, Any]:
        """Execute one indicator job and flatten the latest values."""
        params = dict(job.params)
        result = self._calculator.latest_value(job.name, bars, **params)
        if result is None:
            return {}

        if isinstance(result, dict):
            return result

        return {job.name: result}

    def _row_from_clean_bar(self, bar: CleanBarEvent) -> dict[str, Any]:
        """Convert a clean 1-minute event into a buffer row."""
        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        )
        return {
            "timestamp": bar.timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": bar.volume,
        }

    def _row_from_aggregated_bar(self, bar: AggregatedBar) -> dict[str, Any]:
        """Convert an aggregated bar into a buffer row."""
        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        )
        return {
            "timestamp": bar.timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": bar.volume,
        }


if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone

    from data_aggregator import DataAggregator

    coordinator = IndicatorCoordinator()
    coordinator.register(
        SymbolIndicatorConfig(
            symbol="AAPL",
            jobs=(
                build_dema_job("5m", period=200, source="close"),
                build_supertrend_job("5m", atr_period=12, source="hl2", multiplier=3.0),
            ),
        )
    )

    base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    aggregator = DataAggregator(target_timeframes=("5m",))

    for offset in range(30):
        minute_bar = CleanBarEvent(
            symbol="AAPL",
            timeframe="1m",
            timestamp=base + timedelta(minutes=offset),
            open=180 + offset * 0.1,
            high=181 + offset * 0.1,
            low=179 + offset * 0.1,
            close=180.5 + offset * 0.1,
            volume=1000,
        )
        for aggregated in aggregator.on_bar(minute_bar):
            snapshot = coordinator.on_aggregated_bar(aggregated)
            if snapshot and aggregated.is_complete:
                print(f"{snapshot.timeframe} indicators: {snapshot.values}")
