"""Tests for OHLC outlier repair."""

from __future__ import annotations

import pandas as pd

from data_aggregator import DataAggregator
from ohlc_sanity import repair_ohlc_bar, repair_ohlcv_dataframe
from stream_data_processor import CleanBarEvent
from datetime import datetime, timezone


def test_repair_ohlc_bar_clamps_sequence_open_low() -> None:
    open_price, high_price, low_price, close_price = repair_ohlc_bar(
        357.0,
        752.55,
        357.0,
        752.5,
    )
    assert open_price == 752.5
    assert low_price == 752.5
    assert high_price == 752.55
    assert close_price == 752.5


def test_repair_ohlcv_dataframe_repairs_all_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 6, 16, 16, 57, tzinfo=timezone.utc),
                "open": 357.0,
                "high": 752.55,
                "low": 357.0,
                "close": 752.5,
                "volume": 100.0,
            }
        ]
    )
    repaired = repair_ohlcv_dataframe(frame)
    assert repaired.loc[0, "open"] == 752.5
    assert repaired.loc[0, "low"] == 752.5


def test_data_aggregator_repairs_corrupt_one_minute_bar() -> None:
    aggregator = DataAggregator(target_timeframes=("3m",))
    bar = CleanBarEvent(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 6, 16, 16, 57, tzinfo=timezone.utc),
        open=357.0,
        high=752.55,
        low=357.0,
        close=752.5,
        volume=100.0,
    )
    aggregated = aggregator.on_bar(bar)
    partial = [item for item in aggregated if item.timeframe == "3m"][-1]
    assert partial.open == 752.5
    assert partial.low == 752.5
