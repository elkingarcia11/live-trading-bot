"""Tests for Schwab market data chunking and OHLCV normalization."""

from __future__ import annotations

from datetime import datetime, timezone

from market_data_transformer import MarketDataTransformer, SCHWAB_PRICE_HISTORY_FIELDS
from schwab_market_data_client import (
    SchwabMarketDataClient,
    _chunk_date_range,
    _dedupe_candles,
    _to_epoch_millis,
)


class _FakeAuthClient:
    def get_access_token(self, *, force_refresh: bool = False) -> str:
        return "fake-token"


def test_chunk_date_range_splits_long_minute_window() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 25, tzinfo=timezone.utc)
    chunks = _chunk_date_range(start, end, chunk_days=10)
    assert len(chunks) == 3
    assert chunks[0][0] == start
    assert chunks[-1][1] == end


def test_epoch_millis_conversion() -> None:
    value = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    assert _to_epoch_millis(value) == 1705329000000


def test_normalize_and_transform_schwab_candles() -> None:
    client = SchwabMarketDataClient(_FakeAuthClient())
    raw = [
        {
            "datetime": 1705329000000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 1000,
        }
    ]
    normalized = client._normalize_candles(raw)
    ohlcv = MarketDataTransformer().from_bars(
        normalized,
        field_map=SCHWAB_PRICE_HISTORY_FIELDS,
    )
    assert len(ohlcv) == 1
    assert ohlcv.iloc[0]["open"] == 100.0
    assert ohlcv.iloc[0]["close"] == 100.5


def test_dedupe_candles_keeps_latest_order() -> None:
    candles = [
        {"datetime": "2024-01-15T14:30:00+00:00", "close": 1.0},
        {"datetime": "2024-01-15T14:31:00+00:00", "close": 2.0},
        {"datetime": "2024-01-15T14:30:00+00:00", "close": 1.5},
    ]
    deduped = _dedupe_candles(candles)
    assert len(deduped) == 2
    assert deduped[0]["close"] == 1.0
