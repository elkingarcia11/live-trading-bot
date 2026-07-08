"""Tests for IBKR historical market data normalization."""

from __future__ import annotations

from datetime import datetime, timezone

from ibkr_market_data_client import (
    IbkrMarketDataClient,
    _format_ibkr_start_time,
    _parse_ibkr_bar_timestamp,
)


def test_format_ibkr_start_time() -> None:
    value = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    assert _format_ibkr_start_time(value) == "20240115-14:30:00"


def test_parse_ibkr_bar_timestamp() -> None:
    parsed = _parse_ibkr_bar_timestamp(16923456000)
    assert parsed == datetime.fromtimestamp(1692345600, tz=timezone.utc)


def test_normalize_history_payload() -> None:
    client = IbkrMarketDataClient.__new__(IbkrMarketDataClient)
    candles = client._normalize_history_payload(
        {
            "priceFactor": 100,
            "volumeFactor": 1,
            "data": [
                {"o": 17340, "c": 17470, "h": 17510, "l": 17170, "v": 1000, "t": 16923456000}
            ],
        },
        symbol="AAPL",
    )
    assert len(candles) == 1
    assert candles[0]["open"] == 173.4
    assert candles[0]["close"] == 174.7
    assert candles[0]["volume"] == 1000.0


def test_fetch_paginated_delegates_to_price_history() -> None:
    client = IbkrMarketDataClient.__new__(IbkrMarketDataClient)
    captured: dict[str, object] = {}

    def _fetch(symbol: str, timeframe: str, *, start, end):
        captured["symbol"] = symbol
        captured["timeframe"] = timeframe
        return [{"datetime": "2024-01-15T14:30:00+00:00"}]

    client.fetch_price_history = _fetch  # type: ignore[method-assign]
    candles = client.fetch_paginated(
        "SPY",
        params={
            "timeframe": "1m",
            "start": "2024-01-15T14:30:00+00:00",
            "end": "2024-01-15T16:00:00+00:00",
        },
    )
    assert captured["symbol"] == "SPY"
    assert captured["timeframe"] == "1m"
    assert candles[0]["datetime"].startswith("2024-01-15")
