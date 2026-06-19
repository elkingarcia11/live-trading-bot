"""Tests for Schwab market data chunking and OHLCV normalization."""

from __future__ import annotations

from datetime import datetime, timezone

from market_data_transformer import MarketDataTransformer, SCHWAB_PRICE_HISTORY_FIELDS
from schwab_market_data_client import (
    SchwabMarketDataClient,
    _chunk_date_range,
    _dedupe_candles,
    _schwab_query_bool,
    _to_epoch_millis,
)


class _FakeAuthClient:
    def get_access_token(self, *, force_refresh: bool = False) -> str:
        return "fake-token"


def test_schwab_query_bool_uses_lowercase_strings() -> None:
    assert _schwab_query_bool(True) == "true"
    assert _schwab_query_bool(False) == "false"


def test_pricehistory_request_includes_extended_hours_flag() -> None:
    captured: list[dict[str, object]] = []

    class _Session:
        def get(self, url, *, params=None, headers=None, timeout=None):
            captured.append(dict(params or {}))

            class _Response:
                ok = True
                status_code = 200
                content = b'{"candles": []}'
                headers = {}

                def json(self):
                    return {"candles": []}

            return _Response()

    client = SchwabMarketDataClient(
        _FakeAuthClient(),
        need_extended_hours_data=True,
        session=_Session(),
    )
    start = datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)
    client.fetch_price_history("SPY", "1m", start=start, end=end)

    assert captured
    assert captured[0]["needExtendedHoursData"] == "true"
    assert captured[0]["needPreviousClose"] == "false"


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


def test_aggregate_minute_candles_builds_three_minute_bars() -> None:
    from schwab_market_data_client import _aggregate_minute_candles

    candles = [
        {
            "datetime": "2024-01-15T14:30:00+00:00",
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 100,
        },
        {
            "datetime": "2024-01-15T14:31:00+00:00",
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 200,
        },
        {
            "datetime": "2024-01-15T14:32:00+00:00",
            "open": 101.5,
            "high": 103.0,
            "low": 101.0,
            "close": 102.5,
            "volume": 150,
        },
        {
            "datetime": "2024-01-15T14:33:00+00:00",
            "open": 102.5,
            "high": 104.0,
            "low": 102.0,
            "close": 103.0,
            "volume": 175,
        },
    ]
    aggregated = _aggregate_minute_candles(candles, 3)
    assert len(aggregated) == 2
    assert aggregated[0]["open"] == 100.0
    assert aggregated[0]["close"] == 102.5
    assert aggregated[0]["volume"] == 450
    assert aggregated[1]["open"] == 102.5
    assert aggregated[1]["close"] == 103.0
