"""IBKR TWS historical market data client.

Responsibility: Historical OHLCV transport via reqHistoricalData for backfill.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, TYPE_CHECKING

import pandas as pd

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime
from ibkr_tws_contracts import equity_contract
from market_data_api_client import MarketDataApiError, _RateLimiter
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from config import AppConfig

DERIVED_MINUTE_TIMEFRAMES: dict[str, int] = {"3m": 3}

TIMEFRAME_BAR_SIZE: dict[str, str] = {
    "1m": "1 min",
    "5m": "5 mins",
    "10m": "10 mins",
    "15m": "15 mins",
    "30m": "30 mins",
    "1h": "1 hour",
    "1d": "1 day",
}


@dataclass(frozen=True)
class IbkrTwsHistoricalSpec:
    bar_size: str
    chunk_days: int


TIMEFRAME_SPECS: dict[str, IbkrTwsHistoricalSpec] = {
    "1m": IbkrTwsHistoricalSpec("1 min", 1),
    "5m": IbkrTwsHistoricalSpec("5 mins", 2),
    "10m": IbkrTwsHistoricalSpec("10 mins", 5),
    "15m": IbkrTwsHistoricalSpec("15 mins", 7),
    "30m": IbkrTwsHistoricalSpec("30 mins", 14),
    "1h": IbkrTwsHistoricalSpec("1 hour", 30),
    "1d": IbkrTwsHistoricalSpec("1 day", 365),
}


class IbkrTwsMarketDataClient:
    """TWS historical market data client with chunked date windows."""

    def __init__(
        self,
        runtime: IbkrTwsRuntime,
        *,
        exchange: str = "SMART",
        currency: str = "USD",
        what_to_show: str = "TRADES",
        use_rth: int = 0,
        requests_per_minute: int = 120,
        connect_timeout_seconds: float = 30.0,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 2,
        owns_connection: bool = True,
    ) -> None:
        self._runtime = runtime
        self._exchange = exchange
        self._currency = currency
        self._what_to_show = what_to_show
        self._use_rth = use_rth
        self._rate_limiter = _RateLimiter(requests_per_minute)
        self._connect_timeout_seconds = connect_timeout_seconds
        self._host = host
        self._port = port
        self._client_id = client_id
        self._owns_connection = owns_connection

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrTwsMarketDataClient:
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        return cls.from_config(get_config(reload=True))

    @classmethod
    def from_config(cls, app: "AppConfig") -> IbkrTwsMarketDataClient:
        ibkr = app.ibkr
        runtime = IbkrTwsRuntime.from_config()
        return cls(
            runtime,
            exchange=ibkr.exchange,
            currency=ibkr.currency,
            what_to_show=ibkr.historical_what_to_show,
            use_rth=ibkr.historical_use_rth,
            requests_per_minute=app.market_data.requests_per_minute,
            connect_timeout_seconds=ibkr.connect_timeout_seconds,
            host=ibkr.host,
            port=ibkr.port,
            client_id=ibkr.market_data_client_id,
            owns_connection=True,
        )

    def __enter__(self) -> IbkrTwsMarketDataClient:
        self._ensure_connected()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_connection:
            self._runtime.disconnect_session()

    def _ensure_connected(self) -> None:
        if self._runtime.isConnected():
            return
        self._runtime.connect_session(
            host=self._host,
            port=self._port,
            client_id=self._client_id,
            timeout_seconds=self._connect_timeout_seconds,
        )
        self._runtime.set_market_data_type(
            __import__("config").get_config().ibkr.market_data_type
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        del json, headers
        if method.upper() != "GET":
            raise MarketDataApiError(
                f"Unsupported HTTP method for IBKR TWS market data: {method}"
            )
        return self.fetch_paginated(path, params=params or {})

    def fetch_paginated(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        collection_key: str = "candles",
        page_token_key: Optional[str] = "next_page_token",
        page_token_param: str = "page_token",
    ) -> list[dict[str, Any]]:
        del page_token_key, page_token_param

        request_params = dict(params or {})
        symbol = str(request_params.pop("symbol", self._symbol_from_path(path))).upper()
        timeframe = str(request_params.pop("timeframe"))
        start = self._parse_iso_datetime(request_params.pop("start"))
        end = self._parse_iso_datetime(request_params.pop("end"))
        candles = self.fetch_price_history(symbol, timeframe, start=start, end=end)
        if collection_key != "candles":
            return [{collection_key: candle} for candle in candles]
        return candles

    def fetch_price_history(
        self,
        symbol: str,
        timeframe: str,
        *,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        rollup_minutes = DERIVED_MINUTE_TIMEFRAMES.get(timeframe)
        if rollup_minutes is not None:
            minute_candles = self._fetch_native_price_history(
                symbol,
                "1m",
                start=start,
                end=end,
            )
            return _aggregate_minute_candles(minute_candles, rollup_minutes)
        return self._fetch_native_price_history(symbol, timeframe, start=start, end=end)

    def _fetch_native_price_history(
        self,
        symbol: str,
        timeframe: str,
        *,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        self._ensure_connected()
        spec = self._resolve_timeframe(timeframe)
        start = self._to_utc(start)
        end = self._to_utc(end)
        if start >= end:
            return []

        contract = equity_contract(
            symbol,
            exchange=self._exchange,
            currency=self._currency,
        )
        collected: list[dict[str, Any]] = []
        for chunk_start, chunk_end in _chunk_date_range(start, end, spec.chunk_days):
            self._rate_limiter.acquire()
            duration = _duration_for_chunk(chunk_start, chunk_end)
            end_datetime = _format_tws_end_datetime(chunk_end)
            logger.debug(
                "IBKR TWS historical %s %s %s -> %s",
                symbol,
                timeframe,
                chunk_start.isoformat(),
                chunk_end.isoformat(),
            )
            try:
                bars = self._runtime.request_historical_bars(
                    contract,
                    end_datetime=end_datetime,
                    duration=duration,
                    bar_size=spec.bar_size,
                    what_to_show=self._what_to_show,
                    use_rth=self._use_rth,
                )
            except IbkrTwsError as exc:
                raise MarketDataApiError(str(exc)) from exc
            chunk_candles = [_bar_to_candle(bar, symbol=symbol) for bar in bars]
            collected.extend(
                candle
                for candle in chunk_candles
                if start <= self._parse_iso_datetime(candle["datetime"]) < end
            )
            time.sleep(0.25)
        return _dedupe_candles(collected)

    def _resolve_timeframe(self, timeframe: str) -> IbkrTwsHistoricalSpec:
        spec = TIMEFRAME_SPECS.get(timeframe)
        if spec is None:
            raise MarketDataApiError(f"Unsupported IBKR TWS timeframe: {timeframe}")
        return spec

    def _symbol_from_path(self, path: str) -> str:
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            raise MarketDataApiError("History path must include a symbol")
        return segments[-1].upper()

    def _parse_iso_datetime(self, value: object) -> datetime:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC").to_pydatetime()
        return timestamp.tz_convert("UTC").to_pydatetime()

    def _to_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def build_ibkr_tws_backfill_executor(
    storage: Any,
    app: Optional["AppConfig"] = None,
) -> Any:
    from backfill_executor import BackfillExecutor
    from market_data_transformer import IBKR_HISTORY_BAR_FIELDS

    client = IbkrTwsMarketDataClient.from_config(app) if app is not None else IbkrTwsMarketDataClient.from_env()
    client._ensure_connected()
    return BackfillExecutor(
        client,
        storage,
        field_map=IBKR_HISTORY_BAR_FIELDS,
        bars_path_template="{symbol}",
        collection_key="candles",
    )


def _bar_to_candle(bar: Any, *, symbol: str) -> dict[str, Any]:
    timestamp = _parse_bar_timestamp(bar.date)
    return {
        "datetime": timestamp.isoformat(),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
        "symbol": symbol,
    }


def _parse_bar_timestamp(value: str) -> datetime:
    text = str(value).strip()
    if text.isdigit():
        epoch = int(text)
        if epoch > 10_000_000_000:
            epoch //= 1000
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    parsed = pd.to_datetime(text, utc=True)
    return parsed.to_pydatetime()


def _format_tws_end_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y%m%d-%H:%M:%S")


def _duration_for_chunk(start: datetime, end: datetime) -> str:
    days = max((end - start).days, 1)
    if days <= 1:
        return "1 D"
    if days <= 7:
        return "1 W"
    if days <= 31:
        return "1 M"
    return "1 Y"


def _chunk_date_range(
    start: datetime,
    end: datetime,
    chunk_days: int,
) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    step = timedelta(days=max(chunk_days, 1))
    while cursor < end:
        chunk_end = min(cursor + step, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def _aggregate_minute_candles(
    candles: list[dict[str, Any]],
    interval_minutes: int,
) -> list[dict[str, Any]]:
    if not candles or interval_minutes <= 1:
        return candles

    from bar_alignment import align_bucket_start

    timeframe = f"{interval_minutes}m"
    buckets: dict[datetime, dict[str, Any]] = {}
    for candle in sorted(candles, key=lambda row: str(row.get("datetime", ""))):
        timestamp = pd.to_datetime(candle["datetime"], utc=True).to_pydatetime()
        bucket_start = align_bucket_start(timestamp, timeframe)
        open_price = float(candle["open"])
        high_price = float(candle["high"])
        low_price = float(candle["low"])
        close_price = float(candle["close"])
        volume = float(candle.get("volume", 0.0) or 0.0)
        existing = buckets.get(bucket_start)
        if existing is None:
            buckets[bucket_start] = {
                "datetime": bucket_start.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }
            continue
        existing["high"] = max(float(existing["high"]), high_price)
        existing["low"] = min(float(existing["low"]), low_price)
        existing["close"] = close_price
        existing["volume"] = float(existing["volume"]) + volume
    return [buckets[key] for key in sorted(buckets)]


def _dedupe_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for candle in sorted(candles, key=lambda row: str(row.get("datetime", ""))):
        key = str(candle.get("datetime"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(candle)
    return unique
