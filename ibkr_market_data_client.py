"""IBKR historical market data client.

Responsibility: IBKR-specific historical bar transport for backfill workflows.

Fetches chunked OHLCV history from GET /iserver/marketdata/history and returns
raw candle payloads compatible with BackfillExecutor. Does not persist storage
or evaluate strategy signals.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, TYPE_CHECKING

import pandas as pd

from ibkr_auth import IbkrAuthError, IbkrSessionClient
from ibkr_trader_client import IbkrTraderClient
from market_data_api_client import MarketDataApiError, _RateLimiter
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from config import AppConfig

MAX_HISTORY_BARS = 1000


@dataclass(frozen=True)
class IbkrTimeframeSpec:
    """IBKR /iserver/marketdata/history parameters for one timeframe."""

    bar: str
    period: str
    chunk_days: int


TIMEFRAME_SPECS: dict[str, IbkrTimeframeSpec] = {
    "1m": IbkrTimeframeSpec("1min", "1d", 1),
    "5m": IbkrTimeframeSpec("5min", "1d", 2),
    "10m": IbkrTimeframeSpec("10min", "1w", 5),
    "15m": IbkrTimeframeSpec("15min", "1w", 7),
    "30m": IbkrTimeframeSpec("30min", "1w", 14),
    "1h": IbkrTimeframeSpec("1h", "1m", 30),
    "1d": IbkrTimeframeSpec("1d", "1y", 365),
}

DERIVED_MINUTE_TIMEFRAMES: dict[str, int] = {
    "3m": 3,
}


class IbkrMarketDataClient:
    """IBKR historical market data client with chunked date-range requests."""

    def __init__(
        self,
        session_client: IbkrSessionClient,
        trader_client: IbkrTraderClient,
        *,
        history_path: str = "iserver/marketdata/history",
        listing_exchange: str = "SMART",
        history_source: str = "Trades",
        outside_rth: bool = True,
        requests_per_minute: int = 120,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        self._session = session_client
        self._trader = trader_client
        self._history_path = history_path.lstrip("/")
        self._listing_exchange = listing_exchange
        self._history_source = history_source
        self._outside_rth = outside_rth
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._rate_limiter = _RateLimiter(requests_per_minute)

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrMarketDataClient:
        """Build a market data client from config.json."""
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        return cls.from_config(get_config(reload=True))

    @classmethod
    def from_config(cls, app: "AppConfig") -> IbkrMarketDataClient:
        """Build a market data client from a loaded AppConfig."""
        ibkr = app.ibkr
        market_data = app.market_data
        session_client = IbkrSessionClient.from_env(load_dotenv=False)
        trader_client = IbkrTraderClient.from_env(load_dotenv=False)
        return cls(
            session_client,
            trader_client,
            history_path=ibkr.marketdata_history_path,
            listing_exchange=ibkr.listing_exchange,
            history_source=ibkr.history_source,
            outside_rth=ibkr.history_outside_rth,
            requests_per_minute=market_data.requests_per_minute,
            timeout=market_data.request_timeout_seconds,
            max_retries=market_data.max_retries,
            retry_backoff_seconds=market_data.retry_backoff_seconds,
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
        """Execute an authenticated IBKR market data HTTP request."""
        del json, headers
        if method.upper() != "GET":
            raise MarketDataApiError(
                f"Unsupported HTTP method for IBKR market data: {method}"
            )
        return self._request_json(path, params=params or {})

    def fetch_paginated(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        collection_key: str = "candles",
        page_token_key: Optional[str] = "next_page_token",
        page_token_param: str = "page_token",
    ) -> list[dict[str, Any]]:
        """Fetch historical candles for a symbol and UTC time range."""
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
        """Fetch and merge IBKR historical bars across chunked windows."""
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
        symbol = symbol.upper()
        spec = self._resolve_timeframe(timeframe)
        start = self._to_utc(start)
        end = self._to_utc(end)
        if start >= end:
            return []

        self._session.ensure_session()
        contract = self._trader.search_contract(symbol)

        collected: list[dict[str, Any]] = []
        for chunk_start, chunk_end in _chunk_date_range(start, end, spec.chunk_days):
            params = {
                "conid": contract.conid,
                "exchange": self._listing_exchange,
                "period": spec.period,
                "bar": spec.bar,
                "startTime": _format_ibkr_start_time(chunk_start),
                "outsideRth": str(self._outside_rth).lower(),
                "source": self._history_source,
            }
            logger.debug(
                "IBKR marketdata/history %s %s %s -> %s",
                symbol,
                timeframe,
                chunk_start.isoformat(),
                chunk_end.isoformat(),
            )
            payload = self._request_json(self._history_path, params=params)
            chunk_candles = self._normalize_history_payload(payload, symbol=symbol)
            collected.extend(
                candle
                for candle in chunk_candles
                if start <= self._parse_iso_datetime(candle["datetime"]) < end
            )

        return _dedupe_candles(collected)

    def _request_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Optional[str] = None
        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire()
            try:
                payload = self._session.request("GET", path, params=params)
            except IbkrAuthError as exc:
                last_error = str(exc)
                if attempt < self._max_retries:
                    _sleep_backoff(self._retry_backoff_seconds, attempt)
                    continue
                raise MarketDataApiError(last_error) from exc

            if not isinstance(payload, dict):
                raise MarketDataApiError("IBKR history response must be a JSON object")

            if payload.get("error"):
                raise MarketDataApiError(str(payload["error"]))

            return payload

        raise MarketDataApiError(
            f"IBKR history request failed after {self._max_retries + 1} attempts: {last_error}"
        )

    def _normalize_history_payload(
        self,
        payload: dict[str, Any],
        *,
        symbol: str,
    ) -> list[dict[str, Any]]:
        bars = payload.get("data", [])
        if not isinstance(bars, list):
            return []

        price_factor = float(payload.get("priceFactor", 1) or 1)
        if price_factor <= 0:
            price_factor = 1.0
        volume_factor = float(payload.get("volumeFactor", 1) or 1)
        if volume_factor <= 0:
            volume_factor = 1.0

        normalized: list[dict[str, Any]] = []
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            timestamp = _parse_ibkr_bar_timestamp(bar.get("t"))
            if timestamp is None:
                continue
            normalized.append(
                {
                    "datetime": timestamp.isoformat(),
                    "open": float(bar.get("o", 0.0) or 0.0) / price_factor,
                    "high": float(bar.get("h", 0.0) or 0.0) / price_factor,
                    "low": float(bar.get("l", 0.0) or 0.0) / price_factor,
                    "close": float(bar.get("c", 0.0) or 0.0) / price_factor,
                    "volume": float(bar.get("v", 0.0) or 0.0) * volume_factor,
                    "symbol": symbol,
                }
            )
        return normalized

    def _resolve_timeframe(self, timeframe: str) -> IbkrTimeframeSpec:
        spec = TIMEFRAME_SPECS.get(timeframe)
        if spec is None:
            raise MarketDataApiError(f"Unsupported IBKR timeframe: {timeframe}")
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


def build_ibkr_backfill_executor(
    storage: Any,
    app: Optional["AppConfig"] = None,
) -> Any:
    """Wire BackfillExecutor for IBKR historical bars."""
    from backfill_executor import BackfillExecutor
    from market_data_transformer import IBKR_HISTORY_BAR_FIELDS

    if app is None:
        client = IbkrMarketDataClient.from_env()
    else:
        client = IbkrMarketDataClient.from_config(app)
    return BackfillExecutor(
        client,
        storage,
        field_map=IBKR_HISTORY_BAR_FIELDS,
        bars_path_template="{symbol}",
        collection_key="candles",
    )


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


def _format_ibkr_start_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y%m%d-%H:%M:%S")


def _parse_ibkr_bar_timestamp(value: object) -> Optional[datetime]:
    if value is None:
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None

    if raw > 1_000_000_000_000:
        seconds = raw / 1000.0
    elif raw > 10_000_000_000:
        seconds = raw / 10.0
    else:
        seconds = raw
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


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


def _sleep_backoff(base_seconds: float, attempt: int) -> None:
    time.sleep(base_seconds * (2**attempt))
