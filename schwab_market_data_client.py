"""Schwab Market Data client.

Responsibility: Schwab-specific historical market data transport.

Fetches chunked price history from the Schwab Trader API and returns raw candle
payloads compatible with BackfillExecutor. Does not normalize OHLCV fields or
persist storage.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

from market_data_api_client import MarketDataApiError, _RateLimiter
from schwab_auth import SchwabAuthClient, SchwabAuthError

logger = logging.getLogger(__name__)

DEFAULT_MARKET_DATA_BASE_URL = "https://api.schwabapi.com/marketdata/v1"
DEFAULT_PRICE_HISTORY_PATH = "pricehistory"
MINUTE_CHUNK_DAYS = 10
DAILY_CHUNK_DAYS = 365 * 5


@dataclass(frozen=True)
class SchwabTimeframeSpec:
    """Schwab pricehistory parameters for one pipeline timeframe."""

    period_type: str
    frequency_type: str
    frequency: int
    chunk_days: int


TIMEFRAME_SPECS: dict[str, SchwabTimeframeSpec] = {
    "1m": SchwabTimeframeSpec("day", "minute", 1, MINUTE_CHUNK_DAYS),
    "5m": SchwabTimeframeSpec("day", "minute", 5, MINUTE_CHUNK_DAYS),
    "10m": SchwabTimeframeSpec("day", "minute", 10, MINUTE_CHUNK_DAYS),
    "15m": SchwabTimeframeSpec("day", "minute", 15, MINUTE_CHUNK_DAYS),
    "30m": SchwabTimeframeSpec("day", "minute", 30, MINUTE_CHUNK_DAYS),
    "1d": SchwabTimeframeSpec("year", "daily", 1, DAILY_CHUNK_DAYS),
}


class SchwabMarketDataClient:
    """Schwab pricehistory client with OAuth and date-range chunking."""

    def __init__(
        self,
        auth_client: SchwabAuthClient,
        *,
        base_url: str = DEFAULT_MARKET_DATA_BASE_URL,
        price_history_path: str = DEFAULT_PRICE_HISTORY_PATH,
        need_extended_hours_data: bool = False,
        need_previous_close: bool = False,
        requests_per_minute: int = 120,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._auth_client = auth_client
        self._base_url = base_url.rstrip("/") + "/"
        self._price_history_path = price_history_path.lstrip("/")
        self._need_extended_hours_data = need_extended_hours_data
        self._need_previous_close = need_previous_close
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._session = session or requests.Session()
        self._rate_limiter = _RateLimiter(requests_per_minute)

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> SchwabMarketDataClient:
        """Build a market data client from environment variables."""
        import os

        if load_dotenv:
            from schwab_auth import _load_dotenv

            _load_dotenv()

        auth_client = SchwabAuthClient.from_env(load_dotenv=False)
        return cls(
            auth_client,
            base_url=_market_data_base_url(os.getenv("SCHWAB_PRICE_HISTORY_PATH")),
            price_history_path=_price_history_path(os.getenv("SCHWAB_PRICE_HISTORY_PATH")),
            need_extended_hours_data=_env_bool("SCHWAB_NEED_EXTENDED_HOURS", False),
            need_previous_close=_env_bool("SCHWAB_NEED_PREVIOUS_CLOSE", False),
            requests_per_minute=int(os.getenv("MARKET_DATA_REQUESTS_PER_MINUTE", "120")),
            timeout=float(os.getenv("MARKET_DATA_REQUEST_TIMEOUT_SECONDS", "30")),
            max_retries=int(os.getenv("MARKET_DATA_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(os.getenv("MARKET_DATA_RETRY_BACKOFF_SECONDS", "1")),
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
        """Execute an authenticated Schwab market data HTTP request."""
        if method.upper() != "GET":
            raise MarketDataApiError(f"Unsupported HTTP method for Schwab market data: {method}")

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
        """Fetch price history candles for a symbol and UTC time range.

        Compatible with BackfillExecutor's generic pagination interface. Schwab
        does not page with tokens; this method chunks by date range instead.

        Args:
            path: Symbol path such as ``SPY`` or ``bars/SPY``.
            params: Must include ``timeframe``, ``start``, and ``end`` as ISO
                strings. Optional ``symbol`` overrides path parsing.
            collection_key: JSON key containing candles. Defaults to ``candles``.
            page_token_key: Ignored for Schwab.
            page_token_param: Ignored for Schwab.

        Returns:
            Raw Schwab candle dictionaries with ISO UTC timestamps.
        """
        del page_token_key, page_token_param

        request_params = dict(params or {})
        symbol = str(request_params.pop("symbol", self._symbol_from_path(path))).upper()
        timeframe = str(request_params.pop("timeframe"))
        start = self._parse_iso_datetime(request_params.pop("start"))
        end = self._parse_iso_datetime(request_params.pop("end"))

        spec = self._resolve_timeframe(timeframe)
        candles = self.fetch_price_history(
            symbol,
            timeframe,
            start=start,
            end=end,
        )

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
        """Fetch and merge Schwab candles across chunked date windows."""
        symbol = symbol.upper()
        spec = self._resolve_timeframe(timeframe)
        start = self._to_utc(start)
        end = self._to_utc(end)

        if start >= end:
            return []

        collected: list[dict[str, Any]] = []
        for chunk_start, chunk_end in _chunk_date_range(start, end, spec.chunk_days):
            params = {
                "symbol": symbol,
                "periodType": spec.period_type,
                "period": self._period_for_chunk(spec.period_type, chunk_start, chunk_end),
                "frequencyType": spec.frequency_type,
                "frequency": spec.frequency,
                "startDate": _to_epoch_millis(chunk_start),
                "endDate": _to_epoch_millis(chunk_end),
                "needExtendedHoursData": self._need_extended_hours_data,
                "needPreviousClose": self._need_previous_close,
            }
            payload = self._request_json(self._price_history_path, params=params)
            chunk_candles = self._extract_candles(payload)
            collected.extend(self._normalize_candles(chunk_candles))

        return _dedupe_candles(collected)

    def _request_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        """Perform a GET request with auth refresh on 401."""
        url = urljoin(self._base_url, path.lstrip("/"))
        last_error: Optional[str] = None
        refreshed = False

        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire()
            headers = {
                "Authorization": f"Bearer {self._auth_client.get_access_token(force_refresh=refreshed)}",
                "accept": "application/json",
            }
            refreshed = False

            try:
                response = self._session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < self._max_retries:
                    _sleep_backoff(self._retry_backoff_seconds, attempt)
                    continue
                raise MarketDataApiError(f"Request to {url} failed: {exc}") from exc

            if response.status_code == 401 and not refreshed:
                try:
                    self._auth_client.get_access_token(force_refresh=True)
                except SchwabAuthError as exc:
                    raise MarketDataApiError(f"Schwab auth refresh failed: {exc}") from exc
                refreshed = True
                continue

            if response.status_code == 429 and attempt < self._max_retries:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    import time

                    time.sleep(float(retry_after))
                else:
                    _sleep_backoff(self._retry_backoff_seconds, attempt)
                continue

            if response.status_code >= 500 and attempt < self._max_retries:
                _sleep_backoff(self._retry_backoff_seconds, attempt)
                continue

            if not response.ok:
                raise MarketDataApiError(
                    f"Request to {url} failed with {response.status_code}: {response.text}"
                )

            if not response.content:
                return {}

            payload = response.json()
            if not isinstance(payload, dict):
                raise MarketDataApiError(
                    f"Expected JSON object from {url}, got {type(payload).__name__}"
                )
            return payload

        raise MarketDataApiError(
            f"Request to {url} failed after {self._max_retries + 1} attempts: {last_error}"
        )

    def _extract_candles(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Return candle rows from a Schwab pricehistory response."""
        if payload.get("empty"):
            return []

        candles = payload.get("candles", [])
        if not isinstance(candles, list):
            raise MarketDataApiError(
                f"Expected list at 'candles', got {type(candles).__name__}"
            )
        return [item for item in candles if isinstance(item, dict)]

    def _normalize_candles(self, candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert Schwab epoch-millisecond timestamps to ISO UTC strings."""
        normalized: list[dict[str, Any]] = []
        for candle in candles:
            timestamp = candle.get("datetime")
            if timestamp is None:
                continue
            normalized.append(
                {
                    **candle,
                    "datetime": pd.to_datetime(timestamp, unit="ms", utc=True).isoformat(),
                }
            )
        return normalized

    def _resolve_timeframe(self, timeframe: str) -> SchwabTimeframeSpec:
        spec = TIMEFRAME_SPECS.get(timeframe)
        if spec is None:
            supported = ", ".join(sorted(TIMEFRAME_SPECS))
            raise MarketDataApiError(
                f"Unsupported Schwab timeframe '{timeframe}'. Supported: {supported}"
            )
        return spec

    def _period_for_chunk(
        self,
        period_type: str,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> int:
        """Choose a valid Schwab period value for the chunk size."""
        day_span = max(1, (chunk_end.date() - chunk_start.date()).days or 1)
        if period_type == "day":
            for allowed in (1, 2, 3, 4, 5, 10):
                if day_span <= allowed:
                    return allowed
            return 10
        if period_type == "year":
            year_span = max(1, chunk_end.year - chunk_start.year + 1)
            for allowed in (1, 2, 3, 5, 10, 15, 20):
                if year_span <= allowed:
                    return allowed
            return 20
        return 1

    def _symbol_from_path(self, path: str) -> str:
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            raise MarketDataApiError("Price history path must include a symbol")
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


def build_schwab_backfill_executor(storage: Any) -> Any:
    """Wire BackfillExecutor for Schwab pricehistory and candle field mapping.

    Args:
        storage: CloudStorageRepository used for OHLCV persistence.

    Returns:
        Configured BackfillExecutor instance.
    """
    from backfill_executor import BackfillExecutor
    from market_data_transformer import SCHWAB_PRICE_HISTORY_FIELDS

    client = SchwabMarketDataClient.from_env()
    return BackfillExecutor(
        client,
        storage,
        field_map=SCHWAB_PRICE_HISTORY_FIELDS,
        bars_path_template="{symbol}",
        collection_key="candles",
    )


def _chunk_date_range(
    start: datetime,
    end: datetime,
    chunk_days: int,
) -> list[tuple[datetime, datetime]]:
    """Split a UTC datetime range into provider-sized chunks."""
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    delta = timedelta(days=chunk_days)

    while cursor < end:
        chunk_end = min(cursor + delta, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end

    return chunks


def _dedupe_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate candles while preserving chronological order."""
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for candle in sorted(candles, key=lambda row: str(row.get("datetime", ""))):
        key = str(candle.get("datetime"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(candle)
    return unique


def _to_epoch_millis(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _sleep_backoff(base_seconds: float, attempt: int) -> None:
    import time

    time.sleep(base_seconds * (2**attempt))


def _env_bool(name: str, default: bool) -> bool:
    import os

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _market_data_base_url(price_history_setting: Optional[str]) -> str:
    import os

    if price_history_setting and price_history_setting.startswith("http"):
        return price_history_setting.rsplit("/", 1)[0] + "/"

    configured = os.getenv("SCHWAB_MARKET_DATA_BASE_URL")
    if configured:
        return configured.rstrip("/") + "/"

    return DEFAULT_MARKET_DATA_BASE_URL


def _price_history_path(price_history_setting: Optional[str]) -> str:
    default = DEFAULT_PRICE_HISTORY_PATH
    if not price_history_setting:
        return default

    if price_history_setting.startswith("http"):
        return price_history_setting.rstrip("/").split("/")[-1] or default

    return price_history_setting.strip("/").split("/")[-1] or default


if __name__ == "__main__":
    import os

    from schwab_auth import _load_dotenv

    _load_dotenv()
    symbol = os.getenv("WATCHLIST_SYMBOLS", "SPY").split(",")[0].strip()
    client = SchwabMarketDataClient.from_env()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=2)
    candles = client.fetch_price_history(symbol, "1m", start=start, end=end)
    print(f"Fetched {len(candles)} candles for {symbol}")
    if candles:
        print("First:", candles[0])
        print("Last:", candles[-1])
