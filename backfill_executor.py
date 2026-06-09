"""Backfill Executor.

Responsibility: Execute historical gap backfills.

Coordinates fetching missing data through the Market Data API Client,
normalizing vendor payloads, and persisting results through the Cloud Storage
Repository. Does not decide which gaps exist or own gap-detection logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

EARLIEST_FALLBACK_START = datetime(1970, 1, 1, tzinfo=timezone.utc)

from cloud_storage_repository import CloudStorageRepository
from market_data_api_client import MarketDataApiClient
from market_data_transformer import MarketDataTransformer, OhlcvFieldMap, SHORT_BAR_FIELDS


@dataclass(frozen=True)
class BackfillRequest:
    """One backfill operation for a symbol, timeframe, and time range."""

    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    partition_date: Optional[date] = None


@dataclass(frozen=True)
class BackfillResult:
    """Outcome of a single backfill execution."""

    request: BackfillRequest
    rows_written: int
    storage_uri: str


class BackfillExecutor:
    """Fetches missing historical bars and writes them to cloud storage."""

    def __init__(
        self,
        api_client: MarketDataApiClient,
        storage: CloudStorageRepository,
        *,
        transformer: Optional[MarketDataTransformer] = None,
        field_map: Optional[OhlcvFieldMap] = None,
        bars_path_template: str = "bars/{symbol}",
        availability_path_template: str = "bars/{symbol}/availability",
        collection_key: str = "bars",
        earliest_key: str = "earliest",
        use_daily_partitions: bool = True,
    ) -> None:
        """Initialize the executor with API and storage dependencies.

        Args:
            api_client: HTTP client used to fetch raw vendor bar payloads.
            storage: Repository used to persist standard OHLCV data.
            transformer: Optional transformer for vendor payload normalization.
            field_map: Provider-specific OHLCV field mapping.
            bars_path_template: Relative API path template for bar requests.
            availability_path_template: Relative API path for earliest-bar metadata.
            collection_key: JSON key containing bar items in API responses.
            earliest_key: Response key holding the earliest available timestamp.
            use_daily_partitions: Write one partition per request day when True.
        """
        self._api_client = api_client
        self._storage = storage
        self._transformer = transformer or MarketDataTransformer()
        self._field_map = field_map or SHORT_BAR_FIELDS
        self._bars_path_template = bars_path_template
        self._availability_path_template = availability_path_template
        self._collection_key = collection_key
        self._earliest_key = earliest_key
        self._use_daily_partitions = use_daily_partitions

    def execute(self, request: BackfillRequest) -> BackfillResult:
        """Fetch, normalize, and persist one backfill request.

        Args:
            request: Target symbol, timeframe, and missing time range.

        Returns:
            Metadata describing how many rows were written and where.

        Raises:
            MarketDataApiError: If the remote API request fails.
            ValueError: If vendor payloads cannot be normalized.
        """
        path = self._bars_path_template.format(symbol=request.symbol.upper())
        raw_bars = self._api_client.fetch_paginated(
            path,
            params={
                "timeframe": request.timeframe,
                "start": request.start.isoformat(),
                "end": request.end.isoformat(),
            },
            collection_key=self._collection_key,
        )

        ohlcv = self._transformer.from_bars(raw_bars, field_map=self._field_map)
        if ohlcv.empty:
            return BackfillResult(
                request=request,
                rows_written=0,
                storage_uri="",
            )

        ohlcv = self._filter_to_request_window(ohlcv, request)
        partition_date = self._resolve_partition_date(request)
        ohlcv = self._merge_with_existing_partition(
            request.symbol,
            request.timeframe,
            ohlcv,
            partition_date=partition_date,
        )
        storage_uri = self._storage.write(
            request.symbol,
            request.timeframe,
            ohlcv,
            partition_date=partition_date,
        )

        return BackfillResult(
            request=request,
            rows_written=len(ohlcv),
            storage_uri=storage_uri,
        )

    def discover_earliest_available(
        self,
        symbol: str,
        timeframe: str,
        *,
        end: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Discover the earliest historical bars available from the provider.

        Args:
            symbol: Ticker symbol to query.
            timeframe: Bar interval label (e.g. "1m").
            end: Optional upper bound used by the fallback probe request.

        Returns:
            Earliest available UTC timestamp, or None if it cannot be determined.
        """
        symbol = symbol.upper()
        end = end or datetime.now(timezone.utc)

        earliest = self._fetch_earliest_from_availability(symbol, timeframe)
        if earliest is not None:
            return earliest

        return self._probe_earliest_bar(symbol, timeframe, end=end)

    def execute_many(self, requests: list[BackfillRequest]) -> list[BackfillResult]:
        """Execute multiple backfill requests sequentially.

        Args:
            requests: Backfill operations planned by the orchestrator.

        Returns:
            One result per request, in the same order.
        """
        return [self.execute(request) for request in requests]

    def _fetch_earliest_from_availability(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[datetime]:
        """Read earliest availability metadata from the provider, if supported."""
        path = self._availability_path_template.format(symbol=symbol)
        try:
            payload = self._api_client.request(
                "GET",
                path,
                params={"timeframe": timeframe},
            )
        except Exception as exc:
            logger.debug("Availability lookup failed for %s: %s", symbol, exc)
            return None

        if not isinstance(payload, dict):
            return None

        raw_value = payload.get(self._earliest_key)
        if raw_value is None:
            return None

        return self._parse_timestamp(raw_value)

    def _probe_earliest_bar(
        self,
        symbol: str,
        timeframe: str,
        *,
        end: datetime,
    ) -> Optional[datetime]:
        """Fallback probe that infers earliest availability from the first bar."""
        path = self._bars_path_template.format(symbol=symbol)
        try:
            raw_bars = self._api_client.fetch_paginated(
                path,
                params={
                    "timeframe": timeframe,
                    "start": EARLIEST_FALLBACK_START.isoformat(),
                    "end": end.isoformat(),
                    "limit": 1,
                    "sort": "asc",
                },
                collection_key=self._collection_key,
                page_token_key=None,
            )
        except Exception as exc:
            logger.debug("Earliest bar probe failed for %s: %s", symbol, exc)
            return None

        if not raw_bars:
            return None

        try:
            ohlcv = self._transformer.from_bars(raw_bars[:1], field_map=self._field_map)
        except ValueError:
            return None

        if ohlcv.empty:
            return None

        return ohlcv.iloc[0]["timestamp"].to_pydatetime()

    def _parse_timestamp(self, value: object) -> Optional[datetime]:
        """Parse provider timestamp values into timezone-aware UTC datetimes."""
        if value is None:
            return None

        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC").to_pydatetime()
        return timestamp.tz_convert("UTC").to_pydatetime()

    def _filter_to_request_window(
        self,
        ohlcv: pd.DataFrame,
        request: BackfillRequest,
    ) -> pd.DataFrame:
        """Keep only rows that fall inside the requested backfill window."""
        start = pd.Timestamp(request.start, tz="UTC")
        end = pd.Timestamp(request.end, tz="UTC")
        filtered = ohlcv[
            (ohlcv["timestamp"] >= start) & (ohlcv["timestamp"] < end)
        ]
        return filtered.reset_index(drop=True)

    def _resolve_partition_date(self, request: BackfillRequest) -> Optional[date]:
        """Choose the storage partition for a backfill request."""
        if not self._use_daily_partitions:
            return None
        if request.partition_date is not None:
            return request.partition_date
        return request.start.date()

    def _merge_with_existing_partition(
        self,
        symbol: str,
        timeframe: str,
        incoming: pd.DataFrame,
        *,
        partition_date: Optional[date],
    ) -> pd.DataFrame:
        """Merge fetched rows into an existing partition without dropping prior bars."""
        if incoming.empty:
            return incoming

        exists = (
            self._storage.exists(symbol, timeframe, partition_date=partition_date)
            if self._use_daily_partitions
            else self._storage.exists(symbol, timeframe)
        )
        if not exists:
            return incoming

        try:
            existing = self._storage.read(
                symbol,
                timeframe,
                partition_date=partition_date,
            )
        except FileNotFoundError:
            return incoming

        merged = pd.concat([existing, incoming], ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
        return merged.sort_values("timestamp").reset_index(drop=True)


if __name__ == "__main__":
    from datetime import time, timezone

    from gap_detector import TimeGap

    # Example wiring only; requires real API credentials and bucket access.
    api_client = MarketDataApiClient("https://api.example.com/v1", "your-api-key")
    storage = CloudStorageRepository("my-trading-bucket")
    executor = BackfillExecutor(api_client, storage)

    request = BackfillRequest(
        symbol="AAPL",
        timeframe="1m",
        start=datetime.combine(date(2024, 1, 15), time(9, 30), tzinfo=timezone.utc),
        end=datetime.combine(date(2024, 1, 15), time(16, 0), tzinfo=timezone.utc),
        partition_date=date(2024, 1, 15),
    )
    print(f"Prepared backfill for {request.symbol}: {request.start} -> {request.end}")

    gap = TimeGap(start=request.start, end=request.end)
    print(f"Gap range: {gap.start} -> {gap.end}")
