"""Backfill Executor.

Responsibility: Execute historical gap backfills.

Coordinates fetching missing data through the Market Data API Client,
normalizing vendor payloads, and persisting results through the Cloud Storage
Repository. Does not decide which gaps exist or own gap-detection logic.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

from bar_alignment import align_bucket_start, timeframe_timedelta

EARLIEST_FALLBACK_START = datetime(1970, 1, 1, tzinfo=timezone.utc)
BOOTSTRAP_SCAN_CHUNK_DAYS = 10

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

    def execute(
        self,
        request: BackfillRequest,
        *,
        index: Optional[int] = None,
        total: Optional[int] = None,
    ) -> BackfillResult:
        """Fetch, normalize, and persist one backfill request.

        Args:
            request: Target symbol, timeframe, and missing time range.
            index: Optional 1-based progress index for logging.
            total: Optional total request count for logging.

        Returns:
            Metadata describing how many rows were written and where.

        Raises:
            MarketDataApiError: If the remote API request fails.
            ValueError: If vendor payloads cannot be normalized.
        """
        progress = ""
        if index is not None and total is not None:
            progress = f" [{index}/{total}]"

        if self._request_satisfied_by_storage(request):
            logger.info(
                "Skipping Schwab pricehistory%s: %s %s already stored (%s -> %s)",
                progress,
                request.symbol,
                request.timeframe,
                request.start.isoformat(),
                request.end.isoformat(),
            )
            return BackfillResult(
                request=request,
                rows_written=0,
                storage_uri="",
            )

        logger.info(
            "Fetching Schwab pricehistory%s: %s %s (%s -> %s)",
            progress,
            request.symbol,
            request.timeframe,
            request.start.isoformat(),
            request.end.isoformat(),
        )
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
            logger.info(
                "No %s %s bars returned for %s -> %s",
                request.symbol,
                request.timeframe,
                request.start.isoformat(),
                request.end.isoformat(),
            )
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
        if ohlcv.empty:
            logger.info(
                "No %s %s bars to write for %s -> %s after merge",
                request.symbol,
                request.timeframe,
                request.start.isoformat(),
                request.end.isoformat(),
            )
            return BackfillResult(
                request=request,
                rows_written=0,
                storage_uri="",
            )
        storage_uri = self._storage.write(
            request.symbol,
            request.timeframe,
            ohlcv,
            partition_date=partition_date,
        )
        logger.info(
            "Wrote %d %s %s bar(s) to %s",
            len(ohlcv),
            request.symbol,
            request.timeframe,
            storage_uri,
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
        not_before: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Discover the earliest historical bars available from the provider.

        Args:
            symbol: Ticker symbol to query.
            timeframe: Bar interval label (e.g. "1m").
            end: Optional upper bound used by the fallback probe request.
            not_before: Earliest UTC timestamp to scan from when walking forward.

        Returns:
            Earliest available UTC timestamp, or None if it cannot be determined.
        """
        symbol = symbol.upper()
        end = _to_utc_datetime(end or datetime.now(timezone.utc))
        floor = _to_utc_datetime(not_before or EARLIEST_FALLBACK_START)

        earliest = self._fetch_earliest_from_availability(symbol, timeframe)
        if earliest is not None:
            return max(_to_utc_datetime(earliest), floor)

        return self._scan_forward_for_earliest_bar(
            symbol,
            timeframe,
            not_before=floor,
            end=end,
        )

    def execute_many(self, requests: list[BackfillRequest]) -> list[BackfillResult]:
        """Execute multiple backfill requests sequentially.

        Args:
            requests: Backfill operations planned by the orchestrator.

        Returns:
            One result per request, in the same order.
        """
        total = len(requests)
        return [
            self.execute(request, index=index, total=total)
            for index, request in enumerate(requests, start=1)
        ]

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

    def _scan_forward_for_earliest_bar(
        self,
        symbol: str,
        timeframe: str,
        *,
        not_before: datetime,
        end: datetime,
    ) -> Optional[datetime]:
        """Walk forward in small chunks until the first provider bar is found."""
        cursor = _to_utc_datetime(not_before)
        end = _to_utc_datetime(end)
        if cursor >= end:
            return None

        path = self._bars_path_template.format(symbol=symbol.upper())
        delta = timedelta(days=BOOTSTRAP_SCAN_CHUNK_DAYS)

        while cursor < end:
            chunk_end = min(cursor + delta, end)
            logger.info(
                "Scanning for earliest %s %s data: %s -> %s",
                symbol,
                timeframe,
                cursor.isoformat(),
                chunk_end.isoformat(),
            )
            try:
                raw_bars = self._api_client.fetch_paginated(
                    path,
                    params={
                        "timeframe": timeframe,
                        "start": cursor.isoformat(),
                        "end": chunk_end.isoformat(),
                    },
                    collection_key=self._collection_key,
                    page_token_key=None,
                )
            except Exception as exc:
                logger.debug("Earliest scan chunk failed for %s: %s", symbol, exc)
                cursor = chunk_end
                continue

            if raw_bars:
                try:
                    ohlcv = self._transformer.from_bars(
                        raw_bars,
                        field_map=self._field_map,
                    )
                except ValueError:
                    ohlcv = pd.DataFrame()

                if not ohlcv.empty:
                    earliest = ohlcv.iloc[0]["timestamp"].to_pydatetime()
                    logger.info(
                        "Found earliest %s %s bar at %s",
                        symbol,
                        timeframe,
                        earliest.isoformat(),
                    )
                    return _to_utc_datetime(earliest)

            cursor = chunk_end

        return None

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
        start = _to_utc_timestamp(request.start)
        end = _to_utc_timestamp(request.end)
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

    def _request_satisfied_by_storage(self, request: BackfillRequest) -> bool:
        """Return whether stored bars already cover the requested window."""
        partition_date = self._resolve_partition_date(request)
        try:
            if self._use_daily_partitions and partition_date is not None:
                existing = self._storage.read(
                    request.symbol,
                    request.timeframe,
                    partition_date=partition_date,
                )
            else:
                existing = self._storage.read(
                    request.symbol,
                    request.timeframe,
                    start=request.start,
                    end=request.end,
                )
        except FileNotFoundError:
            return False

        if existing.empty:
            return False

        start = _to_utc_timestamp(request.start)
        end = _to_utc_timestamp(request.end)
        in_window = existing[
            (existing["timestamp"] >= start) & (existing["timestamp"] < end)
        ]
        if in_window.empty:
            return False

        interval = timeframe_timedelta(request.timeframe)
        expected = _expected_bucket_starts(
            request.start,
            request.end,
            request.timeframe,
            interval,
        )
        if not expected:
            return True

        present = {
            align_bucket_start(
                _to_utc_datetime(timestamp.to_pydatetime()),
                request.timeframe,
            )
            for timestamp in in_window["timestamp"]
        }
        return all(bucket in present for bucket in expected)


def _to_utc_timestamp(value: datetime) -> pd.Timestamp:
    """Normalize a datetime to a timezone-aware UTC pandas Timestamp."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _to_utc_datetime(value: datetime) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC").to_pydatetime()
    return timestamp.tz_convert("UTC").to_pydatetime()


def _expected_bucket_starts(
    start: datetime,
    end: datetime,
    timeframe: str,
    interval: timedelta,
) -> list[datetime]:
    """Return aligned bar left-edges expected in [start, end)."""
    if interval <= timedelta(0):
        raise ValueError("interval must be positive")

    cursor = align_bucket_start(_to_utc_datetime(start), timeframe)
    end_dt = _to_utc_datetime(end)
    expected: list[datetime] = []
    while cursor < end_dt:
        expected.append(cursor)
        cursor += interval
    return expected


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
