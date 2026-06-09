"""Historical Orchestrator.

Responsibility: High-level historical workflow coordination.

Accepts a target symbol and range, inspects storage, detects gaps, and plans
or executes backfill work. Does not perform HTTP requests or vendor payload
normalization directly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

from backfill_executor import BackfillExecutor, BackfillRequest, BackfillResult
from cloud_storage_repository import CloudStorageRepository
from gap_detector import GapDetector, GapReport


@dataclass(frozen=True)
class HistoricalSyncPlan:
    """Backfill work derived from storage inspection and gap analysis."""

    symbol: str
    timeframe: str
    requested_start: datetime
    range_start: datetime
    range_end: datetime
    bootstrapped_from_earliest: bool
    gaps: GapReport
    backfill_requests: tuple[BackfillRequest, ...]


class HistoricalOrchestrator:
    """Coordinates storage inspection, gap detection, and backfill execution."""

    def __init__(
        self,
        storage: CloudStorageRepository,
        backfill_executor: BackfillExecutor,
        *,
        gap_detector: Optional[GapDetector] = None,
        use_daily_partitions: bool = True,
        session_start: time = time(9, 30),
        session_end: time = time(16, 0),
        trading_days_only: bool = True,
        bootstrap_if_empty: bool = True,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            storage: Repository used to inspect and read stored OHLCV data.
            backfill_executor: Executor that performs fetch/normalize/persist work.
            gap_detector: Optional pure gap-detection helper.
            use_daily_partitions: Inspect and plan backfills per daily partition.
            session_start: Expected UTC session open for intraday gap detection.
            session_end: Expected UTC session close for intraday gap detection.
            trading_days_only: Ignore weekends when building the expected timeline.
            bootstrap_if_empty: When storage is empty, backfill from the provider's
                earliest available data instead of only the requested start date.
        """
        self._storage = storage
        self._backfill_executor = backfill_executor
        self._gap_detector = gap_detector or GapDetector()
        self._use_daily_partitions = use_daily_partitions
        self._session_start = session_start
        self._session_end = session_end
        self._trading_days_only = trading_days_only
        self._bootstrap_if_empty = bootstrap_if_empty

    def plan(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> HistoricalSyncPlan:
        """Inspect storage, detect gaps, and build backfill requests.

        Args:
            symbol: Target ticker symbol.
            timeframe: Expected bar interval (e.g. "1m").
            start: Inclusive UTC lower bound for the sync window.
            end: Inclusive UTC upper bound for the sync window.

        Returns:
            A plan describing detected gaps and the backfill work to run.
        """
        symbol = symbol.upper()
        interval = self._parse_timeframe(timeframe)
        effective_start = start
        bootstrapped = False

        present_dates, timestamps_by_date = self._inspect_storage(
            symbol,
            timeframe,
            start.date(),
            end.date(),
        )

        if not present_dates and self._bootstrap_if_empty:
            earliest = self._backfill_executor.discover_earliest_available(
                symbol,
                timeframe,
                end=end,
            )
            if earliest is not None and earliest < end:
                effective_start = earliest
                bootstrapped = True
                logger.info(
                    "Storage empty for %s; bootstrapping from earliest available %s",
                    symbol,
                    earliest.isoformat(),
                )
            elif earliest is None:
                logger.warning(
                    "Storage empty for %s but earliest available data could not be determined",
                    symbol,
                )

        gaps = self._gap_detector.analyze(
            range_start=effective_start.date(),
            range_end=end.date(),
            present_dates=present_dates,
            present_timestamps_by_date=timestamps_by_date,
            interval=interval,
            session_start=self._session_start,
            session_end=self._session_end,
            trading_days_only=self._trading_days_only,
        )

        backfill_requests = self._build_backfill_requests(
            symbol=symbol,
            timeframe=timeframe,
            gaps=gaps,
        )

        return HistoricalSyncPlan(
            symbol=symbol,
            timeframe=timeframe,
            requested_start=start,
            range_start=effective_start,
            range_end=end,
            bootstrapped_from_earliest=bootstrapped,
            gaps=gaps,
            backfill_requests=tuple(backfill_requests),
        )

    def run(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> tuple[HistoricalSyncPlan, list[BackfillResult]]:
        """Plan and execute all required backfills for a symbol and range.

        Args:
            symbol: Target ticker symbol.
            timeframe: Expected bar interval (e.g. "1m").
            start: Inclusive UTC lower bound for the sync window.
            end: Inclusive UTC upper bound for the sync window.

        Returns:
            The generated plan and one backfill result per planned request.
        """
        plan = self.plan(symbol, timeframe, start, end)
        if not plan.backfill_requests:
            return plan, []

        results = self._backfill_executor.execute_many(list(plan.backfill_requests))
        return plan, results

    def _inspect_storage(
        self,
        symbol: str,
        timeframe: str,
        range_start: date,
        range_end: date,
    ) -> tuple[list[date], dict[date, list[datetime]]]:
        """Read stored partitions and collect present dates and timestamps."""
        present_dates: list[date] = []
        timestamps_by_date: dict[date, list[datetime]] = {}

        expected_dates = self._gap_detector.build_expected_dates(
            range_start,
            range_end,
            trading_days_only=self._trading_days_only,
        )

        for day in expected_dates:
            if self._use_daily_partitions:
                partition_exists = self._storage.exists(
                    symbol,
                    timeframe,
                    partition_date=day,
                )
                if not partition_exists:
                    continue
                try:
                    df = self._storage.read(
                        symbol,
                        timeframe,
                        partition_date=day,
                    )
                except FileNotFoundError:
                    continue
            else:
                if day != expected_dates[0]:
                    continue
                if not self._storage.exists(symbol, timeframe):
                    continue
                try:
                    df = self._storage.read(symbol, timeframe)
                except FileNotFoundError:
                    continue

            if df.empty:
                continue

            present_dates.append(day)
            timestamps_by_date[day] = [
                self._to_utc_datetime(timestamp)
                for timestamp in df["timestamp"].tolist()
            ]

        return present_dates, timestamps_by_date

    def _build_backfill_requests(
        self,
        *,
        symbol: str,
        timeframe: str,
        gaps: GapReport,
    ) -> list[BackfillRequest]:
        """Translate gap analysis into concrete backfill requests."""
        requests: list[BackfillRequest] = []

        for missing_day in gaps.missing_dates:
            start = datetime.combine(
                missing_day,
                self._session_start,
                tzinfo=timezone.utc,
            )
            end = datetime.combine(
                missing_day,
                self._session_end,
                tzinfo=timezone.utc,
            )
            requests.append(
                BackfillRequest(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                    partition_date=missing_day if self._use_daily_partitions else None,
                )
            )

        for gap in gaps.missing_intervals:
            requests.append(
                BackfillRequest(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=gap.start,
                    end=gap.end,
                    partition_date=gap.start.date() if self._use_daily_partitions else None,
                )
            )

        return requests

    def _parse_timeframe(self, timeframe: str) -> timedelta:
        """Convert a timeframe label into a timedelta."""
        if len(timeframe) < 2:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        unit = timeframe[-1]
        value = int(timeframe[:-1])

        if unit == "m":
            return timedelta(minutes=value)
        if unit == "h":
            return timedelta(hours=value)
        if unit == "d":
            return timedelta(days=value)

        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def _to_utc_datetime(self, timestamp: datetime | pd.Timestamp) -> datetime:
        """Normalize a timestamp to a timezone-aware UTC datetime."""
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)


if __name__ == "__main__":
    from market_data_api_client import MarketDataApiClient

    storage = CloudStorageRepository("my-trading-bucket")
    executor = BackfillExecutor(
        MarketDataApiClient("https://api.example.com/v1", "your-api-key"),
        storage,
    )
    orchestrator = HistoricalOrchestrator(storage, executor)

    start = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    end = datetime(2024, 1, 19, 16, 0, tzinfo=timezone.utc)
    plan = orchestrator.plan("AAPL", "1m", start, end)

    print(f"Requested start: {plan.requested_start}")
    print(f"Effective start: {plan.range_start}")
    print(f"Bootstrapped from earliest: {plan.bootstrapped_from_earliest}")
    print(f"Missing dates: {plan.gaps.missing_dates}")
    print(f"Missing intervals: {plan.gaps.missing_intervals}")
    print(f"Backfill requests: {len(plan.backfill_requests)}")

    for request in plan.backfill_requests:
        print(f"- {request.symbol} {request.start} -> {request.end}")
