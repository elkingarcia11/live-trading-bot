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
from gap_detector import GapDetector, GapReport, session_bounds_for_day


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
        need_extended_hours: bool = False,
        extended_session_start: time = time(8, 0),
        extended_session_end: time = time(1, 0),
        trading_days_only: bool = True,
        bootstrap_if_empty: bool = True,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            storage: Repository used to inspect and read stored OHLCV data.
            backfill_executor: Executor that performs fetch/normalize/persist work.
            gap_detector: Optional pure gap-detection helper.
            use_daily_partitions: Inspect and plan backfills per daily partition.
            session_start: Expected UTC regular-session open for intraday gap detection.
            session_end: Expected UTC regular-session close for intraday gap detection.
            need_extended_hours: When true, use extended session bounds for gaps and
                full-day backfills instead of regular session times.
            extended_session_start: UTC pre-market open when extended hours are enabled.
            extended_session_end: UTC after-hours close when extended hours are enabled.
                Values not after ``extended_session_start`` roll to the next UTC day.
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
        self._need_extended_hours = need_extended_hours
        self._extended_session_start = extended_session_start
        self._extended_session_end = extended_session_end
        self._trading_days_only = trading_days_only
        self._bootstrap_if_empty = bootstrap_if_empty

    @property
    def _fetch_session_start(self) -> time:
        if self._need_extended_hours:
            return self._extended_session_start
        return self._session_start

    @property
    def _fetch_session_end(self) -> time:
        if self._need_extended_hours:
            return self._extended_session_end
        return self._session_end

    def plan(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        bootstrap_start: Optional[datetime] = None,
    ) -> HistoricalSyncPlan:
        """Inspect storage, detect gaps, and build backfill requests.

        Args:
            symbol: Target ticker symbol.
            timeframe: Expected bar interval (e.g. "1m").
            start: Inclusive UTC lower bound for incremental sync windows.
            end: Inclusive UTC upper bound for the sync window.
            bootstrap_start: Earliest UTC timestamp to use when storage is empty.
        """
        symbol = symbol.upper()
        interval = self._parse_timeframe(timeframe)
        bootstrap_floor = bootstrap_start or start
        effective_start = bootstrap_floor
        bootstrapped = False

        present_dates, timestamps_by_date = self._inspect_storage(
            symbol,
            timeframe,
            bootstrap_floor.date(),
            end.date(),
        )

        if present_dates:
            latest_stored = self._latest_timestamp(timestamps_by_date)
            logger.info(
                "Storage for %s %s has %d day partition(s) through %s",
                symbol,
                timeframe,
                len(present_dates),
                latest_stored.isoformat() if latest_stored is not None else "unknown",
            )
        else:
            latest_stored = None

        if self._bootstrap_if_empty and not present_dates:
            bootstrapped = True
            logger.info(
                "Storage empty for %s; bootstrapping history from %s",
                symbol,
                bootstrap_floor.isoformat(),
            )
            discovered = self._backfill_executor.discover_earliest_available(
                symbol,
                timeframe,
                end=end,
                not_before=bootstrap_floor,
            )
            if discovered is not None and discovered > effective_start:
                effective_start = discovered
                logger.info(
                    "Provider earliest %s %s bar is %s",
                    symbol,
                    timeframe,
                    discovered.isoformat(),
                )
        elif latest_stored is not None:
            logger.info(
                "Incremental sync for %s: filling gaps after last stored bar %s",
                symbol,
                latest_stored.isoformat(),
            )

        if latest_stored is not None and present_dates:
            missing_dates_start = latest_stored.date()
            intraday_scan_start = latest_stored.date()
            logger.info(
                "Incremental gap scan for %s from %s (not re-scanning bootstrap window from %s)",
                symbol,
                missing_dates_start.isoformat(),
                effective_start.date().isoformat(),
            )
        else:
            missing_dates_start = effective_start.date()
            intraday_scan_start = missing_dates_start

        gaps = self._gap_detector.analyze(
            range_start=missing_dates_start,
            range_end=end.date(),
            present_dates=present_dates,
            present_timestamps_by_date=timestamps_by_date,
            interval=interval,
            session_start=self._fetch_session_start,
            session_end=self._fetch_session_end,
            trading_days_only=self._trading_days_only,
            range_end_datetime=end,
            intraday_gap_start=intraday_scan_start,
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
        *,
        bootstrap_start: Optional[datetime] = None,
    ) -> tuple[HistoricalSyncPlan, list[BackfillResult]]:
        """Plan and execute all required backfills for a symbol and range.

        Args:
            symbol: Target ticker symbol.
            timeframe: Expected bar interval (e.g. "1m").
            start: Inclusive UTC lower bound for incremental sync windows.
            end: Inclusive UTC upper bound for the sync window.
            bootstrap_start: Earliest UTC timestamp to use when storage is empty.

        Returns:
            The generated plan and one backfill result per planned request.
        """
        plan = self.plan(
            symbol,
            timeframe,
            start,
            end,
            bootstrap_start=bootstrap_start,
        )
        logger.info(
            "Gap plan for %s %s: %d missing day(s), %d interval gap(s), %d backfill request(s)",
            symbol,
            timeframe,
            len(plan.gaps.missing_dates),
            len(plan.gaps.missing_intervals),
            len(plan.backfill_requests),
        )
        if plan.bootstrapped_from_earliest:
            logger.info(
                "Bootstrapping %s full history from %s",
                symbol,
                plan.range_start.isoformat(),
            )
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
            start, end = session_bounds_for_day(
                missing_day,
                self._fetch_session_start,
                self._fetch_session_end,
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

    def _latest_timestamp(
        self,
        timestamps_by_date: dict[date, list[datetime]],
    ) -> Optional[datetime]:
        """Return the newest stored bar timestamp across inspected partitions."""
        latest: Optional[datetime] = None
        for timestamps in timestamps_by_date.values():
            for timestamp in timestamps:
                candidate = self._to_utc_datetime(timestamp)
                if latest is None or candidate > latest:
                    latest = candidate
        return latest


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
