"""Tests for historical orchestrator session windows."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import MagicMock

from backfill_executor import BackfillExecutor
from cloud_storage_repository import CloudStorageRepository
from gap_detector import session_bounds_for_day
from historical_orchestrator import HistoricalOrchestrator


def test_session_bounds_for_day_rolls_end_to_next_utc_day() -> None:
    start, end = session_bounds_for_day(
        date(2026, 6, 15),
        time(8, 0),
        time(1, 0),
    )
    assert start == datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 6, 16, 1, 0, tzinfo=timezone.utc)


def test_full_day_backfill_uses_extended_session_when_enabled() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    storage.exists.return_value = False
    executor = MagicMock(spec=BackfillExecutor)
    executor.discover_earliest_available.return_value = None

    orchestrator = HistoricalOrchestrator(
        storage,
        executor,
        use_daily_partitions=True,
        session_start=time(14, 30),
        session_end=time(21, 0),
        need_extended_hours=True,
        extended_session_start=time(8, 0),
        extended_session_end=time(1, 0),
        trading_days_only=False,
        bootstrap_if_empty=False,
    )

    start = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 17, 0, tzinfo=timezone.utc)
    plan = orchestrator.plan("SPY", "3m", start, end)

    assert len(plan.backfill_requests) == 1
    request = plan.backfill_requests[0]
    assert request.start == datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    assert request.end == datetime(2026, 6, 16, 1, 0, tzinfo=timezone.utc)


def test_plan_bootstraps_when_storage_empty() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    storage.exists.return_value = False
    executor = MagicMock(spec=BackfillExecutor)
    executor.discover_earliest_available.return_value = datetime(
        2026, 4, 20, 8, 0, tzinfo=timezone.utc
    )

    orchestrator = HistoricalOrchestrator(
        storage,
        executor,
        use_daily_partitions=True,
        trading_days_only=False,
        bootstrap_if_empty=True,
    )
    bootstrap_start = datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 17, 0, tzinfo=timezone.utc)
    plan = orchestrator.plan(
        "SPY",
        "3m",
        bootstrap_start,
        end,
        bootstrap_start=bootstrap_start,
    )

    assert plan.bootstrapped_from_earliest
    assert plan.range_start == datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    executor.discover_earliest_available.assert_called_once()


def test_plan_gap_fills_after_last_stored_bar_without_rebootstrap() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    executor = MagicMock(spec=BackfillExecutor)

    stored_day = date(2026, 6, 13)
    stored_ts = datetime(2026, 6, 13, 20, 57, tzinfo=timezone.utc)

    def _exists(
        symbol: str,
        timeframe: str,
        *,
        partition_date: date | None = None,
    ) -> bool:
        return partition_date == stored_day

    def _read(
        symbol: str,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        partition_date: date | None = None,
    ):
        import pandas as pd

        return pd.DataFrame({"timestamp": [stored_ts]})

    storage.exists.side_effect = _exists
    storage.read.side_effect = _read

    orchestrator = HistoricalOrchestrator(
        storage,
        executor,
        use_daily_partitions=True,
        trading_days_only=False,
        bootstrap_if_empty=True,
    )
    bootstrap_start = datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 17, 0, tzinfo=timezone.utc)
    plan = orchestrator.plan(
        "SPY",
        "3m",
        bootstrap_start,
        end,
        bootstrap_start=bootstrap_start,
    )

    assert not plan.bootstrapped_from_earliest
    executor.discover_earliest_available.assert_not_called()
    assert plan.gaps.missing_dates
    assert date(2026, 6, 15) in plan.gaps.missing_dates or plan.gaps.missing_intervals


def test_intraday_gap_detection_includes_premarket_when_extended() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    executor = MagicMock(spec=BackfillExecutor)

    day = date(2026, 6, 15)
    rth_open = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    present_timestamps = [
        rth_open + timedelta(minutes=3 * index)
        for index in range(10)
    ]

    def _exists(
        symbol: str,
        timeframe: str,
        *,
        partition_date: date | None = None,
    ) -> bool:
        return partition_date == day

    def _read(
        symbol: str,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        partition_date: date | None = None,
    ):
        import pandas as pd

        return pd.DataFrame({"timestamp": present_timestamps})

    storage.exists.side_effect = _exists
    storage.read.side_effect = _read

    orchestrator = HistoricalOrchestrator(
        storage,
        executor,
        use_daily_partitions=True,
        session_start=time(14, 30),
        session_end=time(21, 0),
        need_extended_hours=True,
        extended_session_start=time(8, 0),
        extended_session_end=time(1, 0),
        trading_days_only=False,
        bootstrap_if_empty=False,
    )

    start = datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 15, 17, 0, tzinfo=timezone.utc)
    plan = orchestrator.plan("SPY", "3m", start, end)

    assert not plan.gaps.missing_dates
    assert plan.gaps.missing_intervals
    first_gap = plan.gaps.missing_intervals[0]
    assert first_gap.start == datetime(2026, 6, 15, 8, 0, tzinfo=timezone.utc)
    assert first_gap.start < rth_open


def test_plan_incremental_sync_skips_bootstrap_missing_days() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    executor = MagicMock(spec=BackfillExecutor)

    may_day = date(2026, 5, 4)
    june_day = date(2026, 6, 16)
    latest_ts = datetime(2026, 6, 16, 17, 0, tzinfo=timezone.utc)

    def _exists(
        symbol: str,
        timeframe: str,
        *,
        partition_date: date | None = None,
    ) -> bool:
        return partition_date in {may_day, june_day}

    def _read(
        symbol: str,
        timeframe: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        partition_date: date | None = None,
    ):
        import pandas as pd

        if partition_date == june_day:
            return pd.DataFrame({"timestamp": [latest_ts]})
        return pd.DataFrame(
            {
                "timestamp": [
                    datetime(2026, 5, 4, 14, 30, tzinfo=timezone.utc),
                ]
            }
        )

    storage.exists.side_effect = _exists
    storage.read.side_effect = _read

    orchestrator = HistoricalOrchestrator(
        storage,
        executor,
        use_daily_partitions=True,
        trading_days_only=False,
        bootstrap_if_empty=True,
    )
    bootstrap_start = datetime(2026, 4, 16, 8, 0, tzinfo=timezone.utc)
    end = datetime(2026, 6, 16, 16, 57, tzinfo=timezone.utc)
    plan = orchestrator.plan(
        "SPY",
        "3m",
        bootstrap_start,
        end,
        bootstrap_start=bootstrap_start,
    )

    assert not plan.bootstrapped_from_earliest
    assert date(2026, 4, 16) not in plan.gaps.missing_dates
    assert all(
        missing >= latest_ts.date() for missing in plan.gaps.missing_dates
    )


def test_intraday_gap_scan_skips_older_stored_days() -> None:
    from gap_detector import GapDetector

    detector = GapDetector()
    day = date(2026, 6, 15)
    rth_open = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc)
    present_timestamps = [rth_open + timedelta(minutes=3 * index) for index in range(10)]

    report = detector.analyze(
        range_start=day,
        range_end=day,
        present_dates=[day],
        present_timestamps_by_date={day: present_timestamps},
        interval=timedelta(minutes=3),
        session_start=time(8, 0),
        session_end=time(1, 0),
        trading_days_only=False,
        intraday_gap_start=day,
    )
    assert report.missing_intervals

    report_tail_only = detector.analyze(
        range_start=day,
        range_end=day,
        present_dates=[day],
        present_timestamps_by_date={day: present_timestamps},
        interval=timedelta(minutes=3),
        session_start=time(8, 0),
        session_end=time(1, 0),
        trading_days_only=False,
        intraday_gap_start=date(2026, 6, 16),
    )
    assert not report_tail_only.missing_intervals
