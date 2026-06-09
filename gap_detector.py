"""Gap Detector.

Responsibility: Pure algorithmic gap detection.

Compares a dataset against a calendar timeline to flag missing dates or time
intervals. Has no database, storage, or API dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Sequence


@dataclass(frozen=True)
class TimeGap:
    """A contiguous missing time range.

    `start` is inclusive. `end` is exclusive.
    """

    start: datetime
    end: datetime


@dataclass(frozen=True)
class GapReport:
    """Missing dates and intraday intervals detected in a dataset."""

    missing_dates: tuple[date, ...]
    missing_intervals: tuple[TimeGap, ...]


class GapDetector:
    """Detects missing dates and timestamps against an expected timeline."""

    def build_expected_dates(
        self,
        start: date,
        end: date,
        *,
        trading_days_only: bool = True,
    ) -> list[date]:
        """Build the expected calendar dates between two bounds.

        Args:
            start: Inclusive start date.
            end: Inclusive end date.
            trading_days_only: Skip Saturday and Sunday when True.

        Returns:
            Ordered list of expected dates.
        """
        if end < start:
            raise ValueError("end must be on or after start")

        expected: list[date] = []
        current = start
        while current <= end:
            if not trading_days_only or current.weekday() < 5:
                expected.append(current)
            current += timedelta(days=1)

        return expected

    def detect_missing_dates(
        self,
        expected_dates: Sequence[date],
        present_dates: Iterable[date],
    ) -> list[date]:
        """Return calendar dates that are expected but not present.

        Args:
            expected_dates: Full calendar timeline to compare against.
            present_dates: Dates that already have data.

        Returns:
            Missing dates in chronological order.
        """
        present = set(present_dates)
        return [day for day in expected_dates if day not in present]

    def build_expected_intervals(
        self,
        day: date,
        *,
        session_start: time,
        session_end: time,
        interval: timedelta,
    ) -> list[datetime]:
        """Build expected intraday timestamps for one session.

        Args:
            day: Trading date to build intervals for.
            session_start: Session open time in UTC.
            session_end: Session close time in UTC. The final expected bar
                begins before this time.
            interval: Expected spacing between bars.

        Returns:
            Ordered list of expected UTC timestamps.
        """
        current = datetime.combine(day, session_start, tzinfo=timezone.utc)
        session_close = datetime.combine(day, session_end, tzinfo=timezone.utc)
        expected: list[datetime] = []

        while current < session_close:
            expected.append(current)
            current += interval

        return expected

    def detect_missing_intervals(
        self,
        present_timestamps: Sequence[datetime],
        expected_timestamps: Sequence[datetime],
        *,
        interval: timedelta,
    ) -> list[TimeGap]:
        """Return contiguous missing intervals from an expected timeline.

        Args:
            present_timestamps: Timestamps already available in storage.
            expected_timestamps: Full timeline that should exist.
            interval: Spacing between expected bars.

        Returns:
            Contiguous missing ranges suitable for backfill requests.
        """
        present = {
            self._normalize_timestamp(timestamp, interval)
            for timestamp in present_timestamps
        }
        missing = [
            timestamp
            for timestamp in expected_timestamps
            if self._normalize_timestamp(timestamp, interval) not in present
        ]
        return self.group_contiguous_gaps(missing, interval)

    def analyze(
        self,
        *,
        range_start: date,
        range_end: date,
        present_dates: Iterable[date],
        present_timestamps_by_date: dict[date, Sequence[datetime]],
        interval: timedelta,
        session_start: time,
        session_end: time,
        trading_days_only: bool = True,
    ) -> GapReport:
        """Detect missing dates and intraday intervals for a target range.

        Args:
            range_start: Inclusive start date for the analysis window.
            range_end: Inclusive end date for the analysis window.
            present_dates: Dates that already have stored partitions.
            present_timestamps_by_date: Known timestamps for dates that exist.
            interval: Expected spacing between intraday bars.
            session_start: Session open time in UTC.
            session_end: Session close time in UTC.
            trading_days_only: Skip weekends in the expected date timeline.

        Returns:
            A report containing missing dates and missing intraday intervals.
        """
        expected_dates = self.build_expected_dates(
            range_start,
            range_end,
            trading_days_only=trading_days_only,
        )
        missing_dates = self.detect_missing_dates(expected_dates, present_dates)

        missing_intervals: list[TimeGap] = []
        present_date_set = set(present_dates)
        for day in expected_dates:
            if day in missing_dates:
                continue
            if day not in present_date_set:
                continue

            expected_timestamps = self.build_expected_intervals(
                day,
                session_start=session_start,
                session_end=session_end,
                interval=interval,
            )
            day_timestamps = present_timestamps_by_date.get(day, [])
            missing_intervals.extend(
                self.detect_missing_intervals(
                    day_timestamps,
                    expected_timestamps,
                    interval=interval,
                )
            )

        return GapReport(
            missing_dates=tuple(missing_dates),
            missing_intervals=tuple(missing_intervals),
        )

    def group_contiguous_gaps(
        self,
        missing_timestamps: Sequence[datetime],
        interval: timedelta,
    ) -> list[TimeGap]:
        """Group individual missing timestamps into contiguous backfill ranges.

        Args:
            missing_timestamps: Timestamps that are absent from storage.
            interval: Spacing between adjacent expected bars.

        Returns:
            Contiguous gaps where each `end` is exclusive.
        """
        if not missing_timestamps:
            return []

        ordered = sorted(
            self._normalize_timestamp(timestamp, interval)
            for timestamp in missing_timestamps
        )
        gaps: list[TimeGap] = []
        gap_start = ordered[0]
        previous = ordered[0]

        for current in ordered[1:]:
            if current - previous > interval:
                gaps.append(TimeGap(start=gap_start, end=previous + interval))
                gap_start = current
            previous = current

        gaps.append(TimeGap(start=gap_start, end=previous + interval))
        return gaps

    def _normalize_timestamp(self, timestamp: datetime, interval: timedelta) -> datetime:
        """Floor a timestamp to the nearest interval boundary in UTC."""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        interval_seconds = int(interval.total_seconds())
        elapsed_seconds = int((timestamp - epoch).total_seconds())
        aligned_seconds = elapsed_seconds - (elapsed_seconds % interval_seconds)
        return epoch + timedelta(seconds=aligned_seconds)


if __name__ == "__main__":
    detector = GapDetector()

    expected_dates = detector.build_expected_dates(
        date(2024, 1, 15),
        date(2024, 1, 19),
    )
    missing_dates = detector.detect_missing_dates(
        expected_dates,
        [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 18)],
    )
    print(f"Missing dates: {missing_dates}")

    expected_intervals = detector.build_expected_intervals(
        date(2024, 1, 15),
        session_start=time(9, 30),
        session_end=time(16, 0),
        interval=timedelta(minutes=1),
    )
    present = expected_intervals[:2] + expected_intervals[4:6]
    gaps = detector.detect_missing_intervals(
        present,
        expected_intervals,
        interval=timedelta(minutes=1),
    )
    print(f"Missing intervals: {gaps}")
