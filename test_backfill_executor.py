"""Tests for backfill executor storage short-circuiting."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pandas as pd

from backfill_executor import BackfillExecutor, BackfillRequest
from cloud_storage_repository import CloudStorageRepository


def _sample_bars(start: datetime, count: int, *, minutes: int = 3) -> pd.DataFrame:
    timestamps = [
        start + pd.Timedelta(minutes=minutes * index)
        for index in range(count)
    ]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * count,
            "high": [101.0] * count,
            "low": [99.0] * count,
            "close": [100.5] * count,
            "volume": [1000.0] * count,
        }
    )


def test_execute_skips_fetch_when_storage_covers_request() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    api_client = MagicMock()
    executor = BackfillExecutor(api_client, storage, use_daily_partitions=True)

    start = datetime(2026, 6, 16, 16, 21, tzinfo=timezone.utc)
    end = datetime(2026, 6, 16, 16, 24, tzinfo=timezone.utc)
    storage.read.return_value = _sample_bars(start, 1, minutes=3)

    request = BackfillRequest(
        symbol="SPY",
        timeframe="3m",
        start=start,
        end=end,
        partition_date=date(2026, 6, 16),
    )
    result = executor.execute(request)

    assert result.rows_written == 0
    api_client.fetch_paginated.assert_not_called()


def test_execute_fetches_when_storage_missing_interval() -> None:
    storage = MagicMock(spec=CloudStorageRepository)
    api_client = MagicMock()
    executor = BackfillExecutor(api_client, storage, use_daily_partitions=True)

    start = datetime(2026, 6, 16, 16, 21, tzinfo=timezone.utc)
    end = datetime(2026, 6, 16, 16, 27, tzinfo=timezone.utc)
    storage.exists.return_value = False
    storage.read.side_effect = FileNotFoundError("missing")
    api_client.fetch_paginated.return_value = [
        {
            "t": int(start.timestamp() * 1000),
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000,
        }
    ]

    request = BackfillRequest(
        symbol="SPY",
        timeframe="3m",
        start=start,
        end=end,
        partition_date=date(2026, 6, 16),
    )
    executor.execute(request)

    api_client.fetch_paginated.assert_called_once()
