"""Tests for local and layered OHLCV storage."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from local_storage_repository import (
    LayeredOhlcvRepository,
    LocalParquetRepository,
    gcs_bucket_exists,
)


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-06-15 14:30:00", "2026-06-15 14:33:00"],
                utc=True,
            ),
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.05, 2.05],
            "volume": [100.0, 200.0],
        }
    )


def test_local_parquet_repository_round_trip(tmp_path) -> None:
    repo = LocalParquetRepository(tmp_path, prefix="ohlcv")
    day = date(2026, 6, 15)
    uri = repo.write("SPY", "3m", _sample_frame(), partition_date=day)

    assert uri.endswith("2026-06-15.parquet")
    assert repo.exists("SPY", "3m", partition_date=day)
    loaded = repo.read("SPY", "3m", partition_date=day)
    assert len(loaded) == 2


def test_layered_repository_writes_locally_when_gcs_disabled(tmp_path) -> None:
    local = LocalParquetRepository(tmp_path, prefix="ohlcv")
    remote = MagicMock()
    layered = LayeredOhlcvRepository(local, remote, remote_enabled=False)
    day = date(2026, 6, 15)

    uri = layered.write("SPY", "3m", _sample_frame(), partition_date=day)

    assert uri.endswith("2026-06-15.parquet")
    remote.write.assert_not_called()
    assert layered.exists("SPY", "3m", partition_date=day)


def test_layered_repository_continues_when_gcs_write_fails(tmp_path) -> None:
    local = LocalParquetRepository(tmp_path, prefix="ohlcv")
    remote = MagicMock()
    remote._bucket_name = "missing-bucket"
    remote.write.side_effect = RuntimeError("bucket missing")
    remote.read.side_effect = FileNotFoundError("missing")
    layered = LayeredOhlcvRepository(local, remote, remote_enabled=True)
    day = date(2026, 6, 15)

    uri = layered.write("SPY", "3m", _sample_frame(), partition_date=day)

    assert uri.endswith("2026-06-15.parquet")
    remote.write.assert_called_once()
    loaded = layered.read("SPY", "3m", partition_date=day)
    assert len(loaded) == 2


def test_layered_repository_reads_local_when_remote_missing(tmp_path) -> None:
    local = LocalParquetRepository(tmp_path, prefix="ohlcv")
    remote = MagicMock()
    remote.read.side_effect = FileNotFoundError("missing")
    layered = LayeredOhlcvRepository(local, remote, remote_enabled=True)
    day = date(2026, 6, 15)
    local.write("SPY", "3m", _sample_frame(), partition_date=day)

    loaded = layered.read("SPY", "3m", partition_date=day)
    assert len(loaded) == 2


def test_gcs_bucket_exists_uses_list_blobs_not_bucket_get() -> None:
    client = MagicMock()
    client.list_blobs.return_value = iter([])

    assert gcs_bucket_exists("live-trading-bot", client) is True
    client.list_blobs.assert_called_once_with("live-trading-bot", max_results=1)
    client.bucket.assert_not_called()


def test_layered_repository_merges_local_and_remote(tmp_path) -> None:
    local = LocalParquetRepository(tmp_path, prefix="ohlcv")
    remote = MagicMock()
    remote_frame = _sample_frame().head(1)
    remote.read.return_value = remote_frame
    layered = LayeredOhlcvRepository(local, remote, remote_enabled=True)
    day = date(2026, 6, 15)
    local.write("SPY", "3m", _sample_frame(), partition_date=day)

    loaded = layered.read(
        "SPY",
        "3m",
        partition_date=day,
    )
    assert len(loaded) == 2
    remote.read.assert_called_once()
