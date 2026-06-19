"""Local and layered OHLCV storage.

Mirrors the CloudStorageRepository path layout on disk and optionally
replicates objects to GCS when the bucket is available.
"""

from __future__ import annotations

import io
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import storage

from cloud_storage_repository import (
    CloudStorageRepository,
    _to_utc_timestamp,
    parquet_file_has_rows,
)
from ohlcv_schema import ensure_standard_ohlcv

logger = logging.getLogger(__name__)


class LocalParquetRepository:
    """Read and write standardized OHLCV parquet files on local disk."""

    def __init__(self, root: str | Path, *, prefix: str = "ohlcv") -> None:
        self._root = Path(root)
        self._prefix = prefix.rstrip("/")

    def read(
        self,
        symbol: str,
        timeframe: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        partition_date: Optional[date] = None,
    ) -> pd.DataFrame:
        path = self._file_path(symbol, timeframe, partition_date)
        if not path.exists():
            raise FileNotFoundError(f"No OHLCV data at {path}")

        df = ensure_standard_ohlcv(pd.read_parquet(path))
        if start is not None:
            df = df[df["timestamp"] >= _to_utc_timestamp(start)]
        if end is not None:
            df = df[df["timestamp"] <= _to_utc_timestamp(end)]
        return df.reset_index(drop=True)

    def write(
        self,
        symbol: str,
        timeframe: str,
        data: pd.DataFrame,
        *,
        partition_date: Optional[date] = None,
    ) -> str:
        path = self._file_path(symbol, timeframe, partition_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized = ensure_standard_ohlcv(data)
        normalized.to_parquet(path, index=False)
        return str(path.resolve())

    def exists(
        self,
        symbol: str,
        timeframe: str,
        *,
        partition_date: Optional[date] = None,
    ) -> bool:
        path = self._file_path(symbol, timeframe, partition_date)
        if not path.exists():
            return False
        return parquet_file_has_rows(path)

    def _file_path(
        self,
        symbol: str,
        timeframe: str,
        partition_date: Optional[date] = None,
    ) -> Path:
        base = self._root / self._prefix / symbol.upper() / timeframe
        if partition_date is not None:
            return base / f"{partition_date.isoformat()}.parquet"
        return base / "data.parquet"


class LayeredOhlcvRepository:
    """Persist OHLCV locally and replicate to GCS when available."""

    def __init__(
        self,
        local: LocalParquetRepository,
        remote: CloudStorageRepository,
        *,
        remote_enabled: bool,
    ) -> None:
        self._local = local
        self._remote = remote
        self._remote_enabled = remote_enabled

    def read(
        self,
        symbol: str,
        timeframe: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        partition_date: Optional[date] = None,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for reader in self._read_sources():
            try:
                frames.append(
                    reader.read(
                        symbol,
                        timeframe,
                        start=start,
                        end=end,
                        partition_date=partition_date,
                    )
                )
            except FileNotFoundError:
                continue

        if not frames:
            path = self._local._file_path(symbol, timeframe, partition_date)
            raise FileNotFoundError(f"No OHLCV data at {path}")

        if len(frames) == 1:
            return frames[0]

        return (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    def _read_sources(self) -> list[LocalParquetRepository | CloudStorageRepository]:
        if self._remote_enabled:
            return [self._local, self._remote]
        return [self._local]

    def write(
        self,
        symbol: str,
        timeframe: str,
        data: pd.DataFrame,
        *,
        partition_date: Optional[date] = None,
    ) -> str:
        local_uri = self._local.write(
            symbol,
            timeframe,
            data,
            partition_date=partition_date,
        )
        if not self._remote_enabled:
            return local_uri

        try:
            self._remote.write(
                symbol,
                timeframe,
                data,
                partition_date=partition_date,
            )
        except Exception as exc:
            logger.warning(
                "Could not write OHLCV to gs://%s; saved locally at %s (%s)",
                self._remote._bucket_name,
                local_uri,
                exc,
            )
        return local_uri

    def exists(
        self,
        symbol: str,
        timeframe: str,
        *,
        partition_date: Optional[date] = None,
    ) -> bool:
        if self._local.exists(symbol, timeframe, partition_date=partition_date):
            return True
        if not self._remote_enabled:
            return False
        try:
            return self._remote.exists(symbol, timeframe, partition_date=partition_date)
        except Exception:
            return False


def gcs_bucket_exists(bucket_name: str, client: Optional[storage.Client] = None) -> bool:
    """Return True when the bucket is reachable with the current credentials.

    Uses ``list_blobs`` instead of ``bucket.exists()`` because service accounts
    are often granted object access without ``storage.buckets.get``.
    """
    client = client or storage.Client()
    try:
        next(client.list_blobs(bucket_name, max_results=1), None)
        return True
    except StopIteration:
        return True
    except Exception as exc:
        if _is_gcs_forbidden(exc):
            logger.warning(
                "GCS bucket gs://%s is not accessible with current credentials: %s",
                bucket_name,
                exc,
            )
        else:
            logger.debug("GCS bucket gs://%s unavailable: %s", bucket_name, exc)
        return False


def _is_gcs_forbidden(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 403:
        return True
    message = str(exc).lower()
    return "403" in message or "forbidden" in message or "denied" in message
