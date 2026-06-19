"""Cloud Storage Repository.

Responsibility: Pure I/O adapter for Google Cloud Storage.

Reads and writes historical OHLCV data that already conforms to the standard
schema. Does not fetch remote market data, parse vendor payloads, manage
WebSocket connections, or validate live stream semantics.
"""

from __future__ import annotations

import io
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound

from ohlcv_schema import OHLCV_COLUMNS, ensure_standard_ohlcv

__all__ = ("CloudStorageRepository", "OHLCV_COLUMNS")


class CloudStorageRepository:
    """Pure I/O adapter for standardized OHLCV data in Google Cloud Storage."""

    def __init__(
        self,
        bucket_name: str,
        *,
        client: Optional[storage.Client] = None,
        prefix: str = "ohlcv",
    ) -> None:
        """Initialize the repository for a single GCS bucket.

        Args:
            bucket_name: Name of the Google Cloud Storage bucket.
            client: Optional pre-configured GCS client. A default client is
                created when omitted.
            prefix: Root folder inside the bucket for OHLCV objects.
        """
        self._bucket_name = bucket_name
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket_name)
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
        """Download and return OHLCV data for a symbol and timeframe.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            timeframe: Bar interval label (e.g. "1m", "5m", "1h").
            start: Optional inclusive lower bound for returned rows.
            end: Optional inclusive upper bound for returned rows.
            partition_date: Optional date partition to read instead of
                the default `data.parquet` object.

        Returns:
            A DataFrame with standardized OHLCV columns sorted by timestamp.

        Raises:
            FileNotFoundError: If the target object does not exist in GCS.
            ValueError: If the stored data is missing required OHLCV columns.
        """
        blob_path = self._blob_path(symbol, timeframe, partition_date)
        blob = self._bucket.blob(blob_path)

        # Download the Parquet object into memory.
        try:
            raw = blob.download_as_bytes()
        except NotFound as exc:
            raise FileNotFoundError(
                f"No OHLCV data at gs://{self._bucket_name}/{blob_path}"
            ) from exc

        df = ensure_standard_ohlcv(pd.read_parquet(io.BytesIO(raw)))

        # Apply optional time filters after normalization.
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
        """Serialize and upload OHLCV data to Google Cloud Storage.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            timeframe: Bar interval label (e.g. "1m", "5m", "1h").
            data: DataFrame containing OHLCV rows to persist.
            partition_date: Optional date partition to write instead of
                the default `data.parquet` object.

        Returns:
            The `gs://` URI of the uploaded object.

        Raises:
            ValueError: If the input data is missing required OHLCV columns.
        """
        blob_path = self._blob_path(symbol, timeframe, partition_date)
        blob = self._bucket.blob(blob_path)

        # Validate standard schema, then upload as Parquet.
        normalized = ensure_standard_ohlcv(data)
        buffer = io.BytesIO()
        normalized.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        return f"gs://{self._bucket_name}/{blob_path}"

    def exists(
        self,
        symbol: str,
        timeframe: str,
        *,
        partition_date: Optional[date] = None,
    ) -> bool:
        """Check whether OHLCV data exists at the expected GCS path.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            timeframe: Bar interval label (e.g. "1m", "5m", "1h").
            partition_date: Optional date partition to check instead of
                the default `data.parquet` object.

        Returns:
            True if the object exists, otherwise False.
        """
        return self._blob_has_rows(symbol, timeframe, partition_date)

    def _blob_path(
        self,
        symbol: str,
        timeframe: str,
        partition_date: Optional[date] = None,
    ) -> str:
        """Build the object path for a symbol/timeframe within the bucket.

        Args:
            symbol: Ticker symbol; normalized to uppercase in the path.
            timeframe: Bar interval label used as a folder segment.
            partition_date: Optional date used for daily partitioned files.

        Returns:
            Relative blob path inside the configured bucket.
        """
        base = f"{self._prefix}/{symbol.upper()}/{timeframe}"
        if partition_date is not None:
            return f"{base}/{partition_date.isoformat()}.parquet"
        return f"{base}/data.parquet"

    def _blob_has_rows(
        self,
        symbol: str,
        timeframe: str,
        partition_date: Optional[date],
    ) -> bool:
        """Return whether a parquet object contains at least one row."""
        blob = self._bucket.blob(self._blob_path(symbol, timeframe, partition_date))
        if not blob.exists():
            return False
        try:
            raw = blob.download_as_bytes()
        except NotFound:
            return False
        return parquet_bytes_have_rows(raw)


def parquet_bytes_have_rows(payload: bytes) -> bool:
    """Return whether parquet bytes contain at least one row."""
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(io.BytesIO(payload)).num_rows > 0
    except Exception:
        return False


def parquet_file_has_rows(path: Path) -> bool:
    """Return whether a parquet file on disk contains at least one row."""
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(path).num_rows > 0
    except Exception:
        return False


def _to_utc_timestamp(value: datetime) -> pd.Timestamp:
    """Normalize a datetime to a timezone-aware UTC pandas Timestamp."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


if __name__ == "__main__":
    # Example usage (requires GCS credentials and an existing bucket).
    bucket_name = "my-trading-bucket"
    repo = CloudStorageRepository(bucket_name)

    sample_ohlcv = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-15 09:30:00", "2024-01-15 09:31:00"],
                utc=True,
            ),
            "open": [185.0, 185.2],
            "high": [185.5, 185.6],
            "low": [184.8, 185.1],
            "close": [185.3, 185.4],
            "volume": [1000, 1200],
        }
    )

    # Write a single file per symbol/timeframe.
    uri = repo.write("aapl", "1m", sample_ohlcv)
    print(f"Wrote OHLCV data to {uri}")

    # Or write a daily partition.
    partition = date(2024, 1, 15)
    partition_uri = repo.write("aapl", "1m", sample_ohlcv, partition_date=partition)
    print(f"Wrote partition to {partition_uri}")

    # Check whether data exists before reading.
    if repo.exists("AAPL", "1m"):
        df = repo.read(
            "AAPL",
            "1m",
            start=datetime(2024, 1, 15, 9, 30),
            end=datetime(2024, 1, 15, 16, 0),
        )
        print(df)

    # Read a specific daily partition.
    if repo.exists("AAPL", "1m", partition_date=partition):
        partition_df = repo.read("AAPL", "1m", partition_date=partition)
        print(partition_df)
