"""Buffer raw trade prints and merge them into local+GCS storage on flush.

Responsibility: Persist streamed trade ticks from a live session by merging into
daily parquet partitions under the configured trades prefix. Captures the full
Databento ``trades`` schema field set when available. Does not aggregate bars or
evaluate strategies.
"""

from __future__ import annotations

import io
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Optional

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound

from config import normalize_market_symbol
from local_storage_repository import gcs_bucket_exists

if TYPE_CHECKING:
    from config import AppConfig

logger = logging.getLogger(__name__)

# Full Databento trades schema (+ symbol labels). Older parquet files with a
# subset of these columns are upgraded on merge.
TRADE_COLUMNS = (
    "timestamp",
    "symbol",
    "raw_symbol",
    "price",
    "price_raw",
    "size",
    "side",
    "action",
    "depth",
    "flags",
    "sequence",
    "instrument_id",
    "publisher_id",
    "rtype",
    "ts_event",
    "ts_recv",
    "ts_in_delta",
    "ts_index",
)

_DEDUPE_KEYS_FULL = ("ts_event", "sequence", "instrument_id")
_DEDUPE_KEYS_LEGACY = ("timestamp", "price", "size")


@dataclass(frozen=True)
class SessionTradeFlushSummary:
    """Outcome of flushing buffered raw trades to storage."""

    rows_buffered: int
    rows_written: int
    partitions_written: int
    storage_uris: tuple[str, ...] = ()


@dataclass
class _BufferedTrades:
    rows: list[dict[str, object]] = field(default_factory=list)


class SessionTradeRecorder:
    """Accumulate raw trades in memory and merge-write them on shutdown."""

    def __init__(
        self,
        *,
        local_root: str | Path,
        bucket_name: str,
        prefix: str = "trades",
        use_daily_partitions: bool = True,
        remote_enabled: bool = True,
        client: Optional[storage.Client] = None,
    ) -> None:
        self._local_root = Path(local_root)
        self._bucket_name = bucket_name
        self._prefix = prefix.rstrip("/")
        self._use_daily_partitions = use_daily_partitions
        self._remote_enabled = remote_enabled
        self._client = client
        self._bucket = None
        self._buffers: dict[str, _BufferedTrades] = {}
        self._lock = threading.Lock()

    @property
    def buffered_row_count(self) -> int:
        with self._lock:
            return sum(len(bucket.rows) for bucket in self._buffers.values())

    def record_trade(
        self,
        *,
        symbol: str,
        timestamp: datetime,
        price: float,
        size: float,
        side: str = "",
        raw_symbol: str = "",
        price_raw: Optional[int] = None,
        action: str = "",
        depth: Optional[int] = None,
        flags: Optional[int] = None,
        sequence: Optional[int] = None,
        instrument_id: Optional[int] = None,
        publisher_id: Optional[int] = None,
        rtype: Optional[int] = None,
        ts_event: Optional[int] = None,
        ts_recv: Optional[int] = None,
        ts_in_delta: Optional[int] = None,
        ts_index: Optional[int] = None,
    ) -> None:
        """Buffer one trade print (basic or full Databento trades fields)."""
        symbol = normalize_market_symbol(symbol)
        if not symbol or price <= 0:
            return
        row: dict[str, object] = {
            "timestamp": _to_utc(timestamp),
            "symbol": symbol,
            "raw_symbol": str(raw_symbol or ""),
            "price": float(price),
            "price_raw": int(price_raw) if price_raw is not None else None,
            "size": float(max(size, 0.0)),
            "side": str(side or ""),
            "action": str(action or ""),
            "depth": int(depth) if depth is not None else None,
            "flags": int(flags) if flags is not None else None,
            "sequence": int(sequence) if sequence is not None else None,
            "instrument_id": int(instrument_id) if instrument_id is not None else None,
            "publisher_id": int(publisher_id) if publisher_id is not None else None,
            "rtype": int(rtype) if rtype is not None else None,
            "ts_event": int(ts_event) if ts_event is not None else None,
            "ts_recv": int(ts_recv) if ts_recv is not None else None,
            "ts_in_delta": int(ts_in_delta) if ts_in_delta is not None else None,
            "ts_index": int(ts_index) if ts_index is not None else None,
        }
        with self._lock:
            bucket = self._buffers.get(symbol)
            if bucket is None:
                bucket = _BufferedTrades()
                self._buffers[symbol] = bucket
            bucket.rows.append(row)

    def record_databento_trade(
        self,
        record: Any,
        *,
        symbol: str,
        raw_symbol: str = "",
    ) -> bool:
        """Buffer one Databento TradeMsg with every trades-schema field.

        Returns True when the record was accepted.
        """
        price = _databento_pretty_price(record)
        if price is None or price <= 0:
            return False
        timestamp = _databento_event_time(record)
        self.record_trade(
            symbol=symbol,
            timestamp=timestamp,
            price=price,
            size=float(getattr(record, "size", 0) or 0.0) or 1.0,
            side=_enum_value(getattr(record, "side", "")),
            raw_symbol=raw_symbol,
            price_raw=_optional_int(getattr(record, "price", None)),
            action=_enum_value(getattr(record, "action", "")),
            depth=_optional_int(getattr(record, "depth", None)),
            flags=_optional_int(getattr(record, "flags", None)),
            sequence=_optional_int(getattr(record, "sequence", None)),
            instrument_id=_optional_int(getattr(record, "instrument_id", None)),
            publisher_id=_optional_int(getattr(record, "publisher_id", None)),
            rtype=_optional_int(getattr(record, "rtype", None)),
            ts_event=_optional_int(getattr(record, "ts_event", None)),
            ts_recv=_optional_int(getattr(record, "ts_recv", None)),
            ts_in_delta=_optional_int(getattr(record, "ts_in_delta", None)),
            ts_index=_optional_int(getattr(record, "ts_index", None)),
        )
        return True

    def flush(self) -> SessionTradeFlushSummary:
        """Merge buffered trades into cumulative daily partitions and upload."""
        with self._lock:
            pending = {
                symbol: list(bucket.rows)
                for symbol, bucket in self._buffers.items()
                if bucket.rows
            }
            self._buffers.clear()

        if not pending:
            logger.info("Session trade flush: no new trades to merge into history")
            return SessionTradeFlushSummary(
                rows_buffered=0,
                rows_written=0,
                partitions_written=0,
            )

        rows_buffered = sum(len(rows) for rows in pending.values())
        logger.info(
            "Merging %d raw trade(s) across %d symbol(s) into %s (local+GCS)",
            rows_buffered,
            len(pending),
            self._prefix,
        )

        rows_written = 0
        partitions_written = 0
        storage_uris: list[str] = []

        for symbol, rows in sorted(pending.items()):
            frame = _rows_to_frame(rows)
            if frame.empty:
                continue

            if self._use_daily_partitions:
                for partition_date, partition_frame in _split_by_partition_date(frame):
                    uri = self._merge_and_write(
                        symbol,
                        partition_frame,
                        partition_date=partition_date,
                    )
                    partitions_written += 1
                    rows_written += len(partition_frame)
                    storage_uris.append(uri)
                    logger.info(
                        "Saved %d %s raw trade(s) to %s",
                        len(partition_frame),
                        symbol,
                        uri,
                    )
            else:
                uri = self._merge_and_write(symbol, frame)
                partitions_written += 1
                rows_written += len(frame)
                storage_uris.append(uri)
                logger.info("Saved %d %s raw trade(s) to %s", len(frame), symbol, uri)

        summary = SessionTradeFlushSummary(
            rows_buffered=rows_buffered,
            rows_written=rows_written,
            partitions_written=partitions_written,
            storage_uris=tuple(storage_uris),
        )
        logger.info(
            "Session trade flush complete: %d row(s) written to %d partition(s)",
            summary.rows_written,
            summary.partitions_written,
        )
        return summary

    def _merge_and_write(
        self,
        symbol: str,
        incoming: pd.DataFrame,
        *,
        partition_date: Optional[date] = None,
    ) -> str:
        existing = self._read_existing(symbol, partition_date=partition_date)
        existing_rows = 0 if existing is None else len(existing)
        if existing is None or existing.empty:
            merged = incoming
        else:
            merged = pd.concat([existing, incoming], ignore_index=True)

        merged = _dedupe_trades(merged)
        uri = self._write(symbol, merged, partition_date=partition_date)
        logger.info(
            "Raw trade history %s%s: prior=%d incoming=%d merged=%d",
            symbol,
            f"/{partition_date.isoformat()}" if partition_date is not None else "",
            existing_rows,
            len(incoming),
            len(merged),
        )
        return uri

    def _read_existing(
        self,
        symbol: str,
        *,
        partition_date: Optional[date],
    ) -> Optional[pd.DataFrame]:
        frames: list[pd.DataFrame] = []
        local_path = self._local_path(symbol, partition_date)
        if local_path.exists():
            try:
                frames.append(_normalize_trades(pd.read_parquet(local_path)))
            except Exception:
                logger.exception("Failed reading local trades at %s", local_path)

        if self._remote_enabled:
            blob_path = self._blob_path(symbol, partition_date)
            try:
                bucket = self._ensure_bucket()
                blob = bucket.blob(blob_path)
                raw = blob.download_as_bytes()
                frames.append(_normalize_trades(pd.read_parquet(io.BytesIO(raw))))
            except NotFound:
                pass
            except Exception:
                logger.exception(
                    "Failed reading GCS trades at gs://%s/%s",
                    self._bucket_name,
                    blob_path,
                )

        if not frames:
            return None
        if len(frames) == 1:
            return frames[0]
        return _dedupe_trades(pd.concat(frames, ignore_index=True))

    def _write(
        self,
        symbol: str,
        data: pd.DataFrame,
        *,
        partition_date: Optional[date],
    ) -> str:
        normalized = _normalize_trades(data)
        local_path = self._local_path(symbol, partition_date)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(local_path, index=False)
        local_uri = str(local_path.resolve())

        if not self._remote_enabled:
            return local_uri

        blob_path = self._blob_path(symbol, partition_date)
        try:
            bucket = self._ensure_bucket()
            blob = bucket.blob(blob_path)
            buffer = io.BytesIO()
            normalized.to_parquet(buffer, index=False)
            buffer.seek(0)
            blob.upload_from_file(buffer, content_type="application/octet-stream")
            return f"gs://{self._bucket_name}/{blob_path}"
        except Exception as exc:
            logger.warning(
                "Could not write trades to gs://%s/%s; saved locally at %s (%s)",
                self._bucket_name,
                blob_path,
                local_uri,
                exc,
            )
            return local_uri

    def _local_path(self, symbol: str, partition_date: Optional[date]) -> Path:
        base = self._local_root / self._prefix / normalize_market_symbol(symbol)
        if partition_date is not None:
            return base / f"{partition_date.isoformat()}.parquet"
        return base / "data.parquet"

    def _blob_path(self, symbol: str, partition_date: Optional[date]) -> str:
        base = f"{self._prefix}/{normalize_market_symbol(symbol)}"
        if partition_date is not None:
            return f"{base}/{partition_date.isoformat()}.parquet"
        return f"{base}/data.parquet"

    def _ensure_bucket(self):
        if self._bucket is not None:
            return self._bucket
        client = self._client or storage.Client()
        self._client = client
        self._bucket = client.bucket(self._bucket_name)
        return self._bucket


def build_trade_recorder(app: AppConfig) -> SessionTradeRecorder:
    """Build a session trade recorder from application GCS settings."""
    gcs = app.gcs
    if gcs.credentials_path:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", gcs.credentials_path)
    if gcs.project_id:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)

    client = storage.Client()
    remote_enabled = gcs_bucket_exists(gcs.bucket_name, client)
    prefix = gcs.trades_prefix or "trades"
    if remote_enabled:
        logger.info(
            "Raw trade storage: local %s/%s with GCS replication to gs://%s/%s",
            gcs.local_fallback_path,
            prefix,
            gcs.bucket_name,
            prefix,
        )
    else:
        logger.warning(
            "GCS bucket gs://%s not found; raw trades will be stored locally under %s/%s",
            gcs.bucket_name,
            gcs.local_fallback_path,
            prefix,
        )

    return SessionTradeRecorder(
        local_root=gcs.local_fallback_path,
        bucket_name=gcs.bucket_name,
        prefix=prefix,
        use_daily_partitions=gcs.use_daily_partitions,
        remote_enabled=remote_enabled,
        client=client,
    )


def _rows_to_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(TRADE_COLUMNS))
    frame = pd.DataFrame(rows)
    return _normalize_trades(frame)


def _normalize_trades(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in TRADE_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out.loc[:, list(TRADE_COLUMNS)]
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    out["symbol"] = out["symbol"].astype(str).map(normalize_market_symbol)
    out["raw_symbol"] = out["raw_symbol"].fillna("").astype(str)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["size"] = pd.to_numeric(out["size"], errors="coerce").fillna(0.0)
    out["side"] = out["side"].fillna("").astype(str)
    out["action"] = out["action"].fillna("").astype(str)
    for column in (
        "price_raw",
        "depth",
        "flags",
        "sequence",
        "instrument_id",
        "publisher_id",
        "rtype",
        "ts_event",
        "ts_recv",
        "ts_in_delta",
        "ts_index",
    ):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out.sort_values(
        by=["timestamp", "sequence", "ts_event"],
        kind="mergesort",
    ).reset_index(drop=True)


def _dedupe_trades(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_trades(frame)
    if normalized.empty:
        return normalized
    if normalized["sequence"].notna().any() and normalized["ts_event"].notna().any():
        subset = list(_DEDUPE_KEYS_FULL)
    else:
        subset = list(_DEDUPE_KEYS_LEGACY)
    return (
        normalized.drop_duplicates(subset=subset, keep="last")
        .sort_values(by=["timestamp", "sequence", "ts_event"], kind="mergesort")
        .reset_index(drop=True)
    )


def _split_by_partition_date(frame: pd.DataFrame) -> list[tuple[date, pd.DataFrame]]:
    partitions: list[tuple[date, pd.DataFrame]] = []
    for partition_date in sorted(frame["timestamp"].dt.date.unique()):
        day_frame = frame[frame["timestamp"].dt.date == partition_date].reset_index(
            drop=True
        )
        if not day_frame.empty:
            partitions.append((partition_date, day_frame))
    return partitions


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _databento_pretty_price(record: Any) -> Optional[float]:
    pretty = getattr(record, "pretty_price", None)
    if pretty is not None:
        try:
            price = float(pretty)
            if price > 0:
                return price
        except (TypeError, ValueError):
            pass
    raw = getattr(record, "price", None)
    if raw is None:
        return None
    try:
        price = float(raw)
    except (TypeError, ValueError):
        return None
    if abs(price) > 1e6:
        price = price / 1e9
    return price if price > 0 else None


def _databento_event_time(record: Any) -> datetime:
    for attr in ("ts_event", "ts_recv", "ts_index"):
        parsed = _optional_int(getattr(record, attr, None))
        if parsed is None or parsed <= 0 or parsed > 10**19:
            continue
        try:
            return datetime.fromtimestamp(parsed / 1_000_000_000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            continue
    pretty = getattr(record, "pretty_ts_event", None)
    if pretty is not None:
        try:
            ts = pd.Timestamp(pretty)
            if ts.tzinfo is None:
                return ts.to_pydatetime().replace(tzinfo=timezone.utc)
            return ts.floor("us").to_pydatetime().astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    enum_value = getattr(value, "value", None)
    if enum_value is not None and not isinstance(value, (str, bytes)):
        value = enum_value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _enum_value(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "value"):
        try:
            return str(value.value)
        except Exception:
            pass
    return str(value)
