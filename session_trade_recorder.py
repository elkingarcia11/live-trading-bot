"""Buffer raw trade prints and persist them to local+GCS storage.

Responsibility: Persist streamed trade ticks from a live session. The hot path
only builds row dicts; a background writer appends immutable local parquet
parts and periodically compact/merges them into daily partitions (local+GCS).
Captures the full Databento ``trades`` schema field set when available. Does
not aggregate bars or evaluate strategies.
"""

from __future__ import annotations

import io
import logging
import os
import time
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

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

DEDUPE_KEYS_FULL = ("ts_event", "sequence", "instrument_id")
DEDUPE_KEYS_LEGACY = ("timestamp", "price", "size")
# Back-compat aliases for older imports/tests.
_DEDUPE_KEYS_FULL = DEDUPE_KEYS_FULL
_DEDUPE_KEYS_LEGACY = DEDUPE_KEYS_LEGACY
PARTS_DIRNAME = "_parts"


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
        # Serializes merge/upload so a second flush cannot interrupt an in-flight write.
        # record_trade() still appends under _lock while a write is in progress.
        self._write_lock = threading.Lock()
        self._part_seq = 0
        self._flush_started_monotonic: Optional[float] = None
        self._last_flush_at: Optional[datetime] = None

    @property
    def buffered_row_count(self) -> int:
        with self._lock:
            return sum(len(bucket.rows) for bucket in self._buffers.values())

    @property
    def flush_in_progress(self) -> bool:
        return self._write_lock.locked()

    @property
    def flush_elapsed_seconds(self) -> Optional[float]:
        """Seconds the current flush has been running, else None."""
        started = self._flush_started_monotonic
        if started is None:
            return None
        return max(time.monotonic() - started, 0.0)

    @property
    def last_flush_at(self) -> Optional[datetime]:
        return self._last_flush_at

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
        row = build_trade_row(
            symbol=symbol,
            timestamp=timestamp,
            price=price,
            size=size,
            side=side,
            raw_symbol=raw_symbol,
            price_raw=price_raw,
            action=action,
            depth=depth,
            flags=flags,
            sequence=sequence,
            instrument_id=instrument_id,
            publisher_id=publisher_id,
            rtype=rtype,
            ts_event=ts_event,
            ts_recv=ts_recv,
            ts_in_delta=ts_in_delta,
            ts_index=ts_index,
        )
        if row is None:
            return
        self._append_row(row)

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
        row = databento_trade_row(record, symbol=symbol, raw_symbol=raw_symbol)
        if row is None:
            return False
        self._append_row(row)
        return True

    def _append_row(self, row: dict[str, object]) -> None:
        symbol = str(row["symbol"])
        with self._lock:
            bucket = self._buffers.get(symbol)
            if bucket is None:
                bucket = _BufferedTrades()
                self._buffers[symbol] = bucket
            bucket.rows.append(row)

    def append_rows(
        self, rows: Sequence[Mapping[str, Any]]
    ) -> SessionTradeFlushSummary:
        """Write rows as immutable local parquet parts (no merge, no GCS)."""
        grouped: dict[tuple[str, Optional[date]], list[dict[str, object]]] = {}
        for raw in rows:
            row = dict(raw)
            symbol = normalize_market_symbol(str(row.get("symbol") or ""))
            if not symbol:
                continue
            row["symbol"] = symbol
            partition_date: Optional[date] = None
            if self._use_daily_partitions:
                partition_date = _partition_date(row.get("timestamp"))
                if partition_date is None:
                    continue
            grouped.setdefault((symbol, partition_date), []).append(row)

        if not grouped:
            return SessionTradeFlushSummary(
                rows_buffered=0,
                rows_written=0,
                partitions_written=0,
            )

        storage_uris: list[str] = []
        rows_written = 0
        for (symbol, partition_date), group in sorted(
            grouped.items(), key=lambda item: (item[0][0], str(item[0][1]))
        ):
            frame = _rows_to_frame(group)
            if frame.empty:
                continue
            path = self._new_part_path(symbol, partition_date)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".parquet.tmp")
            frame.to_parquet(tmp, index=False)
            tmp.replace(path)
            storage_uris.append(str(path.resolve()))
            rows_written += len(frame)
            logger.info(
                "Appended %d %s trade(s) to %s",
                len(frame),
                symbol,
                path,
            )

        return SessionTradeFlushSummary(
            rows_buffered=rows_written,
            rows_written=rows_written,
            partitions_written=len(storage_uris),
            storage_uris=tuple(storage_uris),
        )

    def part_file_count(self) -> int:
        return len(self._iter_part_files())

    def compact(self) -> SessionTradeFlushSummary:
        """Merge local parts into daily parquet partitions and upload to GCS."""
        with self._write_lock:
            return self._compact_unlocked()

    def _compact_unlocked(self) -> SessionTradeFlushSummary:
        grouped: dict[tuple[str, Optional[date]], list[Path]] = {}
        for symbol, partition_date, path in self._iter_part_files():
            grouped.setdefault((symbol, partition_date), []).append(path)

        if not grouped:
            logger.info("Session trade compact: no local parts to merge")
            return SessionTradeFlushSummary(
                rows_buffered=0,
                rows_written=0,
                partitions_written=0,
            )

        rows_written = 0
        partitions_written = 0
        storage_uris: list[str] = []
        for (symbol, partition_date), paths in sorted(
            grouped.items(), key=lambda item: (item[0][0], str(item[0][1]))
        ):
            frames: list[pd.DataFrame] = []
            for path in paths:
                frames.append(_normalize_trades(pd.read_parquet(path)))
            incoming = (
                pd.concat(frames, ignore_index=True)
                if len(frames) > 1
                else frames[0]
            )
            if incoming.empty:
                self._remove_part_files(paths)
                continue
            uri = self._merge_and_write(
                symbol,
                incoming,
                partition_date=partition_date,
            )
            self._remove_part_files(paths)
            partitions_written += 1
            rows_written += len(incoming)
            storage_uris.append(uri)
            logger.info(
                "Compacted %d %s part file(s) (%d row(s)) into %s",
                len(paths),
                symbol,
                len(incoming),
                uri,
            )

        summary = SessionTradeFlushSummary(
            rows_buffered=rows_written,
            rows_written=rows_written,
            partitions_written=partitions_written,
            storage_uris=tuple(storage_uris),
        )
        logger.info(
            "Session trade compact complete: %d row(s) merged into %d partition(s)",
            summary.rows_written,
            summary.partitions_written,
        )
        return summary

    def _iter_part_files(self) -> list[tuple[str, Optional[date], Path]]:
        root = self._local_root / self._prefix
        if not root.exists():
            return []
        found: list[tuple[str, Optional[date], Path]] = []
        for symbol_dir in sorted(root.iterdir()):
            parts_root = symbol_dir / PARTS_DIRNAME
            if not parts_root.is_dir():
                continue
            symbol = symbol_dir.name
            for day_dir in sorted(parts_root.iterdir()):
                if not day_dir.is_dir():
                    continue
                partition_date: Optional[date]
                if day_dir.name == "all":
                    partition_date = None
                else:
                    try:
                        partition_date = date.fromisoformat(day_dir.name)
                    except ValueError:
                        continue
                for path in sorted(day_dir.glob("part-*.parquet")):
                    found.append((symbol, partition_date, path))
        return found

    def _new_part_path(self, symbol: str, partition_date: Optional[date]) -> Path:
        self._part_seq += 1
        day_key = partition_date.isoformat() if partition_date is not None else "all"
        name = f"part-{time.time_ns()}-{os.getpid()}-{self._part_seq:06d}.parquet"
        return (
            self._local_root
            / self._prefix
            / normalize_market_symbol(symbol)
            / PARTS_DIRNAME
            / day_key
            / name
        )

    def _remove_part_files(self, paths: Sequence[Path]) -> None:
        for path in paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                logger.warning("Could not delete trade part %s", path, exc_info=True)
            self._cleanup_empty_dirs(path.parent)

    def _cleanup_empty_dirs(self, start: Path) -> None:
        root = (self._local_root / self._prefix).resolve()
        current = start
        for _ in range(4):
            try:
                resolved = current.resolve()
            except OSError:
                return
            if resolved == root or root not in resolved.parents:
                return
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def flush(self, *, block: bool = True) -> Optional[SessionTradeFlushSummary]:
        """Merge buffered trades into cumulative daily partitions and upload.

        Drains the in-memory buffer under a short lock, then merge-writes while
        holding ``_write_lock``. New ``record_trade`` calls during the write go
        into a fresh buffer and are not part of this flush.

        When ``block`` is False and a write is already running, returns ``None``
        without draining (caller should retry later).
        """
        if not self._write_lock.acquire(blocking=block):
            logger.info("Session trade flush skipped: write already in progress")
            return None

        self._flush_started_monotonic = time.monotonic()
        try:
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
                    for partition_date, partition_frame in _split_by_partition_date(
                        frame
                    ):
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
                    logger.info(
                        "Saved %d %s raw trade(s) to %s",
                        len(frame),
                        symbol,
                        uri,
                    )

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
            self._last_flush_at = datetime.now(timezone.utc)
            return summary
        finally:
            self._flush_started_monotonic = None
            self._write_lock.release()

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


def build_trade_row(
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
) -> Optional[dict[str, object]]:
    """Build one pickleable trade-row dict, or None when the print is invalid."""
    symbol = normalize_market_symbol(symbol)
    if not symbol or price <= 0:
        return None
    return {
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


def databento_trade_row(
    record: Any,
    *,
    symbol: str,
    raw_symbol: str = "",
) -> Optional[dict[str, object]]:
    """Convert a Databento TradeMsg into a persistable row dict."""
    price = _databento_pretty_price(record)
    if price is None or price <= 0:
        return None
    timestamp = _databento_event_time(record)
    return build_trade_row(
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


def _partition_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_utc(value).date()
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return parsed.date()


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
        subset = list(DEDUPE_KEYS_FULL)
    else:
        subset = list(DEDUPE_KEYS_LEGACY)
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
