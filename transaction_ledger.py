"""Append-only account transaction CSV for completed round-trips.

Each row is one closed trade: timeframe, entry/exit timestamps, and entry/exit
prices for both the contract and the underlying. Local appends stay cheap;
``upload_daily_to_gcs`` merges into ``transactions/YYYY_MM_DD_{timeframe}.csv``.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from google.cloud import storage
from google.cloud.exceptions import NotFound

from local_storage_repository import gcs_bucket_exists
from option_quote import OptionQuoteSnapshot, quote_csv_fields
from option_selector import parse_occ_symbol

logger = logging.getLogger(__name__)

_TRANSACTION_DEDUPE_KEYS = (
    "entry_timestamp",
    "exit_timestamp",
    "instrument_symbol",
    "quantity",
    "entry_instrument_price",
    "exit_instrument_price",
    "strategy_name",
    "execution_mode",
    "timeframe",
)

_BASE_TRANSACTION_COLUMNS = (
    "timeframe",
    "entry_timestamp",
    "exit_timestamp",
    "underlying_symbol",
    "instrument_symbol",
    "asset_type",
    "strike",
    "option_type",
    "expiration_date",
    "quantity",
    "entry_instrument_price",
    "exit_instrument_price",
    "entry_underlying_price",
    "exit_underlying_price",
    "trade_amount",
    "trade_pnl",
    "max_unrealized_profit",
    "max_unrealized_loss",
    "max_unrealized_profit_pct",
    "max_unrealized_loss_pct",
    "strategy_name",
    "execution_mode",
    "gaussian_ma_fast",
    "gaussian_ma_slow",
    "fast_gma_length",
    "fast_gma_sigma",
    "slow_gma_length",
    "slow_gma_sigma",
    "indicators_json",
)

_QUOTE_COLUMN_PREFIXES = ("", "entry_")
_QUOTE_FIELD_SUFFIXES = ("bid", "ask", "mark", "delta",
                         "gamma", "theta", "vega")

TRANSACTION_CSV_COLUMNS = _BASE_TRANSACTION_COLUMNS + tuple(
    f"{prefix}{suffix}"
    for prefix in _QUOTE_COLUMN_PREFIXES
    for suffix in _QUOTE_FIELD_SUFFIXES
)


@dataclass(frozen=True)
class TransactionRecord:
    """One completed round-trip written to the account transactions CSV."""

    entry_timestamp: datetime
    exit_timestamp: datetime
    timeframe: str
    underlying_symbol: str
    instrument_symbol: str
    asset_type: str
    quantity: float
    entry_instrument_price: float
    exit_instrument_price: float
    entry_underlying_price: Optional[float] = None
    exit_underlying_price: Optional[float] = None
    trade_amount: Optional[float] = None
    trade_pnl: Optional[float] = None
    max_unrealized_profit: Optional[float] = None
    max_unrealized_loss: Optional[float] = None
    max_unrealized_profit_pct: Optional[float] = None
    max_unrealized_loss_pct: Optional[float] = None
    strategy_name: str = ""
    execution_mode: str = "forward_test"
    quote: Optional[OptionQuoteSnapshot] = None
    entry_quote: Optional[OptionQuoteSnapshot] = None
    indicators: Mapping[str, Any] = field(default_factory=dict)
    strike: Optional[float] = None
    option_type: str = ""
    expiration_date: str = ""
    fast_gma_length: Optional[int] = None
    fast_gma_sigma: Optional[float] = None
    slow_gma_length: Optional[int] = None
    slow_gma_sigma: Optional[float] = None


class TransactionLedger:
    """Thread-safe append-only CSV ledger for account transactions."""

    def __init__(self, csv_path: str | Path) -> None:
        self._csv_path = Path(csv_path)
        self._lock = threading.Lock()
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()
        logger.info("Transaction ledger writing to %s", self._csv_path)

    @property
    def path(self) -> Path:
        return self._csv_path

    def record(self, transaction: TransactionRecord) -> None:
        """Append one completed round-trip row."""
        strike, option_type, expiration_date = _resolve_contract_fields(
            transaction)
        indicators = dict(transaction.indicators or {})
        row = {
            "timeframe": (transaction.timeframe or "").strip(),
            "entry_timestamp": _to_utc(transaction.entry_timestamp).isoformat(),
            "exit_timestamp": _to_utc(transaction.exit_timestamp).isoformat(),
            "underlying_symbol": transaction.underlying_symbol.upper(),
            "instrument_symbol": transaction.instrument_symbol.upper(),
            "asset_type": transaction.asset_type.upper(),
            "strike": _format_optional_price(strike),
            "option_type": option_type,
            "expiration_date": expiration_date,
            "quantity": f"{transaction.quantity:g}",
            "entry_instrument_price": f"{transaction.entry_instrument_price:.4f}",
            "exit_instrument_price": f"{transaction.exit_instrument_price:.4f}",
            "entry_underlying_price": _format_optional_price(
                transaction.entry_underlying_price
            ),
            "exit_underlying_price": _format_optional_price(
                transaction.exit_underlying_price
            ),
            "trade_amount": _format_optional_money(transaction.trade_amount),
            "trade_pnl": _format_optional_money(transaction.trade_pnl),
            "max_unrealized_profit": _format_optional_money(
                transaction.max_unrealized_profit
            ),
            "max_unrealized_loss": _format_optional_money(
                transaction.max_unrealized_loss
            ),
            "max_unrealized_profit_pct": _format_optional_pct(
                transaction.max_unrealized_profit_pct
            ),
            "max_unrealized_loss_pct": _format_optional_pct(
                transaction.max_unrealized_loss_pct
            ),
            "strategy_name": transaction.strategy_name,
            "execution_mode": transaction.execution_mode,
            "gaussian_ma_fast": _format_optional_indicator(
                indicators.get("gaussian_ma_fast")
            ),
            "gaussian_ma_slow": _format_optional_indicator(
                indicators.get("gaussian_ma_slow")
            ),
            "fast_gma_length": _format_optional_int(transaction.fast_gma_length),
            "fast_gma_sigma": _format_optional_number(transaction.fast_gma_sigma),
            "slow_gma_length": _format_optional_int(transaction.slow_gma_length),
            "slow_gma_sigma": _format_optional_number(transaction.slow_gma_sigma),
            "indicators_json": _serialize_indicators(indicators),
        }
        row.update(quote_csv_fields(transaction.quote, prefix=""))
        row.update(quote_csv_fields(transaction.entry_quote, prefix="entry_"))
        with self._lock:
            with self._csv_path.open("a", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=TRANSACTION_CSV_COLUMNS)
                writer.writerow(row)

    def upload_to_gcs(
        self,
        *,
        bucket_name: str,
        prefix: str = "transactions",
        blob_name: str = "transactions.csv",
        credentials_path: str = "",
        project_id: str = "",
        client: Optional[storage.Client] = None,
    ) -> Optional[str]:
        """Merge local CSV into existing GCS ledger and upload. Returns gs:// URI."""
        with self._lock:
            local_rows = _read_transaction_rows(self._csv_path)

        if credentials_path:
            os.environ.setdefault(
                "GOOGLE_APPLICATION_CREDENTIALS", credentials_path)
        if project_id:
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)

        storage_client = client or storage.Client()
        if not gcs_bucket_exists(bucket_name, storage_client):
            logger.warning(
                "GCS bucket gs://%s not found; keeping transactions at %s only",
                bucket_name,
                self._csv_path,
            )
            return None

        blob_path = f"{prefix.strip().strip('/')}/{blob_name.strip().lstrip('/')}"
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            remote_rows = _download_transaction_rows(blob)
            merged = _merge_transaction_rows(remote_rows, local_rows)
            if not merged:
                logger.info(
                    "Transaction ledger merge skipped; no rows in local or GCS (%s)",
                    blob_path,
                )
                return None

            with self._lock:
                _write_transaction_rows(self._csv_path, merged)
                payload = self._csv_path.read_text(encoding="utf-8")
            blob.upload_from_string(payload, content_type="text/csv")
            uri = f"gs://{bucket_name}/{blob_path}"
            logger.info(
                "Merged transaction ledger to %s (remote=%d local=%d merged=%d)",
                uri,
                len(remote_rows),
                len(local_rows),
                len(merged),
            )
            return uri
        except Exception:
            logger.exception(
                "Failed merging/uploading transactions to gs://%s/%s",
                bucket_name,
                blob_path,
            )
            return None

    def upload_daily_to_gcs(
        self,
        *,
        bucket_name: str,
        prefix: str = "transactions",
        credentials_path: str = "",
        project_id: str = "",
        client: Optional[storage.Client] = None,
    ) -> list[str]:
        """Append all local rows to daily partitioned GCS CSVs.

        Each row is written to ``transactions/YYYY_MM_DD_{timeframe}.csv``
        keyed by the row's exit date and timeframe. If a file already exists in
        GCS its rows are preserved and only genuinely new rows are appended
        (deduped so repeated shutdown exports never duplicate a trade). Returns
        the list of ``gs://`` URIs written.
        """
        with self._lock:
            local_rows = _read_transaction_rows(self._csv_path)

        if credentials_path:
            os.environ.setdefault(
                "GOOGLE_APPLICATION_CREDENTIALS", credentials_path)
        if project_id:
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)

        storage_client = client or storage.Client()
        if not gcs_bucket_exists(bucket_name, storage_client):
            logger.warning(
                "GCS bucket gs://%s not found; keeping transactions at %s only",
                bucket_name,
                self._csv_path,
            )
            return []

        base_prefix = prefix.strip().strip("/")
        bucket = storage_client.bucket(bucket_name)
        by_partition: dict[tuple[str, str], list[dict[str, str]]] = {}
        for row in local_rows:
            partition = _row_transaction_partition(row)
            if partition is None:
                continue
            by_partition.setdefault(partition, []).append(row)

        uris: list[str] = []
        for partition in sorted(by_partition):
            day, timeframe = partition
            blob_name = _daily_transaction_blob_name(day, timeframe)
            blob_path = f"{base_prefix}/{blob_name}"
            blob = bucket.blob(blob_path)
            try:
                remote_rows = _download_transaction_rows(blob)
            except Exception:
                logger.exception(
                    "Failed downloading existing daily transactions %s",
                    blob_path,
                )
                remote_rows = []
            merged = _merge_transaction_rows(remote_rows, by_partition[partition])
            if not merged:
                logger.info(
                    "Daily transaction export skipped; no rows for %s",
                    blob_path,
                )
                continue
            try:
                payload = _serialize_transaction_rows(merged)
                blob.upload_from_string(payload, content_type="text/csv")
            except Exception:
                logger.exception(
                    "Failed uploading daily transactions to gs://%s/%s",
                    bucket_name,
                    blob_path,
                )
                continue
            uris.append(f"gs://{bucket_name}/{blob_path}")
            logger.info(
                "Appended transactions to %s (existing=%d new=%d total=%d)",
                f"gs://{bucket_name}/{blob_path}",
                len(remote_rows),
                len(by_partition[partition]),
                len(merged),
            )
        return uris

    def _ensure_header(self) -> None:
        if not self._csv_path.exists() or self._csv_path.stat().st_size == 0:
            self._write_header()
            return

        with self._csv_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
        if header == list(TRANSACTION_CSV_COLUMNS):
            return

        logger.info(
            "Migrating transaction CSV to latest columns: %s",
            self._csv_path,
        )
        rows: list[dict[str, str]] = []
        with self._csv_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        with self._csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
            writer.writeheader()
            for old_row in rows:
                migrated = _normalize_transaction_row(old_row)
                if migrated is None:
                    continue
                writer.writerow(migrated)

    def _write_header(self) -> None:
        with self._csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
            writer.writeheader()


def _resolve_contract_fields(
    transaction: TransactionRecord,
) -> tuple[Optional[float], str, str]:
    if (
        transaction.strike is not None
        or transaction.option_type
        or transaction.expiration_date
    ):
        option_type = transaction.option_type.upper()
        if option_type in {"C", "CALL"}:
            option_type = "CALL"
        elif option_type in {"P", "PUT"}:
            option_type = "PUT"
        return transaction.strike, option_type, transaction.expiration_date
    return _contract_fields_from_symbol(
        transaction.instrument_symbol,
        transaction.asset_type,
    )


def _contract_fields_from_symbol(
    instrument_symbol: str,
    asset_type: str,
) -> tuple[Optional[float], str, str]:
    if str(asset_type or "").upper() != "OPTION":
        return None, "", ""
    parsed = parse_occ_symbol(instrument_symbol)
    if parsed is None:
        return None, "", ""
    option_type = "CALL" if parsed.option_right == "C" else "PUT"
    return parsed.strike, option_type, parsed.expiration_date.isoformat()


def _serialize_indicators(indicators: Mapping[str, Any]) -> str:
    if not indicators:
        return ""
    payload: dict[str, Any] = {}
    for key in sorted(indicators):
        value = indicators[key]
        if value is None or isinstance(value, (str, bool, int)):
            payload[key] = value
            continue
        if isinstance(value, float):
            payload[key] = value
            continue
        item = getattr(value, "item", None)
        if callable(item):
            try:
                payload[key] = item()
                continue
            except Exception:
                pass
        payload[key] = str(value)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_optional_price(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.4f}"


def _format_optional_money(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _format_optional_pct(value: Optional[float]) -> str:
    """Format a P&L fraction (0.2 -> '20.00%') for the ledger CSV."""
    if value is None:
        return ""
    return f"{value * 100.0:.2f}%"


def _format_optional_int(value: Optional[int]) -> str:
    """Format an integer indicator parameter (e.g. GMA length) for the ledger CSV."""
    if value is None or value == "":
        return ""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _format_optional_number(value: Any) -> str:
    """Format a numeric indicator parameter (e.g. GMA sigma divisor)."""
    if value is None or value == "":
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:g}"


def _format_optional_indicator(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.8f}"
    except (TypeError, ValueError):
        return str(value)


def _read_transaction_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return _normalize_transaction_rows(csv.DictReader(handle))


def _download_transaction_rows(blob: Any) -> list[dict[str, str]]:
    try:
        raw = blob.download_as_text(encoding="utf-8")
    except NotFound:
        return []
    except Exception:
        logger.exception(
            "Failed downloading existing transaction ledger from GCS")
        return []
    if not raw.strip():
        return []
    return _normalize_transaction_rows(csv.DictReader(io.StringIO(raw)))


def _normalize_transaction_rows(rows: Any) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for row in rows:
        migrated = _normalize_transaction_row(row)
        if migrated is not None:
            normalized.append(migrated)
    return normalized


def _normalize_transaction_row(old_row: Mapping[str, Any]) -> Optional[dict[str, str]]:
    """Map a CSV dict onto the current schema; drop incomplete legacy BUY legs."""
    side = str(old_row.get("side") or "").upper()
    timestamp = str(old_row.get("timestamp") or "")
    instrument_price = str(old_row.get("instrument_price") or "")
    underlying_price = str(old_row.get("underlying_price") or "")

    migrated = {
        column: str(old_row.get(column, "") or "")
        for column in TRANSACTION_CSV_COLUMNS
    }

    if side == "BUY" and not migrated.get("exit_timestamp") and not migrated.get(
        "exit_instrument_price"
    ):
        return None

    if not migrated.get("exit_timestamp") and timestamp and side != "BUY":
        migrated["exit_timestamp"] = timestamp
    if not migrated.get("exit_instrument_price") and instrument_price and side != "BUY":
        migrated["exit_instrument_price"] = instrument_price
    if not migrated.get("exit_underlying_price") and underlying_price and side != "BUY":
        migrated["exit_underlying_price"] = underlying_price

    if not migrated.get("indicators_json") and (
        old_row.get("gaussian_ma_fast") or old_row.get("gaussian_ma_slow")
    ):
        migrated["indicators_json"] = _serialize_indicators(
            {
                key: old_row.get(key)
                for key in ("gaussian_ma_fast", "gaussian_ma_slow")
                if old_row.get(key)
            }
        )
    if not migrated.get("strike") or not migrated.get("option_type"):
        strike, option_type, expiration = _contract_fields_from_symbol(
            str(old_row.get("instrument_symbol", "")),
            str(old_row.get("asset_type", "")),
        )
        if not migrated.get("strike") and strike is not None:
            migrated["strike"] = _format_optional_price(strike)
        if not migrated.get("option_type"):
            migrated["option_type"] = option_type
        if not migrated.get("expiration_date"):
            migrated["expiration_date"] = expiration
    return migrated


def _merge_transaction_rows(
    remote_rows: list[dict[str, str]],
    local_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[tuple[str, ...], dict[str, str]] = {}
    for row in remote_rows + local_rows:
        key = tuple(row.get(column, "") for column in _TRANSACTION_DEDUPE_KEYS)
        merged[key] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            row.get("exit_timestamp", ""),
            row.get("entry_timestamp", ""),
        ),
    )


def _write_transaction_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: row.get(column, "")
                 for column in TRANSACTION_CSV_COLUMNS}
            )


def _serialize_transaction_rows(rows: list[dict[str, str]]) -> str:
    """Render transaction rows as CSV text including the header row."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=TRANSACTION_CSV_COLUMNS)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {column: row.get(column, "")
             for column in TRANSACTION_CSV_COLUMNS}
        )
    return buffer.getvalue()


def _row_transaction_date(row: Mapping[str, str]) -> Optional[str]:
    """Return the ``YYYY-MM-DD`` partition date for a completed trade."""
    for key in ("exit_timestamp", "entry_timestamp"):
        timestamp = row.get(key, "")
        if not timestamp:
            continue
        day = timestamp[:10]
        if len(day) == 10 and "-" in day:
            return day
    return None


def _row_timeframe_label(row: Mapping[str, str]) -> str:
    timeframe = str(row.get("timeframe") or "").strip()
    return timeframe or "unknown"


def _row_transaction_partition(
    row: Mapping[str, str],
) -> Optional[tuple[str, str]]:
    """Return ``(YYYY-MM-DD, timeframe)`` for daily GCS partitioning."""
    day = _row_transaction_date(row)
    if day is None:
        return None
    return day, _row_timeframe_label(row)


def _daily_transaction_blob_name(day: str, timeframe: str) -> str:
    """Return ``YYYY_MM_DD_{timeframe}.csv`` for one daily export file."""
    return f"{day.replace('-', '_')}_{timeframe}.csv"
