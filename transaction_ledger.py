"""Append-only account transaction CSV for entries and exits.

Records instrument and underlying prices at the time of each buy and sell for
equity and options. Used by forward-test and live execution paths.
"""

from __future__ import annotations

import csv
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from option_quote import OptionQuoteSnapshot, quote_csv_fields

logger = logging.getLogger(__name__)

_BASE_TRANSACTION_COLUMNS = (
    "timestamp",
    "side",
    "underlying_symbol",
    "instrument_symbol",
    "asset_type",
    "quantity",
    "instrument_price",
    "underlying_price",
    "entry_instrument_price",
    "entry_underlying_price",
    "trade_amount",
    "trade_pnl",
    "max_unrealized_profit",
    "max_unrealized_loss",
    "max_unrealized_profit_pct",
    "max_unrealized_loss_pct",
    "strategy_name",
    "execution_mode",
)

_QUOTE_COLUMN_PREFIXES = ("", "entry_")
_QUOTE_FIELD_SUFFIXES = ("bid", "ask", "mark", "delta", "gamma", "theta", "vega")

TRANSACTION_CSV_COLUMNS = _BASE_TRANSACTION_COLUMNS + tuple(
    f"{prefix}{suffix}"
    for prefix in _QUOTE_COLUMN_PREFIXES
    for suffix in _QUOTE_FIELD_SUFFIXES
)


@dataclass(frozen=True)
class TransactionRecord:
    """One buy or sell leg written to the account transactions CSV."""

    timestamp: datetime
    side: str
    underlying_symbol: str
    instrument_symbol: str
    asset_type: str
    quantity: float
    instrument_price: float
    underlying_price: float
    entry_instrument_price: Optional[float] = None
    entry_underlying_price: Optional[float] = None
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
        """Append one transaction row."""
        row = {
            "timestamp": _to_utc(transaction.timestamp).isoformat(),
            "side": transaction.side.upper(),
            "underlying_symbol": transaction.underlying_symbol.upper(),
            "instrument_symbol": transaction.instrument_symbol.upper(),
            "asset_type": transaction.asset_type.upper(),
            "quantity": f"{transaction.quantity:g}",
            "instrument_price": f"{transaction.instrument_price:.4f}",
            "underlying_price": f"{transaction.underlying_price:.4f}",
            "entry_instrument_price": _format_optional_price(
                transaction.entry_instrument_price
            ),
            "entry_underlying_price": _format_optional_price(
                transaction.entry_underlying_price
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
        }
        row.update(quote_csv_fields(transaction.quote, prefix=""))
        row.update(quote_csv_fields(transaction.entry_quote, prefix="entry_"))
        with self._lock:
            with self._csv_path.open("a", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
                writer.writerow(row)

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
            "Migrating transaction CSV to include bid/ask/greeks columns: %s",
            self._csv_path,
        )
        rows: list[dict[str, str]] = []
        with self._csv_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        with self._csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
            writer.writeheader()
            for old_row in rows:
                migrated = {column: old_row.get(column, "") for column in TRANSACTION_CSV_COLUMNS}
                writer.writerow(migrated)

    def _write_header(self) -> None:
        with self._csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
            writer.writeheader()


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
