"""Tests for account transaction CSV ledger."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from transaction_ledger import TransactionLedger, TransactionRecord, TRANSACTION_CSV_COLUMNS
from option_quote import OptionQuoteSnapshot


def test_transaction_ledger_writes_header_and_entry_row(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
            side="BUY",
            underlying_symbol="SPY",
            instrument_symbol="SPY240117C00480000",
            asset_type="OPTION",
            quantity=2,
            instrument_price=5.0,
            underlying_price=480.25,
            entry_instrument_price=5.0,
            entry_underlying_price=480.25,
            trade_amount=1000.0,
            strategy_name="supertrend",
        )
    )

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["side"] == "BUY"
    assert rows[0]["instrument_price"] == "5.0000"
    assert rows[0]["underlying_price"] == "480.2500"
    assert rows[0]["entry_instrument_price"] == "5.0000"
    assert rows[0]["trade_amount"] == "1000.00"


def test_transaction_ledger_appends_exit_with_entry_prices(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
            side="SELL",
            underlying_symbol="SPY",
            instrument_symbol="SPY240117C00480000",
            asset_type="OPTION",
            quantity=2,
            instrument_price=6.0,
            underlying_price=481.10,
            entry_instrument_price=5.0,
            entry_underlying_price=480.25,
            trade_amount=1200.0,
            trade_pnl=200.0,
            strategy_name="supertrend",
        )
    )

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["side"] == "SELL"
    assert rows[0]["entry_instrument_price"] == "5.0000"
    assert rows[0]["entry_underlying_price"] == "480.2500"
    assert rows[0]["instrument_price"] == "6.0000"
    assert rows[0]["trade_pnl"] == "200.00"


def test_transaction_ledger_records_max_unrealized_pnl(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
            side="SELL",
            underlying_symbol="SPY",
            instrument_symbol="SPY240117C00480000",
            asset_type="OPTION",
            quantity=2,
            instrument_price=6.0,
            underlying_price=481.10,
            entry_instrument_price=5.0,
            trade_pnl=198.7,
            max_unrealized_profit=320.5,
            max_unrealized_loss=-145.25,
            max_unrealized_profit_pct=0.3205,
            max_unrealized_loss_pct=-0.1452,
            strategy_name="supertrend",
        )
    )

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["max_unrealized_profit"] == "320.50"
    assert rows[0]["max_unrealized_loss"] == "-145.25"
    assert rows[0]["max_unrealized_profit_pct"] == "32.05%"
    assert rows[0]["max_unrealized_loss_pct"] == "-14.52%"


def test_transaction_ledger_writes_bid_ask_and_greeks(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
            side="SELL",
            underlying_symbol="SPY",
            instrument_symbol="SPY240117C00480000",
            asset_type="OPTION",
            quantity=2,
            instrument_price=6.0,
            underlying_price=481.10,
            quote=OptionQuoteSnapshot(
                bid=5.95,
                ask=6.05,
                mark=6.0,
                delta=0.52,
                gamma=0.02,
                theta=-0.11,
                vega=0.07,
            ),
            entry_quote=OptionQuoteSnapshot(
                bid=4.95,
                ask=5.05,
                mark=5.0,
                delta=0.48,
            ),
            strategy_name="supertrend",
        )
    )

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert list(rows[0].keys()) == list(TRANSACTION_CSV_COLUMNS)
    assert rows[0]["bid"] == "5.9500"
    assert rows[0]["ask"] == "6.0500"
    assert rows[0]["delta"] == "0.520000"
    assert rows[0]["entry_bid"] == "4.9500"
    assert rows[0]["entry_delta"] == "0.480000"
