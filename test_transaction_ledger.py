"""Tests for account transaction CSV ledger."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

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
            indicators={
                "gaussian_ma_fast": 480.1,
                "gaussian_ma_slow": 480.0,
            },
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
    assert rows[0]["strike"] == "480.0000"
    assert rows[0]["option_type"] == "CALL"
    assert rows[0]["expiration_date"] == "2024-01-17"
    assert rows[0]["gaussian_ma_fast"] == "480.10000000"
    assert rows[0]["gaussian_ma_slow"] == "480.00000000"
    assert json.loads(rows[0]["indicators_json"]) == {
        "gaussian_ma_fast": 480.1,
        "gaussian_ma_slow": 480.0,
    }


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
    assert rows[0]["option_type"] == "CALL"


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


def test_transaction_ledger_parses_put_contract(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2026, 8, 12, 17, 0, tzinfo=timezone.utc),
            side="BUY",
            underlying_symbol="SPY",
            instrument_symbol="SPY   260814P00771000",
            asset_type="OPTION",
            quantity=5,
            instrument_price=1.77,
            underlying_price=771.0,
            strategy_name="gaussian_ma_crossover",
        )
    )
    with csv_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["strike"] == "771.0000"
    assert row["option_type"] == "PUT"
    assert row["expiration_date"] == "2026-08-14"


def test_transaction_ledger_upload_to_gcs_merges_existing(tmp_path: Path) -> None:
    remote_path = tmp_path / "remote.csv"
    remote_ledger = TransactionLedger(remote_path)
    remote_ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 14, 15, 0, tzinfo=timezone.utc),
            side="SELL",
            underlying_symbol="SPY",
            instrument_symbol="SPY",
            asset_type="EQUITY",
            quantity=1,
            instrument_price=479.0,
            underlying_price=479.0,
            strategy_name="prior",
        )
    )
    remote_csv = remote_path.read_text(encoding="utf-8")

    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        TransactionRecord(
            timestamp=datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
            side="BUY",
            underlying_symbol="SPY",
            instrument_symbol="SPY",
            asset_type="EQUITY",
            quantity=1,
            instrument_price=480.0,
            underlying_price=480.0,
            strategy_name="live",
        )
    )

    blob = MagicMock()
    blob.download_as_text.return_value = remote_csv
    bucket = MagicMock()
    bucket.blob.return_value = blob
    client = MagicMock()
    client.bucket.return_value = bucket

    with patch(
        "transaction_ledger.gcs_bucket_exists",
        return_value=True,
    ):
        uri = ledger.upload_to_gcs(
            bucket_name="live-trading-bot",
            prefix="transactions",
            client=client,
        )

    assert uri == "gs://live-trading-bot/transactions/transactions.csv"
    bucket.blob.assert_called_once_with("transactions/transactions.csv")
    uploaded = blob.upload_from_string.call_args.args[0]
    assert "2024-01-14T15:00:00+00:00" in uploaded
    assert "2024-01-15T15:00:00+00:00" in uploaded
    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["strategy_name"] == "prior"
    assert rows[1]["strategy_name"] == "live"
