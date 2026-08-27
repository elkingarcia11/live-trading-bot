"""Tests for account transaction CSV ledger."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import NotFound
from option_quote import OptionQuoteSnapshot
from transaction_ledger import (
    TRANSACTION_CSV_COLUMNS,
    TransactionLedger,
    TransactionRecord,
)


def _record(**overrides: object) -> TransactionRecord:
    values: dict[str, object] = {
        "entry_timestamp": datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
        "exit_timestamp": datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
        "timeframe": "50t",
        "underlying_symbol": "SPY",
        "instrument_symbol": "SPY240117C00480000",
        "asset_type": "OPTION",
        "quantity": 2,
        "entry_instrument_price": 5.0,
        "exit_instrument_price": 6.0,
        "entry_underlying_price": 480.25,
        "exit_underlying_price": 481.10,
        "strategy_name": "supertrend",
    }
    values.update(overrides)
    return TransactionRecord(**values)  # type: ignore[arg-type]


def test_transaction_ledger_writes_round_trip_row(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        _record(
            trade_amount=1200.0,
            trade_pnl=200.0,
            indicators={
                "gaussian_ma_fast": 480.1,
                "gaussian_ma_slow": 480.0,
            },
        )
    )

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["timeframe"] == "50t"
    assert rows[0]["entry_timestamp"] == "2024-01-15T15:00:00+00:00"
    assert rows[0]["exit_timestamp"] == "2024-01-15T18:00:00+00:00"
    assert rows[0]["entry_instrument_price"] == "5.0000"
    assert rows[0]["exit_instrument_price"] == "6.0000"
    assert rows[0]["entry_underlying_price"] == "480.2500"
    assert rows[0]["exit_underlying_price"] == "481.1000"
    assert rows[0]["trade_amount"] == "1200.00"
    assert rows[0]["trade_pnl"] == "200.00"
    assert rows[0]["strike"] == "480.0000"
    assert rows[0]["option_type"] == "CALL"
    assert rows[0]["expiration_date"] == "2024-01-17"
    assert rows[0]["gaussian_ma_fast"] == "480.10000000"
    assert rows[0]["gaussian_ma_slow"] == "480.00000000"
    assert json.loads(rows[0]["indicators_json"]) == {
        "gaussian_ma_fast": 480.1,
        "gaussian_ma_slow": 480.0,
    }
    assert "side" not in rows[0]


def test_transaction_ledger_records_max_unrealized_pnl(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        _record(
            trade_pnl=198.7,
            max_unrealized_profit=320.5,
            max_unrealized_loss=-145.25,
            max_unrealized_profit_pct=0.3205,
            max_unrealized_loss_pct=-0.1452,
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
        _record(
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
        _record(
            entry_timestamp=datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc),
            exit_timestamp=datetime(2026, 8, 12, 17, 0, tzinfo=timezone.utc),
            instrument_symbol="SPY   260814P00771000",
            quantity=5,
            entry_instrument_price=1.77,
            exit_instrument_price=1.55,
            entry_underlying_price=771.0,
            exit_underlying_price=769.5,
            strategy_name="gaussian_ma_crossover",
        )
    )
    with csv_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["strike"] == "771.0000"
    assert row["option_type"] == "PUT"
    assert row["expiration_date"] == "2026-08-14"


def test_transaction_ledger_migrates_legacy_buy_sell_legs(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(
        "timestamp,side,underlying_symbol,instrument_symbol,asset_type,"
        "quantity,instrument_price,underlying_price,entry_instrument_price,"
        "entry_underlying_price,strategy_name,execution_mode\n"
        "2024-01-15T15:00:00+00:00,BUY,SPY,SPY240117C00480000,OPTION,2,"
        "5.0000,480.2500,5.0000,480.2500,supertrend,forward_test\n"
        "2024-01-15T18:00:00+00:00,SELL,SPY,SPY240117C00480000,OPTION,2,"
        "6.0000,481.1000,5.0000,480.2500,supertrend,forward_test\n",
        encoding="utf-8",
    )

    ledger = TransactionLedger(csv_path)
    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["exit_timestamp"] == "2024-01-15T18:00:00+00:00"
    assert rows[0]["entry_instrument_price"] == "5.0000"
    assert rows[0]["exit_instrument_price"] == "6.0000"
    assert rows[0]["entry_underlying_price"] == "480.2500"
    assert rows[0]["exit_underlying_price"] == "481.1000"
    assert ledger.path == csv_path


def test_transaction_ledger_upload_to_gcs_merges_existing(tmp_path: Path) -> None:
    remote_path = tmp_path / "remote.csv"
    remote_ledger = TransactionLedger(remote_path)
    remote_ledger.record(
        _record(
            entry_timestamp=datetime(2024, 1, 14, 14, 0, tzinfo=timezone.utc),
            exit_timestamp=datetime(2024, 1, 14, 15, 0, tzinfo=timezone.utc),
            instrument_symbol="SPY",
            asset_type="EQUITY",
            quantity=1,
            entry_instrument_price=478.0,
            exit_instrument_price=479.0,
            entry_underlying_price=478.0,
            exit_underlying_price=479.0,
            strategy_name="prior",
            timeframe="5m",
        )
    )
    remote_csv = remote_path.read_text(encoding="utf-8")

    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        _record(
            instrument_symbol="SPY",
            asset_type="EQUITY",
            quantity=1,
            entry_instrument_price=480.0,
            exit_instrument_price=481.0,
            entry_underlying_price=480.0,
            exit_underlying_price=481.0,
            strategy_name="live",
            timeframe="5m",
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
    assert "2024-01-15T18:00:00+00:00" in uploaded
    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["strategy_name"] == "prior"
    assert rows[1]["strategy_name"] == "live"


def test_transaction_ledger_upload_daily_uses_exit_date(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        _record(
            entry_timestamp=datetime(2024, 1, 15, 20, 0, tzinfo=timezone.utc),
            exit_timestamp=datetime(2024, 1, 16, 14, 30, tzinfo=timezone.utc),
            timeframe="50t",
        )
    )

    blob = MagicMock()
    blob.download_as_text.side_effect = NotFound("missing")
    bucket = MagicMock()
    bucket.blob.return_value = blob
    client = MagicMock()
    client.bucket.return_value = bucket

    with patch(
        "transaction_ledger.gcs_bucket_exists",
        return_value=True,
    ):
        uris = ledger.upload_daily_to_gcs(
            bucket_name="live-trading-bot",
            prefix="transactions",
            client=client,
        )

    assert uris == ["gs://live-trading-bot/transactions/2024_01_16_50t.csv"]
    bucket.blob.assert_called_once_with("transactions/2024_01_16_50t.csv")
    uploaded = blob.upload_from_string.call_args.args[0]
    assert "50t" in uploaded
    assert "2024-01-15T20:00:00+00:00" in uploaded
    assert "2024-01-16T14:30:00+00:00" in uploaded


def test_transaction_ledger_upload_daily_partitions_by_timeframe(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "transactions.csv"
    ledger = TransactionLedger(csv_path)
    ledger.record(
        _record(
            exit_timestamp=datetime(2024, 1, 16, 14, 30, tzinfo=timezone.utc),
            timeframe="100t",
        )
    )
    ledger.record(
        _record(
            exit_timestamp=datetime(2024, 1, 16, 15, 30, tzinfo=timezone.utc),
            timeframe="400t",
        )
    )

    blobs: dict[str, MagicMock] = {}

    def _blob(name: str) -> MagicMock:
        blob = MagicMock()
        blob.download_as_text.side_effect = NotFound("missing")
        blobs[name] = blob
        return blob

    bucket = MagicMock()
    bucket.blob.side_effect = _blob
    client = MagicMock()
    client.bucket.return_value = bucket

    with patch(
        "transaction_ledger.gcs_bucket_exists",
        return_value=True,
    ):
        uris = ledger.upload_daily_to_gcs(
            bucket_name="live-trading-bot",
            prefix="transactions",
            client=client,
        )

    assert sorted(uris) == [
        "gs://live-trading-bot/transactions/2024_01_16_100t.csv",
        "gs://live-trading-bot/transactions/2024_01_16_400t.csv",
    ]
    assert set(blobs) == {
        "transactions/2024_01_16_100t.csv",
        "transactions/2024_01_16_400t.csv",
    }
