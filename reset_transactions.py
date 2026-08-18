#!/usr/bin/env python3
"""Reset transaction ledger and forward-test account for a fresh run."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from config import AppConfig
from forward_test_account import ForwardTestAccountState
from transaction_ledger import TRANSACTION_CSV_COLUMNS


def write_header(path: Path, columns: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        writer.writeheader()


def clear_log(path: Path, header: Iterable[str] | None = None) -> None:
    if header is not None:
        write_header(path, header)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    print(f"Cleared {path}")


def _configure_gcs_env(app: AppConfig) -> None:
    gcs = app.gcs
    if gcs.credentials_path:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", gcs.credentials_path)
    if gcs.project_id:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)


def reset_gcs_transactions(app: AppConfig) -> str:
    """Replace the GCS transaction ledger with an empty header-only CSV."""
    from google.cloud import storage

    _configure_gcs_env(app)
    gcs = app.gcs
    blob_path = f"{gcs.transactions_prefix.rstrip('/')}/transactions.csv"

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(TRANSACTION_CSV_COLUMNS))
    writer.writeheader()

    client = storage.Client()
    bucket = client.bucket(gcs.bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(buffer.getvalue(), content_type="text/csv")
    uri = f"gs://{gcs.bucket_name}/{blob_path}"
    print(f"Reset GCS transactions ledger at {uri}")
    return uri


def reset_gcs_forward_test_account(app: AppConfig, *, initial_balance: float) -> str:
    """Replace the forward-test account snapshot with a fresh starting balance."""
    from google.cloud import storage

    _configure_gcs_env(app)
    settings = app.forward_test
    gcs = app.gcs
    blob_path = f"{settings.state_prefix.rstrip('/')}/account.json"

    state = ForwardTestAccountState(
        cash_balance=initial_balance,
        initial_balance=initial_balance,
        realized_pnl=0.0,
        buy_count=0,
        sell_count=0,
        updated_at=datetime.now(timezone.utc).isoformat(),
        open_positions=[],
        trades=[],
    )
    payload = {
        "cash_balance": state.cash_balance,
        "initial_balance": state.initial_balance,
        "realized_pnl": state.realized_pnl,
        "buy_count": state.buy_count,
        "sell_count": state.sell_count,
        "updated_at": state.updated_at,
        "open_positions": [],
        "trades": [],
    }

    client = storage.Client()
    bucket = client.bucket(gcs.bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json",
    )
    uri = f"gs://{gcs.bucket_name}/{blob_path}"
    print(
        f"Reset GCS forward-test account at {uri} "
        f"(cash=${initial_balance:,.2f})"
    )
    return uri


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset transaction and forward-test logs for a fresh run."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Application config path (used for GCS paths and balance).",
    )
    parser.add_argument(
        "--transactions-path",
        default="",
        help="Path to the local transactions CSV (default: from config).",
    )
    parser.add_argument(
        "--audit-path",
        default="logs/audit.jsonl",
        help="Path to the audit JSONL file to clear.",
    )
    parser.add_argument(
        "--initial-balance",
        type=float,
        default=None,
        help="Forward-test starting cash (default: forward_test.initial_balance).",
    )
    parser.add_argument(
        "--gcs",
        action="store_true",
        help="Also reset GCS transactions ledger and forward-test account.",
    )
    parser.add_argument(
        "--clear-audit",
        action="store_true",
        help="Also clear the audit log file.",
    )
    args = parser.parse_args()

    app = AppConfig.load(args.config)
    transactions_path = Path(
        args.transactions_path or app.forward_test.transactions_csv_path
    )
    initial_balance = (
        args.initial_balance
        if args.initial_balance is not None
        else app.forward_test.initial_balance
    )

    clear_log(transactions_path, header=TRANSACTION_CSV_COLUMNS)

    if args.clear_audit:
        clear_log(Path(args.audit_path))

    if args.gcs:
        reset_gcs_transactions(app)
        reset_gcs_forward_test_account(app, initial_balance=initial_balance)

    print(
        "Reset complete. Restart workflow to load the fresh "
        f"${initial_balance:,.2f} balance."
    )


if __name__ == "__main__":
    main()
