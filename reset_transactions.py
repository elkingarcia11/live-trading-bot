#!/usr/bin/env python3
"""Reset the transaction ledger file for a fresh forward-test run."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset transaction and audit logs for a fresh run."
    )
    parser.add_argument(
        "--transactions-path",
        default="data/transactions.csv",
        help="Path to the transactions CSV file.",
    )
    parser.add_argument(
        "--audit-path",
        default="logs/audit.jsonl",
        help="Path to the audit JSONL file to clear.",
    )
    parser.add_argument(
        "--clear-audit",
        action="store_true",
        help="Also clear the audit log file.",
    )
    args = parser.parse_args()

    transactions_path = Path(args.transactions_path)
    clear_log(transactions_path, header=TRANSACTION_CSV_COLUMNS)

    if args.clear_audit:
        audit_path = Path(args.audit_path)
        clear_log(audit_path)

    print("Transaction ledger has been reset. New entries will start from an empty ledger.")


if __name__ == "__main__":
    main()
