#!/usr/bin/env python3
"""Verify a daily raw-trades parquet partition (local and/or GCS).

Checks row counts, dedupe-key duplicates, timestamp sort order, and large gaps.
Use after the persist writer compact/merges to confirm daily partitions look healthy.

Examples:
  .venv/bin/python verify_trade_partition.py --symbol ES.n.0
  .venv/bin/python verify_trade_partition.py --symbol ES.n.0 --date 2026-08-17
  .venv/bin/python verify_trade_partition.py --symbol ES.n.0 --gap-warn-seconds 120
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound

from schwab_auth import _load_dotenv
from trade_capture_runtime import (
    PartitionVerifyConfig,
    compare_local_remote_counts,
    verify_trade_frame,
)

logger = logging.getLogger("verify_trade_partition")


def _parse_date(raw: Optional[str]) -> date:
    if not raw:
        return datetime.now().date()
    return date.fromisoformat(raw)


def _local_path(app, symbol: str, day: date) -> Path:
    root = Path(app.gcs.local_fallback_path)
    prefix = (app.gcs.trades_prefix or "trades").rstrip("/")
    return root / prefix / symbol / f"{day.isoformat()}.parquet"


def _blob_path(app, symbol: str, day: date) -> str:
    prefix = (app.gcs.trades_prefix or "trades").rstrip("/")
    return f"{prefix}/{symbol}/{day.isoformat()}.parquet"


def _read_local(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _read_gcs(bucket_name: str, blob_path: str) -> Optional[pd.DataFrame]:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_path)
    try:
        raw = blob.download_as_bytes()
    except NotFound:
        return None
    return pd.read_parquet(io.BytesIO(raw))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify daily ES/SPY raw-trades parquet merges (local + GCS)."
    )
    parser.add_argument("--symbol", default=None, help="Symbol folder (default: ES.n.0)")
    parser.add_argument(
        "--date",
        default=None,
        help="Partition date YYYY-MM-DD (default: today local).",
    )
    parser.add_argument(
        "--gap-warn-seconds",
        type=float,
        default=60.0,
        help="Flag consecutive timestamp gaps larger than this (default: 60).",
    )
    parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Treat large gaps as errors (default: warn only; ES has a daily break).",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Skip GCS read/compare.",
    )
    parser.add_argument(
        "--remote-only",
        action="store_true",
        help="Skip local read/compare.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    _load_dotenv()

    from config import get_config, normalize_market_symbol

    app = get_config(reload=True)
    if args.symbol:
        symbol = normalize_market_symbol(args.symbol)
    elif app.market.stream_symbols:
        symbol = app.market.stream_symbols[0]
    else:
        symbol = "ES.n.0"
    day = _parse_date(args.date)
    cfg = PartitionVerifyConfig(
        gap_warn_seconds=max(0.0, float(args.gap_warn_seconds)),
        fail_on_gaps=bool(args.fail_on_gaps),
    )

    local_frame: Optional[pd.DataFrame] = None
    remote_frame: Optional[pd.DataFrame] = None
    local_path = _local_path(app, symbol, day)
    blob_path = _blob_path(app, symbol, day)

    if not args.remote_only:
        local_frame = _read_local(local_path)
        logger.info(
            "Local %s: %s",
            local_path,
            "missing" if local_frame is None else f"{len(local_frame)} rows",
        )

    if not args.local_only:
        try:
            remote_frame = _read_gcs(app.gcs.bucket_name, blob_path)
        except Exception:
            logger.exception("Failed reading gs://%s/%s", app.gcs.bucket_name, blob_path)
            return 2
        logger.info(
            "GCS gs://%s/%s: %s",
            app.gcs.bucket_name,
            blob_path,
            "missing" if remote_frame is None else f"{len(remote_frame)} rows",
        )

    results = []
    exit_code = 0

    if local_frame is not None:
        result = verify_trade_frame(local_frame, source=f"local:{local_path}", config=cfg)
        results.append(result)
    if remote_frame is not None:
        result = verify_trade_frame(
            remote_frame,
            source=f"gcs:gs://{app.gcs.bucket_name}/{blob_path}",
            config=cfg,
        )
        results.append(result)

    if not results:
        logger.error("No partition found for %s %s", symbol, day.isoformat())
        return 1

    if local_frame is not None and not args.local_only:
        count_issues = compare_local_remote_counts(
            len(local_frame),
            None if remote_frame is None else len(remote_frame),
        )
        for issue in count_issues:
            logger.error("Compare: %s", issue)
            exit_code = 1

    for result in results:
        logger.info(
            "Verify %s: rows=%d dups=%d unsorted=%s max_gap_s=%.1f "
            "gaps_over=%d first=%s last=%s",
            result.source,
            result.rows,
            result.duplicate_rows,
            result.unsorted,
            result.max_gap_seconds,
            result.gap_count_over_threshold,
            result.first_ts,
            result.last_ts,
        )
        for warning in result.warnings:
            logger.warning("  warning: %s", warning)
        if result.issues:
            exit_code = 1
            for issue in result.issues:
                logger.error("  issue: %s", issue)
        elif not result.warnings:
            logger.info("  ok")
        else:
            logger.info("  ok (with gap warnings)")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
