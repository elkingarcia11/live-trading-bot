#!/usr/bin/env python3
"""Capture-only Databento raw trades → local + GCS (no bars, no trading).

Ingest stays on the Databento callback. Durability is ``put_nowait`` into a
bounded queue consumed by a background writer process. The writer appends
local parquet parts and periodically compact/merges them into the daily
partition under ``gcs.trades_prefix`` (then GCS). ``queue.Full`` drops the
row and increments ``persist_drops`` — ingest never blocks.

Writer policy (Phase 1): append every 5000 rows by default — never per-tick.
Optional ``--flush-interval`` still exists if you want time-based appends.
Compact/merge+GCS defaults to every 300s plus shutdown.

Examples:
  .venv/bin/python capture_raw_trades.py --quiet
  .venv/bin/python capture_raw_trades.py --flush-every 5000 --metrics-every 5000
  .venv/bin/python capture_raw_trades.py --max-trades 500 --flush-every 100
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from typing import Any

from schwab_auth import _load_dotenv
from trade_capture_runtime import CaptureMetrics, FlushPolicy
from trade_persist import (
    DEFAULT_COMPACT_INTERVAL_S,
    DEFAULT_QUEUE_MAXSIZE,
    TradePersistClient,
    TradePersistWriterConfig,
)

logger = logging.getLogger("capture_raw_trades")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stream Databento raw trades and dump to GCS (no aggregation/trading)."
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Override stream symbol (default: first market.stream_symbols)",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        help="Override Databento dataset (default: config databento.dataset)",
    )
    parser.add_argument(
        "--stype-in",
        default=None,
        help="Override symbology (default: config databento.stype_in)",
    )
    parser.add_argument(
        "--max-trades",
        type=int,
        default=0,
        help="Stop after N trades (0 = run until Ctrl-C).",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=5000,
        help="Writer appends a local parquet part after N queued prints (0 = disable row trigger).",
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=0.0,
        help="Also append every N seconds when the writer batch is non-empty (0 = row-only, default).",
    )
    parser.add_argument(
        "--compact-interval",
        type=float,
        default=DEFAULT_COMPACT_INTERVAL_S,
        help="Merge local parts into daily parquet+GCS every N seconds (0 = shutdown only).",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=DEFAULT_QUEUE_MAXSIZE,
        help="Max queued trades before persist drops (never blocks ingest).",
    )
    parser.add_argument(
        "--metrics-every",
        type=int,
        default=5000,
        help="Log capture metrics every N accepted trades (0 = disable row metrics).",
    )
    parser.add_argument(
        "--metrics-interval",
        type=float,
        default=0.0,
        help="Also log metrics every N seconds (0 = row-only, default).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print every tick (still logs maps/flushes/metrics).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    _load_dotenv()

    from config import get_config, normalize_market_symbol, secret
    from session_trade_recorder import databento_trade_row

    app = get_config(reload=True)
    api_key = secret("DATABENTO_API_KEY") or app.databento.api_key
    if not api_key.strip():
        logger.error("DATABENTO_API_KEY is missing in .env")
        return 1

    if args.symbol:
        symbols = (normalize_market_symbol(args.symbol),)
    elif app.market.stream_symbols:
        symbols = app.market.stream_symbols
    else:
        symbols = app.market.symbols

    dataset = (args.dataset or app.databento.dataset).strip()
    stype_in = (args.stype_in or app.databento.stype_in or "raw_symbol").strip()
    policy = FlushPolicy(
        every_rows=max(0, int(args.flush_every)),
        interval_seconds=max(0.0, float(args.flush_interval)),
    )
    writer_config = TradePersistWriterConfig.from_app(
        app,
        every_rows=policy.every_rows,
        interval_seconds=policy.interval_seconds,
        compact_interval_seconds=max(0.0, float(args.compact_interval)),
    )
    persist = TradePersistClient.start(
        writer_config,
        queue_maxsize=max(1, int(args.queue_size)),
    )
    metrics = CaptureMetrics()
    now0 = time.monotonic()
    metrics.started_at = now0
    metrics.window_started_at = now0

    logger.info("Raw trade capture (no bars / no trading)")
    logger.info("  dataset=%s schema=trades stype_in=%s", dataset, stype_in)
    logger.info("  symbols=%s", ", ".join(symbols))
    logger.info(
        "  storage=gs://%s/%s (+ local %s/%s)",
        app.gcs.bucket_name,
        app.gcs.trades_prefix,
        app.gcs.local_fallback_path,
        app.gcs.trades_prefix,
    )
    logger.info(
        "  persist=put_nowait queue_size=%d; writer append every %s rows%s; "
        "compact every %s; max_trades=%s",
        max(1, int(args.queue_size)),
        policy.every_rows or "∞",
        (
            f" OR every {policy.interval_seconds:g}s"
            if policy.interval_seconds > 0
            else " (row-only)"
        ),
        (
            f"{writer_config.compact_interval_seconds:g}s"
            if writer_config.compact_interval_seconds > 0
            else "shutdown"
        ),
        args.max_trades or "unlimited",
    )
    logger.info(
        "  metrics=every %s rows%s",
        args.metrics_every or "∞",
        (
            f" OR every {args.metrics_interval:g}s"
            if args.metrics_interval > 0
            else " (row-only)"
        ),
    )

    try:
        import databento as db
    except ImportError:
        logger.error("Install databento: .venv/bin/pip install databento")
        persist.shutdown()
        return 1

    rows_since_metrics = 0
    last_metrics_at = time.monotonic()
    symbol_by_id: dict[int, str] = {}
    raw_symbol_by_id: dict[int, str] = {}
    stop = {"flag": False}

    def _request_stop(*_args: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    def _drain_writer_stats() -> None:
        for event in persist.poll_stats():
            metrics.apply_writer_event(event)

    def _log_metrics() -> None:
        nonlocal rows_since_metrics, last_metrics_at
        _drain_writer_stats()
        now = time.monotonic()
        last_metrics_at = now
        rows_since_metrics = 0
        logger.info(metrics.snapshot_line(now=now, buffered=persist.approx_queued()))

    def _maybe_log_metrics(*, force: bool = False) -> None:
        if force:
            _log_metrics()
            return
        row_hit = args.metrics_every > 0 and rows_since_metrics >= args.metrics_every
        time_hit = (
            args.metrics_interval > 0
            and (time.monotonic() - last_metrics_at) >= args.metrics_interval
        )
        if row_hit or time_hit:
            _log_metrics()

    client = db.Live(key=api_key.strip())

    def on_record(record: Any) -> None:
        nonlocal rows_since_metrics
        if stop["flag"]:
            return

        rtype_name = type(record).__name__
        if rtype_name == "SymbolMappingMsg":
            instrument_id = int(getattr(record, "instrument_id", 0) or 0)
            in_sym = str(getattr(record, "stype_in_symbol", "") or "").strip()
            out_sym = str(getattr(record, "stype_out_symbol", "") or "").strip()
            mapped = normalize_market_symbol(in_sym or out_sym)
            if instrument_id and mapped:
                symbol_by_id[instrument_id] = mapped
                raw_symbol_by_id[instrument_id] = out_sym or mapped
                logger.info(
                    "Mapped instrument_id=%s -> %s%s",
                    instrument_id,
                    mapped,
                    f" (raw={out_sym})" if out_sym and out_sym != mapped else "",
                )
            return

        if rtype_name == "SystemMsg":
            logger.info("Databento: %s", getattr(record, "msg", record))
            return

        if rtype_name == "ErrorMsg":
            logger.error("Databento error: %s", getattr(record, "err", record))
            return

        if rtype_name != "TradeMsg":
            return

        instrument_id = int(getattr(record, "instrument_id", 0) or 0)
        sym = symbol_by_id.get(instrument_id, symbols[0])
        raw_sym = raw_symbol_by_id.get(instrument_id, "")
        row = databento_trade_row(record, symbol=sym, raw_symbol=raw_sym)
        if row is None:
            metrics.note_rejected()
            return

        if not persist.try_put(row):
            metrics.note_persist_drop()

        metrics.note_trade()
        rows_since_metrics += 1
        price = float(getattr(record, "pretty_price", 0) or 0)
        size = float(getattr(record, "size", 0) or 0)
        side = str(
            getattr(getattr(record, "side", None), "value", getattr(record, "side", ""))
            or ""
        )
        seq = getattr(record, "sequence", None)

        if not args.quiet:
            logger.info(
                "TICK #%d %s raw=%s px=%.4f sz=%g side=%s seq=%s",
                metrics.trades_captured,
                sym,
                raw_sym or "-",
                price,
                size,
                side or "-",
                seq,
            )

        _maybe_log_metrics()

        if args.max_trades > 0 and metrics.trades_captured >= args.max_trades:
            stop["flag"] = True

    def on_error(exc: Exception) -> None:
        logger.exception("Databento callback error: %s", exc)

    client.subscribe(
        dataset=dataset,
        schema="trades",
        symbols=list(symbols),
        stype_in=stype_in,
    )
    client.add_callback(on_record, exception_callback=on_error)
    client.start()
    logger.info("Streaming raw trades… Ctrl-C to drain persist writer and exit")

    try:
        while not stop["flag"]:
            time.sleep(0.25)
            _maybe_log_metrics()
    finally:
        try:
            client.stop()
        except Exception:
            pass
        try:
            client.terminate()
        except Exception:
            pass
        persist.shutdown()
        _maybe_log_metrics(force=True)
        logger.info(
            "Done. trades_captured=%d rejected=%d persist_drops=%d flushes=%d compacts=%d",
            metrics.trades_captured,
            metrics.rejected_ticks,
            metrics.persist_drops,
            metrics.flushes,
            metrics.compacts,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
