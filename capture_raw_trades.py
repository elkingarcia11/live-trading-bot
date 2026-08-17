#!/usr/bin/env python3
"""Capture-only Databento raw trades → local + GCS (no bars, no trading).

Buffers every trade print in memory and merges into the existing daily parquet
under ``gcs.trades_prefix`` on flush (periodic + shutdown).

Examples:
  .venv/bin/python capture_raw_trades.py
  .venv/bin/python capture_raw_trades.py --max-trades 500 --flush-every 100
  .venv/bin/python capture_raw_trades.py --symbol ES.n.0 --quiet
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

from schwab_auth import _load_dotenv

logger = logging.getLogger("capture_raw_trades")


def _ts_to_utc(record: Any) -> datetime:
    for attr in ("ts_event", "ts_recv"):
        value = getattr(record, attr, None)
        if value is None:
            continue
        try:
            nanos = int(value)
        except (TypeError, ValueError):
            continue
        if nanos <= 0:
            continue
        return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _price(record: Any) -> Optional[float]:
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


def _size(record: Any) -> float:
    for attr in ("size", "quantity"):
        raw = getattr(record, attr, None)
        if raw is None:
            continue
        try:
            size = float(raw)
        except (TypeError, ValueError):
            continue
        if size > 0:
            return size
    return 1.0


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
        default=500,
        help="Flush buffered trades to local+GCS every N prints (0 = only on exit).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print every tick (still logs maps/flushes).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    _load_dotenv()

    from config import get_config, normalize_market_symbol, secret
    from session_trade_recorder import build_trade_recorder

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
    recorder = build_trade_recorder(app)

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
        "  flush_every=%s max_trades=%s",
        args.flush_every or "exit-only",
        args.max_trades or "unlimited",
    )

    try:
        import databento as db
    except ImportError:
        logger.error("Install databento: .venv/bin/pip install databento")
        return 1

    trade_count = 0
    since_flush = 0
    symbol_by_id: dict[int, str] = {}
    raw_symbol_by_id: dict[int, str] = {}
    stop = {"flag": False}

    def _request_stop(*_args: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    def _flush(*, reason: str) -> None:
        nonlocal since_flush
        buffered = recorder.buffered_row_count
        if buffered <= 0:
            logger.info("Flush (%s): nothing buffered", reason)
            return
        summary = recorder.flush()
        since_flush = 0
        logger.info(
            "Flush (%s): buffered=%d written=%d partitions=%d uris=%s",
            reason,
            summary.rows_buffered,
            summary.rows_written,
            summary.partitions_written,
            list(summary.storage_uris),
        )

    client = db.Live(key=api_key.strip())

    def on_record(record: Any) -> None:
        nonlocal trade_count, since_flush
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
        if not recorder.record_databento_trade(
            record,
            symbol=sym,
            raw_symbol=raw_sym,
        ):
            return

        trade_count += 1
        since_flush += 1
        price = float(getattr(record, "pretty_price", 0) or 0)
        size = float(getattr(record, "size", 0) or 0)
        side = str(getattr(getattr(record, "side", None), "value", getattr(record, "side", "")) or "")
        seq = getattr(record, "sequence", None)

        if not args.quiet:
            logger.info(
                "TICK #%d %s raw=%s px=%.4f sz=%g side=%s seq=%s",
                trade_count,
                sym,
                raw_sym or "-",
                price,
                size,
                side or "-",
                seq,
            )
        elif trade_count % 100 == 0:
            logger.info(
                "Captured %d trades (buffered=%d)",
                trade_count,
                recorder.buffered_row_count,
            )

        if args.flush_every > 0 and since_flush >= args.flush_every:
            _flush(reason=f"every-{args.flush_every}")

        if args.max_trades > 0 and trade_count >= args.max_trades:
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
    logger.info("Streaming raw trades… Ctrl-C to flush and exit")

    try:
        while not stop["flag"]:
            time.sleep(0.25)
    finally:
        try:
            client.stop()
        except Exception:
            pass
        try:
            client.terminate()
        except Exception:
            pass
        _flush(reason="shutdown")
        logger.info("Done. trades_captured=%d", trade_count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
