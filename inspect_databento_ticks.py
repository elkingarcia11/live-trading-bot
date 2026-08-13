#!/usr/bin/env python3
"""Inspect Databento live trades and optional client-side N-tick aggregation.

Databento live schemas for bars are time-based only (ohlcv-1s / 1m / 1h / 1d).
There is no vendor schema for \"50-tick\" OHLCV — those bars must be built from
``trades`` on the client (same approach as ``TickBarBuilder`` / the live workflow).

Examples:
  .venv/bin/python inspect_databento_ticks.py
  .venv/bin/python inspect_databento_ticks.py --symbol SPY --ticks 50 --max-trades 200
  .venv/bin/python inspect_databento_ticks.py --raw-only
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

from schwab_auth import _load_dotenv
from tick_bar_builder import TickBarBuilder


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
        description="Stream Databento trades and optionally build N-tick bars."
    )
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument(
        "--dataset",
        default=None,
        help="Override dataset (default: config.json databento.dataset)",
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=50,
        help="Client-side tick-bar size (default 50). Ignored with --raw-only.",
    )
    parser.add_argument(
        "--max-trades",
        type=int,
        default=250,
        help="Stop after this many trade prints (0 = run until Ctrl-C).",
    )
    parser.add_argument(
        "--max-bars",
        type=int,
        default=5,
        help="Stop after this many completed tick bars (0 = ignore).",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Print raw trades only; do not aggregate.",
    )
    parser.add_argument(
        "--quiet-ticks",
        action="store_true",
        help="Do not print every trade; only completed bars / summary.",
    )
    args = parser.parse_args()

    _load_dotenv()
    from config import get_config, secret

    app = get_config(reload=True)
    api_key = secret("DATABENTO_API_KEY") or app.databento.api_key
    if not api_key.strip():
        print("DATABENTO_API_KEY is missing in .env", file=sys.stderr)
        return 1

    dataset = (args.dataset or app.databento.dataset).strip()
    symbol = args.symbol.upper()
    ticks = max(args.ticks, 1)

    print("Databento tick inspector")
    print(f"  dataset     : {dataset}")
    print(f"  schema      : trades  (vendor has no N-tick OHLCV schema)")
    print(f"  symbol      : {symbol}")
    print(
        f"  aggregate   : "
        + ("off (--raw-only)" if args.raw_only else f"client-side {ticks}t bars")
    )
    print(
        "  note        : live OHLCV schemas are time-based only "
        "(ohlcv-1s / ohlcv-1m / ohlcv-1h / ohlcv-1d)"
    )
    print()

    try:
        import databento as db
    except ImportError:
        print("Install databento: .venv/bin/pip install databento", file=sys.stderr)
        return 1

    builder = None if args.raw_only else TickBarBuilder(ticks_per_bar=ticks)
    trade_count = 0
    bar_count = 0
    symbol_by_id: dict[int, str] = {}
    stop = {"flag": False}

    def _request_stop(*_args: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    client = db.Live(key=api_key.strip())

    def on_record(record: Any) -> None:
        nonlocal trade_count, bar_count
        if stop["flag"]:
            return

        rtype_name = type(record).__name__
        if rtype_name == "SymbolMappingMsg":
            instrument_id = int(getattr(record, "instrument_id", 0) or 0)
            raw_symbol = str(
                getattr(record, "stype_out_symbol", "")
                or getattr(record, "stype_in_symbol", "")
                or ""
            ).upper()
            if instrument_id and raw_symbol:
                symbol_by_id[instrument_id] = raw_symbol
                print(f"MAP  instrument_id={instrument_id} -> {raw_symbol}")
            return

        if rtype_name == "SystemMsg":
            print(f"SYS  {getattr(record, 'msg', record)}")
            return

        if rtype_name == "ErrorMsg":
            print(f"ERR  {getattr(record, 'err', record)}", file=sys.stderr)
            return

        # TradeMsg / similar
        price = _price(record)
        if price is None:
            return
        size = _size(record)
        ts = _ts_to_utc(record)
        instrument_id = int(getattr(record, "instrument_id", 0) or 0)
        sym = symbol_by_id.get(instrument_id, symbol)

        trade_count += 1
        if not args.quiet_ticks:
            print(
                f"TICK #{trade_count:<5} {sym}  "
                f"px={price:.4f}  sz={size:g}  ts={ts.isoformat()}"
            )

        if builder is not None:
            bar = builder.update(
                symbol=sym,
                price=price,
                size=size,
                timestamp=ts,
            )
            if bar is not None:
                bar_count += 1
                body = bar.get("bar") or {}
                print(
                    f"BAR  #{bar_count:<5} {bar.get('symbol')} {bar.get('timeframe')}  "
                    f"O={float(body.get('open', 0)):.4f} "
                    f"H={float(body.get('high', 0)):.4f} "
                    f"L={float(body.get('low', 0)):.4f} "
                    f"C={float(body.get('close', 0)):.4f} "
                    f"V={float(body.get('volume', 0)):g}  ticks={ticks}  "
                    f"ts={body.get('datetime')}"
                )
                if args.max_bars > 0 and bar_count >= args.max_bars:
                    stop["flag"] = True

        if args.max_trades > 0 and trade_count >= args.max_trades:
            stop["flag"] = True

    def on_error(exc: Exception) -> None:
        print(f"CALLBACK ERROR: {exc}", file=sys.stderr)

    client.subscribe(
        dataset=dataset,
        schema="trades",
        symbols=[symbol],
        stype_in=app.databento.stype_in or "raw_symbol",
    )
    client.add_callback(on_record, exception_callback=on_error)
    client.start()
    print("Streaming… (Ctrl-C to stop)\n")

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

    print()
    print(f"Done. trades={trade_count} completed_{ticks}t_bars={bar_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
