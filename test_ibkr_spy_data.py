"""Smoke test IBKR TWS auth and historical SPY tick/bar data.

Usage:
  python test_ibkr_spy_data.py
  python test_ibkr_spy_data.py --symbol SPY --minutes 15 --ticks 200
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime
from ibkr_tws_contracts import equity_contract

logger = logging.getLogger(__name__)


def load_ibkr_settings(config_path: Path) -> dict:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    ibkr = payload.get("ibkr", {})
    if not isinstance(ibkr, dict):
        raise ValueError("config.json must include an ibkr object")
    return ibkr


def format_tws_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y%m%d %H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test IBKR TWS connection and historical SPY tick data.",
    )
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--minutes", type=int, default=10, help="Lookback window in minutes")
    parser.add_argument("--ticks", type=int, default=100, help="Max historical ticks to request")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    ibkr = load_ibkr_settings(Path(args.config))
    host = str(ibkr.get("host", "127.0.0.1"))
    port = int(ibkr.get("port", 4002))
    client_id = int(ibkr.get("client_id", 1))
    timeout = float(ibkr.get("connect_timeout_seconds", 30))
    exchange = str(ibkr.get("exchange", "SMART"))
    currency = str(ibkr.get("currency", "USD"))
    market_data_type = int(ibkr.get("market_data_type", 1))
    use_rth = int(ibkr.get("historical_use_rth", 0))

    runtime = IbkrTwsRuntime.from_config()
    symbol = args.symbol.upper()
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=max(args.minutes, 1))

    try:
        runtime.connect_session(
            host=host,
            port=port,
            client_id=client_id,
            timeout_seconds=timeout,
        )
        runtime.set_market_data_type(market_data_type)
        contract = equity_contract(symbol, exchange=exchange, currency=currency)

        auth = {
            "connected": runtime.isConnected(),
            "host": host,
            "port": port,
            "client_id": client_id,
            "managed_accounts": runtime.managed_accounts(),
            "market_data_type": market_data_type,
        }
        logger.info("Authenticated to TWS/IB Gateway as client_id=%s", client_id)

        ticks = runtime.request_historical_ticks(
            contract,
            start_datetime=format_tws_datetime(start),
            end_datetime=format_tws_datetime(end),
            number_of_ticks=max(args.ticks, 1),
            what_to_show="TRADES",
            use_rth=use_rth,
        )

        bars = runtime.request_historical_bars(
            contract,
            end_datetime=end.strftime("%Y%m%d-%H:%M:%S"),
            duration="600 S",
            bar_size="1 min",
            what_to_show=str(ibkr.get("historical_what_to_show", "TRADES")),
            use_rth=use_rth,
        )

        output = {
            "auth": auth,
            "symbol": symbol,
            "window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "historical_ticks": {
                "count": len(ticks),
                "sample": ticks[:5],
                "last": ticks[-1:] if ticks else [],
            },
            "historical_bars_1m": {
                "count": len(bars),
                "sample": [
                    {
                        "date": bar.date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                    }
                    for bar in bars[-3:]
                ],
            },
            "errors": list(runtime._errors),
        }

        if args.json:
            print(json.dumps(output, indent=2, default=str))
        else:
            print("IBKR TWS connection OK")
            print(f"  accounts: {auth['managed_accounts'] or '(pending)'}")
            print(f"  historical ticks ({symbol}): {len(ticks)}")
            for tick in ticks[:5]:
                print(f"    {tick['time']}  price={tick['price']}  size={tick['size']}")
            if len(ticks) > 5:
                print(f"    ... {len(ticks) - 5} more")
            print(f"  historical 1m bars ({symbol}): {len(bars)}")
            for bar in bars[-3:]:
                print(
                    f"    {bar.date}  O={bar.open} H={bar.high} L={bar.low} "
                    f"C={bar.close} V={bar.volume}"
                )
            if runtime._errors:
                print("Warnings/errors from TWS:")
                for message in runtime._errors:
                    print(f"  - {message}")

        if not ticks:
            print(
                "\nNo historical ticks returned. Check market data subscriptions "
                "and whether the requested window had trades.",
                file=sys.stderr,
            )
            return 1
        return 0
    except IbkrTwsError as exc:
        logger.error("%s", exc)
        print(
            "\nFailed. Start IB Gateway/TWS, enable API access, and confirm "
            f"config.json → ibkr (host={host}, port={port}).",
            file=sys.stderr,
        )
        return 1
    finally:
        runtime.disconnect_session()


if __name__ == "__main__":
    raise SystemExit(main())
