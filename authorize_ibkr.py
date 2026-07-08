"""IBKR TWS / IB Gateway connection helper.

Responsibility: Verify that IB Gateway or Trader Workstation is running and
accepting API connections on the configured host, port, and client id.

Enable API access in TWS/IB Gateway:
  Configure → API → Settings → Enable ActiveX and Socket Clients
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify a TWS / IB Gateway socket API connection.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _load_dotenv()
    args = build_parser().parse_args(argv)

    from config import get_config

    app = get_config(reload=True)
    ibkr = app.ibkr
    runtime = IbkrTwsRuntime.from_config()

    try:
        runtime.connect_session(
            host=ibkr.host,
            port=ibkr.port,
            client_id=ibkr.client_id,
            timeout_seconds=ibkr.connect_timeout_seconds,
        )
        output = {
            "connected": runtime.isConnected(),
            "host": ibkr.host,
            "port": ibkr.port,
            "client_id": ibkr.client_id,
            "managed_accounts": runtime.managed_accounts(),
            "next_order_id": runtime.next_order_id(),
        }
        if args.json:
            print(json.dumps(output, indent=2))
        else:
            print("IBKR TWS / IB Gateway connection:")
            for key, value in output.items():
                print(f"  {key}: {value}")
        return 0
    except IbkrTwsError as exc:
        logger.error("%s", exc)
        print(
            "\nCould not connect. Start IB Gateway or TWS, enable API access, "
            f"and confirm config.json → ibkr (host={ibkr.host}, port={ibkr.port}).",
            file=sys.stderr,
        )
        return 1
    finally:
        runtime.disconnect_session()


if __name__ == "__main__":
    raise SystemExit(main())
