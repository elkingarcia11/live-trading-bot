#!/usr/bin/env python3
"""Authorize Schwab locally and upload OAuth tokens to GCS."""

from __future__ import annotations

import argparse
import logging

from schwab_auth import SchwabAuthError
from schwab_oauth import OAuthCallbackError, run_local_oauth_flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Schwab OAuth browser flow locally and save tokens to "
            ".schwab_tokens.json and gs://{bucket}/{schwab_token_path}."
        )
    )
    parser.add_argument(
        "--scope",
        default="readonly",
        help="Schwab OAuth scope (default: readonly)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the authorize URL without opening a browser automatically",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for the OAuth callback (default: 300)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    try:
        run_local_oauth_flow(
            scope=args.scope,
            open_browser=not args.no_browser,
            timeout_seconds=args.timeout,
        )
    except (SchwabAuthError, OAuthCallbackError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
