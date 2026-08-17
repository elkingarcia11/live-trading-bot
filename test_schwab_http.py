"""Tests for non-blocking Schwab HTTP/trading execution."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import requests

from schwab_account_sync import SchwabAccountSync
from schwab_http import SchwabHttpRuntime


def test_submit_trading_returns_without_waiting_for_job() -> None:
    runtime = SchwabHttpRuntime(http_workers=1)
    started = threading.Event()
    release = threading.Event()

    def blocking_job() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "done"

    before = time.perf_counter()
    future = runtime.submit_trading(blocking_job)
    elapsed = time.perf_counter() - before

    try:
        assert elapsed < 0.1
        assert started.wait(timeout=1.0)
        assert not future.done()
        release.set()
        assert future.result(timeout=1.0) == "done"
    finally:
        release.set()
        runtime.shutdown(wait=True)


def test_account_sync_accepts_shared_session_and_auth_client() -> None:
    session = requests.Session()
    auth_client = MagicMock()
    trader_client = MagicMock()

    with patch(
        "schwab_account_sync.SchwabTraderClient.from_env",
        return_value=trader_client,
    ) as build:
        sync = SchwabAccountSync.from_env(
            session=session,
            auth_client=auth_client,
        )

    assert sync._trader_client is trader_client
    build.assert_called_once_with(session=session, auth_client=auth_client)
    session.close()
