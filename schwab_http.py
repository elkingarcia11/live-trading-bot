"""Shared Schwab HTTP runtime.

Responsibility: One connection-pooled ``requests.Session`` plus thread pools so
live tick/bar callbacks never block on Schwab REST. Trading work is serial
(one worker) to preserve strategy-state order; independent HTTP (option marks,
chain fetches fired from that worker) uses a small pool.

Does not own OAuth credentials, evaluate strategies, or submit orders.
"""

from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Optional, TypeVar

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_HTTP_WORKERS = 4


class SchwabHttpRuntime:
    """Shared Session + executors for Schwab REST off the feed thread."""

    def __init__(self, *, http_workers: int = DEFAULT_HTTP_WORKERS) -> None:
        workers = max(1, int(http_workers))
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=workers, pool_maxsize=workers)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._http_pool = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="schwab-http",
        )
        self._trading_pool = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="trading-path",
        )
        self._closed = False

    def submit_trading(
        self,
        fn: Callable[..., T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Queue one trading-path job. Never wait on the caller thread."""
        return self._trading_pool.submit(fn, *args, **kwargs)

    def submit_http(
        self,
        fn: Callable[..., T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Fire a Schwab HTTP job without waiting on the caller thread."""
        return self._http_pool.submit(fn, *args, **kwargs)

    def shutdown(self, *, wait: bool = False) -> None:
        """Stop executors and close the shared session."""
        if self._closed:
            return
        self._closed = True
        self._trading_pool.shutdown(wait=wait, cancel_futures=False)
        self._http_pool.shutdown(wait=wait, cancel_futures=False)
        try:
            self.session.close()
        except Exception:
            logger.debug("Schwab HTTP session close failed", exc_info=True)
