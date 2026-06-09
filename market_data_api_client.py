"""Market Data API Client.

Responsibility: HTTP transport to external market data APIs.

Executes authenticated HTTP requests and handles rate limits/retries. Returns
raw provider responses. Does not normalize vendor payloads, persist data, or
manage WebSocket connections.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Optional
from urllib.parse import urljoin

import requests


class MarketDataApiError(Exception):
    """Raised when a market data API request fails."""


class _RateLimiter:
    """Simple sliding-window limiter to respect provider request quotas."""

    def __init__(self, requests_per_minute: int) -> None:
        self._window_seconds = 60.0
        self._max_requests = requests_per_minute
        self._request_times: deque[float] = deque()

    def acquire(self) -> None:
        """Block until another request is allowed under the current window."""
        now = time.monotonic()

        # Drop request timestamps that have fallen outside the window.
        while self._request_times and now - self._request_times[0] >= self._window_seconds:
            self._request_times.popleft()

        if len(self._request_times) >= self._max_requests:
            sleep_for = self._window_seconds - (now - self._request_times[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            self.acquire()
            return

        self._request_times.append(time.monotonic())


class MarketDataApiClient:
    """HTTP client for external market data APIs with auth and rate limiting."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        api_key_header: str = "Authorization",
        api_key_prefix: str = "Bearer ",
        auth_query_param: Optional[str] = None,
        requests_per_minute: int = 60,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        """Initialize the client for a single market data API.

        Args:
            base_url: Provider API root URL (e.g. "https://api.example.com/v1").
            api_key: API key or token used for authentication.
            api_key_header: Header name for key-based auth. Ignored when
                `auth_query_param` is set.
            api_key_prefix: Optional prefix added before the API key in headers,
                such as "Bearer ".
            auth_query_param: Optional query parameter name for key-based auth.
            requests_per_minute: Maximum requests allowed per 60-second window.
            timeout: Per-request timeout in seconds.
            max_retries: Number of retries for retryable HTTP failures.
            retry_backoff_seconds: Base delay used for exponential backoff.
            session: Optional pre-configured `requests.Session`.
        """
        self._base_url = base_url.rstrip("/") + "/"
        self._api_key = api_key
        self._api_key_header = api_key_header
        self._api_key_prefix = api_key_prefix
        self._auth_query_param = auth_query_param
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._session = session or requests.Session()
        self._rate_limiter = _RateLimiter(requests_per_minute)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        """Execute an authenticated HTTP request with rate limiting and retries.

        Args:
            method: HTTP method (e.g. "GET", "POST").
            path: Relative API path (e.g. "bars/AAPL").
            params: Optional query string parameters.
            json: Optional JSON request body.
            headers: Optional extra request headers.

        Returns:
            Parsed JSON response body.

        Raises:
            MarketDataApiError: If the request fails after retries.
        """
        url = urljoin(self._base_url, path.lstrip("/"))
        request_params = self._with_auth_params(params)
        request_headers = self._with_auth_headers(headers)

        last_error: Optional[str] = None
        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire()

            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    params=request_params,
                    json=json,
                    headers=request_headers,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < self._max_retries:
                    self._sleep_before_retry(attempt)
                    continue
                raise MarketDataApiError(
                    f"Request to {url} failed: {exc}"
                ) from exc

            if response.status_code == 429 and attempt < self._max_retries:
                self._sleep_for_rate_limit(response, attempt)
                continue

            if response.status_code >= 500 and attempt < self._max_retries:
                self._sleep_before_retry(attempt)
                continue

            if not response.ok:
                raise MarketDataApiError(
                    f"Request to {url} failed with {response.status_code}: "
                    f"{response.text}"
                )

            if not response.content:
                return {}

            return response.json()

        raise MarketDataApiError(
            f"Request to {url} failed after {self._max_retries + 1} attempts: "
            f"{last_error}"
        )

    def fetch_paginated(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        collection_key: str = "bars",
        page_token_key: Optional[str] = "next_page_token",
        page_token_param: str = "page_token",
    ) -> list[Any]:
        """Fetch and concatenate items from a paginated HTTP endpoint.

        Args:
            path: Relative API path for the paginated endpoint.
            params: Query parameters sent with each page request.
            collection_key: JSON key containing the page items.
            page_token_key: Response key holding the next page token. Set to
                None to disable pagination.
            page_token_param: Query parameter name used to request the next page.

        Returns:
            Accumulated raw items from all fetched pages.

        Raises:
            MarketDataApiError: If any underlying HTTP request fails or the
                response shape is unsupported.
        """
        request_params = dict(params or {})
        collected: list[Any] = []

        while True:
            payload = self.request("GET", path, params=request_params)
            items = (
                payload.get(collection_key, [])
                if isinstance(payload, dict)
                else payload
            )

            if not isinstance(items, list):
                raise MarketDataApiError(
                    f"Expected a list at '{collection_key}', got {type(items).__name__}"
                )

            collected.extend(items)

            if not page_token_key or not isinstance(payload, dict):
                break

            next_page_token = payload.get(page_token_key)
            if not next_page_token:
                break

            request_params[page_token_param] = next_page_token

        return collected

    def _with_auth_params(
        self,
        params: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Attach API key authentication to query parameters when configured."""
        request_params = dict(params or {})
        if self._auth_query_param is not None:
            request_params[self._auth_query_param] = self._api_key
        return request_params

    def _with_auth_headers(
        self,
        headers: Optional[dict[str, str]],
    ) -> dict[str, str]:
        """Attach API key authentication to request headers when configured."""
        request_headers = dict(headers or {})
        if self._auth_query_param is None:
            request_headers[self._api_key_header] = (
                f"{self._api_key_prefix}{self._api_key}"
            )
        return request_headers

    def _sleep_before_retry(self, attempt: int) -> None:
        """Wait using exponential backoff before retrying a failed request."""
        delay = self._retry_backoff_seconds * (2**attempt)
        time.sleep(delay)

    def _sleep_for_rate_limit(self, response: requests.Response, attempt: int) -> None:
        """Wait for provider-imposed rate limits before retrying."""
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            time.sleep(float(retry_after))
            return
        self._sleep_before_retry(attempt)


if __name__ == "__main__":
    from datetime import datetime

    from market_data_transformer import MarketDataTransformer, OhlcvFieldMap

    # HTTP transport only; normalization happens in MarketDataTransformer.
    client = MarketDataApiClient(
        base_url="https://api.example.com/v1",
        api_key="your-api-key",
        requests_per_minute=120,
    )

    status = client.request("GET", "status")
    print(status)

    raw_bars = client.fetch_paginated(
        "bars/AAPL",
        params={
            "timeframe": "1m",
            "start": datetime(2024, 1, 15, 9, 30).isoformat(),
            "end": datetime(2024, 1, 15, 16, 0).isoformat(),
        },
    )

    ohlcv = MarketDataTransformer().from_bars(
        raw_bars,
        field_map=OhlcvFieldMap(),
    )
    print(ohlcv)
