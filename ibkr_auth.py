"""Interactive Brokers Web API session management.

Responsibility: Maintain an authenticated Client Portal Gateway session.

Handles cookie-based session tokens (/tickle), brokerage session initialization
(/iserver/auth/ssodh/init), and request pacing. Does not place orders or
normalize market data.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_GATEWAY_BASE_URL = "https://localhost:5000/v1/api"


class IbkrAuthError(Exception):
    """Raised when IBKR session operations fail."""


@dataclass(frozen=True)
class IbkrAuthStatus:
    """Normalized brokerage session status from /iserver/auth/status."""

    connected: bool
    authenticated: bool
    established: bool = False
    competing: bool = False
    message: str = ""

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> IbkrAuthStatus:
        return cls(
            connected=bool(payload.get("connected")),
            authenticated=bool(payload.get("authenticated")),
            established=bool(payload.get("established", payload.get("authenticated"))),
            competing=bool(payload.get("competing")),
            message=str(payload.get("message", "") or ""),
        )


@dataclass(frozen=True)
class IbkrSessionConfig:
    """Client Portal Gateway connection settings."""

    base_url: str = DEFAULT_GATEWAY_BASE_URL
    verify_ssl: bool = False
    request_timeout_seconds: float = 30.0
    max_requests_per_second: float = 10.0
    tickle_interval_seconds: float = 60.0
    compete_for_session: bool = True
    auth_status_path: str = "iserver/auth/status"
    ssodh_init_path: str = "iserver/auth/ssodh/init"
    tickle_path: str = "tickle"
    logout_path: str = "logout"


class IbkrSessionClient:
    """HTTP client for an authenticated IBKR Client Portal Gateway session."""

    def __init__(
        self,
        config: IbkrSessionConfig,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._base_url = config.base_url.rstrip("/") + "/"
        self._session = session or requests.Session()
        self._session.verify = config.verify_ssl
        self._min_request_interval = (
            1.0 / config.max_requests_per_second
            if config.max_requests_per_second > 0
            else 0.0
        )
        self._last_request_at = 0.0
        self._request_lock = threading.Lock()
        self._keepalive_thread: Optional[threading.Thread] = None
        self._keepalive_stop = threading.Event()

    @classmethod
    def from_config(cls, config: IbkrSessionConfig) -> IbkrSessionClient:
        return cls(config)

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrSessionClient:
        """Build a session client from config.json."""
        if load_dotenv:
            from schwab_auth import _load_dotenv

            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        ibkr = app.ibkr
        return cls(
            IbkrSessionConfig(
                base_url=ibkr.gateway_base_url,
                verify_ssl=ibkr.verify_ssl,
                request_timeout_seconds=ibkr.request_timeout_seconds,
                max_requests_per_second=ibkr.max_requests_per_second,
                tickle_interval_seconds=ibkr.tickle_interval_seconds,
                compete_for_session=ibkr.compete_for_session,
                auth_status_path=ibkr.auth_status_path,
                ssodh_init_path=ibkr.ssodh_init_path,
                tickle_path=ibkr.tickle_path,
                logout_path=ibkr.logout_path,
            )
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        expect_json: bool = True,
    ) -> Any:
        """Send a paced request to the gateway and return the response body."""
        url = self._build_url(path)
        self._pace()
        try:
            response = self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_body,
                timeout=self._config.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise IbkrAuthError(f"IBKR request failed for {method} {path}: {exc}") from exc

        if response.status_code == 429:
            raise IbkrAuthError(
                f"IBKR rate limit exceeded for {method} {path}; retry after backing off"
            )

        if response.status_code >= 400:
            detail = response.text.strip() or response.reason
            raise IbkrAuthError(
                f"IBKR request failed ({response.status_code}) for {method} {path}: {detail}"
            )

        if not expect_json:
            return response
        if not response.text:
            return {}
        return response.json()

    def tickle(self) -> dict[str, Any]:
        """Refresh the gateway session token and api cookie."""
        payload = self.request("POST", self._config.tickle_path, json_body={})
        if not isinstance(payload, dict):
            raise IbkrAuthError("Unexpected /tickle response shape")

        session_token = payload.get("session")
        if session_token:
            self._session.cookies.set("api", str(session_token))
        return payload

    def get_auth_status(self) -> IbkrAuthStatus:
        """Return the current brokerage session status."""
        payload = self.request("GET", self._config.auth_status_path)
        if not isinstance(payload, dict):
            raise IbkrAuthError("Unexpected /iserver/auth/status response shape")
        return IbkrAuthStatus.from_payload(payload)

    def init_brokerage_session(self) -> IbkrAuthStatus:
        """Establish or refresh the trading-enabled brokerage session."""
        payload = self.request(
            "POST",
            self._config.ssodh_init_path,
            json_body={
                "publish": True,
                "compete": self._config.compete_for_session,
            },
        )
        if not isinstance(payload, dict):
            raise IbkrAuthError("Unexpected /iserver/auth/ssodh/init response shape")
        return IbkrAuthStatus.from_payload(payload)

    def ensure_session(self, *, init_brokerage: bool = True) -> IbkrAuthStatus:
        """Tickle, then initialize the brokerage session when needed."""
        tickle_payload = self.tickle()
        status = self._auth_status_from_tickle(tickle_payload)
        if status.authenticated and status.connected:
            return status

        if init_brokerage:
            status = self.init_brokerage_session()
            if status.authenticated:
                return status

        raise IbkrAuthError(
            "IBKR brokerage session is not authenticated. "
            "Log in through the Client Portal Gateway (https://localhost:5000) "
            "and ensure only one active brokerage session exists."
        )

    def start_keepalive(self) -> None:
        """Start a background thread that calls /tickle on an interval."""
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            return

        self._keepalive_stop.clear()

        def _loop() -> None:
            while not self._keepalive_stop.is_set():
                try:
                    self.tickle()
                except IbkrAuthError:
                    logger.exception("IBKR keepalive tickle failed")
                self._keepalive_stop.wait(self._config.tickle_interval_seconds)

        self._keepalive_thread = threading.Thread(
            target=_loop,
            name="ibkr-keepalive",
            daemon=True,
        )
        self._keepalive_thread.start()

    def stop_keepalive(self) -> None:
        """Stop the background keepalive thread."""
        self._keepalive_stop.set()
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            self._keepalive_thread.join(timeout=2.0)
        self._keepalive_thread = None

    def logout(self) -> None:
        """Terminate the gateway session and clear cookies."""
        try:
            self.request("POST", self._config.logout_path, json_body={})
        finally:
            self._session.cookies.clear()

    def _build_url(self, path: str) -> str:
        return self._base_url + path.lstrip("/")

    def _pace(self) -> None:
        if self._min_request_interval <= 0:
            return
        with self._request_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_at
            if elapsed < self._min_request_interval:
                time.sleep(self._min_request_interval - elapsed)
            self._last_request_at = time.monotonic()

    @staticmethod
    def _auth_status_from_tickle(payload: dict[str, Any]) -> IbkrAuthStatus:
        iserver = payload.get("iserver")
        if isinstance(iserver, dict):
            auth_status = iserver.get("authStatus")
            if isinstance(auth_status, dict):
                return IbkrAuthStatus.from_payload(auth_status)
        return IbkrAuthStatus(connected=False, authenticated=False)
