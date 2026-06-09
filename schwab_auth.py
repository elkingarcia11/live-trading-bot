"""Schwab OAuth token management.

Responsibility: Acquire and refresh Schwab API access tokens.

Handles OAuth token exchange and refresh. Does not perform market data
requests, WebSocket streaming, or order submission.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://api.schwabapi.com"
DEFAULT_TOKEN_PATH = "/v1/oauth/token"


class SchwabAuthError(Exception):
    """Raised when Schwab OAuth operations fail."""


@dataclass(frozen=True)
class SchwabAuthConfig:
    """OAuth credentials and endpoint configuration."""

    app_key: str
    app_secret: str
    api_base_url: str = DEFAULT_API_BASE_URL
    token_path: str = DEFAULT_TOKEN_PATH
    callback_url: str = "https://127.0.0.1:8182/callback"


@dataclass
class SchwabTokens:
    """Cached OAuth tokens with expiry metadata."""

    access_token: str
    refresh_token: str
    expires_at: float

    @classmethod
    def from_token_response(cls, payload: dict[str, Any]) -> SchwabTokens:
        """Build tokens from a Schwab OAuth token endpoint response."""
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        if not access_token or not refresh_token:
            raise SchwabAuthError("Token response missing access_token or refresh_token")

        expires_in = int(payload.get("expires_in", 1800))
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=time.time() + max(expires_in - 60, 0),
        )

    def is_expired(self) -> bool:
        """Return whether the access token should be refreshed."""
        return time.time() >= self.expires_at


class SchwabTokenStore:
    """Read and write Schwab OAuth tokens from disk."""

    def __init__(self, token_file: str | Path) -> None:
        self._path = Path(token_file)

    def load(self) -> Optional[SchwabTokens]:
        """Load tokens from disk when the token file exists."""
        if not self._path.exists():
            return None

        payload = json.loads(self._path.read_text(encoding="utf-8"))
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        expires_at = payload.get("expires_at")
        if not access_token or not refresh_token or expires_at is None:
            raise SchwabAuthError(f"Invalid token file at {self._path}")

        return SchwabTokens(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=float(expires_at),
        )

    def save(self, tokens: SchwabTokens) -> None:
        """Persist tokens to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
            "expires_at": tokens.expires_at,
        }
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class SchwabAuthClient:
    """Refresh and cache Schwab OAuth access tokens."""

    def __init__(
        self,
        config: SchwabAuthConfig,
        *,
        token_store: Optional[SchwabTokenStore] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._token_store = token_store
        self._session = session or requests.Session()
        self._tokens: Optional[SchwabTokens] = None

        if self._token_store is not None:
            self._tokens = self._token_store.load()

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> SchwabAuthClient:
        """Build an auth client from environment variables."""
        if load_dotenv:
            _load_dotenv()

        app_key = _require_env("SCHWAB_APP_KEY")
        app_secret = _require_env("SCHWAB_APP_SECRET")
        config = SchwabAuthConfig(
            app_key=app_key,
            app_secret=app_secret,
            api_base_url=os.getenv("SCHWAB_API_BASE_URL", DEFAULT_API_BASE_URL),
            token_path=os.getenv("SCHWAB_OAUTH_TOKEN_PATH", DEFAULT_TOKEN_PATH),
            callback_url=os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/callback"),
        )

        token_file = os.getenv("SCHWAB_TOKEN_FILE")
        store = SchwabTokenStore(token_file) if token_file else None
        client = cls(config, token_store=store)

        access_token = os.getenv("SCHWAB_ACCESS_TOKEN")
        refresh_token = os.getenv("SCHWAB_REFRESH_TOKEN")
        if access_token and refresh_token and client._tokens is None:
            client._tokens = SchwabTokens(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=0.0,
            )

        return client

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid access token, refreshing when needed."""
        if self._tokens is None:
            raise SchwabAuthError(
                "No Schwab tokens available. Complete OAuth and set "
                "SCHWAB_REFRESH_TOKEN or SCHWAB_TOKEN_FILE."
            )

        if force_refresh or self._tokens.is_expired():
            self._tokens = self.refresh_tokens(self._tokens.refresh_token)

        return self._tokens.access_token

    def refresh_tokens(self, refresh_token: str) -> SchwabTokens:
        """Exchange a refresh token for a new access token."""
        url = urljoin(self._config.api_base_url.rstrip("/") + "/", self._config.token_path.lstrip("/"))
        credentials = f"{self._config.app_key}:{self._config.app_secret}".encode("utf-8")
        headers = {
            "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        try:
            response = self._session.post(url, headers=headers, data=data, timeout=30.0)
        except requests.RequestException as exc:
            raise SchwabAuthError(f"Token refresh request failed: {exc}") from exc

        if not response.ok:
            raise SchwabAuthError(
                f"Token refresh failed with {response.status_code}: {response.text}"
            )

        tokens = SchwabTokens.from_token_response(response.json())
        if self._token_store is not None:
            self._token_store.save(tokens)
        self._tokens = tokens
        logger.info("Refreshed Schwab access token")
        return tokens

    def build_authorize_url(self, *, scope: str = "readonly") -> str:
        """Build the browser URL used to start the OAuth authorization flow."""
        from urllib.parse import urlencode

        params = {
            "response_type": "code",
            "client_id": self._config.app_key,
            "redirect_uri": self._config.callback_url,
            "scope": scope,
        }
        authorize_path = os.getenv("SCHWAB_OAUTH_AUTHORIZE_PATH", "/v1/oauth/authorize")
        base = self._config.api_base_url.rstrip("/")
        return f"{base}{authorize_path}?{urlencode(params)}"

    def exchange_code(self, authorization_code: str) -> SchwabTokens:
        """Exchange an authorization code for access and refresh tokens."""
        url = urljoin(self._config.api_base_url.rstrip("/") + "/", self._config.token_path.lstrip("/"))
        credentials = f"{self._config.app_key}:{self._config.app_secret}".encode("utf-8")
        headers = {
            "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": self._config.callback_url,
        }

        try:
            response = self._session.post(url, headers=headers, data=data, timeout=30.0)
        except requests.RequestException as exc:
            raise SchwabAuthError(f"Token exchange request failed: {exc}") from exc

        if not response.ok:
            raise SchwabAuthError(
                f"Token exchange failed with {response.status_code}: {response.text}"
            )

        tokens = SchwabTokens.from_token_response(response.json())
        if self._token_store is not None:
            self._token_store.save(tokens)
        self._tokens = tokens
        return tokens


def _load_dotenv() -> None:
    """Load a local .env file when python-dotenv is installed."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SchwabAuthError(f"Missing required environment variable: {name}")
    return value
