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

from config import get_config

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
    callback_url: str = "https://127.0.0.1"


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

    is_gcs = False

    def __init__(self, token_file: str | Path) -> None:
        self._path = Path(token_file)

    @property
    def location(self) -> str:
        return str(self._path)

    def load(self) -> Optional[SchwabTokens]:
        """Load tokens from disk when the token file exists."""
        if not self._path.exists():
            return None

        return tokens_from_payload(json.loads(self._path.read_text(encoding="utf-8")))

    def save(self, tokens: SchwabTokens) -> str:
        """Persist tokens to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(tokens_to_payload(tokens), indent=2),
            encoding="utf-8",
        )
        return str(self._path)


class SchwabGcsTokenStore:
    """Read and write Schwab OAuth tokens from a GCS object."""

    is_gcs = True

    def __init__(
        self,
        bucket_name: str,
        *,
        blob_path: str,
        client: Optional[Any] = None,
    ) -> None:
        from google.cloud import storage

        self._bucket_name = bucket_name
        self._blob_path = blob_path.lstrip("/")
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket_name)

    @property
    def location(self) -> str:
        return f"gs://{self._bucket_name}/{self._blob_path}"

    def load(self) -> Optional[SchwabTokens]:
        """Load tokens from GCS when the object exists."""
        blob = self._bucket.blob(self._blob_path)
        try:
            raw = blob.download_as_text(encoding="utf-8")
        except Exception as exc:
            if _is_gcs_not_found(exc):
                return None
            logger.warning(
                "Could not load Schwab tokens from %s: %s",
                self.location,
                exc,
            )
            return None
        return tokens_from_payload(json.loads(raw))

    def save(self, tokens: SchwabTokens) -> str:
        """Persist tokens to GCS and return the object URI."""
        blob = self._bucket.blob(self._blob_path)
        blob.upload_from_string(
            json.dumps(tokens_to_payload(tokens), indent=2),
            content_type="application/json",
        )
        return self.location


class LayeredSchwabTokenStore:
    """Load from the first available store; save locally and replicate to GCS."""

    def __init__(self, stores: list[SchwabTokenStore | SchwabGcsTokenStore]) -> None:
        if not stores:
            raise ValueError("At least one token store is required")
        self._stores = stores

    def load(self) -> Optional[SchwabTokens]:
        tokens: Optional[SchwabTokens] = None
        source_index: Optional[int] = None

        for index, store in enumerate(self._stores):
            candidate = store.load()
            if candidate is not None:
                tokens = candidate
                source_index = index
                break

        if tokens is None:
            return None

        self._sync_missing_stores(tokens, skip_index=source_index)
        return tokens

    def save(self, tokens: SchwabTokens) -> str:
        primary_location = ""

        for store in self._stores:
            try:
                location = store.save(tokens)
            except Exception as exc:
                if store.is_gcs:
                    logger.warning(
                        "Failed to persist Schwab tokens to %s; continuing with local store (%s)",
                        store.location,
                        exc,
                    )
                    continue
                raise

            if not store.is_gcs:
                primary_location = location

        if not primary_location:
            raise SchwabAuthError("Failed to persist Schwab tokens to any configured store")

        return primary_location

    def _sync_missing_stores(
        self,
        tokens: SchwabTokens,
        *,
        skip_index: Optional[int],
    ) -> None:
        """Best-effort upload when tokens exist locally but not in GCS."""
        for index, store in enumerate(self._stores):
            if index == skip_index:
                continue
            try:
                if store.load() is not None:
                    continue
            except Exception as exc:
                logger.warning(
                    "Could not inspect Schwab token store %s before sync: %s",
                    store.location,
                    exc,
                )
            try:
                store.save(tokens)
                logger.info("Synced Schwab tokens to %s", store.location)
            except Exception as exc:
                logger.warning(
                    "Failed to sync Schwab tokens to %s: %s",
                    store.location,
                    exc,
                )


def _is_gcs_not_found(exc: Exception) -> bool:
    """Return True when a GCS client error means the object or bucket is missing."""
    try:
        from google.api_core import exceptions as gcp_exceptions
        from google.cloud.exceptions import NotFound
    except ImportError:
        return False

    if isinstance(exc, (NotFound, gcp_exceptions.NotFound)):
        return True

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code == 404


def tokens_to_payload(tokens: SchwabTokens) -> dict[str, Any]:
    """Serialize Schwab tokens for JSON storage."""
    return {
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "expires_at": tokens.expires_at,
    }


def tokens_from_payload(payload: dict[str, Any]) -> SchwabTokens:
    """Deserialize Schwab tokens from JSON storage."""
    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token")
    expires_at = payload.get("expires_at")
    if not access_token or not refresh_token or expires_at is None:
        raise SchwabAuthError("Token payload missing access_token, refresh_token, or expires_at")
    return SchwabTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=float(expires_at),
    )


def build_token_store_from_config() -> Optional[LayeredSchwabTokenStore | SchwabTokenStore | SchwabGcsTokenStore]:
    """Build local and/or GCS token stores from config.json and environment."""
    import os

    from config import get_config, secret

    app = get_config()
    stores: list[SchwabTokenStore | SchwabGcsTokenStore] = []

    token_file = secret("SCHWAB_TOKEN_FILE") or app.schwab.token_file
    if token_file:
        stores.append(SchwabTokenStore(token_file))

    gcs = app.gcs
    if gcs.schwab_token_path:
        if gcs.credentials_path:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", gcs.credentials_path)
        if gcs.project_id:
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)
        stores.append(
            SchwabGcsTokenStore(
                gcs.bucket_name,
                blob_path=gcs.schwab_token_path,
            )
        )

    if not stores:
        return None
    if len(stores) == 1:
        return stores[0]
    return LayeredSchwabTokenStore(stores)


class SchwabAuthClient:
    """Refresh and cache Schwab OAuth access tokens."""

    def __init__(
        self,
        config: SchwabAuthConfig,
        *,
        token_store: Optional[
            SchwabTokenStore | SchwabGcsTokenStore | LayeredSchwabTokenStore
        ] = None,
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

        from config import get_config, secret

        schwab = get_config(reload=True).schwab
        app_key = _require_env("SCHWAB_APP_KEY")
        app_secret = _require_env("SCHWAB_APP_SECRET")
        config = SchwabAuthConfig(
            app_key=app_key,
            app_secret=app_secret,
            api_base_url=schwab.api_base_url,
            token_path=schwab.oauth_token_path,
            callback_url=schwab.callback_url,
        )

        token_store = build_token_store_from_config()
        client = cls(config, token_store=token_store)

        access_token = secret("SCHWAB_ACCESS_TOKEN")
        refresh_token = secret("SCHWAB_REFRESH_TOKEN")
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
        authorize_path = get_config().schwab.oauth_authorize_path
        base = self._config.api_base_url.rstrip("/")
        return f"{base}{authorize_path}?{urlencode(params)}"

    def exchange_code(self, authorization_code: str) -> SchwabTokens:
        """Exchange an authorization code for access and refresh tokens."""
        authorization_code = normalize_authorization_code(authorization_code)
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


def normalize_authorization_code(authorization_code: str) -> str:
    """Strip Schwab's trailing metadata suffix from an authorization code."""
    return authorization_code.split("@", 1)[0]


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
