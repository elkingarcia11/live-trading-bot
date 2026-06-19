"""Local Schwab OAuth browser flow with callback capture.

Runs a short-lived local HTTP(S) server, opens the Schwab authorize URL,
exchanges the returned authorization code, and persists tokens through the
configured token store(s).
"""

from __future__ import annotations

import logging
import ssl
import subprocess
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from schwab_auth import (
    LayeredSchwabTokenStore,
    SchwabAuthClient,
    SchwabAuthError,
    SchwabTokens,
    _load_dotenv,
    build_token_store_from_config,
    normalize_authorization_code,
)

logger = logging.getLogger(__name__)

CERT_DIR = Path(".schwab_oauth_certs")


class OAuthCallbackError(Exception):
    """Raised when the local OAuth callback flow fails."""


def run_local_oauth_flow(
    *,
    scope: str = "readonly",
    open_browser: bool = True,
    timeout_seconds: float = 300.0,
) -> SchwabTokens:
    """Complete Schwab OAuth locally and persist tokens to disk and/or GCS."""
    _load_dotenv()
    client = SchwabAuthClient.from_env(load_dotenv=False)
    authorize_url = client.build_authorize_url(scope=scope)
    callback_url = client._config.callback_url  # noqa: SLF001

    print(f"Authorize URL:\n{authorize_url}\n")
    if open_browser:
        webbrowser.open(authorize_url, new=1)

    code = capture_authorization_code(
        callback_url,
        timeout_seconds=timeout_seconds,
    )
    tokens = client.exchange_code(code)
    _log_saved_tokens(tokens)
    return tokens


def capture_authorization_code(
    callback_url: str,
    *,
    timeout_seconds: float = 300.0,
) -> str:
    """Listen for Schwab's redirect and return the authorization code."""
    parsed = urlparse(callback_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    use_https = parsed.scheme == "https"

    result: dict[str, Optional[str]] = {"code": None, "error": None}
    done = threading.Event()

    class _CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            logger.debug("OAuth callback: " + format, *args)

        def do_GET(self) -> None:
            query = parse_qs(urlparse(self.path).query)
            if "error" in query:
                result["error"] = query["error"][0]
                self._respond("Authorization failed. You can close this tab.")
                done.set()
                return

            if "code" not in query:
                self._respond("Missing authorization code.")
                done.set()
                return

            result["code"] = normalize_authorization_code(query["code"][0])
            self._respond(
                "Schwab authorization complete. You can close this tab and return to the terminal."
            )
            done.set()

        def _respond(self, message: str) -> None:
            body = f"<html><body><h1>{message}</h1></body></html>".encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd = HTTPServer((host, port), _CallbackHandler)
    httpd.timeout = 1.0

    if use_https:
        cert_path, key_path = _ensure_localhost_certificates()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    print(
        f"Waiting for Schwab redirect on {callback_url} "
        f"(timeout {int(timeout_seconds)}s)..."
    )

    try:
        deadline = timeout_seconds
        while not done.is_set() and deadline > 0:
            httpd.handle_request()
            deadline -= httpd.timeout
    finally:
        httpd.server_close()

    if result["error"]:
        raise OAuthCallbackError(f"Schwab authorization error: {result['error']}")
    if not result["code"]:
        raise OAuthCallbackError(
            f"Timed out waiting for OAuth callback on {callback_url}"
        )
    return result["code"]


def _ensure_localhost_certificates() -> tuple[Path, Path]:
    """Create or reuse a self-signed localhost certificate for HTTPS callbacks."""
    CERT_DIR.mkdir(parents=True, exist_ok=True)
    cert_path = CERT_DIR / "localhost.pem"
    key_path = CERT_DIR / "localhost-key.pem"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    try:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                str(key_path),
                "-out",
                str(cert_path),
                "-days",
                "365",
                "-nodes",
                "-subj",
                "/CN=127.0.0.1",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise OAuthCallbackError(
            "openssl is required for https://127.0.0.1 callbacks. "
            "Install OpenSSL or change schwab.callback_url to an http:// URL "
            "registered in your Schwab app."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise OAuthCallbackError(
            f"Failed to generate localhost TLS certificate: {exc.stderr or exc}"
        ) from exc

    return cert_path, key_path


def _log_saved_tokens(_tokens: SchwabTokens) -> None:
    store = build_token_store_from_config()
    if store is None:
        print("Tokens received but no token store is configured.")
        return

    print("Saved Schwab tokens (refresh token ready for cloud use).")
    if isinstance(store, LayeredSchwabTokenStore):
        for child in store._stores:  # noqa: SLF001
            print(f"  - {child.location}")
    else:
        print(f"  - {store.location}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        run_local_oauth_flow()
    except (SchwabAuthError, OAuthCallbackError) as exc:
        raise SystemExit(str(exc)) from exc
