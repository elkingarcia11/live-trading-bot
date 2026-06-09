"""Stream Connection Manager.

Responsibility: Low-level WebSocket transport.

Manages WebSocket lifecycles, reconnect loops, heartbeats, and connection
state. Forwards raw text frames to callbacks. Does not parse market data,
normalize vendor payloads, validate bars, or persist storage.
"""

from __future__ import annotations

import logging
import threading
import time
from enum import Enum
from typing import Callable, Optional

import websocket

logger = logging.getLogger(__name__)

MessageHandler = Callable[[str], None]
OpenHandler = Callable[[], None]
CloseHandler = Callable[[Optional[int], Optional[str]], None]
ErrorHandler = Callable[[Exception], None]


class ConnectionState(Enum):
    """Lifecycle states tracked by the connection manager."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"


class StreamConnectionError(Exception):
    """Raised when a WebSocket operation fails."""


class StreamConnectionManager:
    """Manages WebSocket connections with reconnect loops and heartbeats."""

    def __init__(
        self,
        url: str,
        *,
        on_message: MessageHandler,
        on_open: Optional[OpenHandler] = None,
        on_close: Optional[CloseHandler] = None,
        on_error: Optional[ErrorHandler] = None,
        headers: Optional[dict[str, str]] = None,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        heartbeat_interval: Optional[float] = None,
        heartbeat_message: Optional[str] = None,
        reconnect_backoff_seconds: float = 1.0,
        max_reconnect_backoff_seconds: float = 60.0,
        max_reconnect_attempts: Optional[int] = None,
    ) -> None:
        """Initialize a managed WebSocket connection.

        Args:
            url: WebSocket endpoint URL (e.g. "wss://stream.example.com/v1").
            on_message: Callback invoked for each inbound text frame.
            on_open: Optional callback invoked after a successful connection.
            on_close: Optional callback invoked when the socket closes.
            on_error: Optional callback invoked when an error occurs.
            headers: Optional HTTP headers sent during the handshake.
            ping_interval: Seconds between protocol-level WebSocket ping frames.
            ping_timeout: Seconds to wait for a pong before treating the
                connection as unhealthy.
            heartbeat_interval: Optional seconds between application-level
                heartbeat messages.
            heartbeat_message: Optional text payload sent as an application
                heartbeat when `heartbeat_interval` is set.
            reconnect_backoff_seconds: Initial delay before reconnect attempts.
            max_reconnect_backoff_seconds: Upper bound for exponential backoff.
            max_reconnect_attempts: Maximum reconnect attempts before stopping.
                None means retry indefinitely until `disconnect()` is called.
        """
        self._url = url
        self._on_message = on_message
        self._on_open = on_open
        self._on_close = on_close
        self._on_error = on_error
        self._headers = headers or {}
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_message = heartbeat_message
        self._reconnect_backoff_seconds = reconnect_backoff_seconds
        self._max_reconnect_backoff_seconds = max_reconnect_backoff_seconds
        self._max_reconnect_attempts = max_reconnect_attempts

        self._state = ConnectionState.DISCONNECTED
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._reconnect_attempts = 0

    @property
    def state(self) -> ConnectionState:
        """Return the current connection lifecycle state."""
        with self._lock:
            return self._state

    @property
    def url(self) -> str:
        """Return the configured WebSocket endpoint URL."""
        return self._url

    def connect(self) -> None:
        """Start the managed connection and reconnect loop in a background thread.

        Raises:
            StreamConnectionError: If a connection is already active.
        """
        with self._lock:
            if self._thread and self._thread.is_alive():
                raise StreamConnectionError("Connection manager is already running")

            self._stop_event.clear()
            self._reconnect_attempts = 0
            self._thread = threading.Thread(
                target=self._connection_loop,
                name="stream-connection-manager",
                daemon=True,
            )
            self._thread.start()

    def disconnect(self) -> None:
        """Gracefully stop the connection and suppress further reconnect attempts."""
        self._stop_event.set()
        self._set_state(ConnectionState.CLOSING)

        ws = self._ws
        if ws is not None:
            ws.close()

        if self._thread is not None:
            self._thread.join(timeout=self._ping_timeout + self._ping_interval)

        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1.0)

        self._set_state(ConnectionState.DISCONNECTED)

    def send(self, message: str) -> None:
        """Send a text frame on the active WebSocket connection.

        Args:
            message: Text payload to send to the server.

        Raises:
            StreamConnectionError: If the socket is not currently connected.
        """
        with self._lock:
            if self._state != ConnectionState.CONNECTED or self._ws is None:
                raise StreamConnectionError(
                    f"Cannot send message while state is {self._state.value}"
                )
            ws = self._ws

        try:
            ws.send(message)
        except Exception as exc:
            self._emit_error(exc)
            raise StreamConnectionError(f"Failed to send message: {exc}") from exc

    def _connection_loop(self) -> None:
        """Connect, run until closed, and retry with exponential backoff."""
        while not self._stop_event.is_set():
            self._set_state(ConnectionState.CONNECTING)

            self._ws = websocket.WebSocketApp(
                self._url,
                header=[f"{key}: {value}" for key, value in self._headers.items()],
                on_open=self._handle_open,
                on_message=self._handle_message,
                on_error=self._handle_error,
                on_close=self._handle_close,
            )

            # Blocks until the socket closes or the manager is stopped.
            self._ws.run_forever(
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_timeout,
            )

            if self._stop_event.is_set():
                break

            self._reconnect_attempts += 1
            if (
                self._max_reconnect_attempts is not None
                and self._reconnect_attempts >= self._max_reconnect_attempts
            ):
                logger.warning(
                    "Reached max reconnect attempts (%s); stopping.",
                    self._max_reconnect_attempts,
                )
                self._set_state(ConnectionState.DISCONNECTED)
                break

            self._set_state(ConnectionState.RECONNECTING)
            delay = self._next_reconnect_delay()
            logger.info("Reconnecting to %s in %.1f seconds.", self._url, delay)
            if self._stop_event.wait(delay):
                break

        self._set_state(ConnectionState.DISCONNECTED)

    def _handle_open(self, _ws: websocket.WebSocketApp) -> None:
        """Reset reconnect counters and start optional application heartbeats."""
        self._reconnect_attempts = 0
        self._set_state(ConnectionState.CONNECTED)
        self._start_heartbeat_thread()

        if self._on_open is not None:
            self._on_open()

    def _handle_message(self, _ws: websocket.WebSocketApp, message: str) -> None:
        """Forward inbound messages to the configured callback."""
        self._on_message(message)

    def _handle_error(self, _ws: websocket.WebSocketApp, error: Exception) -> None:
        """Surface low-level WebSocket errors to the configured callback."""
        self._emit_error(error)

    def _handle_close(
        self,
        _ws: websocket.WebSocketApp,
        close_status_code: Optional[int],
        close_msg: Optional[str],
    ) -> None:
        """Stop heartbeats and notify callers that the socket has closed."""
        self._stop_heartbeat_thread()

        if self._on_close is not None:
            self._on_close(close_status_code, close_msg)

        if not self._stop_event.is_set():
            self._set_state(ConnectionState.RECONNECTING)

    def _start_heartbeat_thread(self) -> None:
        """Start a background thread for application-level heartbeat messages."""
        if self._heartbeat_interval is None or self._heartbeat_message is None:
            return

        self._stop_heartbeat_thread()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="stream-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self) -> None:
        """Stop the application heartbeat thread if it is running."""
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)
        self._heartbeat_thread = None

    def _heartbeat_loop(self) -> None:
        """Send application heartbeat messages while the socket remains connected."""
        while not self._stop_event.is_set() and self.state == ConnectionState.CONNECTED:
            if self._stop_event.wait(self._heartbeat_interval):
                break

            if self.state != ConnectionState.CONNECTED:
                break

            try:
                self.send(self._heartbeat_message)
            except StreamConnectionError:
                break

    def _next_reconnect_delay(self) -> float:
        """Calculate the next reconnect delay using exponential backoff."""
        delay = self._reconnect_backoff_seconds * (2 ** max(self._reconnect_attempts - 1, 0))
        return min(delay, self._max_reconnect_backoff_seconds)

    def _set_state(self, state: ConnectionState) -> None:
        """Update the current connection state in a thread-safe way."""
        with self._lock:
            self._state = state

    def _emit_error(self, error: Exception) -> None:
        """Log and forward errors to the optional callback."""
        logger.error("WebSocket error on %s: %s", self._url, error)
        if self._on_error is not None:
            self._on_error(error)


if __name__ == "__main__":
    # Example usage with a public echo test server.
    def handle_message(message: str) -> None:
        print(f"Received: {message}")

    def handle_open() -> None:
        print("Connected")

    def handle_close(code: Optional[int], reason: Optional[str]) -> None:
        print(f"Closed: code={code}, reason={reason}")

    manager = StreamConnectionManager(
        "wss://echo.websocket.events",
        on_message=handle_message,
        on_open=handle_open,
        on_close=handle_close,
        heartbeat_interval=25.0,
        heartbeat_message='{"action":"heartbeat"}',
        max_reconnect_attempts=3,
    )

    manager.connect()
    time.sleep(1)

    if manager.state == ConnectionState.CONNECTED:
        manager.send("Hello from StreamConnectionManager")

    time.sleep(2)
    manager.disconnect()
    print(f"Final state: {manager.state.value}")
