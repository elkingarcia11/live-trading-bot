"""Schwab WebSocket streamer.

Responsibility: Schwab-specific live streaming transport and message parsing.

Fetches user preference, performs streamer LOGIN, subscribes to CHART_EQUITY,
and translates raw stream frames into clean 1-minute bars for the stream data
processor. Does not evaluate strategies or persist OHLCV data.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Sequence

import pandas as pd

from market_data_transformer import SCHWAB_CHART_EQUITY_FIELDS
from ohlc_sanity import repair_ohlc_bar as _repair_ohlc_bar
from schwab_auth import SchwabAuthClient, _load_dotenv
from schwab_trader_client import SchwabStreamerInfo, SchwabTraderClient
from stream_connection_manager import StreamConnectionManager
from stream_data_processor import CleanBarEvent, StreamDataProcessor

logger = logging.getLogger(__name__)

CHART_EQUITY_FIELDS = "0,1,2,3,4,5,6,7"
CHART_EQUITY_SERVICE = "CHART_EQUITY"
ADMIN_SERVICE = "ADMIN"

RESPONSE_SUCCESS = 0
RESPONSE_LOGIN_DENIED = 3
RESPONSE_CLOSE_CONNECTION = 12


class StreamEventType(Enum):
    """High-level events extracted from Schwab stream frames."""

    HEARTBEAT = "heartbeat"
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    SUBSCRIBE_SUCCESS = "subscribe_success"
    SUBSCRIBE_FAILURE = "subscribe_failure"
    CHART_BAR = "chart_bar"
    CONNECTION_CLOSED = "connection_closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StreamEvent:
    """Parsed Schwab stream event."""

    event_type: StreamEventType
    payload: dict[str, Any] | None = None
    message: str = ""
    response_code: int = -1


class SchwabStreamMessageParser:
    """Parse Schwab streamer JSON frames into structured events."""

    def __init__(self, *, chart_service: str = CHART_EQUITY_SERVICE) -> None:
        self._chart_service = chart_service

    def parse(self, raw_message: str) -> list[StreamEvent]:
        """Parse one inbound WebSocket text frame."""
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON stream frame")
            return [StreamEvent(StreamEventType.UNKNOWN)]

        if not isinstance(payload, dict):
            return [StreamEvent(StreamEventType.UNKNOWN)]

        events: list[StreamEvent] = []
        for heartbeat in payload.get("notify", []) or []:
            if isinstance(heartbeat, dict) and "heartbeat" in heartbeat:
                events.append(StreamEvent(StreamEventType.HEARTBEAT))

        for response in payload.get("response", []) or []:
            event = self._parse_response(response)
            if event is not None:
                events.append(event)

        for data_frame in payload.get("data", []) or []:
            events.extend(self._parse_data_frame(data_frame))

        return events or [StreamEvent(StreamEventType.UNKNOWN)]

    def _parse_response(self, response: object) -> Optional[StreamEvent]:
        if not isinstance(response, dict):
            return None

        service = str(response.get("service", ""))
        command = str(response.get("command", ""))
        content = response.get("content", {})
        if not isinstance(content, dict):
            content = {}

        code = int(content.get("code", -1))
        message = str(content.get("msg", ""))
        success = code == RESPONSE_SUCCESS

        if code == RESPONSE_CLOSE_CONNECTION:
            return StreamEvent(
                StreamEventType.CONNECTION_CLOSED,
                message=message,
                response_code=code,
            )

        if service == ADMIN_SERVICE and command == "LOGIN":
            event_type = StreamEventType.LOGIN_SUCCESS if success else StreamEventType.LOGIN_FAILURE
            return StreamEvent(event_type, message=message, response_code=code)

        if service == self._chart_service and command in {"SUBS", "ADD", "VIEW"}:
            event_type = (
                StreamEventType.SUBSCRIBE_SUCCESS if success else StreamEventType.SUBSCRIBE_FAILURE
            )
            return StreamEvent(event_type, message=message, response_code=code)

        return None

    def _parse_data_frame(self, data_frame: object) -> list[StreamEvent]:
        if not isinstance(data_frame, dict):
            return []

        if str(data_frame.get("service", "")) != self._chart_service:
            return []

        content = data_frame.get("content", [])
        if not isinstance(content, list):
            return []

        events: list[StreamEvent] = []
        for item in content:
            bar = self._chart_equity_item_to_bar(item)
            if bar is not None:
                events.append(StreamEvent(StreamEventType.CHART_BAR, payload=bar))
        return events

    def _chart_equity_item_to_bar(self, item: object) -> Optional[dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        symbol = str(item.get("key") or item.get("0") or "").upper()
        if not symbol:
            return None

        chart_time = item.get("7", item.get("CHART_TIME_MILLIS", item.get("CHART_TIME")))
        if chart_time is None:
            return None

        try:
            timestamp = _chart_time_to_minute_iso(chart_time)
            open_price, high_price, low_price, close_price, volume = (
                _extract_chart_equity_ohlcv(item)
            )
            sequence = int(
                item.get("6", item.get("SEQUENCE", item.get("seq", 0))) or 0
            )
        except (TypeError, ValueError):
            return None

        normalized = _normalize_forming_chart_ohlc(
            open_price,
            high_price,
            low_price,
            close_price,
        )
        if normalized is None:
            return None
        open_price, high_price, low_price, close_price = normalized

        return {
            "symbol": symbol,
            "timeframe": "1m",
            "sequence": sequence,
            "bar": {
                "datetime": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            },
        }


class SchwabStreamSession:
    """Managed Schwab stream session for CHART_EQUITY 1-minute bars."""

    def __init__(
        self,
        *,
        trader_client: SchwabTraderClient,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        streamer_info: Optional[SchwabStreamerInfo] = None,
        stream_service: str = CHART_EQUITY_SERVICE,
        chart_fields: str = CHART_EQUITY_FIELDS,
        subscribe_on_connect: bool = True,
        on_open_external: Optional[Callable[[], None]] = None,
        on_close_external: Optional[Callable[[Optional[int], Optional[str]], None]] = None,
        on_error_external: Optional[Callable[[Exception], None]] = None,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        connection_options: Optional[dict[str, Any]] = None,
    ) -> None:
        self._trader_client = trader_client
        self._symbols = tuple(symbol.upper() for symbol in symbols)
        self._processor = processor
        self._streamer_info = streamer_info
        self._stream_service = stream_service
        self._chart_fields = chart_fields
        self._subscribe_on_connect = subscribe_on_connect
        self._parser = SchwabStreamMessageParser(chart_service=stream_service)
        self._request_id = 0
        self._logged_in = False
        self._subscribed = False
        self._login_retries = 0

        self._on_open_external = on_open_external
        self._on_close_external = on_close_external
        self._on_error_external = on_error_external

        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._connection_options = connection_options or {}
        self._connection_manager: Optional[StreamConnectionManager] = None

    @classmethod
    def from_env(
        cls,
        *,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        auth_client: Optional[SchwabAuthClient] = None,
        load_dotenv: bool = True,
        **kwargs: Any,
    ) -> SchwabStreamSession:
        """Build a stream session using config.json-backed Schwab clients."""
        if load_dotenv:
            _load_dotenv()

        from config import StreamSettings, get_config

        app = get_config(reload=True)
        stream = app.stream
        trader_client = SchwabTraderClient(
            auth_client or SchwabAuthClient.from_env(load_dotenv=False),
        )
        return cls(
            trader_client=trader_client,
            symbols=symbols,
            processor=processor,
            stream_service=stream.schwab_stream_service,
            chart_fields=stream.schwab_chart_equity_fields,
            subscribe_on_connect=kwargs.pop(
                "subscribe_on_connect",
                app.workflow.subscribe_on_connect,
            ),
            ping_interval=stream.ping_interval_seconds,
            ping_timeout=stream.ping_timeout_seconds,
            connection_options=_connection_options_from_settings(stream),
            **kwargs,
        )

    @property
    def connection_manager(self) -> StreamConnectionManager:
        """Return the lazily initialized WebSocket connection manager."""
        if self._connection_manager is None:
            self._connection_manager = StreamConnectionManager(
                self._resolve_streamer_url(),
                on_message=self._on_message,
                on_open=self._on_open,
                on_close=self._on_close,
                on_error=self._on_error,
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_timeout,
                **self._connection_options,
            )
        return self._connection_manager

    def refresh_streamer_info(self) -> SchwabStreamerInfo:
        """Fetch the latest streamer connection metadata."""
        self._streamer_info = self._trader_client.get_streamer_info()
        return self._streamer_info

    def connect(self) -> None:
        """Start the managed WebSocket connection."""
        self.connection_manager.connect()

    def disconnect(self) -> None:
        """Logout when possible and stop the managed WebSocket connection."""
        if self._logged_in:
            self._send_logout()
        self.connection_manager.disconnect()
        self._logged_in = False
        self._subscribed = False

    def add_symbols(self, symbols: Sequence[str]) -> None:
        """Add symbols to the active chart subscription using the ADD command."""
        if not self._logged_in:
            raise RuntimeError("Cannot add symbols before Schwab stream login succeeds")
        self._send_chart_equity_command("ADD", symbols)

    def _resolve_streamer_url(self) -> str:
        from config import get_config

        configured = get_config().stream.schwab_streamer_url.strip()
        if configured:
            return configured

        if self._streamer_info is None:
            self._streamer_info = self._trader_client.get_streamer_info()
        return self._streamer_info.streamer_socket_url

    def _on_open(self) -> None:
        self._logged_in = False
        self._subscribed = False
        self._login_retries = 0
        self._send_login()
        if self._on_open_external is not None:
            self._on_open_external()

    def _on_close(self, code: Optional[int], reason: Optional[str]) -> None:
        self._logged_in = False
        self._subscribed = False
        if self._on_close_external is not None:
            self._on_close_external(code, reason)

    def _on_error(self, error: Exception) -> None:
        if self._on_error_external is not None:
            self._on_error_external(error)

    def _on_message(self, raw_message: str) -> None:
        for event in self._parser.parse(raw_message):
            if event.event_type == StreamEventType.HEARTBEAT:
                continue
            if event.event_type == StreamEventType.CONNECTION_CLOSED:
                logger.warning("Schwab streamer closed the connection: %s", event.message)
                self._force_reconnect()
                continue
            if event.event_type == StreamEventType.LOGIN_SUCCESS:
                self._logged_in = True
                self._login_retries = 0
                logger.info("Schwab stream login succeeded")
                if self._subscribe_on_connect:
                    self._send_chart_equity_subs()
                continue
            if event.event_type == StreamEventType.LOGIN_FAILURE:
                self._handle_login_failure(event)
                continue
            if event.event_type == StreamEventType.SUBSCRIBE_SUCCESS:
                self._subscribed = True
                logger.info(
                    "Subscribed to Schwab %s for %s",
                    self._stream_service,
                    ", ".join(self._symbols),
                )
                logger.info(
                    "Waiting for live %s bars (updates arrive during market hours)",
                    ", ".join(self._symbols),
                )
                continue
            if event.event_type == StreamEventType.SUBSCRIBE_FAILURE:
                logger.error("Schwab chart subscription failed: %s", event.message)
                continue
            if event.event_type == StreamEventType.CHART_BAR and event.payload is not None:
                self._publish_chart_bar(event.payload)

    def _handle_login_failure(self, event: StreamEvent) -> None:
        logger.error("Schwab stream login failed: %s", event.message)
        if event.response_code != RESPONSE_LOGIN_DENIED:
            return

        if self._login_retries < 1:
            self._login_retries += 1
            try:
                self._trader_client.get_access_token(force_refresh=True)
                logger.info("Refreshing Schwab access token and retrying stream login")
                self._send_login()
                return
            except Exception:
                logger.exception("Failed to refresh Schwab access token for stream login")

        self._force_reconnect()

    def _force_reconnect(self) -> None:
        self._logged_in = False
        self._subscribed = False
        try:
            self.connection_manager.disconnect()
            self.connection_manager.connect()
        except Exception:
            logger.exception("Failed to force Schwab stream reconnect")

    def _publish_chart_bar(self, payload: dict[str, Any]) -> Optional[CleanBarEvent]:
        symbol = str(payload.get("symbol", "")).upper()
        bar = payload.get("bar")
        if not symbol or not isinstance(bar, dict):
            return None

        return self._processor.process_bar(
            bar,
            symbol=symbol,
            timeframe=str(payload.get("timeframe", "1m")),
        )

    def _next_request_id(self) -> str:
        self._request_id += 1
        return str(self._request_id)

    def _streamer_info_or_raise(self) -> SchwabStreamerInfo:
        if self._streamer_info is None:
            self._streamer_info = self._trader_client.get_streamer_info()
        return self._streamer_info

    def _send_login(self) -> None:
        info = self._streamer_info_or_raise()
        access_token = self._trader_client.get_access_token()
        request = {
            "requests": [
                {
                    "requestid": self._next_request_id(),
                    "service": ADMIN_SERVICE,
                    "command": "LOGIN",
                    "SchwabClientCustomerId": info.schwab_client_customer_id,
                    "SchwabClientCorrelId": info.schwab_client_correl_id,
                    "parameters": {
                        "Authorization": access_token,
                        "SchwabClientChannel": info.schwab_client_channel,
                        "SchwabClientFunctionId": info.schwab_client_function_id,
                    },
                }
            ]
        }
        self._send_request(request)

    def _send_logout(self) -> None:
        info = self._streamer_info_or_raise()
        request = {
            "requests": [
                {
                    "requestid": self._next_request_id(),
                    "service": ADMIN_SERVICE,
                    "command": "LOGOUT",
                    "SchwabClientCustomerId": info.schwab_client_customer_id,
                    "SchwabClientCorrelId": info.schwab_client_correl_id,
                    "parameters": {},
                }
            ]
        }
        try:
            self._send_request(request)
        except Exception:
            logger.debug("Schwab stream logout failed during disconnect", exc_info=True)

    def _send_chart_equity_subs(self) -> None:
        if not self._logged_in:
            return
        self._send_chart_equity_command("SUBS", self._symbols)

    def _send_chart_equity_command(
        self,
        command: str,
        symbols: Sequence[str],
    ) -> None:
        if not self._logged_in:
            return

        normalized = [symbol.upper() for symbol in symbols if str(symbol).strip()]
        if not normalized:
            return

        info = self._streamer_info_or_raise()
        request = {
            "requests": [
                {
                    "requestid": self._next_request_id(),
                    "service": self._stream_service,
                    "command": command,
                    "SchwabClientCustomerId": info.schwab_client_customer_id,
                    "SchwabClientCorrelId": info.schwab_client_correl_id,
                    "parameters": {
                        "keys": ",".join(normalized),
                        "fields": self._chart_fields,
                    },
                }
            ]
        }
        self._send_request(request)

    def _send_request(self, payload: dict[str, Any]) -> None:
        try:
            self.connection_manager.send(json.dumps(payload))
        except Exception:
            logger.exception("Failed to send Schwab stream request")


def build_schwab_stream_processor(
    *,
    symbols: Sequence[str],
    consumers: Optional[list[Callable[[CleanBarEvent], None]]] = None,
    timeframe: str = "1m",
    require_minute_alignment: Optional[bool] = None,
    dedup_window: Optional[int] = None,
    stream_settings: Optional["StreamSettings"] = None,
) -> StreamDataProcessor:
    """Create a stream processor configured for Schwab chart equity bars."""
    from config import StreamSettings, get_config

    stream = stream_settings or get_config().stream
    if require_minute_alignment is None:
        require_minute_alignment = stream.require_minute_alignment
    if dedup_window is None:
        dedup_window = stream.dedup_window

    return StreamDataProcessor(
        symbols=symbols,
        timeframe=timeframe,
        consumers=consumers or [],
        field_map=SCHWAB_CHART_EQUITY_FIELDS,
        bar_key="bar",
        symbol_key="symbol",
        timeframe_key="timeframe",
        require_minute_alignment=require_minute_alignment,
        dedup_window=dedup_window,
    )


def _chart_time_to_minute_iso(chart_time: object) -> str:
    """Normalize Schwab chart time to the UTC minute the candle belongs to."""
    millis = int(float(chart_time))
    if millis < 1_000_000_000_000:
        millis *= 1000
    return pd.to_datetime(millis, unit="ms", utc=True).floor("min").isoformat()


def _normalize_forming_chart_ohlc(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
) -> Optional[tuple[float, float, float, float]]:
    """Accept in-progress 1m chart updates until a usable close is available."""
    if close_price <= 0:
        return None

    if open_price <= 0:
        open_price = close_price
    if high_price <= 0:
        high_price = max(open_price, close_price)
    if low_price <= 0:
        low_price = min(open_price, close_price)

    return _repair_ohlc_bar(open_price, high_price, low_price, close_price)


def _extract_chart_equity_ohlcv(item: dict[str, Any]) -> tuple[float, float, float, float, float]:
    """Map Schwab CHART_EQUITY fields, including shifted SUBS snapshot layouts."""
    if "OPEN_PRICE" in item:
        return (
            float(item["OPEN_PRICE"]),
            float(item["HIGH_PRICE"]),
            float(item["LOW_PRICE"]),
            float(item["CLOSE_PRICE"]),
            float(item.get("VOLUME", 0.0)),
        )

    field_1 = _optional_float(item.get("1"))
    field_2 = _optional_float(item.get("2"))
    field_3 = _optional_float(item.get("3"))
    field_4 = _optional_float(item.get("4"))
    field_5 = _optional_float(item.get("5"))
    field_6 = _optional_float(item.get("6"))

    if None in {field_1, field_2, field_3, field_4, field_5}:
        raise ValueError("chart equity item missing OHLC fields")

    seq = _optional_float(item.get("seq"))
    if seq is not None and field_1 == seq and field_2 is not None:
        volume = field_6 if field_6 is not None else 0.0
        return field_2, field_3, field_4, field_5, volume

    if _chart_field_looks_like_sequence(field_1, field_2, field_3, field_4, field_5):
        volume = field_6 if field_6 is not None else 0.0
        return field_2, field_3, field_4, field_5, volume

    volume = float(item.get("5", 0.0) or 0.0)
    return field_1, field_2, field_3, field_4, volume


def _chart_field_looks_like_sequence(
    field_1: float,
    field_2: float,
    field_3: float,
    field_4: float,
    field_5: float,
    *,
    tolerance: float = 0.05,
) -> bool:
    """True when field 1 is not a plausible price but fields 2-5 cluster like OHLC."""
    anchor = field_4
    if anchor <= 0:
        return False

    field_1_is_price = abs(field_1 - anchor) / anchor <= tolerance
    if field_1_is_price:
        return False

    clustered_prices = sum(
        abs(price - anchor) / anchor <= tolerance for price in (field_2, field_3, field_4, field_5)
    )
    return clustered_prices >= 3


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _connection_options_from_settings(stream: "StreamSettings") -> dict[str, Any]:
    options: dict[str, Any] = {
        "reconnect_backoff_seconds": stream.reconnect_backoff_seconds,
        "max_reconnect_backoff_seconds": stream.max_reconnect_backoff_seconds,
    }
    if stream.max_reconnect_attempts is not None:
        options["max_reconnect_attempts"] = stream.max_reconnect_attempts

    if stream.heartbeat_interval_seconds is not None:
        options["heartbeat_interval"] = stream.heartbeat_interval_seconds
        options["heartbeat_message"] = stream.heartbeat_message
    return options


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def on_bar(event: CleanBarEvent) -> None:
        print(event.to_dict())

    processor = build_schwab_stream_processor(symbols=("SPY",), consumers=[on_bar])
    session = SchwabStreamSession.from_env(symbols=("SPY",), processor=processor)
    print(f"Connecting to {session.connection_manager.url}")
    session.refresh_streamer_info()
    session.connect()

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        session.disconnect()
