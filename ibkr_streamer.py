"""IBKR WebSocket streamer.

Responsibility: IBKR-specific live streaming transport and bar aggregation.

Subscribes to websocket smd top-of-book updates, aggregates ticks into 1-minute
bars, and publishes clean events to StreamDataProcessor. Does not evaluate
strategies or persist OHLCV data.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional, Sequence

import pandas as pd

from ibkr_auth import IbkrSessionClient
from ibkr_trader_client import IbkrTraderClient
from market_data_transformer import IBKR_STREAM_BAR_FIELDS
from ohlc_sanity import repair_ohlc_bar as _repair_ohlc_bar
from schwab_auth import _load_dotenv
from stream_connection_manager import StreamConnectionManager
from stream_data_processor import CleanBarEvent, StreamDataProcessor

logger = logging.getLogger(__name__)

IBKR_WS_HEARTBEAT_TOPIC = "tic"


class StreamEventType(Enum):
    """High-level events extracted from IBKR websocket frames."""

    HEARTBEAT = "heartbeat"
    MARKET_TICK = "market_tick"
    SUBSCRIBE_SUCCESS = "subscribe_success"
    CONNECTION_CLOSED = "connection_closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StreamEvent:
    """Parsed IBKR stream event."""

    event_type: StreamEventType
    payload: dict[str, Any] | None = None
    message: str = ""


@dataclass
class _FormingMinuteBar:
    symbol: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    last_day_volume: Optional[float] = None


class IbkrMinuteBarBuilder:
    """Aggregate IBKR smd ticks into forming 1-minute OHLCV bars."""

    def __init__(self, symbol_by_conid: dict[int, str]) -> None:
        self._symbol_by_conid = symbol_by_conid
        self._forming: dict[str, _FormingMinuteBar] = {}
        self._lock = threading.Lock()

    def update(
        self,
        *,
        conid: int,
        last_price: float,
        last_size: Optional[float] = None,
        day_volume: Optional[float] = None,
        updated_at_ms: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        symbol = self._symbol_by_conid.get(conid)
        if not symbol or last_price <= 0:
            return None

        minute = _floor_minute(updated_at_ms)
        with self._lock:
            forming = self._forming.get(symbol)
            if forming is None or forming.minute != minute:
                forming = _FormingMinuteBar(
                    symbol=symbol,
                    minute=minute,
                    open=last_price,
                    high=last_price,
                    low=last_price,
                    close=last_price,
                    volume=0.0,
                )
                self._forming[symbol] = forming
            else:
                forming.high = max(forming.high, last_price)
                forming.low = min(forming.low, last_price)
                forming.close = last_price

            if last_size is not None and last_size > 0:
                forming.volume += last_size
            elif day_volume is not None:
                if forming.last_day_volume is None:
                    forming.last_day_volume = day_volume
                else:
                    delta = max(day_volume - forming.last_day_volume, 0.0)
                    forming.volume += delta
                    forming.last_day_volume = day_volume

            normalized = _normalize_forming_ohlc(
                forming.open,
                forming.high,
                forming.low,
                forming.close,
            )
            if normalized is None:
                return None
            open_price, high_price, low_price, close_price = normalized

            return {
                "symbol": symbol,
                "timeframe": "1m",
                "bar": {
                    "datetime": forming.minute.isoformat(),
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": forming.volume,
                },
            }


class IbkrStreamMessageParser:
    """Parse IBKR websocket JSON frames into structured events."""

    def parse(self, raw_message: str) -> list[StreamEvent]:
        if raw_message.strip() == IBKR_WS_HEARTBEAT_TOPIC:
            return [StreamEvent(StreamEventType.HEARTBEAT)]

        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON IBKR stream frame")
            return [StreamEvent(StreamEventType.UNKNOWN)]

        if not isinstance(payload, dict):
            return [StreamEvent(StreamEventType.UNKNOWN)]

        topic = str(payload.get("topic", ""))
        if topic.startswith("smd+"):
            return [StreamEvent(StreamEventType.MARKET_TICK, payload=payload)]
        if topic.startswith("system"):
            return [StreamEvent(StreamEventType.SUBSCRIBE_SUCCESS, message=topic)]
        if payload.get("conid") is not None and "31" in payload:
            return [StreamEvent(StreamEventType.MARKET_TICK, payload=payload)]
        return [StreamEvent(StreamEventType.UNKNOWN)]


class IbkrStreamSession:
    """Managed IBKR websocket session for 1-minute equity bars."""

    def __init__(
        self,
        *,
        session_client: IbkrSessionClient,
        trader_client: IbkrTraderClient,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        websocket_url: str,
        snapshot_path: str,
        snapshot_fields: str,
        stream_fields: Sequence[str],
        listing_exchange: str = "SMART",
        subscribe_on_connect: bool = True,
        on_open_external: Optional[Callable[[], None]] = None,
        on_close_external: Optional[Callable[[Optional[int], Optional[str]], None]] = None,
        on_error_external: Optional[Callable[[Exception], None]] = None,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        verify_ssl: bool = False,
        connection_options: Optional[dict[str, Any]] = None,
    ) -> None:
        self._session_client = session_client
        self._trader_client = trader_client
        self._symbols = tuple(symbol.upper() for symbol in symbols)
        self._processor = processor
        self._websocket_url = websocket_url
        self._snapshot_path = snapshot_path
        self._snapshot_fields = snapshot_fields
        self._stream_fields = tuple(stream_fields)
        self._listing_exchange = listing_exchange
        self._subscribe_on_connect = subscribe_on_connect
        self._verify_ssl = verify_ssl
        self._parser = IbkrStreamMessageParser()
        self._symbol_by_conid: dict[int, str] = {}
        self._bar_builder = IbkrMinuteBarBuilder(self._symbol_by_conid)
        self._subscribed = False

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
        load_dotenv: bool = True,
        **kwargs: Any,
    ) -> IbkrStreamSession:
        """Build a stream session using config.json-backed IBKR clients."""
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        ibkr = app.ibkr
        stream = app.stream
        return cls(
            session_client=IbkrSessionClient.from_env(load_dotenv=False),
            trader_client=IbkrTraderClient.from_env(load_dotenv=False),
            symbols=symbols,
            processor=processor,
            websocket_url=ibkr.websocket_url,
            snapshot_path=ibkr.marketdata_snapshot_path,
            snapshot_fields=ibkr.snapshot_fields,
            stream_fields=ibkr.stream_fields,
            listing_exchange=ibkr.listing_exchange,
            verify_ssl=ibkr.verify_ssl,
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
        if self._connection_manager is None:
            headers = self._websocket_headers()
            sslopt = None
            if not self._verify_ssl:
                sslopt = {"cert_reqs": ssl.CERT_NONE}

            heartbeat_interval = self._connection_options.get("heartbeat_interval")
            heartbeat_message = self._connection_options.get("heartbeat_message")
            if heartbeat_interval is None:
                heartbeat_interval = 60.0
                heartbeat_message = IBKR_WS_HEARTBEAT_TOPIC

            self._connection_manager = StreamConnectionManager(
                self._websocket_url,
                on_message=self._on_message,
                on_open=self._on_open,
                on_close=self._on_close,
                on_error=self._on_error,
                headers=headers,
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_timeout,
                heartbeat_interval=heartbeat_interval,
                heartbeat_message=heartbeat_message,
                reconnect_backoff_seconds=float(
                    self._connection_options.get("reconnect_backoff_seconds", 1.0)
                ),
                max_reconnect_backoff_seconds=float(
                    self._connection_options.get("max_reconnect_backoff_seconds", 60.0)
                ),
                max_reconnect_attempts=self._connection_options.get("max_reconnect_attempts"),
                sslopt=sslopt,
            )
        return self._connection_manager

    def connect(self) -> None:
        """Authenticate, start keepalive, and open the websocket."""
        self._session_client.ensure_session()
        self._session_client.start_keepalive()
        self._resolve_contracts()
        self.connection_manager.connect()

    def disconnect(self) -> None:
        """Stop streaming and tear down the gateway session helpers."""
        if self._subscribed:
            self._unsubscribe_all()
        self._session_client.stop_keepalive()
        self.connection_manager.disconnect()
        self._subscribed = False

    def add_symbols(self, symbols: Sequence[str]) -> None:
        """Subscribe to additional symbols on the active websocket."""
        for symbol in symbols:
            normalized = symbol.upper()
            if normalized in self._symbols:
                continue
            contract = self._trader_client.search_contract(normalized)
            self._symbol_by_conid[contract.conid] = normalized
            self._preflight_snapshot(contract.conid)
            self._subscribe_market_data(contract.conid)

    def _resolve_contracts(self) -> None:
        self._session_client.ensure_session()
        self._symbol_by_conid.clear()
        for symbol in self._symbols:
            contract = self._trader_client.search_contract(symbol)
            self._symbol_by_conid[contract.conid] = symbol

    def _websocket_headers(self) -> dict[str, str]:
        tickle_payload = self._session_client.tickle()
        session_token = str(tickle_payload.get("session", "") or "")
        if not session_token:
            raise RuntimeError("IBKR /tickle did not return a session token for websocket auth")
        return {"Cookie": f"api={session_token}"}

    def _on_open(self) -> None:
        logger.info("IBKR websocket connected")
        if self._subscribe_on_connect:
            for conid in self._symbol_by_conid:
                self._preflight_snapshot(conid)
                self._subscribe_market_data(conid)
            self._subscribed = True
            logger.info(
                "Subscribed to IBKR smd streams for %s",
                ", ".join(self._symbols),
            )
        if self._on_open_external is not None:
            self._on_open_external()

    def _on_close(self, code: Optional[int], reason: Optional[str]) -> None:
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
            if event.event_type == StreamEventType.MARKET_TICK and event.payload is not None:
                bar_payload = self._market_tick_to_bar(event.payload)
                if bar_payload is not None:
                    self._publish_bar(bar_payload)

    def _market_tick_to_bar(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        conid = _optional_int(payload.get("conid"))
        if conid is None:
            return None

        last_price = _optional_float(payload.get("31"))
        if last_price is None:
            return None

        return self._bar_builder.update(
            conid=conid,
            last_price=last_price,
            last_size=_optional_float(payload.get("88")),
            day_volume=_optional_float(payload.get("7762")),
            updated_at_ms=_optional_int(payload.get("_updated")),
        )

    def _preflight_snapshot(self, conid: int) -> None:
        self._session_client.request(
            "GET",
            self._snapshot_path,
            params={
                "conids": str(conid),
                "fields": self._snapshot_fields,
            },
        )

    def _subscribe_market_data(self, conid: int) -> None:
        fields_json = json.dumps({"fields": list(self._stream_fields)})
        message = f"smd+{conid}+{fields_json}"
        self.connection_manager.send(message)

    def _unsubscribe_market_data(self, conid: int) -> None:
        self.connection_manager.send(f"umd+{conid}+{{}}")

    def _unsubscribe_all(self) -> None:
        try:
            for conid in self._symbol_by_conid:
                self._unsubscribe_market_data(conid)
        except Exception:
            logger.exception("Failed to unsubscribe IBKR market data streams")

    def _publish_bar(self, payload: dict[str, Any]) -> None:
        message = json.dumps(payload)
        try:
            self._processor.process_message(message)
        except Exception:
            logger.exception("Failed to publish IBKR bar to stream processor")


def build_ibkr_stream_processor(
    *,
    symbols: Sequence[str],
    consumers: Optional[list[Callable[[CleanBarEvent], None]]] = None,
    timeframe: str = "1m",
    require_minute_alignment: Optional[bool] = None,
    dedup_window: Optional[int] = None,
    stream_settings: Optional["StreamSettings"] = None,
) -> StreamDataProcessor:
    """Create a stream processor configured for IBKR 1-minute bars."""
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
        field_map=IBKR_STREAM_BAR_FIELDS,
        bar_key="bar",
        symbol_key="symbol",
        timeframe_key="timeframe",
        require_minute_alignment=require_minute_alignment,
        dedup_window=dedup_window,
    )


def _floor_minute(updated_at_ms: Optional[int]) -> datetime:
    if updated_at_ms is None:
        return datetime.now(timezone.utc).replace(second=0, microsecond=0)
    millis = int(updated_at_ms)
    if millis < 1_000_000_000_000:
        millis *= 1000
    return pd.to_datetime(millis, unit="ms", utc=True).floor("min").to_pydatetime()


def _normalize_forming_ohlc(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
) -> Optional[tuple[float, float, float, float]]:
    if close_price <= 0:
        return None
    if open_price <= 0:
        open_price = close_price
    if high_price <= 0:
        high_price = max(open_price, close_price)
    if low_price <= 0:
        low_price = min(open_price, close_price)
    return _repair_ohlc_bar(open_price, high_price, low_price, close_price)


def _optional_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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

    processor = build_ibkr_stream_processor(symbols=("SPY",), consumers=[on_bar])
    session = IbkrStreamSession.from_env(symbols=("SPY",), processor=processor)
    print(f"Connecting to {session.connection_manager.url}")
    session.connect()

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        session.disconnect()
