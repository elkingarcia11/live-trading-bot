"""IBKR TWS tick-by-tick streamer.

Responsibility: Stream equity ticks from reqTickByTickData and aggregate them
into 1-minute bars for StreamDataProcessor.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Sequence

import pandas as pd

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime
from ibkr_tws_contracts import equity_contract
from market_data_transformer import IBKR_STREAM_BAR_FIELDS
from ohlc_sanity import repair_ohlc_bar as _repair_ohlc_bar
from schwab_auth import _load_dotenv
from stream_data_processor import CleanBarEvent, StreamDataProcessor

logger = logging.getLogger(__name__)


@dataclass
class _FormingMinuteBar:
    symbol: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class IbkrTwsMinuteBarBuilder:
    """Aggregate tick-by-tick prints into forming 1-minute OHLCV bars."""

    def __init__(self, symbol_by_req_id: dict[int, str]) -> None:
        self._symbol_by_req_id = symbol_by_req_id
        self._forming: dict[str, _FormingMinuteBar] = {}
        self._lock = threading.Lock()

    def update(
        self,
        *,
        req_id: int,
        price: float,
        size: float,
        epoch_seconds: int,
    ) -> Optional[dict[str, Any]]:
        symbol = self._symbol_by_req_id.get(req_id)
        if not symbol or price <= 0:
            return None

        minute = _floor_minute(epoch_seconds)
        trade_size = max(float(size), 0.0)
        with self._lock:
            forming = self._forming.get(symbol)
            if forming is None or forming.minute != minute:
                forming = _FormingMinuteBar(
                    symbol=symbol,
                    minute=minute,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=trade_size,
                )
                self._forming[symbol] = forming
            else:
                forming.high = max(forming.high, price)
                forming.low = min(forming.low, price)
                forming.close = price
                if trade_size > 0:
                    forming.volume += trade_size

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


class IbkrTwsStreamSession:
    """Managed TWS session that streams tick-by-tick data into 1-minute bars."""

    def __init__(
        self,
        runtime: IbkrTwsRuntime,
        *,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        exchange: str = "SMART",
        currency: str = "USD",
        tick_by_tick_type: str = "Last",
        on_open_external: Optional[Callable[[], None]] = None,
        on_close_external: Optional[Callable[[], None]] = None,
        on_error_external: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        self._runtime = runtime
        self._symbols = tuple(symbol.upper() for symbol in symbols)
        self._processor = processor
        self._exchange = exchange
        self._currency = currency
        self._tick_by_tick_type = tick_by_tick_type
        self._symbol_by_req_id: dict[int, str] = {}
        self._req_ids: list[int] = []
        self._bar_builder = IbkrTwsMinuteBarBuilder(self._symbol_by_req_id)
        self._on_open_external = on_open_external
        self._on_close_external = on_close_external
        self._on_error_external = on_error_external
        self._connected = False

    @classmethod
    def from_env(
        cls,
        *,
        symbols: Sequence[str],
        processor: StreamDataProcessor,
        runtime: Optional[IbkrTwsRuntime] = None,
        load_dotenv: bool = True,
        **kwargs: Any,
    ) -> IbkrTwsStreamSession:
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        ibkr = app.ibkr
        shared_runtime = runtime or IbkrTwsRuntime.from_config()
        if not shared_runtime.isConnected():
            shared_runtime.connect_session(
                host=ibkr.host,
                port=ibkr.port,
                client_id=ibkr.client_id,
                timeout_seconds=ibkr.connect_timeout_seconds,
            )
            shared_runtime.set_market_data_type(ibkr.market_data_type)
        return cls(
            shared_runtime,
            symbols=symbols,
            processor=processor,
            exchange=ibkr.exchange,
            currency=ibkr.currency,
            tick_by_tick_type=ibkr.tick_by_tick_type,
            **kwargs,
        )

    @property
    def runtime(self) -> IbkrTwsRuntime:
        return self._runtime

    def connect(self) -> None:
        if self._connected:
            return
        for symbol in self._symbols:
            contract = equity_contract(
                symbol,
                exchange=self._exchange,
                currency=self._currency,
            )
            req_id = self._runtime.subscribe_tick_by_tick(
                contract,
                tick_type=self._tick_by_tick_type,
                handler=self._on_tick,
            )
            self._symbol_by_req_id[req_id] = symbol
            self._req_ids.append(req_id)
        self._connected = True
        logger.info(
            "Subscribed to IBKR TWS tick-by-tick (%s) for %s",
            self._tick_by_tick_type,
            ", ".join(self._symbols),
        )
        if self._on_open_external is not None:
            self._on_open_external()

    def disconnect(self) -> None:
        for req_id in self._req_ids:
            try:
                self._runtime.unsubscribe_tick_by_tick(req_id)
            except Exception:
                logger.exception("Failed to unsubscribe IBKR tick stream %s", req_id)
        self._req_ids.clear()
        self._symbol_by_req_id.clear()
        self._connected = False
        if self._on_close_external is not None:
            self._on_close_external()

    def _on_tick(self, req_id: int, price: float, size: float, epoch_seconds: int) -> None:
        try:
            payload = self._bar_builder.update(
                req_id=req_id,
                price=price,
                size=size,
                epoch_seconds=epoch_seconds,
            )
            if payload is None:
                return
            self._processor.process_message(json.dumps(payload))
        except Exception as exc:
            logger.exception("Failed to process IBKR TWS tick")
            if self._on_error_external is not None:
                self._on_error_external(exc)


def build_ibkr_tws_stream_processor(
    *,
    symbols: Sequence[str],
    consumers: Optional[list[Callable[[CleanBarEvent], None]]] = None,
    timeframe: str = "1m",
    require_minute_alignment: Optional[bool] = None,
    dedup_window: Optional[int] = None,
    stream_settings: Optional["StreamSettings"] = None,
) -> StreamDataProcessor:
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


def _floor_minute(epoch_seconds: int) -> datetime:
    if epoch_seconds > 10_000_000_000:
        epoch_seconds //= 1000
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(
        second=0,
        microsecond=0,
    )


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
