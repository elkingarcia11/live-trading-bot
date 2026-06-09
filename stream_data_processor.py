"""Stream Data Processor.

Responsibility: Live stream semantics for 1-minute bars.

Parses stream message envelopes, validates live 1-minute bars, performs
duplicate detection, and publishes clean events to internal consumers. Does
not manage WebSocket connections, execute HTTP requests, map vendor field
names, or persist data.
"""

from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional, Sequence

import pandas as pd

from market_data_transformer import MarketDataTransformer, OhlcvFieldMap, SHORT_BAR_FIELDS

logger = logging.getLogger(__name__)

BarEventHandler = Callable[["CleanBarEvent"], None]


@dataclass(frozen=True)
class CleanBarEvent:
    """Validated 1-minute OHLCV bar published to internal consumers."""

    symbol: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize the event for logging or downstream messaging."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class StreamDataProcessor:
    """Validates live 1-minute bars, deduplicates them, and publishes clean events."""

    def __init__(
        self,
        *,
        symbols: Optional[Sequence[str]] = None,
        symbol: Optional[str] = None,
        timeframe: str = "1m",
        consumers: Optional[list[BarEventHandler]] = None,
        transformer: Optional[MarketDataTransformer] = None,
        field_map: Optional[OhlcvFieldMap] = None,
        bar_key: str = "bar",
        symbol_key: str = "symbol",
        timeframe_key: str = "timeframe",
        require_minute_alignment: bool = True,
        dedup_window: int = 500,
    ) -> None:
        """Initialize the processor for one or more symbol streams.

        Args:
            symbols: Ticker symbols to accept from the live stream.
            symbol: Optional single-symbol shorthand for `symbols`.
            timeframe: Expected bar interval. Only matching bars are accepted.
            consumers: Optional list of callbacks invoked for each clean event.
            transformer: Optional transformer used to normalize vendor payloads.
            field_map: Provider-specific OHLCV field mapping for bar objects.
            bar_key: JSON key containing the bar object in stream messages.
            symbol_key: JSON key containing the ticker symbol in stream messages.
            timeframe_key: JSON key containing the bar interval in stream messages.
            require_minute_alignment: Reject bars whose timestamps are not aligned
                to minute boundaries.
            dedup_window: Number of recent (symbol, timestamp) bars kept for
                duplicate detection.
        """
        resolved_symbols = self._resolve_symbols(symbols=symbols, symbol=symbol)
        self._symbols = frozenset(resolved_symbols)
        self._default_symbol = (
            next(iter(self._symbols)) if len(self._symbols) == 1 else None
        )
        self._timeframe = timeframe
        self._consumers = list(consumers or [])
        self._transformer = transformer or MarketDataTransformer()
        self._field_map = field_map or SHORT_BAR_FIELDS
        self._bar_key = bar_key
        self._symbol_key = symbol_key
        self._timeframe_key = timeframe_key
        self._require_minute_alignment = require_minute_alignment
        self._dedup_window = dedup_window

        self._seen_bars: OrderedDict[tuple[str, datetime], tuple[float, ...]] = (
            OrderedDict()
        )
        self._lock = threading.Lock()

    def subscribe(self, consumer: BarEventHandler) -> None:
        """Register an internal consumer for clean bar events.

        Args:
            consumer: Callback invoked with each validated, deduplicated bar.
        """
        self._consumers.append(consumer)

    def process_message(self, raw_message: str) -> Optional[CleanBarEvent]:
        """Parse a stream envelope, then validate, deduplicate, and publish.

        Handles only the stream message wrapper (JSON envelope and metadata).
        Vendor bar field mapping is delegated to `MarketDataTransformer`.

        Args:
            raw_message: Raw JSON payload received from the WebSocket stream.

        Returns:
            The published `CleanBarEvent`, or None if the message was dropped.
        """
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("Dropping non-JSON stream message: %s", raw_message)
            return None

        if not isinstance(payload, dict):
            logger.warning("Dropping stream message with unsupported shape.")
            return None

        bar_object = payload.get(self._bar_key)
        if not isinstance(bar_object, dict):
            logger.warning("Dropping stream message without bar object at '%s'.", self._bar_key)
            return None

        raw_symbol = payload.get(self._symbol_key, self._default_symbol)
        if raw_symbol is None:
            logger.warning("Dropping stream message without symbol for multi-symbol feed.")
            return None

        symbol = str(raw_symbol).upper()
        timeframe = str(payload.get(self._timeframe_key, self._timeframe))

        return self.process_bar(
            bar_object,
            symbol=symbol,
            timeframe=timeframe,
        )

    def process_bar(
        self,
        bar: dict[str, Any],
        *,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> Optional[CleanBarEvent]:
        """Validate, deduplicate, and publish one vendor bar object.

        Args:
            bar: Provider bar dictionary from the live stream.
            symbol: Ticker symbol for the bar. Defaults to the configured symbol.
            timeframe: Bar interval label. Defaults to the configured timeframe.

        Returns:
            The published `CleanBarEvent`, or None if the bar was dropped.
        """
        if symbol is None:
            if self._default_symbol is None:
                logger.warning("Dropping bar without symbol for multi-symbol feed.")
                return None
            symbol = self._default_symbol
        symbol = symbol.upper()
        timeframe = timeframe or self._timeframe

        if symbol not in self._symbols:
            logger.debug("Dropping bar for unsubscribed symbol %s.", symbol)
            return None

        if timeframe != self._timeframe:
            logger.warning(
                "Dropping %s bar for %s with unexpected timeframe %s.",
                self._timeframe,
                symbol,
                timeframe,
            )
            return None

        try:
            ohlcv = self._transformer.from_bars([bar], field_map=self._field_map)
        except ValueError as exc:
            logger.warning("Dropping invalid bar for %s: %s", symbol, exc)
            return None

        if ohlcv.empty:
            logger.warning("Dropping empty bar for %s.", symbol)
            return None

        row = ohlcv.iloc[0]
        if not self._is_valid_bar(row):
            logger.warning("Dropping bar that failed validation for %s.", symbol)
            return None

        event = CleanBarEvent(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=row["timestamp"].to_pydatetime(),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )

        if self._is_duplicate(event):
            logger.debug("Dropping duplicate bar for %s at %s.", symbol, event.timestamp)
            return None

        self._remember_bar(event)
        self._publish(event)
        return event

    def _is_valid_bar(self, row: pd.Series) -> bool:
        """Return whether a normalized bar satisfies 1-minute bar constraints."""
        timestamp = row["timestamp"]
        if not isinstance(timestamp, pd.Timestamp):
            timestamp = pd.to_datetime(timestamp, utc=True)

        if self._require_minute_alignment and (
            timestamp.second != 0 or timestamp.microsecond != 0
        ):
            logger.warning(
                "Bar timestamp %s is not aligned to a minute boundary.", timestamp
            )
            return False

        open_price = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        volume = float(row["volume"])

        if volume < 0:
            return False
        if min(open_price, high, low, close) <= 0:
            return False
        if high < low:
            return False
        if high < max(open_price, close):
            return False
        if low > min(open_price, close):
            return False

        return True

    def _bar_signature(self, event: CleanBarEvent) -> tuple[float, ...]:
        """Build a value signature used for duplicate detection."""
        return (event.open, event.high, event.low, event.close, event.volume)

    def _is_duplicate(self, event: CleanBarEvent) -> bool:
        """Return whether an identical bar has already been published."""
        key = (event.symbol, event.timestamp)
        signature = self._bar_signature(event)

        with self._lock:
            previous = self._seen_bars.get(key)
            return previous == signature

    def _remember_bar(self, event: CleanBarEvent) -> None:
        """Store a published bar for future duplicate detection."""
        key = (event.symbol, event.timestamp)
        signature = self._bar_signature(event)

        with self._lock:
            self._seen_bars[key] = signature
            self._seen_bars.move_to_end(key)

            while len(self._seen_bars) > self._dedup_window:
                self._seen_bars.popitem(last=False)

    def _publish(self, event: CleanBarEvent) -> None:
        """Deliver a clean bar event to all registered consumers."""
        for consumer in self._consumers:
            consumer(event)

    @property
    def symbols(self) -> frozenset[str]:
        """Return the subscribed ticker symbols."""
        return self._symbols

    def _resolve_symbols(
        self,
        *,
        symbols: Optional[Sequence[str]],
        symbol: Optional[str],
    ) -> tuple[str, ...]:
        """Normalize symbol inputs into an uppercase symbol tuple."""
        if symbols is not None and symbol is not None:
            raise ValueError("Pass either symbols or symbol, not both")

        if symbols is not None:
            normalized = tuple(str(item).upper() for item in symbols if str(item).strip())
        elif symbol is not None:
            normalized = (str(symbol).upper(),)
        else:
            raise ValueError("StreamDataProcessor requires symbols or symbol")

        if not normalized:
            raise ValueError("At least one symbol is required")

        return normalized


if __name__ == "__main__":
    # Example usage with StreamConnectionManager.
    from stream_connection_manager import StreamConnectionManager

    published: list[CleanBarEvent] = []

    def on_clean_bar(event: CleanBarEvent) -> None:
        published.append(event)
        print(event.to_dict())

    processor = StreamDataProcessor(
        symbol="AAPL",
        timeframe="1m",
        consumers=[on_clean_bar],
    )

    # Process a raw stream message.
    sample_message = json.dumps(
        {
            "symbol": "AAPL",
            "timeframe": "1m",
            "bar": {
                "t": "2024-01-15T09:30:00Z",
                "o": 185.0,
                "h": 185.5,
                "l": 184.8,
                "c": 185.3,
                "v": 1000,
            },
        }
    )

    processor.process_message(sample_message)

    # Duplicate replay is dropped.
    assert processor.process_message(sample_message) is None
    assert len(published) == 1

    # Wire into a WebSocket stream.
    manager = StreamConnectionManager(
        "wss://echo.websocket.events",
        on_message=processor.process_message,
    )
    print("Ready to process live 1-minute bars for AAPL")
