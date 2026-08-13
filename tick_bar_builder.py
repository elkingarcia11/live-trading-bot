"""N-tick OHLCV bar aggregation.

Responsibility: Aggregate individual trade prints into completed tick bars
(e.g. 50t). Does not own vendor I/O, stream validation, or persistence.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ohlc_sanity import repair_ohlc_bar as _repair_ohlc_bar


@dataclass
class _FormingTickBar:
    symbol: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    ticks: int


def parse_tick_timeframe(timeframe: str) -> int:
    """Parse a tick timeframe label such as ``50t`` into a positive tick count."""
    if len(timeframe) < 2 or timeframe[-1].lower() != "t":
        raise ValueError(f"Unsupported tick timeframe: {timeframe}")
    ticks = int(timeframe[:-1])
    if ticks <= 0:
        raise ValueError(f"Tick timeframe must be positive: {timeframe}")
    return ticks


def is_tick_timeframe(timeframe: str) -> bool:
    """Return whether ``timeframe`` is an N-tick label (e.g. ``50t``)."""
    try:
        parse_tick_timeframe(timeframe)
        return True
    except ValueError:
        return False


class TickBarBuilder:
    """Aggregate trade prints into completed N-tick OHLCV bars."""

    def __init__(self, *, ticks_per_bar: int = 50) -> None:
        if ticks_per_bar <= 0:
            raise ValueError("ticks_per_bar must be positive")
        self._ticks_per_bar = ticks_per_bar
        self._timeframe = f"{ticks_per_bar}t"
        self._forming: dict[str, _FormingTickBar] = {}
        self._lock = threading.Lock()

    @property
    def ticks_per_bar(self) -> int:
        return self._ticks_per_bar

    @property
    def timeframe(self) -> str:
        return self._timeframe

    def update(
        self,
        *,
        symbol: str,
        price: float,
        size: float,
        timestamp: datetime,
    ) -> Optional[dict[str, Any]]:
        """Ingest one trade print.

        Returns a completed bar envelope when ``ticks_per_bar`` trades have
        accumulated; otherwise ``None``.
        """
        symbol = symbol.upper()
        if not symbol or price <= 0:
            return None

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)

        trade_size = max(float(size), 0.0)
        with self._lock:
            forming = self._forming.get(symbol)
            if forming is None:
                forming = _FormingTickBar(
                    symbol=symbol,
                    start=timestamp,
                    end=timestamp,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=trade_size,
                    ticks=1,
                )
                self._forming[symbol] = forming
            else:
                forming.high = max(forming.high, price)
                forming.low = min(forming.low, price)
                forming.close = price
                forming.end = timestamp
                if trade_size > 0:
                    forming.volume += trade_size
                forming.ticks += 1

            if forming.ticks < self._ticks_per_bar:
                return None

            normalized = _normalize_forming_ohlc(
                forming.open,
                forming.high,
                forming.low,
                forming.close,
            )
            del self._forming[symbol]
            if normalized is None:
                return None

            open_price, high_price, low_price, close_price = normalized
            return {
                "symbol": symbol,
                "timeframe": self._timeframe,
                "bar": {
                    "datetime": forming.start.isoformat(),
                    "end": forming.end.isoformat(),
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": forming.volume,
                },
            }

    def forming_ticks(self, symbol: str) -> Optional[int]:
        """Return how many prints are in the open bar for ``symbol``, if any."""
        with self._lock:
            forming = self._forming.get(symbol.upper())
            return None if forming is None else forming.ticks

    def flush(self, symbol: Optional[str] = None) -> list[dict[str, Any]]:
        """Emit in-progress bars as completed envelopes (for shutdown persistence).

        Partial bars keep their current OHLCV and tick count; forming state is
        cleared for the flushed symbol(s).
        """
        with self._lock:
            if symbol is None:
                symbols = list(self._forming.keys())
            else:
                symbols = [symbol.upper()]

            payloads: list[dict[str, Any]] = []
            for key in symbols:
                forming = self._forming.pop(key, None)
                if forming is None:
                    continue
                normalized = _normalize_forming_ohlc(
                    forming.open,
                    forming.high,
                    forming.low,
                    forming.close,
                )
                if normalized is None:
                    continue
                open_price, high_price, low_price, close_price = normalized
                payloads.append(
                    {
                        "symbol": forming.symbol,
                        "timeframe": self._timeframe,
                        "bar": {
                            "datetime": forming.start.isoformat(),
                            "end": forming.end.isoformat(),
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                            "volume": forming.volume,
                            "partial": True,
                            "ticks": forming.ticks,
                        },
                    }
                )
            return payloads

    def reset(self, symbol: Optional[str] = None) -> None:
        """Drop forming bar state for one symbol or all symbols."""
        with self._lock:
            if symbol is None:
                self._forming.clear()
                return
            self._forming.pop(symbol.upper(), None)


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
