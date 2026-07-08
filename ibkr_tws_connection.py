"""IBKR TWS / IB Gateway socket session.

Responsibility: Manage a shared ibapi EClient/EWrapper connection for live
workflows. Handles connection lifecycle, order ids, positions, historical
bars, and tick-by-tick callbacks.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ibapi.client import EClient
from ibapi.common import BarData, TickAttribLast
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.wrapper import EWrapper

logger = logging.getLogger(__name__)

TickHandler = Callable[[int, float, float, int], None]


class IbkrTwsError(Exception):
    """Raised when TWS / IB Gateway operations fail."""


@dataclass
class IbkrTwsOrderState:
    """Latest broker order status from TWS callbacks."""

    order_id: int
    status: str
    filled: float
    remaining: float
    average_fill_price: float = 0.0
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class IbkrTwsPosition:
    """Normalized open position from TWS."""

    account: str
    symbol: str
    quantity: float
    average_price: float


class IbkrTwsRuntime(EWrapper, EClient):
    """Shared ibapi session used by stream, broker, and account sync code."""

    def __init__(self) -> None:
        EClient.__init__(self, self)
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._next_order_id: Optional[int] = None
        self._req_id = 1000
        self._req_lock = threading.Lock()
        self._order_states: dict[int, IbkrTwsOrderState] = {}
        self._positions: list[IbkrTwsPosition] = []
        self._positions_done = threading.Event()
        self._historical_bars: dict[int, list[BarData]] = {}
        self._historical_done: dict[int, threading.Event] = {}
        self._historical_ticks: dict[int, list[dict[str, Any]]] = {}
        self._historical_ticks_done: dict[int, threading.Event] = {}
        self._tick_handlers: dict[int, TickHandler] = {}
        self._managed_accounts: str = ""
        self._errors: list[str] = []

    @classmethod
    def from_config(cls) -> IbkrTwsRuntime:
        return cls()

    def connect_session(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        timeout_seconds: float = 30.0,
    ) -> None:
        """Open the socket and wait for nextValidId."""
        if self.isConnected():
            return

        self.connect(host, port, client_id)
        self._thread = threading.Thread(target=self.run, name="ibkr-tws-api", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout_seconds):
            raise IbkrTwsError(
                f"TWS/IB Gateway connection timed out after {timeout_seconds:.0f}s "
                f"({host}:{port}, client_id={client_id})"
            )
        logger.info(
            "Connected to TWS/IB Gateway at %s:%s (client_id=%s, next_order_id=%s)",
            host,
            port,
            client_id,
            self._next_order_id,
        )

    def disconnect_session(self) -> None:
        """Disconnect from TWS / IB Gateway."""
        if self.isConnected():
            self.disconnect()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._thread = None
        self._ready.clear()

    def next_req_id(self) -> int:
        with self._req_lock:
            self._req_id += 1
            return self._req_id

    def next_order_id(self) -> int:
        if self._next_order_id is None:
            raise IbkrTwsError("TWS session is not ready; next order id is unavailable")
        order_id = self._next_order_id
        self._next_order_id += 1
        return order_id

    def set_market_data_type(self, market_data_type: int) -> None:
        self.reqMarketDataType(market_data_type)

    def subscribe_tick_by_tick(
        self,
        contract: Contract,
        *,
        tick_type: str = "Last",
        handler: TickHandler,
    ) -> int:
        """Subscribe to tick-by-tick data and route callbacks to handler."""
        req_id = self.next_req_id()
        self._tick_handlers[req_id] = handler
        self.reqTickByTickData(req_id, contract, tick_type, 0, False)
        return req_id

    def unsubscribe_tick_by_tick(self, req_id: int) -> None:
        self.cancelTickByTickData(req_id)
        self._tick_handlers.pop(req_id, None)

    def request_positions(self, *, timeout_seconds: float = 10.0) -> list[IbkrTwsPosition]:
        self._positions.clear()
        self._positions_done.clear()
        self.reqPositions()
        if not self._positions_done.wait(timeout_seconds):
            raise IbkrTwsError("Timed out waiting for TWS positions")
        return list(self._positions)

    def request_historical_bars(
        self,
        contract: Contract,
        *,
        end_datetime: str,
        duration: str,
        bar_size: str,
        what_to_show: str,
        use_rth: int,
        timeout_seconds: float = 60.0,
    ) -> list[BarData]:
        req_id = self.next_req_id()
        self._historical_bars[req_id] = []
        done = threading.Event()
        self._historical_done[req_id] = done
        self.reqHistoricalData(
            req_id,
            contract,
            end_datetime,
            duration,
            bar_size,
            what_to_show,
            use_rth,
            1,
            False,
            [],
        )
        if not done.wait(timeout_seconds):
            raise IbkrTwsError(
                f"Timed out waiting for historical bars ({contract.symbol}, {bar_size})"
            )
        bars = self._historical_bars.pop(req_id, [])
        self._historical_done.pop(req_id, None)
        return bars

    def request_historical_ticks(
        self,
        contract: Contract,
        *,
        start_datetime: str,
        end_datetime: str,
        number_of_ticks: int = 1000,
        what_to_show: str = "TRADES",
        use_rth: int = 0,
        ignore_size: bool = False,
        timeout_seconds: float = 60.0,
    ) -> list[dict[str, Any]]:
        """Fetch historical tick prints via reqHistoricalTicks."""
        req_id = self.next_req_id()
        self._historical_ticks[req_id] = []
        done = threading.Event()
        self._historical_ticks_done[req_id] = done
        self.reqHistoricalTicks(
            req_id,
            contract,
            start_datetime,
            end_datetime,
            number_of_ticks,
            what_to_show,
            use_rth,
            ignore_size,
            [],
        )
        if not done.wait(timeout_seconds):
            raise IbkrTwsError(
                f"Timed out waiting for historical ticks ({contract.symbol}, {what_to_show})"
            )
        ticks = self._historical_ticks.pop(req_id, [])
        self._historical_ticks_done.pop(req_id, None)
        return ticks

    def place_contract_order(self, contract: Contract, order: Order) -> int:
        order_id = self.next_order_id()
        self.placeOrder(order_id, contract, order)
        return order_id

    def cancel_broker_order(self, order_id: int) -> None:
        self.cancelOrder(order_id)

    def get_order_state(self, order_id: int) -> Optional[IbkrTwsOrderState]:
        return self._order_states.get(order_id)

    def managed_accounts(self) -> str:
        return self._managed_accounts

    # --- EWrapper callbacks ---

    def nextValidId(self, orderId: int) -> None:
        self._next_order_id = orderId
        self._ready.set()

    def managedAccounts(self, accountsList: str) -> None:
        self._managed_accounts = accountsList

    def error(
        self,
        reqId: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        if errorCode in {2104, 2106, 2158, 2119, 2103, 2105, 2108, 2176}:
            logger.debug("TWS info %s (%s): %s", errorCode, reqId, errorString)
            return
        if errorCode == 504:
            self._ready.clear()
        message = f"TWS error {errorCode} (reqId={reqId}): {errorString}"
        if errorCode >= 1000:
            logger.warning(message)
        else:
            logger.error(message)
        self._errors.append(message)

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        del permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice
        self._order_states[orderId] = IbkrTwsOrderState(
            order_id=orderId,
            status=status,
            filled=float(filled),
            remaining=float(remaining),
            average_fill_price=float(avgFillPrice or 0.0),
        )

    def position(
        self,
        account: str,
        contract: Contract,
        position: float,
        avgCost: float,
    ) -> None:
        if position == 0:
            return
        symbol = str(contract.symbol or "").upper()
        if not symbol:
            return
        self._positions.append(
            IbkrTwsPosition(
                account=account,
                symbol=symbol,
                quantity=float(position),
                average_price=float(avgCost),
            )
        )

    def positionEnd(self) -> None:
        self._positions_done.set()

    def historicalData(self, reqId: int, bar: BarData) -> None:
        self._historical_bars.setdefault(reqId, []).append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        del start, end
        event = self._historical_done.get(reqId)
        if event is not None:
            event.set()

    def historicalTicks(
        self,
        reqId: int,
        ticks: list,
        done: bool,
    ) -> None:
        self._append_historical_ticks(reqId, ticks, done, kind="MIDPOINT")

    def historicalTicksBidAsk(
        self,
        reqId: int,
        ticks: list,
        done: bool,
    ) -> None:
        self._append_historical_ticks(reqId, ticks, done, kind="BID_ASK")

    def historicalTicksLast(
        self,
        reqId: int,
        ticks: list,
        done: bool,
    ) -> None:
        self._append_historical_ticks(reqId, ticks, done, kind="TRADES")

    def _append_historical_ticks(
        self,
        req_id: int,
        ticks: list,
        done: bool,
        *,
        kind: str,
    ) -> None:
        bucket = self._historical_ticks.setdefault(req_id, [])
        for tick in ticks:
            bucket.append(_normalize_historical_tick(tick, kind=kind))
        if done:
            event = self._historical_ticks_done.get(req_id)
            if event is not None:
                event.set()

    def tickByTickAllLast(
        self,
        reqId: int,
        tickType: int,
        time_: int,
        price: float,
        size: int,
        tickAttribLast: TickAttribLast,
        exchange: str,
        specialConditions: str,
    ) -> None:
        self._dispatch_tick(reqId, price, size, time_)

    def tickByTickLast(
        self,
        reqId: int,
        tickType: int,
        time_: int,
        price: float,
        size: int,
        tickAttribLast: TickAttribLast,
        exchange: str,
        specialConditions: str,
    ) -> None:
        del tickType, tickAttribLast, exchange, specialConditions
        self._dispatch_tick(reqId, price, size, time_)

    def _dispatch_tick(
        self,
        req_id: int,
        price: float,
        size: int,
        epoch_seconds: int,
    ) -> None:
        handler = self._tick_handlers.get(req_id)
        if handler is not None:
            handler(req_id, float(price), float(size), int(epoch_seconds))


def _normalize_historical_tick(tick: Any, *, kind: str) -> dict[str, Any]:
    timestamp = _epoch_to_datetime(getattr(tick, "time", 0))
    if kind == "BID_ASK":
        return {
            "kind": kind,
            "time": timestamp.isoformat(),
            "price_bid": float(getattr(tick, "priceBid", 0.0) or 0.0),
            "price_ask": float(getattr(tick, "priceAsk", 0.0) or 0.0),
            "size_bid": float(getattr(tick, "sizeBid", 0) or 0),
            "size_ask": float(getattr(tick, "sizeAsk", 0) or 0),
        }
    if kind == "MIDPOINT":
        return {
            "kind": kind,
            "time": timestamp.isoformat(),
            "price": float(getattr(tick, "price", 0.0) or 0.0),
            "size": float(getattr(tick, "size", 0) or 0),
        }
    return {
        "kind": kind,
        "time": timestamp.isoformat(),
        "price": float(getattr(tick, "price", 0.0) or 0.0),
        "size": float(getattr(tick, "size", 0) or 0),
        "exchange": str(getattr(tick, "exchange", "") or ""),
        "special_conditions": str(getattr(tick, "specialConditions", "") or ""),
    }


def _epoch_to_datetime(epoch: int) -> datetime:
    if epoch > 10_000_000_000:
        epoch //= 1000
    return datetime.fromtimestamp(epoch, tz=timezone.utc)
