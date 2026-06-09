"""Order Manager.

Responsibility: Broker-facing order execution.

Translates trading signals into buy/sell orders, tracks pending and filled
status, and handles exchange execution reporting. Does not calculate PnL,
monitor stops, or maintain portfolio state.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional, Protocol

logger = logging.getLogger(__name__)

OrderUpdateHandler = Callable[["Order"], None]


class OrderSide(Enum):
    """Direction of an order or fill."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Supported order types."""

    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(Enum):
    """Lifecycle states for broker orders."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class TradingSignal:
    """Internal instruction to enter or exit a position."""

    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    signal_id: Optional[str] = None


@dataclass(frozen=True)
class FillEvent:
    """Normalized fill event emitted when an order execution completes."""

    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    timestamp: datetime


@dataclass(frozen=True)
class ExecutionReport:
    """Broker execution update for an existing order."""

    broker_order_id: str
    status: OrderStatus
    filled_quantity: float
    average_fill_price: Optional[float] = None
    updated_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None


@dataclass
class Order:
    """Tracked broker order derived from a trading signal."""

    id: str
    signal: TradingSignal
    status: OrderStatus
    broker_order_id: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    submitted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    rejection_reason: Optional[str] = None


class BrokerGateway(Protocol):
    """Broker transport contract used by the order manager."""

    def submit_order(self, order: Order) -> str:
        """Submit an order to the broker and return the broker order id."""

    def cancel_order(self, broker_order_id: str) -> None:
        """Request cancellation for a broker order."""

    def get_order_status(self, broker_order_id: str) -> ExecutionReport:
        """Fetch the latest execution report for a broker order."""


class OrderManager:
    """Translates trading signals into broker orders and tracks their lifecycle."""

    def __init__(
        self,
        broker: BrokerGateway,
        *,
        on_update: Optional[OrderUpdateHandler] = None,
    ) -> None:
        """Initialize the order manager.

        Args:
            broker: Broker gateway responsible for physical order submission.
            on_update: Optional callback invoked whenever an order changes state.
        """
        self._broker = broker
        self._on_update = on_update
        self._orders: dict[str, Order] = {}
        self._orders_by_broker_id: dict[str, str] = {}
        self._lock = threading.Lock()

    def submit_signal(self, signal: TradingSignal) -> Order:
        """Translate a trading signal into a broker order.

        Args:
            signal: Internal buy or sell instruction.

        Returns:
            The created order after broker submission.

        Raises:
            ValueError: If the signal is invalid.
            RuntimeError: If broker submission fails.
        """
        self._validate_signal(signal)

        order = Order(
            id=str(uuid.uuid4()),
            signal=signal,
            status=OrderStatus.PENDING,
        )

        with self._lock:
            self._orders[order.id] = order

        try:
            broker_order_id = self._broker.submit_order(order)
        except Exception as exc:
            rejected = self._transition(
                order,
                status=OrderStatus.REJECTED,
                rejection_reason=str(exc),
            )
            raise RuntimeError(f"Broker rejected order {order.id}: {exc}") from exc

        submitted = self._transition(
            order,
            status=OrderStatus.SUBMITTED,
            broker_order_id=broker_order_id,
            submitted_at=datetime.now(timezone.utc),
        )
        return submitted

    def cancel_order(self, order_id: str) -> Order:
        """Cancel a submitted or partially filled order.

        Args:
            order_id: Internal order identifier.

        Returns:
            The updated cancelled order.

        Raises:
            KeyError: If the order does not exist.
            RuntimeError: If the order cannot be cancelled.
        """
        order = self.get_order(order_id)
        if order.status in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}:
            raise RuntimeError(f"Order {order_id} cannot be cancelled in state {order.status.value}")

        if order.broker_order_id is None:
            raise RuntimeError(f"Order {order_id} has no broker order id")

        self._broker.cancel_order(order.broker_order_id)
        return self._transition(order, status=OrderStatus.CANCELLED)

    def refresh_order(self, order_id: str) -> Order:
        """Poll the broker for the latest execution status of an order.

        Args:
            order_id: Internal order identifier.

        Returns:
            The updated order.
        """
        order = self.get_order(order_id)
        if order.broker_order_id is None:
            return order

        report = self._broker.get_order_status(order.broker_order_id)
        return self.apply_execution_report(report)

    def apply_execution_report(self, report: ExecutionReport) -> Order:
        """Apply a broker execution report to a tracked order.

        Args:
            report: Latest broker-side order status.

        Returns:
            The updated order.

        Raises:
            KeyError: If the broker order id is unknown.
        """
        with self._lock:
            order_id = self._orders_by_broker_id.get(report.broker_order_id)
            if order_id is None:
                raise KeyError(f"Unknown broker order id: {report.broker_order_id}")
            order = self._orders[order_id]

        return self._transition(
            order,
            status=report.status,
            filled_quantity=report.filled_quantity,
            average_fill_price=report.average_fill_price,
            updated_at=report.updated_at or datetime.now(timezone.utc),
            rejection_reason=report.rejection_reason,
        )

    def get_order(self, order_id: str) -> Order:
        """Return a tracked order by internal id."""
        with self._lock:
            order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"Unknown order id: {order_id}")
        return order

    def list_orders(
        self,
        *,
        status: Optional[OrderStatus] = None,
        symbol: Optional[str] = None,
    ) -> list[Order]:
        """List tracked orders with optional status and symbol filters."""
        with self._lock:
            orders = list(self._orders.values())

        if status is not None:
            orders = [order for order in orders if order.status == status]
        if symbol is not None:
            symbol = symbol.upper()
            orders = [order for order in orders if order.signal.symbol.upper() == symbol]
        return sorted(orders, key=lambda order: order.created_at)

    def _validate_signal(self, signal: TradingSignal) -> None:
        """Validate signal fields before order creation."""
        if signal.quantity <= 0:
            raise ValueError("quantity must be positive")
        if signal.order_type == OrderType.LIMIT and signal.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if not signal.symbol.strip():
            raise ValueError("symbol is required")

    def _transition(
        self,
        order: Order,
        *,
        status: OrderStatus,
        broker_order_id: Optional[str] = None,
        submitted_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        filled_quantity: Optional[float] = None,
        average_fill_price: Optional[float] = None,
        rejection_reason: Optional[str] = None,
    ) -> Order:
        """Update an order and notify subscribers."""
        updated = Order(
            id=order.id,
            signal=order.signal,
            status=status,
            broker_order_id=broker_order_id or order.broker_order_id,
            created_at=order.created_at,
            submitted_at=submitted_at or order.submitted_at,
            updated_at=updated_at or order.updated_at,
            filled_quantity=order.filled_quantity if filled_quantity is None else filled_quantity,
            average_fill_price=(
                order.average_fill_price
                if average_fill_price is None
                else average_fill_price
            ),
            rejection_reason=rejection_reason or order.rejection_reason,
        )

        with self._lock:
            self._orders[updated.id] = updated
            if updated.broker_order_id is not None:
                self._orders_by_broker_id[updated.broker_order_id] = updated.id

        if self._on_update is not None:
            self._on_update(updated)

        logger.info(
            "Order %s %s %s status=%s filled=%s",
            updated.id,
            updated.signal.side.value,
            updated.signal.symbol,
            updated.status.value,
            updated.filled_quantity,
        )
        return updated

    def to_fill_event(self, order: Order) -> Optional[FillEvent]:
        """Build a fill event from a filled order, if execution data is present."""
        if order.status not in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
            return None
        if order.average_fill_price is None or order.filled_quantity <= 0:
            return None

        return FillEvent(
            order_id=order.id,
            symbol=order.signal.symbol.upper(),
            side=order.signal.side,
            quantity=order.filled_quantity,
            price=order.average_fill_price,
            timestamp=order.updated_at or datetime.now(timezone.utc),
        )

class InMemoryBrokerGateway:
    """Simple broker gateway used for local examples and tests."""

    def __init__(self, *, immediate_fill: bool = True, fill_price: float = 100.0) -> None:
        self._immediate_fill = immediate_fill
        self._fill_price = fill_price
        self._orders: dict[str, dict[str, object]] = {}

    def submit_order(self, order: Order) -> str:
        broker_order_id = f"broker-{order.id[:8]}"
        self._orders[broker_order_id] = {
            "status": (
                OrderStatus.FILLED if self._immediate_fill else OrderStatus.SUBMITTED
            ),
            "quantity": order.signal.quantity,
        }
        return broker_order_id

    def cancel_order(self, broker_order_id: str) -> None:
        if broker_order_id in self._orders:
            self._orders[broker_order_id]["status"] = OrderStatus.CANCELLED

    def get_order_status(self, broker_order_id: str) -> ExecutionReport:
        record = self._orders.get(broker_order_id)
        if record is None:
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.REJECTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
                rejection_reason="Unknown broker order id",
            )

        status = record["status"]
        assert isinstance(status, OrderStatus)
        filled_quantity = 0.0
        average_fill_price = None
        if status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
            filled_quantity = float(record["quantity"])
            average_fill_price = self._fill_price

        return ExecutionReport(
            broker_order_id=broker_order_id,
            status=status,
            filled_quantity=filled_quantity,
            average_fill_price=average_fill_price,
            updated_at=datetime.now(timezone.utc),
        )


if __name__ == "__main__":
    broker = InMemoryBrokerGateway(fill_price=185.25)
    manager = OrderManager(broker)

    signal = TradingSignal(
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        order_type=OrderType.MARKET,
        signal_id="entry-1",
    )
    order = manager.submit_signal(signal)
    print(f"Submitted order: {order.id} status={order.status.value}")

    refreshed = manager.refresh_order(order.id)
    print(
        f"Broker status: filled={refreshed.filled_quantity} "
        f"avg_price={refreshed.average_fill_price}"
    )
