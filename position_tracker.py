"""Position Tracker.

Responsibility: Real-time portfolio state and risk monitoring.

Tracks active positions, calculates live open PnL, monitors trailing stops and
take-profit targets, and emits exit notifications when a stop level is breached.
Does not submit broker orders or translate trading signals.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional

from option_quote import OptionQuoteSnapshot
from order_manager import FillEvent, OrderSide

logger = logging.getLogger(__name__)

ExitNotificationHandler = Callable[["ExitNotification"], None]


class ExitReason(Enum):
    """Why a position exit notification was triggered."""

    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"


@dataclass
class Position:
    """Active portfolio position for one symbol."""

    symbol: str
    quantity: float
    average_entry_price: float
    opened_at: datetime
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    trailing_stop_price: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    max_mark_price: Optional[float] = None
    last_mark_price: Optional[float] = None
    last_updated_at: Optional[datetime] = None
    asset_type: str = "EQUITY"
    underlying_symbol: Optional[str] = None
    underlying_entry_price: Optional[float] = None
    entry_quote: Optional[OptionQuoteSnapshot] = None
    max_unrealized_profit: Optional[float] = None
    max_unrealized_loss: Optional[float] = None
    max_unrealized_profit_pct: Optional[float] = None
    max_unrealized_loss_pct: Optional[float] = None
    gex_trigger_level: Optional[float] = None
    entry_iv: Optional[float] = None
    force_review_after: Optional[datetime] = None


@dataclass(frozen=True)
class PositionSnapshot:
    """Live mark-to-market view of an open position."""

    position: Position
    mark_price: float
    unrealized_pnl: float
    unrealized_pnl_pct: float


@dataclass(frozen=True)
class ExitNotification:
    """Signal that a monitored exit level has been breached."""

    symbol: str
    reason: ExitReason
    mark_price: float
    position: Position
    triggered_at: datetime

    def to_dict(self) -> dict[str, object]:
        """Serialize the notification for logging or downstream consumers."""
        return {
            "symbol": self.symbol,
            "reason": self.reason.value,
            "mark_price": self.mark_price,
            "quantity": self.position.quantity,
            "average_entry_price": self.position.average_entry_price,
            "triggered_at": self.triggered_at.isoformat(),
        }


class PositionTracker:
    """Tracks open positions, PnL, and stop/target breaches."""

    def __init__(
        self,
        *,
        exit_handlers: Optional[list[ExitNotificationHandler]] = None,
    ) -> None:
        """Initialize the position tracker.

        Args:
            exit_handlers: Optional callbacks invoked when an exit level is hit.
        """
        self._positions: dict[str, Position] = {}
        self._exit_handlers = list(exit_handlers or [])
        self._lock = threading.Lock()

    def subscribe_exits(self, handler: ExitNotificationHandler) -> None:
        """Register a callback for exit notifications."""
        self._exit_handlers.append(handler)

    def open_position(
        self,
        symbol: str,
        quantity: float,
        entry_price: float,
        *,
        opened_at: Optional[datetime] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_distance: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        asset_type: str = "EQUITY",
        underlying_symbol: Optional[str] = None,
        underlying_entry_price: Optional[float] = None,
        entry_quote: Optional[OptionQuoteSnapshot] = None,
        gex_trigger_level: Optional[float] = None,
        entry_iv: Optional[float] = None,
        force_review_after: Optional[datetime] = None,
    ) -> Position:
        """Open or replace a position for a symbol.

        Args:
            symbol: Ticker symbol.
            quantity: Signed quantity. Positive for long, negative for short.
            entry_price: Average entry price for the position.
            opened_at: Optional position open timestamp.
            stop_loss: Optional fixed stop-loss price.
            take_profit: Optional fixed take-profit price.
            trailing_stop_distance: Optional trailing stop distance in price units.
            trailing_stop_pct: Optional percentage (0-1) trailing stop measured
                from the peak mark observed after entry. Used for option marks.

        Returns:
            The created position.

        Raises:
            ValueError: If quantity is zero or risk settings are invalid.
        """
        symbol = symbol.upper()
        if quantity == 0:
            raise ValueError("quantity cannot be zero")
        if entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if trailing_stop_pct is not None and not 0.0 < trailing_stop_pct < 1.0:
            raise ValueError("trailing_stop_pct must be between 0 and 1 (exclusive)")

        self._validate_risk_levels(
            quantity=quantity,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_distance=trailing_stop_distance,
        )

        trailing_stop_price = self._initial_trailing_stop(
            quantity=quantity,
            entry_price=entry_price,
            trailing_stop_distance=trailing_stop_distance,
        )

        position = Position(
            symbol=symbol,
            quantity=quantity,
            average_entry_price=entry_price,
            opened_at=opened_at or datetime.now(timezone.utc),
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_distance=trailing_stop_distance,
            trailing_stop_price=trailing_stop_price,
            trailing_stop_pct=trailing_stop_pct,
            max_mark_price=entry_price,
            last_mark_price=entry_price,
            last_updated_at=opened_at or datetime.now(timezone.utc),
            asset_type=asset_type.upper(),
            underlying_symbol=(underlying_symbol or symbol).upper(),
            underlying_entry_price=underlying_entry_price,
            entry_quote=entry_quote,
            gex_trigger_level=gex_trigger_level,
            entry_iv=entry_iv,
            force_review_after=force_review_after,
        )

        with self._lock:
            self._positions[symbol] = position
        return position

    def on_fill(self, fill: FillEvent) -> Optional[Position]:
        """Update portfolio state from an order fill event.

        Args:
            fill: Execution details from the order manager.

        Returns:
            The updated position, or None if the position was closed.
        """
        symbol = fill.symbol.upper()
        signed_quantity = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity

        with self._lock:
            current = self._positions.get(symbol)

        if current is None:
            return self.open_position(
                symbol=symbol,
                quantity=signed_quantity,
                entry_price=fill.price,
                opened_at=fill.timestamp,
                asset_type=fill.asset_type,
                underlying_symbol=fill.underlying_symbol,
            )

        new_quantity = current.quantity + signed_quantity
        if new_quantity == 0:
            with self._lock:
                self._positions.pop(symbol, None)
            return None

        if (current.quantity > 0 and new_quantity > 0) or (current.quantity < 0 and new_quantity < 0):
            total_cost = (
                abs(current.quantity) * current.average_entry_price
                + abs(signed_quantity) * fill.price
            )
            average_entry_price = total_cost / abs(new_quantity)
        else:
            average_entry_price = fill.price

        updated = Position(
            symbol=symbol,
            quantity=new_quantity,
            average_entry_price=average_entry_price,
            opened_at=current.opened_at,
            stop_loss=current.stop_loss,
            take_profit=current.take_profit,
            trailing_stop_distance=current.trailing_stop_distance,
            trailing_stop_price=self._initial_trailing_stop(
                quantity=new_quantity,
                entry_price=average_entry_price,
                trailing_stop_distance=current.trailing_stop_distance,
            ),
            trailing_stop_pct=current.trailing_stop_pct,
            max_mark_price=fill.price,
            last_mark_price=fill.price,
            last_updated_at=fill.timestamp,
            asset_type=current.asset_type,
            underlying_symbol=current.underlying_symbol,
            underlying_entry_price=current.underlying_entry_price,
            entry_quote=current.entry_quote,
        )

        with self._lock:
            self._positions[symbol] = updated
        return updated

    def update_price(
        self,
        symbol: str,
        mark_price: float,
        *,
        timestamp: Optional[datetime] = None,
        evaluate_exits: bool = True,
    ) -> list[ExitNotification]:
        """Update mark price, refresh PnL state, and evaluate exit conditions.

        Args:
            symbol: Ticker symbol to update.
            mark_price: Latest market price used for PnL and stop checks.
            timestamp: Optional timestamp associated with the price update.
            evaluate_exits: When False, only refresh mark price; no stop/target exits.

        Returns:
            Exit notifications triggered by breached stop or target levels.
        """
        symbol = symbol.upper()
        timestamp = timestamp or datetime.now(timezone.utc)

        with self._lock:
            position = self._positions.get(symbol)
            if position is None:
                return []

            if evaluate_exits:
                trailing_stop_price = self._update_trailing_stop(position, mark_price)
            else:
                trailing_stop_price = position.trailing_stop_price
            updated = Position(
                symbol=position.symbol,
                quantity=position.quantity,
                average_entry_price=position.average_entry_price,
                opened_at=position.opened_at,
                stop_loss=position.stop_loss,
                take_profit=position.take_profit,
                trailing_stop_distance=position.trailing_stop_distance,
                trailing_stop_price=trailing_stop_price,
                trailing_stop_pct=position.trailing_stop_pct,
                max_mark_price=position.max_mark_price,
                last_mark_price=mark_price,
                last_updated_at=timestamp,
                asset_type=position.asset_type,
                underlying_symbol=position.underlying_symbol,
                underlying_entry_price=position.underlying_entry_price,
                entry_quote=position.entry_quote,
                max_unrealized_profit=position.max_unrealized_profit,
                max_unrealized_loss=position.max_unrealized_loss,
                max_unrealized_profit_pct=position.max_unrealized_profit_pct,
                max_unrealized_loss_pct=position.max_unrealized_loss_pct,
            )
            self._positions[symbol] = updated

        if not evaluate_exits:
            return []

        notifications = self._evaluate_exit_conditions(updated, mark_price, timestamp)
        for notification in notifications:
            self._publish_exit(notification)
        return notifications

    def record_mark(
        self,
        symbol: str,
        mark_price: float,
        *,
        unrealized_pnl: float,
        timestamp: Optional[datetime] = None,
    ) -> Optional[Position]:
        """Update the latest mark and running max unrealized profit/loss.

        Args:
            symbol: Instrument symbol (OCC option symbol for options).
            mark_price: Latest observed mark price.
            unrealized_pnl: Unrealized P&L at this mark (net of entry costs).
            timestamp: Optional time of the mark update.

        Returns:
            The updated position, or None when no position is open.
        """
        symbol = symbol.upper()
        timestamp = timestamp or datetime.now(timezone.utc)
        with self._lock:
            position = self._positions.get(symbol)
            if position is None:
                return None

            position.last_mark_price = mark_price
            position.last_updated_at = timestamp
            if (
                position.max_unrealized_profit is None
                or unrealized_pnl > position.max_unrealized_profit
            ):
                position.max_unrealized_profit = unrealized_pnl
            if (
                position.max_unrealized_loss is None
                or unrealized_pnl < position.max_unrealized_loss
            ):
                position.max_unrealized_loss = unrealized_pnl
            return position

    def record_option_mark(
        self,
        symbol: str,
        mark_price: float,
        *,
        unrealized_pnl: float,
        unrealized_pnl_pct: Optional[float] = None,
        timestamp: Optional[datetime] = None,
    ) -> Optional[ExitNotification]:
        """Record a streamed/polled option mark and check the peak-mark trailing stop.

        Updates the latest mark, running max unrealized profit/loss (in both
        dollar and percentage terms), and the peak mark observed since entry.
        When ``trailing_stop_pct`` is set, returns a ``TRAILING_STOP``
        notification once the mark falls that far below the peak.

        Args:
            symbol: OCC option symbol.
            mark_price: Latest observed option mark price.
            unrealized_pnl: Unrealized P&L at this mark (net of entry costs).
            unrealized_pnl_pct: Unrealized P&L as a fraction of cost basis
                (e.g. ``0.2`` for +20%). Recorded alongside the dollar peak.
            timestamp: Optional time of the mark update.

        Returns:
            A trailing-stop exit notification when the threshold is breached,
            otherwise None. Registered exit handlers are NOT invoked; the caller
            is responsible for routing the exit (paper vs live).
        """
        symbol = symbol.upper()
        timestamp = timestamp or datetime.now(timezone.utc)
        if mark_price <= 0:
            return None

        with self._lock:
            position = self._positions.get(symbol)
            if position is None:
                return None

            position.last_mark_price = mark_price
            position.last_updated_at = timestamp
            if (
                position.max_unrealized_profit is None
                or unrealized_pnl > position.max_unrealized_profit
            ):
                position.max_unrealized_profit = unrealized_pnl
                position.max_unrealized_profit_pct = unrealized_pnl_pct
            if (
                position.max_unrealized_loss is None
                or unrealized_pnl < position.max_unrealized_loss
            ):
                position.max_unrealized_loss = unrealized_pnl
                position.max_unrealized_loss_pct = unrealized_pnl_pct

            if position.max_mark_price is None or mark_price > position.max_mark_price:
                position.max_mark_price = mark_price

            pct = position.trailing_stop_pct
            if pct is None or position.max_mark_price is None:
                return None

            threshold = position.max_mark_price * (1.0 - pct)
            if mark_price > threshold:
                return None

            return ExitNotification(
                symbol=position.symbol,
                reason=ExitReason.TRAILING_STOP,
                mark_price=mark_price,
                position=position,
                triggered_at=timestamp,
            )

    def close_position(self, symbol: str) -> Optional[Position]:
        """Remove a tracked position without placing a broker order.

        Args:
            symbol: Ticker symbol to close locally.

        Returns:
            The removed position, if one existed.
        """
        with self._lock:
            return self._positions.pop(symbol.upper(), None)

    def get_position(self, symbol: str) -> Optional[Position]:
        """Return the current position for a symbol."""
        with self._lock:
            return self._positions.get(symbol.upper())

    def get_position_for_underlying(self, underlying_symbol: str) -> Optional[Position]:
        """Return the open position tied to an underlying symbol."""
        underlying = underlying_symbol.upper()
        with self._lock:
            for position in self._positions.values():
                if position.underlying_symbol == underlying:
                    return position
                if position.asset_type == "EQUITY" and position.symbol == underlying:
                    return position
        return None

    def snapshot(self, symbol: str, mark_price: float) -> Optional[PositionSnapshot]:
        """Return live unrealized PnL for an open position.

        Args:
            symbol: Ticker symbol.
            mark_price: Current market price.

        Returns:
            A position snapshot, or None if no position is open.
        """
        position = self.get_position(symbol)
        if position is None:
            return None

        pnl = self._calculate_unrealized_pnl(position, mark_price)
        pnl_pct = pnl / (abs(position.quantity) * position.average_entry_price)
        return PositionSnapshot(
            position=position,
            mark_price=mark_price,
            unrealized_pnl=pnl,
            unrealized_pnl_pct=pnl_pct,
        )

    def list_positions(self) -> list[Position]:
        """Return all currently tracked positions."""
        with self._lock:
            return list(self._positions.values())

    def sync_broker_position(
        self,
        symbol: str,
        quantity: float,
        entry_price: float,
        *,
        timestamp: Optional[datetime] = None,
        preserve_risk_levels: bool = True,
    ) -> Optional[Position]:
        """Align local state with a broker-reported position.

        Args:
            symbol: Ticker symbol.
            quantity: Signed broker quantity. Zero removes the local position.
            entry_price: Broker average price for the open position.
            timestamp: Optional sync timestamp.
            preserve_risk_levels: Keep existing stop/target settings when updating.

        Returns:
            The synced position, or None if the broker position is flat.
        """
        symbol = symbol.upper()
        timestamp = timestamp or datetime.now(timezone.utc)

        if quantity == 0:
            return self.close_position(symbol)

        existing = self.get_position(symbol)
        stop_loss = existing.stop_loss if preserve_risk_levels and existing else None
        take_profit = existing.take_profit if preserve_risk_levels and existing else None
        trailing_stop_distance = (
            existing.trailing_stop_distance if preserve_risk_levels and existing else None
        )
        opened_at = existing.opened_at if existing is not None else timestamp

        return self.open_position(
            symbol=symbol,
            quantity=quantity,
            entry_price=entry_price,
            opened_at=opened_at,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_distance=trailing_stop_distance,
        )

    def sync_broker_positions(
        self,
        positions: dict[str, tuple[float, float]],
        *,
        watchlist: Optional[set[str]] = None,
        preserve_risk_levels: bool = True,
        timestamp: Optional[datetime] = None,
    ) -> list[Position]:
        """Replace watchlist positions with broker-reported quantities.

        Args:
            positions: Mapping of symbol -> (signed quantity, average entry price).
            watchlist: Optional symbol set to sync. Broker symbols outside the
                watchlist are ignored; watchlist symbols missing from the broker
                payload are closed locally.
            preserve_risk_levels: Keep existing stop/target settings per symbol.
            timestamp: Optional sync timestamp.

        Returns:
            Positions remaining open after the sync.
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        watchlist_symbols = {symbol.upper() for symbol in watchlist} if watchlist else None
        synced: list[Position] = []

        for symbol, (quantity, entry_price) in positions.items():
            symbol = symbol.upper()
            if watchlist_symbols is not None and symbol not in watchlist_symbols:
                continue
            position = self.sync_broker_position(
                symbol,
                quantity,
                entry_price,
                timestamp=timestamp,
                preserve_risk_levels=preserve_risk_levels,
            )
            if position is not None:
                synced.append(position)

        if watchlist_symbols is not None:
            for symbol in watchlist_symbols:
                if symbol not in positions:
                    self.close_position(symbol)

        return synced

    def _validate_risk_levels(
        self,
        *,
        quantity: float,
        entry_price: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        trailing_stop_distance: Optional[float],
    ) -> None:
        """Validate stop and target settings against position direction."""
        if trailing_stop_distance is not None and trailing_stop_distance <= 0:
            raise ValueError("trailing_stop_distance must be positive")

        if quantity > 0:
            if stop_loss is not None and stop_loss >= entry_price:
                raise ValueError("long stop_loss must be below entry_price")
            if take_profit is not None and take_profit <= entry_price:
                raise ValueError("long take_profit must be above entry_price")
            return

        if stop_loss is not None and stop_loss <= entry_price:
            raise ValueError("short stop_loss must be above entry_price")
        if take_profit is not None and take_profit >= entry_price:
            raise ValueError("short take_profit must be below entry_price")

    def _initial_trailing_stop(
        self,
        *,
        quantity: float,
        entry_price: float,
        trailing_stop_distance: Optional[float],
    ) -> Optional[float]:
        """Compute the initial trailing stop for a newly opened position."""
        if trailing_stop_distance is None:
            return None
        if quantity > 0:
            return entry_price - trailing_stop_distance
        return entry_price + trailing_stop_distance

    def _update_trailing_stop(self, position: Position, mark_price: float) -> Optional[float]:
        """Ratchet the trailing stop as price moves in a favorable direction."""
        if position.trailing_stop_distance is None:
            return position.trailing_stop_price

        if position.quantity > 0:
            candidate = mark_price - position.trailing_stop_distance
            if position.trailing_stop_price is None:
                return candidate
            return max(position.trailing_stop_price, candidate)

        candidate = mark_price + position.trailing_stop_distance
        if position.trailing_stop_price is None:
            return candidate
        return min(position.trailing_stop_price, candidate)

    def _evaluate_exit_conditions(
        self,
        position: Position,
        mark_price: float,
        timestamp: datetime,
    ) -> list[ExitNotification]:
        """Return exit notifications for breached stop or target levels."""
        notifications: list[ExitNotification] = []

        if position.quantity > 0:
            if position.stop_loss is not None and mark_price <= position.stop_loss:
                notifications.append(
                    ExitNotification(
                        symbol=position.symbol,
                        reason=ExitReason.STOP_LOSS,
                        mark_price=mark_price,
                        position=position,
                        triggered_at=timestamp,
                    )
                )
            if position.take_profit is not None and mark_price >= position.take_profit:
                notifications.append(
                    ExitNotification(
                        symbol=position.symbol,
                        reason=ExitReason.TAKE_PROFIT,
                        mark_price=mark_price,
                        position=position,
                        triggered_at=timestamp,
                    )
                )
            if (
                position.trailing_stop_price is not None
                and mark_price <= position.trailing_stop_price
            ):
                notifications.append(
                    ExitNotification(
                        symbol=position.symbol,
                        reason=ExitReason.TRAILING_STOP,
                        mark_price=mark_price,
                        position=position,
                        triggered_at=timestamp,
                    )
                )
            return notifications

        if position.stop_loss is not None and mark_price >= position.stop_loss:
            notifications.append(
                ExitNotification(
                    symbol=position.symbol,
                    reason=ExitReason.STOP_LOSS,
                    mark_price=mark_price,
                    position=position,
                    triggered_at=timestamp,
                )
            )
        if position.take_profit is not None and mark_price <= position.take_profit:
            notifications.append(
                ExitNotification(
                    symbol=position.symbol,
                    reason=ExitReason.TAKE_PROFIT,
                    mark_price=mark_price,
                    position=position,
                    triggered_at=timestamp,
                )
            )
        if (
            position.trailing_stop_price is not None
            and mark_price >= position.trailing_stop_price
        ):
            notifications.append(
                ExitNotification(
                    symbol=position.symbol,
                    reason=ExitReason.TRAILING_STOP,
                    mark_price=mark_price,
                    position=position,
                    triggered_at=timestamp,
                )
            )
        return notifications

    def _calculate_unrealized_pnl(self, position: Position, mark_price: float) -> float:
        """Calculate signed unrealized PnL for an open position."""
        return (mark_price - position.average_entry_price) * position.quantity

    def _publish_exit(self, notification: ExitNotification) -> None:
        """Deliver an exit notification to registered handlers."""
        logger.warning(
            "Exit triggered for %s reason=%s mark=%s",
            notification.symbol,
            notification.reason.value,
            notification.mark_price,
        )
        for handler in self._exit_handlers:
            handler(notification)


if __name__ == "__main__":
    from order_manager import (
        InMemoryBrokerGateway,
        Order,
        OrderManager,
        OrderSide,
        OrderStatus,
        TradingSignal,
    )

    tracker = PositionTracker()

    def on_exit(notification: ExitNotification) -> None:
        print(notification.to_dict())

    tracker.subscribe_exits(on_exit)

    position = tracker.open_position(
        symbol="AAPL",
        quantity=10,
        entry_price=185.0,
        stop_loss=184.0,
        take_profit=187.0,
        trailing_stop_distance=1.0,
    )
    print(f"Opened position: {position.symbol} qty={position.quantity}")

    snapshot = tracker.snapshot("AAPL", mark_price=185.5)
    assert snapshot is not None
    print(f"Unrealized PnL: {snapshot.unrealized_pnl:.2f}")

    tracker.update_price("AAPL", 186.2)
    tracker.update_price("AAPL", 183.9)

    # Example wiring from order fills to position state.
    broker = InMemoryBrokerGateway(fill_price=185.25)

    def on_order_update(order: Order) -> None:
        fill = order_manager.to_fill_event(order)
        if fill is not None:
            tracker.on_fill(fill)

    order_manager = OrderManager(broker, on_update=on_order_update)
    submitted = order_manager.submit_signal(
        TradingSignal(symbol="MSFT", side=OrderSide.BUY, quantity=5)
    )
    order_manager.refresh_order(submitted.id)
    print(f"Tracked positions: {[p.symbol for p in tracker.list_positions()]}")
