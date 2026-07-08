"""IBKR TWS broker gateway.

Responsibility: Live IBKR order execution transport for OrderManager via ibapi.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime
from ibkr_tws_contracts import equity_contract
from ibkr_tws_order_builder import build_ibkr_tws_order
from order_manager import ExecutionReport, Order, OrderStatus
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)


class IbkrTwsBrokerError(Exception):
    """Raised when IBKR TWS broker operations fail."""


class IbkrTwsBrokerGateway:
    """BrokerGateway implementation backed by the TWS / IB Gateway socket API."""

    def __init__(
        self,
        runtime: IbkrTwsRuntime,
        *,
        exchange: str = "SMART",
        currency: str = "USD",
        preview: bool = False,
        status_poll_seconds: float = 0.5,
        status_timeout_seconds: float = 10.0,
    ) -> None:
        self._runtime = runtime
        self._exchange = exchange
        self._currency = currency
        self._preview = preview
        self._status_poll_seconds = status_poll_seconds
        self._status_timeout_seconds = status_timeout_seconds

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrTwsBrokerGateway:
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        runtime = IbkrTwsRuntime.from_config()
        ibkr = app.ibkr
        runtime.connect_session(
            host=ibkr.host,
            port=ibkr.port,
            client_id=ibkr.client_id,
            timeout_seconds=ibkr.connect_timeout_seconds,
        )
        runtime.set_market_data_type(ibkr.market_data_type)
        return cls(
            runtime,
            exchange=ibkr.exchange,
            currency=ibkr.currency,
            preview=app.broker.preview_orders,
        )

    @classmethod
    def from_runtime(cls, runtime: IbkrTwsRuntime) -> IbkrTwsBrokerGateway:
        from config import get_config

        app = get_config()
        ibkr = app.ibkr
        return cls(
            runtime,
            exchange=ibkr.exchange,
            currency=ibkr.currency,
            preview=app.broker.preview_orders,
        )

    def submit_order(self, order: Order) -> str:
        contract = equity_contract(
            order.signal.underlying_symbol or order.signal.symbol,
            exchange=self._exchange,
            currency=self._currency,
        )
        ib_order = build_ibkr_tws_order(order)
        if self._preview:
            logger.info(
                "IBKR TWS preview order for %s %s x%s",
                ib_order.action,
                contract.symbol,
                ib_order.totalQuantity,
            )
            return f"preview-{order.id[:8]}"

        try:
            order_id = self._runtime.place_contract_order(contract, ib_order)
        except IbkrTwsError as exc:
            raise IbkrTwsBrokerError(str(exc)) from exc
        logger.info(
            "Placed IBKR TWS %s order for %s shares of %s (order_id=%s)",
            ib_order.action,
            ib_order.totalQuantity,
            contract.symbol,
            order_id,
        )
        return str(order_id)

    def cancel_order(self, broker_order_id: str) -> None:
        if broker_order_id.startswith("preview-"):
            return
        try:
            self._runtime.cancel_broker_order(int(broker_order_id))
        except IbkrTwsError as exc:
            raise IbkrTwsBrokerError(str(exc)) from exc
        logger.info("Cancelled IBKR TWS order %s", broker_order_id)

    def get_order_status(self, broker_order_id: str) -> ExecutionReport:
        if broker_order_id.startswith("preview-"):
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.REJECTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
                rejection_reason="preview order was not submitted",
            )

        deadline = time.monotonic() + self._status_timeout_seconds
        order_id = int(broker_order_id)
        state = self._runtime.get_order_state(order_id)
        while state is None and time.monotonic() < deadline:
            time.sleep(self._status_poll_seconds)
            state = self._runtime.get_order_state(order_id)

        if state is None:
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.SUBMITTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
            )
        return _execution_report_from_tws_state(broker_order_id, state)


def _execution_report_from_tws_state(
    broker_order_id: str,
    state,
) -> ExecutionReport:
    status = _map_tws_status(state.status)
    filled_quantity = float(state.filled)
    remaining = float(state.remaining)
    total = filled_quantity + remaining
    if (
        status not in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
        and filled_quantity > 0
        and total > 0
        and filled_quantity < total
    ):
        status = OrderStatus.PARTIALLY_FILLED
    elif filled_quantity > 0 and total > 0 and filled_quantity >= total:
        status = OrderStatus.FILLED

    average_fill_price = (
        float(state.average_fill_price) if state.average_fill_price > 0 else None
    )
    return ExecutionReport(
        broker_order_id=broker_order_id,
        status=status,
        filled_quantity=filled_quantity,
        average_fill_price=average_fill_price,
        updated_at=state.updated_at,
        rejection_reason=state.status if status == OrderStatus.REJECTED else None,
    )


def _map_tws_status(raw_status: str) -> OrderStatus:
    normalized = raw_status.strip().lower()
    mapping = {
        "filled": OrderStatus.FILLED,
        "cancelled": OrderStatus.CANCELLED,
        "apicancelled": OrderStatus.CANCELLED,
        "inactive": OrderStatus.CANCELLED,
        "submitted": OrderStatus.SUBMITTED,
        "presubmitted": OrderStatus.SUBMITTED,
        "pendingsubmit": OrderStatus.SUBMITTED,
        "pendingcancel": OrderStatus.SUBMITTED,
    }
    return mapping.get(normalized, OrderStatus.SUBMITTED)
