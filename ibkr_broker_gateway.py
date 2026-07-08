"""IBKR broker gateway.

Responsibility: Live IBKR order execution transport for OrderManager.

Submits, polls, and cancels orders through the IBKR Web API. Does not evaluate
strategy signals or maintain portfolio state.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from ibkr_order_builder import build_ibkr_order_payload
from ibkr_trader_client import IbkrTraderClient, IbkrTraderError
from order_manager import ExecutionReport, Order, OrderStatus
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)


class IbkrBrokerError(Exception):
    """Raised when IBKR broker operations fail."""


class IbkrBrokerGateway:
    """BrokerGateway implementation backed by the IBKR Web API."""

    def __init__(
        self,
        trader_client: IbkrTraderClient,
        *,
        account_id: Optional[str] = None,
        account_number: Optional[str] = None,
        listing_exchange: str = "SMART",
        manual_indicator: bool = False,
        ext_operator: str = "live-trading-bot",
        preview: bool = False,
    ) -> None:
        self._trader_client = trader_client
        self._account_id = account_id
        self._account_number = account_number
        self._listing_exchange = listing_exchange
        self._manual_indicator = manual_indicator
        self._ext_operator = ext_operator
        self._preview = preview

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrBrokerGateway:
        """Build an IBKR broker gateway from config.json."""
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        trader_client = IbkrTraderClient.from_env(load_dotenv=False)
        broker = app.broker
        ibkr = app.ibkr
        return cls(
            trader_client,
            account_id=broker.account_number or None,
            account_number=broker.account_number or None,
            listing_exchange=ibkr.listing_exchange,
            manual_indicator=ibkr.manual_indicator,
            ext_operator=ibkr.ext_operator,
            preview=broker.preview_orders,
        )

    def submit_order(self, order: Order) -> str:
        """Place an order and return the IBKR order id."""
        account_id = self._resolve_account_id()
        sec_type = "OPT" if order.signal.asset_type.upper() == "OPTION" else "STK"
        contract = self._trader_client.search_contract(
            order.signal.underlying_symbol or order.signal.symbol,
            sec_type=sec_type,
        )
        payload = build_ibkr_order_payload(
            order,
            conid=contract.conid,
            account_id=account_id,
            listing_exchange=self._listing_exchange,
            manual_indicator=self._manual_indicator,
            ext_operator=self._ext_operator,
        )

        if self._preview:
            logger.info(
                "IBKR preview order for %s (%s x%s, conid=%s)",
                order.signal.symbol,
                payload["orders"][0]["side"],
                payload["orders"][0]["quantity"],
                contract.conid,
            )
            return f"preview-{order.id[:8]}"

        self._trader_client.ensure_session()
        response = self._trader_client.place_order(account_id, payload)
        order_id = _extract_order_id(response)
        if not order_id:
            raise IbkrBrokerError("IBKR place order succeeded but no order id was returned")
        logger.info(
            "Placed IBKR %s order for %s shares of %s (order_id=%s)",
            order.signal.side.value,
            payload["orders"][0]["quantity"],
            order.signal.symbol,
            order_id,
        )
        return order_id

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel a working IBKR order."""
        account_id = self._resolve_account_id()
        self._trader_client.ensure_session()
        self._trader_client.cancel_order(account_id, broker_order_id)
        logger.info("Cancelled IBKR order %s", broker_order_id)

    def get_order_status(self, broker_order_id: str) -> ExecutionReport:
        """Fetch and normalize the latest IBKR order status."""
        if broker_order_id.startswith("preview-"):
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.REJECTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
                rejection_reason="preview order was not submitted",
            )

        self._trader_client.ensure_session()
        payload = self._trader_client.get_order(broker_order_id)
        if payload is None:
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.SUBMITTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
            )
        return _execution_report_from_ibkr_order(broker_order_id, payload)

    def _resolve_account_id(self) -> str:
        return self._trader_client.resolve_account_id(
            account_id=self._account_id,
            account_number=self._account_number,
        )


def _execution_report_from_ibkr_order(
    broker_order_id: str,
    payload: dict[str, Any],
) -> ExecutionReport:
    status = _map_ibkr_status(str(payload.get("status", "") or payload.get("order_ccp_status", "")))
    filled_quantity = float(payload.get("filledQuantity", 0.0) or 0.0)
    remaining_quantity = float(payload.get("remainingQuantity", 0.0) or 0.0)
    total_quantity = filled_quantity + remaining_quantity
    average_fill_price = _optional_float(payload.get("avgPrice"))

    if (
        status not in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
        and filled_quantity > 0
        and total_quantity > 0
        and filled_quantity < total_quantity
    ):
        status = OrderStatus.PARTIALLY_FILLED
    elif filled_quantity > 0 and total_quantity > 0 and filled_quantity >= total_quantity:
        status = OrderStatus.FILLED

    updated_at = _parse_timestamp(
        payload.get("lastExecutionTime_r") or payload.get("lastExecutionTime")
    )

    return ExecutionReport(
        broker_order_id=broker_order_id,
        status=status,
        filled_quantity=filled_quantity,
        average_fill_price=average_fill_price,
        updated_at=updated_at,
        rejection_reason=str(payload.get("status", "") or "") or None,
    )


def _map_ibkr_status(raw_status: str) -> OrderStatus:
    normalized = raw_status.strip().lower()
    mapping = {
        "filled": OrderStatus.FILLED,
        "cancelled": OrderStatus.CANCELLED,
        "canceled": OrderStatus.CANCELLED,
        "inactive": OrderStatus.CANCELLED,
        "rejected": OrderStatus.REJECTED,
        "submitted": OrderStatus.SUBMITTED,
        "presubmitted": OrderStatus.SUBMITTED,
        "pendingsubmit": OrderStatus.SUBMITTED,
        "pendingcancel": OrderStatus.SUBMITTED,
        "apipending": OrderStatus.SUBMITTED,
        "apicancelled": OrderStatus.CANCELLED,
        "apicanceled": OrderStatus.CANCELLED,
    }
    return mapping.get(normalized, OrderStatus.SUBMITTED)


def _extract_order_id(payload: dict[str, Any]) -> Optional[str]:
    for key in ("order_id", "orderId", "id"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _optional_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)


def _parse_timestamp(value: object) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1_000_000_000_000:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value).strip()
    if text.isdigit():
        seconds = float(text)
        if seconds > 1_000_000_000_000:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    return datetime.now(timezone.utc)
