"""Schwab broker gateway.

Responsibility: Live Schwab order execution transport for OrderManager.

Submits, polls, and cancels orders through the Schwab Trader API. Does not
evaluate strategy signals or maintain portfolio state.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import requests

from order_manager import ExecutionReport, Order, OrderStatus
from schwab_auth import SchwabAuthClient, SchwabAuthError, _load_dotenv
from schwab_order_builder import build_schwab_order_payload
from schwab_trader_client import (
    SchwabOrderPreviewResult,
    SchwabTraderClient,
    SchwabTraderError,
)

logger = logging.getLogger(__name__)


class SchwabBrokerError(Exception):
    """Raised when Schwab broker operations fail."""


class SchwabBrokerGateway:
    """BrokerGateway implementation backed by Schwab Trader API orders."""

    def __init__(
        self,
        trader_client: SchwabTraderClient,
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
        orders_path_template: str = "accounts/{account_hash}/orders",
        preview: bool = False,
    ) -> None:
        self._trader_client = trader_client
        self._account_hash = account_hash
        self._account_number = account_number
        self._orders_path_template = orders_path_template
        self._preview = preview

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> SchwabBrokerGateway:
        """Build a Schwab broker gateway from config.json."""
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        trader_client = SchwabTraderClient.from_env(load_dotenv=False)
        broker = app.broker
        return cls(
            trader_client,
            account_hash=broker.account_hash or None,
            account_number=broker.account_number or None,
            orders_path_template=app.schwab.orders_path,
            preview=broker.preview_orders,
        )

    def submit_order(self, order: Order) -> str:
        """Place an order and return the Schwab order id."""
        account_hash = self._resolve_account_hash()
        payload = build_schwab_order_payload(order)
        path = self._orders_path(account_hash)

        if self._preview:
            preview = self.preview_order(order)
            logger.info(
                "Schwab preview order succeeded for %s (commission=%s, order_value=%s)",
                order.signal.symbol,
                preview.projected_commission,
                preview.projected_order_value,
            )
            return f"preview-{order.id[:8]}"

        response = self._trader_client._request(
            "POST",
            path,
            json_body=payload,
            expect_json=False,
        )
        order_id = _extract_order_id(response)
        if not order_id:
            raise SchwabBrokerError(
                "Schwab place order succeeded but no order id was returned in Location"
            )
        logger.info(
            "Placed Schwab %s order for %s shares of %s (order_id=%s)",
            order.signal.side.value,
            payload["orderLegCollection"][0]["quantity"],
            order.signal.symbol,
            order_id,
        )
        return order_id

    def preview_order(self, order: Order) -> SchwabOrderPreviewResult:
        """Validate an order payload without placing it."""
        payload = build_schwab_order_payload(order)
        preview = self._trader_client.preview_order(
            payload,
            account_hash=self._account_hash,
            account_number=self._account_number,
        )
        if not preview.is_valid:
            messages = "; ".join(
                reject.message or reject.validation_rule_name
                for reject in preview.rejects
            )
            raise SchwabBrokerError(f"Schwab preview order rejected: {messages}")
        return preview

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel a working Schwab order."""
        self._trader_client.cancel_order(
            broker_order_id,
            account_hash=self._account_hash,
            account_number=self._account_number,
        )
        logger.info("Cancelled Schwab order %s", broker_order_id)

    def list_orders(
        self,
        *,
        from_entered_time: str,
        to_entered_time: str,
        max_results: Optional[int] = None,
        status: Optional[str] = None,
    ) -> list[ExecutionReport]:
        """Fetch and normalize orders for the configured account."""
        orders = self._trader_client.get_orders(
            from_entered_time=from_entered_time,
            to_entered_time=to_entered_time,
            account_hash=self._account_hash,
            account_number=self._account_number,
            max_results=max_results,
            status=status,
        )
        reports: list[ExecutionReport] = []
        for payload in orders:
            order_id = str(payload.get("orderId", "") or "")
            if not order_id:
                continue
            reports.append(_execution_report_from_schwab_order(order_id, payload))
        return reports

    def get_order_status(self, broker_order_id: str) -> ExecutionReport:
        """Fetch and normalize the latest Schwab order status."""
        if broker_order_id.startswith("preview-"):
            return ExecutionReport(
                broker_order_id=broker_order_id,
                status=OrderStatus.REJECTED,
                filled_quantity=0.0,
                updated_at=datetime.now(timezone.utc),
                rejection_reason="preview order was not submitted",
            )

        payload = self._trader_client.get_order(
            broker_order_id,
            account_hash=self._account_hash,
            account_number=self._account_number,
        )
        return _execution_report_from_schwab_order(broker_order_id, payload)

    def _resolve_account_hash(self) -> str:
        return self._trader_client.resolve_account_hash(
            account_hash=self._account_hash,
            account_number=self._account_number,
        )

    def _orders_path(self, account_hash: str) -> str:
        return self._orders_path_template.format(account_hash=account_hash)


def _execution_report_from_schwab_order(
    broker_order_id: str,
    payload: dict[str, Any],
) -> ExecutionReport:
    status = _map_schwab_status(str(payload.get("status", "")))
    quantity = float(payload.get("quantity", 0.0) or 0.0)
    filled_quantity = float(payload.get("filledQuantity", 0.0) or 0.0)
    average_fill_price = _extract_average_fill_price(payload)

    if (
        status not in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
        and filled_quantity > 0
        and quantity > 0
        and filled_quantity < quantity
    ):
        status = OrderStatus.PARTIALLY_FILLED
    elif filled_quantity > 0 and quantity > 0 and filled_quantity >= quantity:
        status = OrderStatus.FILLED

    updated_at = _parse_timestamp(payload.get("closeTime") or payload.get("enteredTime"))

    return ExecutionReport(
        broker_order_id=broker_order_id,
        status=status,
        filled_quantity=filled_quantity,
        average_fill_price=average_fill_price,
        updated_at=updated_at,
        rejection_reason=str(payload.get("statusDescription", "") or "") or None,
    )


def _map_schwab_status(raw_status: str) -> OrderStatus:
    normalized = raw_status.upper()
    mapping = {
        "FILLED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELLED,
        "CANCELLED": OrderStatus.CANCELLED,
        "REJECTED": OrderStatus.REJECTED,
        "EXPIRED": OrderStatus.CANCELLED,
        "WORKING": OrderStatus.SUBMITTED,
        "ACCEPTED": OrderStatus.SUBMITTED,
        "PENDING_ACTIVATION": OrderStatus.SUBMITTED,
        "QUEUED": OrderStatus.SUBMITTED,
        "AWAITING_PARENT_ORDER": OrderStatus.SUBMITTED,
        "AWAITING_CONDITION": OrderStatus.SUBMITTED,
        "AWAITING_MANUAL_REVIEW": OrderStatus.SUBMITTED,
    }
    return mapping.get(normalized, OrderStatus.SUBMITTED)


def _extract_average_fill_price(payload: dict[str, Any]) -> Optional[float]:
    activities = payload.get("orderActivityCollection", [])
    if not isinstance(activities, list):
        return None

    total_value = 0.0
    total_quantity = 0.0
    for activity in activities:
        if not isinstance(activity, dict):
            continue
        legs = activity.get("executionLegs", [])
        if not isinstance(legs, list):
            continue
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            quantity = float(leg.get("quantity", 0.0) or 0.0)
            price = float(leg.get("price", 0.0) or 0.0)
            if quantity <= 0 or price <= 0:
                continue
            total_value += quantity * price
            total_quantity += quantity

    if total_quantity <= 0:
        return None
    return total_value / total_quantity


def _extract_order_id(response: requests.Response) -> Optional[str]:
    location = response.headers.get("Location") or response.headers.get("location")
    if not location:
        return None
    order_id = location.rstrip("/").rsplit("/", 1)[-1]
    return order_id or None


def _parse_timestamp(value: object) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def build_broker_gateway(
    *,
    use_in_memory: bool,
    fill_price: float = 100.0,
    ibkr_runtime: Optional["IbkrTwsRuntime"] = None,
):
    """Return the configured broker gateway for the workflow."""
    from order_manager import InMemoryBrokerGateway

    if use_in_memory:
        return InMemoryBrokerGateway(fill_price=fill_price)

    from config import get_config

    provider = get_config().broker.provider
    if provider == "ibkr":
        from ibkr_tws_broker_gateway import IbkrTwsBrokerGateway

        if ibkr_runtime is not None:
            return IbkrTwsBrokerGateway.from_runtime(ibkr_runtime)
        return IbkrTwsBrokerGateway.from_env()
    return SchwabBrokerGateway.from_env()
