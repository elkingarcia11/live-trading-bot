"""IBKR order payload builder.

Responsibility: Translate internal order objects into IBKR Web API order JSON.

Builds request bodies for POST /iserver/account/{accountId}/orders. Does not
resolve contract IDs or submit orders.
"""

from __future__ import annotations

import math
import uuid
from typing import Any, Optional

from order_manager import Order, OrderSide, OrderType


def build_ibkr_order_payload(
    order: Order,
    *,
    conid: int,
    account_id: str,
    listing_exchange: str = "SMART",
    manual_indicator: bool = False,
    ext_operator: str = "live-trading-bot",
    customer_order_id: Optional[str] = None,
) -> dict[str, Any]:
    """Build an IBKR order payload from an internal order."""
    signal = order.signal
    side = "BUY" if signal.side == OrderSide.BUY else "SELL"
    asset_type = signal.asset_type.upper()
    if asset_type == "OPTION":
        quantity = _normalize_option_quantity(signal.quantity)
        sec_type = f"{conid}@OPT"
    else:
        quantity = _normalize_equity_quantity(signal.quantity)
        sec_type = f"{conid}@STK"

    payload: dict[str, Any] = {
        "acctId": account_id,
        "conid": conid,
        "conidex": f"{conid}@{listing_exchange}",
        "manualIndicator": manual_indicator,
        "extOperator": ext_operator,
        "secType": sec_type,
        "cOID": customer_order_id or _default_customer_order_id(order.id),
        "orderType": _map_order_type(signal.order_type),
        "listingExchange": listing_exchange,
        "side": side,
        "ticker": (signal.underlying_symbol or signal.symbol).upper(),
        "tif": "DAY",
        "quantity": quantity,
    }

    if signal.order_type == OrderType.LIMIT:
        if signal.limit_price is None:
            raise ValueError("limit orders require limit_price")
        payload["price"] = float(signal.limit_price)

    return {"orders": [payload]}


def _map_order_type(order_type: OrderType) -> str:
    if order_type == OrderType.MARKET:
        return "MKT"
    if order_type == OrderType.LIMIT:
        return "LMT"
    raise ValueError(f"Unsupported order type: {order_type.value}")


def _default_customer_order_id(order_id: str) -> str:
    normalized = order_id.replace("-", "")[:24]
    if normalized:
        return normalized
    return uuid.uuid4().hex[:24]


def _normalize_equity_quantity(quantity: float) -> int:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    whole_shares = int(math.floor(quantity))
    if whole_shares <= 0:
        raise ValueError("IBKR equity orders require at least one whole share")
    if not math.isclose(quantity, whole_shares, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("IBKR equity orders require whole-share quantities")
    return whole_shares


def _normalize_option_quantity(quantity: float) -> int:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    contracts = int(math.floor(quantity))
    if contracts <= 0:
        raise ValueError("IBKR option orders require at least one contract")
    if not math.isclose(quantity, contracts, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("IBKR option orders require whole-contract quantities")
    return contracts
