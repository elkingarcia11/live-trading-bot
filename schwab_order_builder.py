"""Schwab order payload builder.

Responsibility: Translate internal order objects into Schwab order JSON.

Builds request bodies for Schwab Trader API order endpoints. Does not submit
orders or track execution state.
"""

from __future__ import annotations

import math
from typing import Any

from order_manager import Order, OrderSide, OrderType


def build_schwab_order_payload(order: Order) -> dict[str, Any]:
    """Build a Schwab order payload from an internal order."""
    signal = order.signal
    instruction = "BUY" if signal.side == OrderSide.BUY else "SELL"
    position_effect = "OPENING" if signal.side == OrderSide.BUY else "CLOSING"
    asset_type = signal.asset_type.upper()
    if asset_type == "OPTION":
        leg_type = "OPTION"
        quantity = _normalize_option_quantity(signal.quantity)
        instrument = {
            "symbol": signal.symbol.upper(),
            "assetType": "OPTION",
        }
    else:
        leg_type = "EQUITY"
        quantity = _normalize_equity_quantity(signal.quantity)
        instrument = {
            "symbol": signal.symbol.upper(),
            "assetType": "EQUITY",
        }

    payload: dict[str, Any] = {
        "session": "NORMAL",
        "duration": "DAY",
        "orderType": _map_order_type(signal.order_type),
        "complexOrderStrategyType": "NONE",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [
            {
                "orderLegType": leg_type,
                "instruction": instruction,
                "positionEffect": position_effect,
                "quantity": quantity,
                "instrument": instrument,
            }
        ],
    }

    if signal.order_type == OrderType.LIMIT:
        if signal.limit_price is None:
            raise ValueError("limit orders require limit_price")
        payload["price"] = float(signal.limit_price)

    return payload


def _map_order_type(order_type: OrderType) -> str:
    if order_type == OrderType.MARKET:
        return "MARKET"
    if order_type == OrderType.LIMIT:
        return "LIMIT"
    raise ValueError(f"Unsupported order type: {order_type.value}")


def _normalize_equity_quantity(quantity: float) -> int:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    whole_shares = int(math.floor(quantity))
    if whole_shares <= 0:
        raise ValueError("Schwab equity orders require at least one whole share")
    if not math.isclose(quantity, whole_shares, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("Schwab equity orders require whole-share quantities")
    return whole_shares


def _normalize_option_quantity(quantity: float) -> int:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    contracts = int(math.floor(quantity))
    if contracts <= 0:
        raise ValueError("Schwab option orders require at least one contract")
    if not math.isclose(quantity, contracts, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("Schwab option orders require whole-contract quantities")
    return contracts
