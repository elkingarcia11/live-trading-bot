"""IBKR TWS order payload builder.

Responsibility: Translate internal order objects into ibapi Order instances.
"""

from __future__ import annotations

import math

from ibapi.order import Order as IbOrder

from order_manager import Order, OrderSide, OrderType


def build_ibkr_tws_order(order: Order) -> IbOrder:
    """Build an ibapi Order from an internal order."""
    signal = order.signal
    ib_order = IbOrder()
    ib_order.action = "BUY" if signal.side == OrderSide.BUY else "SELL"
    ib_order.orderType = _map_order_type(signal.order_type)
    ib_order.totalQuantity = _normalize_quantity(signal.quantity, signal.asset_type)
    ib_order.tif = "DAY"
    ib_order.transmit = True

    if signal.order_type == OrderType.LIMIT:
        if signal.limit_price is None:
            raise ValueError("limit orders require limit_price")
        ib_order.lmtPrice = float(signal.limit_price)

    return ib_order


def _map_order_type(order_type: OrderType) -> str:
    if order_type == OrderType.MARKET:
        return "MKT"
    if order_type == OrderType.LIMIT:
        return "LMT"
    raise ValueError(f"Unsupported order type: {order_type.value}")


def _normalize_quantity(quantity: float, asset_type: str) -> float:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if asset_type.upper() == "OPTION":
        contracts = int(math.floor(quantity))
        if contracts <= 0:
            raise ValueError("IBKR option orders require at least one contract")
        return float(contracts)
    whole_shares = int(math.floor(quantity))
    if whole_shares <= 0:
        raise ValueError("IBKR equity orders require at least one whole share")
    return float(whole_shares)
