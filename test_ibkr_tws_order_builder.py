"""Unit tests for IBKR TWS order builder."""

from __future__ import annotations

import pytest

from ibkr_tws_order_builder import build_ibkr_tws_order
from order_manager import Order, OrderSide, OrderStatus, OrderType, TradingSignal


def _signal(**kwargs) -> TradingSignal:
    defaults = {
        "symbol": "SPY",
        "side": OrderSide.BUY,
        "quantity": 10,
        "order_type": OrderType.MARKET,
        "asset_type": "EQUITY",
        "underlying_symbol": "SPY",
    }
    defaults.update(kwargs)
    return TradingSignal(**defaults)


def test_build_market_buy_order() -> None:
    order = Order(id="test-1", signal=_signal(), status=OrderStatus.PENDING)
    ib_order = build_ibkr_tws_order(order)
    assert ib_order.action == "BUY"
    assert ib_order.orderType == "MKT"
    assert ib_order.totalQuantity == 10.0
    assert ib_order.tif == "DAY"


def test_build_limit_sell_order() -> None:
    order = Order(
        id="test-2",
        signal=_signal(side=OrderSide.SELL, order_type=OrderType.LIMIT, limit_price=501.25),
        status=OrderStatus.PENDING,
    )
    ib_order = build_ibkr_tws_order(order)
    assert ib_order.action == "SELL"
    assert ib_order.orderType == "LMT"
    assert ib_order.lmtPrice == 501.25


def test_fractional_equity_quantity_floors_to_whole_shares() -> None:
    order = Order(id="test-3", signal=_signal(quantity=10.9), status=OrderStatus.PENDING)
    ib_order = build_ibkr_tws_order(order)
    assert ib_order.totalQuantity == 10.0


def test_limit_order_requires_price() -> None:
    order = Order(
        id="test-4",
        signal=_signal(order_type=OrderType.LIMIT, limit_price=None),
        status=OrderStatus.PENDING,
    )
    with pytest.raises(ValueError, match="limit_price"):
        build_ibkr_tws_order(order)
