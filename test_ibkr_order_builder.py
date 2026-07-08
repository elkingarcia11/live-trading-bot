"""Tests for IBKR order payload building and status mapping."""

from __future__ import annotations

from datetime import datetime, timezone

from ibkr_broker_gateway import _execution_report_from_ibkr_order, _map_ibkr_status
from ibkr_order_builder import build_ibkr_order_payload
from order_manager import Order, OrderSide, OrderStatus, OrderType, TradingSignal


def _sample_order(side: OrderSide = OrderSide.BUY, quantity: float = 10.0) -> Order:
    return Order(
        id="test-order",
        signal=TradingSignal(
            symbol="SPY",
            side=side,
            quantity=quantity,
            order_type=OrderType.MARKET,
        ),
        status=OrderStatus.PENDING,
    )


def test_build_market_buy_order_payload() -> None:
    payload = build_ibkr_order_payload(
        _sample_order(),
        conid=756733,
        account_id="DU1234567",
    )
    order = payload["orders"][0]
    assert order["orderType"] == "MKT"
    assert order["side"] == "BUY"
    assert order["quantity"] == 10
    assert order["conid"] == 756733
    assert order["acctId"] == "DU1234567"
    assert order["ticker"] == "SPY"


def test_build_limit_sell_order_payload() -> None:
    order = Order(
        id="limit-order",
        signal=TradingSignal(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=5,
            order_type=OrderType.LIMIT,
            limit_price=190.5,
        ),
        status=OrderStatus.PENDING,
    )
    payload = build_ibkr_order_payload(
        order,
        conid=265598,
        account_id="DU1234567",
    )
    body = payload["orders"][0]
    assert body["orderType"] == "LMT"
    assert body["side"] == "SELL"
    assert body["price"] == 190.5


def test_map_ibkr_status() -> None:
    assert _map_ibkr_status("Filled") == OrderStatus.FILLED
    assert _map_ibkr_status("Submitted") == OrderStatus.SUBMITTED
    assert _map_ibkr_status("Cancelled") == OrderStatus.CANCELLED


def test_execution_report_from_ibkr_order() -> None:
    report = _execution_report_from_ibkr_order(
        "12345",
        {
            "status": "Filled",
            "filledQuantity": 10.0,
            "remainingQuantity": 0.0,
            "avgPrice": "471.16",
            "lastExecutionTime_r": 1_700_000_000_000,
        },
    )
    assert report.status == OrderStatus.FILLED
    assert report.filled_quantity == 10.0
    assert report.average_fill_price == 471.16
    assert report.updated_at == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)


def test_trader_client_resolves_order_reply() -> None:
    from ibkr_trader_client import IbkrTraderClient

    client = IbkrTraderClient.__new__(IbkrTraderClient)
    client._reply_path_template = "iserver/reply/{reply_id}"

    calls: list[tuple[str, str]] = []

    def _request_json(
        path: str,
        *,
        method: str = "GET",
        params=None,
        json_body=None,
    ):
        calls.append((method, path))
        if method == "POST" and path.endswith("reply-1"):
            return [{"order_id": "999", "order_status": "Submitted"}]
        return None

    client._request_json = _request_json  # type: ignore[method-assign]
    result = client._resolve_order_response([{"id": "reply-1", "message": ["confirm?"]}])
    assert result["order_id"] == "999"
    assert calls == [("POST", "iserver/reply/reply-1")]
