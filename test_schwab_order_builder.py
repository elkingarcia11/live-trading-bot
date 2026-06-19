"""Tests for Schwab order payload building and status mapping."""

from __future__ import annotations

from datetime import datetime, timezone

from order_manager import Order, OrderSide, OrderStatus, OrderType, TradingSignal
from schwab_broker_gateway import _execution_report_from_schwab_order, _map_schwab_status
from schwab_order_builder import build_schwab_order_payload


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
    payload = build_schwab_order_payload(_sample_order())
    assert payload["orderType"] == "MARKET"
    assert payload["orderLegCollection"][0]["instruction"] == "BUY"
    assert payload["orderLegCollection"][0]["quantity"] == 10
    assert payload["orderLegCollection"][0]["instrument"]["symbol"] == "SPY"


def test_build_market_sell_order_payload() -> None:
    payload = build_schwab_order_payload(_sample_order(side=OrderSide.SELL))
    assert payload["orderLegCollection"][0]["instruction"] == "SELL"
    assert payload["orderLegCollection"][0]["positionEffect"] == "CLOSING"


def test_build_market_buy_option_order_payload() -> None:
    order = Order(
        id="test-option-order",
        signal=TradingSignal(
            symbol="SPY   260617C00550000",
            side=OrderSide.BUY,
            quantity=2,
            order_type=OrderType.MARKET,
            asset_type="OPTION",
            underlying_symbol="SPY",
            mark_price=4.5,
        ),
        status=OrderStatus.PENDING,
    )
    payload = build_schwab_order_payload(order)
    leg = payload["orderLegCollection"][0]
    assert leg["orderLegType"] == "OPTION"
    assert leg["instruction"] == "BUY"
    assert leg["quantity"] == 2
    assert leg["instrument"]["assetType"] == "OPTION"
    assert leg["instrument"]["symbol"] == "SPY   260617C00550000"


def test_map_schwab_status() -> None:
    assert _map_schwab_status("FILLED") == OrderStatus.FILLED
    assert _map_schwab_status("WORKING") == OrderStatus.SUBMITTED
    assert _map_schwab_status("AWAITING_PARENT_ORDER") == OrderStatus.SUBMITTED
    assert _map_schwab_status("CANCELED") == OrderStatus.CANCELLED


def test_trader_client_get_orders() -> None:
    from schwab_trader_client import SchwabTraderClient, format_schwab_entered_time

    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._orders_path_template = "accounts/{account_hash}/orders"
    client.resolve_account_hash = lambda **kwargs: "encrypted-hash"  # type: ignore[method-assign]

    captured: dict[str, object] = {}

    def _request_json(path: str, **kwargs: object) -> object:
        captured["path"] = path
        captured["params"] = kwargs.get("params")
        return [
            {
                "orderId": 111,
                "status": "FILLED",
                "quantity": 5,
                "filledQuantity": 5,
            },
            {
                "orderId": 222,
                "status": "WORKING",
                "quantity": 10,
                "filledQuantity": 0,
            },
        ]

    client._request_json = _request_json  # type: ignore[method-assign]

    from_time = format_schwab_entered_time(datetime(2024, 3, 29, tzinfo=timezone.utc))
    to_time = format_schwab_entered_time(datetime(2024, 4, 28, 23, 59, 59, tzinfo=timezone.utc))

    orders = client.get_orders(
        from_entered_time=from_time,
        to_entered_time=to_time,
        account_hash="encrypted-hash",
        max_results=100,
        status="FILLED",
    )
    assert captured["path"] == "accounts/encrypted-hash/orders"
    assert captured["params"] == {
        "fromEnteredTime": from_time,
        "toEnteredTime": to_time,
        "maxResults": 100,
        "status": "FILLED",
    }
    assert len(orders) == 2
    assert orders[0]["orderId"] == 111


def test_broker_gateway_list_orders() -> None:
    from schwab_broker_gateway import SchwabBrokerGateway

    class _FakeTraderClient:
        def get_orders(self, **kwargs: object) -> list[dict[str, object]]:
            return [
                {
                    "orderId": 999,
                    "status": "FILLED",
                    "quantity": 3,
                    "filledQuantity": 3,
                    "orderActivityCollection": [
                        {"executionLegs": [{"quantity": 3, "price": 100.0}]}
                    ],
                }
            ]

    gateway = SchwabBrokerGateway(_FakeTraderClient())
    reports = gateway.list_orders(
        from_entered_time="2024-03-29T00:00:00.000Z",
        to_entered_time="2024-04-28T23:59:59.000Z",
    )
    assert len(reports) == 1
    assert reports[0].broker_order_id == "999"
    assert reports[0].status == OrderStatus.FILLED
    assert reports[0].average_fill_price == 100.0


def test_preview_order_result_parsing() -> None:
    from schwab_trader_client import SchwabOrderPreviewResult

    result = SchwabOrderPreviewResult.from_payload(
        {
            "orderStrategy": {
                "orderBalance": {
                    "orderValue": 4805.0,
                    "projectedBuyingPower": 25000.0,
                    "projectedCommission": 0.0,
                }
            },
            "orderValidationResult": {
                "accepts": [
                    {
                        "validationRuleName": "BUYING_POWER",
                        "message": "Order passes buying power check",
                        "activityMessage": "",
                    }
                ],
                "rejects": [],
            },
        }
    )
    assert result.is_valid
    assert result.projected_order_value == 4805.0
    assert len(result.accepts) == 1


def test_trader_client_preview_order_uses_correct_path() -> None:
    from schwab_trader_client import SchwabTraderClient

    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._preview_order_path_template = "accounts/{account_hash}/previewOrder"
    client.resolve_account_hash = lambda **kwargs: "encrypted-hash"  # type: ignore[method-assign]

    captured: dict[str, object] = {}

    class _Response:
        content = b"{}"

        @staticmethod
        def json() -> dict[str, object]:
            return {
                "orderStrategy": {"orderBalance": {"orderValue": 100.0}},
                "orderValidationResult": {"rejects": []},
            }

    def _request(method: str, path: str, **kwargs: object) -> _Response:
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = kwargs.get("json_body")
        return _Response()

    client._request = _request  # type: ignore[method-assign]

    result = client.preview_order(
        {"orderType": "MARKET", "orderLegCollection": []},
        account_hash="encrypted-hash",
    )
    assert captured["method"] == "POST"
    assert captured["path"] == "accounts/encrypted-hash/previewOrder"
    assert result.is_valid
    assert result.projected_order_value == 100.0


def test_broker_gateway_preview_order_rejects_invalid_payload() -> None:
    from schwab_broker_gateway import SchwabBrokerError, SchwabBrokerGateway
    from schwab_trader_client import SchwabOrderPreviewResult, SchwabOrderValidationMessage

    class _FakeTraderClient:
        def preview_order(self, _payload: object, **kwargs: object) -> SchwabOrderPreviewResult:
            return SchwabOrderPreviewResult(
                rejects=(
                    SchwabOrderValidationMessage(
                        validation_rule_name="MARKET_CLOSED",
                        message="Market is closed",
                        activity_message="",
                    ),
                ),
                warns=(),
                accepts=(),
                alerts=(),
                reviews=(),
                projected_commission=None,
                projected_buying_power=None,
                projected_order_value=None,
            )

    gateway = SchwabBrokerGateway(_FakeTraderClient())
    try:
        gateway.preview_order(_sample_order())
        raise AssertionError("expected SchwabBrokerError")
    except SchwabBrokerError as exc:
        assert "Market is closed" in str(exc)


def test_trader_client_cancel_order() -> None:
    from schwab_trader_client import SchwabTraderClient

    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._orders_path_template = "accounts/{account_hash}/orders"
    client.resolve_account_hash = lambda **kwargs: "encrypted-hash"  # type: ignore[method-assign]

    captured: dict[str, object] = {}

    def _request(method: str, path: str, **kwargs: object) -> object:
        captured["method"] = method
        captured["path"] = path
        captured["expect_json"] = kwargs.get("expect_json")
        return object()

    client._request = _request  # type: ignore[method-assign]
    client.cancel_order("98765", account_hash="encrypted-hash")

    assert captured["method"] == "DELETE"
    assert captured["path"] == "accounts/encrypted-hash/orders/98765"
    assert captured["expect_json"] is False


def test_trader_client_get_order() -> None:
    from schwab_trader_client import SchwabTraderClient

    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._orders_path_template = "accounts/{account_hash}/orders"
    client.resolve_account_hash = lambda **kwargs: "encrypted-hash"  # type: ignore[method-assign]
    client._request_json = lambda path, **kwargs: {  # type: ignore[method-assign]
        "orderId": 12345,
        "status": "WORKING",
        "quantity": 10,
        "filledQuantity": 0,
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "instrument": {"symbol": "SPY"},
            }
        ],
    }

    payload = client.get_order("12345", account_hash="encrypted-hash")
    assert payload["orderId"] == 12345
    assert payload["status"] == "WORKING"


def test_execution_report_from_filled_order() -> None:
    report = _execution_report_from_schwab_order(
        "12345",
        {
            "status": "FILLED",
            "quantity": 10,
            "filledQuantity": 10,
            "orderActivityCollection": [
                {
                    "executionLegs": [
                        {"quantity": 10, "price": 480.5},
                    ]
                }
            ],
        },
    )
    assert report.status == OrderStatus.FILLED
    assert report.filled_quantity == 10.0
    assert report.average_fill_price == 480.5
