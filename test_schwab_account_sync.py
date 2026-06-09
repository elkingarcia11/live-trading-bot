"""Tests for Schwab account position parsing and sync."""

from __future__ import annotations

from position_tracker import PositionTracker
from schwab_trader_client import SchwabTraderClient


class _FakeTraderClient:
    def get_account_snapshot(self, **kwargs: object):
        from schwab_trader_client import (
            SchwabAccountBalances,
            SchwabAccountPosition,
            SchwabAccountSnapshot,
        )

        return SchwabAccountSnapshot(
            account_number="12345678",
            balances=SchwabAccountBalances(
                equity=100000.0,
                buying_power=50000.0,
                cash_available_for_trading=25000.0,
                liquidation_value=100000.0,
            ),
            positions=(
                SchwabAccountPosition(
                    symbol="SPY",
                    quantity=10.0,
                    average_price=480.0,
                    market_value=4805.0,
                    current_day_profit_loss=5.0,
                ),
            ),
        )


def test_parse_position_from_account_payload() -> None:
    client = SchwabTraderClient.__new__(SchwabTraderClient)
    position = client._parse_position(
        {
            "longQuantity": 5,
            "shortQuantity": 0,
            "averagePrice": 100.0,
            "marketValue": 505.0,
            "currentDayProfitLoss": 5.0,
            "instrument": {"symbol": "QQQ", "assetType": "EQUITY"},
        }
    )
    assert position is not None
    assert position.symbol == "QQQ"
    assert position.quantity == 5.0


def test_parse_all_accounts_payload() -> None:
    client = SchwabTraderClient.__new__(SchwabTraderClient)
    snapshot = client._parse_account_snapshot(
        {
            "securitiesAccount": {
                "accountNumber": "11111111",
                "currentBalances": {
                    "equity": 50000.0,
                    "buyingPower": 25000.0,
                    "availableFunds": 10000.0,
                    "liquidationValue": 50000.0,
                },
                "positions": [
                    {
                        "longQuantity": 3,
                        "shortQuantity": 0,
                        "averagePrice": 200.0,
                        "marketValue": 600.0,
                        "currentDayProfitLoss": 1.0,
                        "instrument": {"symbol": "TSLA", "assetType": "EQUITY"},
                    }
                ],
            }
        }
    )
    assert snapshot.account_number == "11111111"
    assert snapshot.balances.equity == 50000.0
    assert len(snapshot.positions) == 1
    assert snapshot.positions[0].symbol == "TSLA"


def test_get_account_single_payload() -> None:
    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._accounts_path = "accounts"
    client.resolve_account_hash = lambda **kwargs: "encrypted-hash"  # type: ignore[method-assign]

    def _request_json(path: str, **kwargs: object) -> object:
        assert path == "accounts/encrypted-hash"
        assert kwargs.get("params") == {"fields": "positions"}
        return {
            "securitiesAccount": {
                "accountNumber": "12345678",
                "currentBalances": {
                    "equity": 75000.0,
                    "buyingPower": 30000.0,
                    "availableFunds": 15000.0,
                },
                "positions": [
                    {
                        "longQuantity": 10,
                        "shortQuantity": 0,
                        "averagePrice": 480.0,
                        "marketValue": 4805.0,
                        "currentDayProfitLoss": 5.0,
                        "instrument": {"symbol": "SPY", "assetType": "EQUITY"},
                    }
                ],
            }
        }

    client._request_json = _request_json  # type: ignore[method-assign]

    snapshot = client.get_account_snapshot(account_hash="encrypted-hash")
    assert snapshot.account_number == "12345678"
    assert snapshot.balances.equity == 75000.0
    assert snapshot.positions[0].symbol == "SPY"
    assert snapshot.positions[0].quantity == 10.0


def test_get_accounts_parses_list_response() -> None:
    client = SchwabTraderClient.__new__(SchwabTraderClient)
    client._accounts_path = "accounts"
    client._request_json = lambda path, **kwargs: [  # type: ignore[method-assign]
        {
            "securitiesAccount": {
                "accountNumber": "22222222",
                "initialBalances": {"equity": 1000.0, "buyingPower": 500.0},
            }
        },
        {
            "securitiesAccount": {
                "accountNumber": "33333333",
                "initialBalances": {"equity": 2000.0, "buyingPower": 1000.0},
            }
        },
    ]

    snapshots = client.get_all_account_snapshots(include_positions=False)
    assert len(snapshots) == 2
    assert snapshots[0].account_number == "22222222"
    assert snapshots[1].balances.equity == 2000.0


def test_sync_positions_updates_tracker() -> None:
    from schwab_account_sync import SchwabAccountSync

    tracker = PositionTracker()
    sync = SchwabAccountSync(_FakeTraderClient())
    sync.sync_positions(tracker, watchlist=("SPY", "QQQ"))
    position = tracker.get_position("SPY")
    assert position is not None
    assert position.quantity == 10.0
    assert tracker.get_position("QQQ") is None
