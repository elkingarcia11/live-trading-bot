"""Tests for forward-test paper account balance tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from config import AppConfig
from forward_test_account import ForwardTestAccount


def test_from_app_config_starts_fresh_in_memory_without_gcs() -> None:
    app = AppConfig.from_dict(
        {
            "forward_test": {
                "initial_balance": 10000,
                "persist_state": True,
            }
        }
    )
    with patch("forward_test_account.ForwardTestAccountStore") as store_cls:
        account = ForwardTestAccount.from_app_config(app)

    store_cls.assert_not_called()
    assert account.cash_balance == 10000.0
    assert account.initial_balance == 10000.0
    assert account.realized_pnl == 0.0


def test_from_app_config_default_balance_is_10000() -> None:
    account = ForwardTestAccount.from_app_config(AppConfig.from_dict({}))
    assert account.cash_balance == 10000.0


def test_forward_test_account_buy_and_sell_updates_cash_and_pnl() -> None:
    account = ForwardTestAccount(
        initial_balance=3000.0,
        store=None,
        persist_state=False,
    )
    opened_at = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)

    buy = account.record_buy(
        symbol="SPY240117C00480000",
        underlying_symbol="SPY",
        quantity=2,
        price=5.0,
        asset_type="OPTION",
        opened_at=opened_at,
    )
    assert buy.amount == 1000.0
    assert account.cash_balance == 2000.0

    sell = account.record_sell(
        symbol="SPY240117C00480000",
        underlying_symbol="SPY",
        quantity=2,
        exit_price=6.0,
        asset_type="OPTION",
        closed_at=datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
    )
    assert sell.trade_pnl == 200.0
    assert account.cash_balance == 3200.0
    assert account.realized_pnl == 200.0


def test_forward_test_account_applies_option_commission_to_cost_basis() -> None:
    account = ForwardTestAccount(
        initial_balance=3000.0,
        store=None,
        persist_state=False,
        option_commission_per_contract=0.65,
    )
    opened_at = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)

    buy = account.record_buy(
        symbol="SPY240117C00480000",
        underlying_symbol="SPY",
        quantity=2,
        price=5.0,
        asset_type="OPTION",
        opened_at=opened_at,
    )
    assert buy.amount == 1001.3
    assert account.cash_balance == 1998.7

    sell = account.record_sell(
        symbol="SPY240117C00480000",
        underlying_symbol="SPY",
        quantity=2,
        exit_price=6.0,
        asset_type="OPTION",
        closed_at=datetime(2024, 1, 15, 18, 0, tzinfo=timezone.utc),
    )
    # Round-trip fees: $0.65 * 2 contracts * 2 legs = $2.60
    assert sell.amount == pytest.approx(1198.7)
    assert sell.trade_pnl == pytest.approx(197.4)
    assert account.cash_balance == pytest.approx(3197.4)
    assert account.realized_pnl == pytest.approx(197.4)


def test_forward_test_account_commission_skips_equity() -> None:
    account = ForwardTestAccount(
        initial_balance=3000.0,
        store=None,
        persist_state=False,
        option_commission_per_contract=0.65,
    )
    buy = account.record_buy(
        symbol="SPY",
        underlying_symbol="SPY",
        quantity=2,
        price=5.0,
        asset_type="EQUITY",
        opened_at=datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
    )
    assert buy.amount == 10.0
    assert account.cash_balance == 2990.0


def test_forward_test_account_sizes_from_remaining_cash() -> None:
    account = ForwardTestAccount(
        initial_balance=3000.0,
        store=None,
        persist_state=False,
    )
    account.record_buy(
        symbol="SPY240117C00480000",
        underlying_symbol="SPY",
        quantity=1,
        price=5.0,
        asset_type="OPTION",
        opened_at=datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
    )
    assert account.cash_balance == 2500.0


def test_forward_test_account_expires_open_option_worthless() -> None:
    account = ForwardTestAccount(
        initial_balance=3000.0,
        store=None,
        persist_state=False,
        option_commission_per_contract=0.65,
    )
    opened_at = datetime(2026, 6, 30, 19, 57, tzinfo=timezone.utc)
    account.record_buy(
        symbol="SPY   260702P00746000",
        underlying_symbol="SPY",
        quantity=1,
        price=5.0,
        asset_type="OPTION",
        opened_at=opened_at,
    )
    assert account.cash_balance == 2499.35

    expired = account.expire_open_position(
        symbol="SPY   260702P00746000",
        underlying_symbol="SPY",
        asset_type="OPTION",
        closed_at=datetime(2026, 7, 3, 20, 0, tzinfo=timezone.utc),
    )
    assert expired.trade_pnl == -500.65
    assert account.cash_balance == 2499.35
    assert account.realized_pnl == -500.65
    assert account.equity_estimate == 2499.35
