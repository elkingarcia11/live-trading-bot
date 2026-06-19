"""Tests for forward-test paper account balance tracking."""

from __future__ import annotations

from datetime import datetime, timezone

from forward_test_account import ForwardTestAccount


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
    assert sell.trade_pnl == 198.7
    assert account.cash_balance == 3198.7
    assert account.realized_pnl == 198.7


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
