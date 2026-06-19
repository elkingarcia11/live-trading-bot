"""Tests for option contract selection and sizing."""

from __future__ import annotations

from datetime import date

from option_selector import (
    build_occ_symbol,
    contract_mark_from_chain,
    contract_quote_from_chain,
    option_contract_type,
    parse_occ_symbol,
    resolve_option_exit_from_chain,
    select_atm_call_from_chain,
    select_atm_put_from_chain,
    synthetic_atm_call,
    synthetic_atm_put,
)
from position_sizer import contracts_for_buy


def test_build_occ_symbol_spy_call() -> None:
    symbol = build_occ_symbol(
        "SPY",
        date(2026, 6, 17),
        strike=550.0,
    )
    assert symbol == "SPY   260617C00550000"


def test_synthetic_atm_call_uses_target_dte() -> None:
    selected = synthetic_atm_call(
        "SPY",
        549.8,
        days_to_expiration=2,
        mark_price=4.5,
        as_of=date(2026, 6, 15),
    )
    assert selected.underlying_symbol == "SPY"
    assert selected.strike == 550.0
    assert selected.days_to_expiration == 2
    assert selected.mark_price == 4.5
    assert "C00550000" in selected.occ_symbol


def test_synthetic_atm_put_uses_target_dte() -> None:
    selected = synthetic_atm_put(
        "SPY",
        549.8,
        days_to_expiration=2,
        mark_price=4.5,
        as_of=date(2026, 6, 15),
    )
    assert selected.underlying_symbol == "SPY"
    assert selected.strike == 550.0
    assert selected.days_to_expiration == 2
    assert selected.mark_price == 4.5
    assert "P00550000" in selected.occ_symbol


def test_select_atm_put_from_chain_picks_closest_dte_and_strike() -> None:
    chain = {
        "putExpDateMap": {
            "2026-06-17:2": {
                "550.0": [
                    {
                        "symbol": "SPY   260617P00550000",
                        "strikePrice": 550.0,
                        "mark": 4.25,
                        "bid": 4.2,
                        "ask": 4.3,
                        "delta": -0.45,
                        "gamma": 0.02,
                        "theta": -0.15,
                        "vega": 0.08,
                    }
                ],
                "551.0": [
                    {
                        "symbol": "SPY   260617P00551000",
                        "strikePrice": 551.0,
                        "mark": 3.9,
                    }
                ],
            }
        }
    }

    selected = select_atm_put_from_chain(
        chain,
        "SPY",
        549.9,
        target_dte=2,
    )
    assert selected.occ_symbol == "SPY   260617P00550000"
    assert selected.strike == 550.0
    assert selected.days_to_expiration == 2
    assert selected.mark_price == 4.25
    assert selected.quote.bid == 4.2
    assert selected.quote.ask == 4.3
    assert selected.quote.delta == -0.45


def test_option_contract_type_detects_put() -> None:
    assert option_contract_type("SPY   260617P00550000") == "PUT"
    assert option_contract_type("SPY   260617C00550000") == "CALL"


def test_select_atm_call_from_chain_picks_exact_2_over_1_and_3() -> None:
    chain = {
        "callExpDateMap": {
            "2026-06-18:1": {
                "550.0": [
                    {
                        "symbol": "SPY   260618C00550000",
                        "strikePrice": 550.0,
                        "mark": 3.0,
                    }
                ]
            },
            "2026-06-19:2": {
                "550.0": [
                    {
                        "symbol": "SPY   260619C00550000",
                        "strikePrice": 550.0,
                        "mark": 4.25,
                    }
                ]
            },
            "2026-06-20:3": {
                "550.0": [
                    {
                        "symbol": "SPY   260620C00550000",
                        "strikePrice": 550.0,
                        "mark": 6.0,
                    }
                ]
            },
        }
    }

    selected = select_atm_call_from_chain(
        chain,
        "SPY",
        549.9,
        target_dte=2,
        as_of=date(2026, 6, 17),
    )
    assert selected.occ_symbol == "SPY   260619C00550000"
    assert selected.days_to_expiration == 2


def test_select_atm_call_prefers_longer_dte_when_exact_missing() -> None:
    chain = {
        "callExpDateMap": {
            "2026-06-18:1": {
                "550.0": [
                    {
                        "symbol": "SPY   260618C00550000",
                        "strikePrice": 550.0,
                        "mark": 3.0,
                    }
                ]
            },
            "2026-06-20:3": {
                "550.0": [
                    {
                        "symbol": "SPY   260620C00550000",
                        "strikePrice": 550.0,
                        "mark": 6.0,
                    }
                ]
            },
        }
    }

    selected = select_atm_call_from_chain(
        chain,
        "SPY",
        549.9,
        target_dte=2,
        as_of=date(2026, 6, 17),
    )
    assert selected.occ_symbol == "SPY   260620C00550000"
    assert selected.days_to_expiration == 3


def test_select_atm_call_from_chain_picks_closest_dte_and_strike() -> None:
    chain = {
        "callExpDateMap": {
            "2026-06-17:2": {
                "550.0": [
                    {
                        "symbol": "SPY   260617C00550000",
                        "strikePrice": 550.0,
                        "mark": 4.25,
                    }
                ],
                "551.0": [
                    {
                        "symbol": "SPY   260617C00551000",
                        "strikePrice": 551.0,
                        "mark": 3.9,
                    }
                ],
            },
            "2026-06-24:9": {
                "550.0": [
                    {
                        "symbol": "SPY   260624C00550000",
                        "strikePrice": 550.0,
                        "mark": 6.0,
                    }
                ]
            },
        }
    }

    selected = select_atm_call_from_chain(
        chain,
        "SPY",
        549.9,
        target_dte=2,
    )
    assert selected.occ_symbol == "SPY   260617C00550000"
    assert selected.strike == 550.0
    assert selected.days_to_expiration == 2
    assert selected.mark_price == 4.25


def test_contracts_for_buy_uses_premium_times_100() -> None:
    contracts = contracts_for_buy(
        100_000,
        premium_per_share=5.0,
        pct=0.30,
        max_dollars=15_000,
    )
    assert contracts == 30


def test_contract_mark_from_chain_finds_occ_symbol() -> None:
    chain = {
        "callExpDateMap": {
            "2024-01-17:2": {
                "480.0": [
                    {
                        "symbol": "SPY240117C00480000",
                        "mark": 5.25,
                        "strikePrice": 480.0,
                    }
                ]
            }
        }
    }
    assert contract_mark_from_chain(chain, "SPY240117C00480000") == 5.25


def test_contract_quote_from_chain_returns_bid_ask_and_greeks() -> None:
    chain = {
        "callExpDateMap": {
            "2024-01-17:2": {
                "480.0": [
                    {
                        "symbol": "SPY240117C00480000",
                        "mark": 5.25,
                        "bid": 5.2,
                        "ask": 5.3,
                        "delta": 0.55,
                        "gamma": 0.03,
                        "theta": -0.12,
                        "vega": 0.09,
                        "strikePrice": 480.0,
                    }
                ]
            }
        }
    }
    quote = contract_quote_from_chain(chain, "SPY240117C00480000")
    assert quote is not None
    assert quote.bid == 5.2
    assert quote.ask == 5.3
    assert quote.delta == 0.55
    assert quote.theta == -0.12


def test_parse_occ_symbol_for_padded_schwab_put() -> None:
    parsed = parse_occ_symbol("SPY   260618P00752000")
    assert parsed is not None
    assert parsed.root == "SPY"
    assert parsed.expiration_date == date(2026, 6, 18)
    assert parsed.option_right == "P"
    assert parsed.strike == 752.0


def test_find_contract_matches_padded_occ_symbols() -> None:
    chain = {
        "putExpDateMap": {
            "2026-06-18:2": {
                "752.0": [
                    {
                        "symbol": "SPY   260618P00752000",
                        "mark": 4.85,
                        "bid": 4.80,
                        "ask": 4.90,
                        "strikePrice": 752.0,
                    }
                ]
            }
        }
    }
    assert contract_mark_from_chain(chain, "SPY260618P00752000") == 4.85


def test_resolve_option_exit_from_chain_includes_underlying_price() -> None:
    chain = {
        "putExpDateMap": {
            "2026-06-18:2": {
                "752.0": [
                    {
                        "symbol": "SPY   260618P00752000",
                        "mark": 4.85,
                        "bid": 4.80,
                        "ask": 4.90,
                        "strikePrice": 752.0,
                    }
                ]
            }
        },
        "underlyingPrice": 750.42,
    }
    exit_mark = resolve_option_exit_from_chain(chain, "SPY   260618P00752000")
    assert exit_mark is not None
    assert exit_mark.premium == 4.85
    assert exit_mark.underlying_price == 750.42
    assert exit_mark.quote is not None
    assert exit_mark.quote.bid == 4.80
