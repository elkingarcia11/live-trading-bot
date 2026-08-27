"""Tests for forward-test email subject formatting."""

from __future__ import annotations

from emailer import _option_right_suffix, _sell_subject


def test_buy_style_right_suffix_from_occ() -> None:
    assert _option_right_suffix("SPY   260814C00775000") == " C"
    assert _option_right_suffix("SPY260814P00771000") == " P"
    assert _option_right_suffix("SPY") == ""


def test_sell_subject_profit_green_with_pct() -> None:
    subject = _sell_subject(
        symbol="SPY",
        right=" C",
        exit_instrument_price=1.55,
        entry_instrument_price=1.47,
        profit=40.0,
    )
    assert subject == "🟢 SELL SPY C @ 1.55 (+5.44%)"


def test_sell_subject_loss_red_with_pct() -> None:
    subject = _sell_subject(
        symbol="SPY",
        right=" P",
        exit_instrument_price=1.55,
        entry_instrument_price=1.64,
        profit=-45.0,
    )
    assert subject == "🔴 SELL SPY P @ 1.55 (-5.49%)"


def test_sell_subject_without_entry_omits_pct_and_dot() -> None:
    subject = _sell_subject(
        symbol="SPY",
        right=" C",
        exit_instrument_price=1.55,
        entry_instrument_price=None,
        profit=None,
    )
    assert subject == "SELL SPY C @ 1.55"


def test_sell_subject_includes_timeframe() -> None:
    subject = _sell_subject(
        symbol="SPY",
        right=" C",
        exit_instrument_price=1.55,
        entry_instrument_price=1.47,
        profit=40.0,
        timeframe="50t",
    )
    assert subject == "🟢 SELL SPY C 50t @ 1.55 (+5.44%)"


def test_sell_subject_blank_timeframe_omits_token() -> None:
    subject = _sell_subject(
        symbol="SPY",
        right=" C",
        exit_instrument_price=1.55,
        entry_instrument_price=None,
        profit=None,
        timeframe="  ",
    )
    assert subject == "SELL SPY C @ 1.55"
