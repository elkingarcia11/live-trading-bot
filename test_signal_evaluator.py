"""Tests for strategy signal evaluation."""

from __future__ import annotations

from datetime import datetime, timezone

from signal_evaluator import SignalEvaluator
from strategy_registry import SignalAction, build_default_registry


def test_supertrend_evaluates_on_configured_strategy_timeframe() -> None:
    evaluator = SignalEvaluator(build_default_registry(strategy_timeframe="3m"))
    timestamp = datetime(2026, 6, 16, 17, 48, tzinfo=timezone.utc)

    signal = evaluator.evaluate(
        symbol="SPY",
        timeframe="3m",
        timestamp=timestamp,
        close=752.14,
        indicators={
            "supertrend_buy_signal": True,
            "supertrend_sell_signal": False,
            "supertrend_trend": 1.0,
            "supertrend": 751.64,
        },
        strategy_name="supertrend",
    )

    assert signal.action == SignalAction.BUY


def test_supertrend_holds_when_registry_timeframe_mismatches_bar() -> None:
    evaluator = SignalEvaluator(build_default_registry(strategy_timeframe="5m"))

    signal = evaluator.evaluate(
        symbol="SPY",
        timeframe="3m",
        timestamp=datetime(2026, 6, 16, 17, 48, tzinfo=timezone.utc),
        close=752.14,
        indicators={
            "supertrend_buy_signal": True,
            "supertrend_sell_signal": False,
        },
        strategy_name="supertrend",
    )

    assert signal.action == SignalAction.HOLD
