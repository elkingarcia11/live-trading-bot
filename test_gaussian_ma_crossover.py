"""Tests for Gaussian MA slow/fast crossover option strategy."""

from __future__ import annotations

from datetime import datetime, timezone

from position_reconciliation import option_position_aligned_with_gaussian_crossover
from strategy_registry import (
    SignalAction,
    StrategyEvaluationContext,
    build_default_registry,
    gaussian_ma_crossover,
)


def _ctx(
    *,
    fast: float,
    slow: float,
    state: dict | None = None,
) -> StrategyEvaluationContext:
    return StrategyEvaluationContext(
        symbol="SPY",
        timeframe="50t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        indicators={"gaussian_ma_fast": fast, "gaussian_ma_slow": slow},
        state=state if state is not None else {},
    )


def test_gaussian_ma_crossover_registered_on_50t() -> None:
    registry = build_default_registry(strategy_timeframe="50t")
    strategy = registry.get("gaussian_ma_crossover")
    assert strategy.timeframe == "50t"
    assert "gaussian_ma_fast" in strategy.required_indicators
    assert "gaussian_ma_slow" in strategy.required_indicators


def test_fast_cross_above_slow_buys_call() -> None:
    state: dict = {"gaussian_ma_relation": "slow_above"}
    action = gaussian_ma_crossover(_ctx(fast=101.0, slow=100.0, state=state))
    assert action == SignalAction.BUY
    assert state["gaussian_ma_relation"] == "fast_above"


def test_slow_cross_above_fast_buys_put() -> None:
    state: dict = {"gaussian_ma_relation": "fast_above"}
    action = gaussian_ma_crossover(_ctx(fast=99.0, slow=100.0, state=state))
    assert action == SignalAction.SELL
    assert state["gaussian_ma_relation"] == "slow_above"


def test_same_regime_holds_after_seed() -> None:
    state: dict = {}
    assert gaussian_ma_crossover(_ctx(fast=101.0, slow=100.0, state=state)) == SignalAction.HOLD
    assert state["gaussian_ma_relation"] == "fast_above"
    assert gaussian_ma_crossover(_ctx(fast=102.0, slow=100.0, state=state)) == SignalAction.HOLD


def test_missing_indicators_hold() -> None:
    ctx = StrategyEvaluationContext(
        symbol="SPY",
        timeframe="50t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
        state={},
    )
    assert gaussian_ma_crossover(ctx) == SignalAction.HOLD


def test_crossover_alignment_helper() -> None:
    assert option_position_aligned_with_gaussian_crossover("CALL", fast=101.0, slow=100.0)
    assert option_position_aligned_with_gaussian_crossover("PUT", fast=99.0, slow=100.0)
    assert not option_position_aligned_with_gaussian_crossover("CALL", fast=99.0, slow=100.0)
