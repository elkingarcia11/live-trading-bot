"""Tests for Gaussian MA threshold option strategy."""

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
    close: float = 500.0,
    state: dict | None = None,
    has_open_position: bool = False,
) -> StrategyEvaluationContext:
    return StrategyEvaluationContext(
        symbol="SPY",
        timeframe="400t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=close,
        indicators={"gaussian_ma_fast": fast, "gaussian_ma_slow": slow},
        state=state if state is not None else {},
        has_open_position=has_open_position,
    )


def test_gaussian_ma_crossover_registered_on_400t() -> None:
    registry = build_default_registry(strategy_timeframe="400t")
    strategy = registry.get("gaussian_ma_crossover")
    assert strategy.timeframe == "400t"
    assert "gaussian_ma_fast" in strategy.required_indicators
    assert "gaussian_ma_slow" in strategy.required_indicators


def test_long_ma_spread_entry_on_edge() -> None:
    state: dict = {"gma_threshold_prev": {"long_ma": False}}
    action = gaussian_ma_crossover(
        _ctx(fast=100.75, slow=100.0, close=100.5, state=state)
    )
    assert action == SignalAction.BUY
    assert state["gma_entry_trigger"] == "ma_spread"
    assert state["gma_entry_side"] == "call"


def test_long_close_spread_entry_on_edge() -> None:
    state: dict = {"gma_threshold_prev": {"long_close": False}}
    action = gaussian_ma_crossover(_ctx(fast=100.2, slow=100.0, close=101.5, state=state))
    assert action == SignalAction.BUY
    assert state["gma_entry_trigger"] == "close_vs_slow"
    assert state["gma_entry_side"] == "call"


def test_short_ma_spread_entry_on_edge() -> None:
    state: dict = {"gma_threshold_prev": {"short_ma": False}}
    action = gaussian_ma_crossover(
        _ctx(fast=100.0, slow=100.75, close=100.5, state=state)
    )
    assert action == SignalAction.SELL
    assert state["gma_entry_trigger"] == "ma_spread"
    assert state["gma_entry_side"] == "put"


def test_short_close_spread_entry_on_edge() -> None:
    state: dict = {"gma_threshold_prev": {"short_close": False}}
    action = gaussian_ma_crossover(_ctx(fast=100.2, slow=100.0, close=98.5, state=state))
    assert action == SignalAction.SELL
    assert state["gma_entry_trigger"] == "close_vs_slow"
    assert state["gma_entry_side"] == "put"


def test_active_condition_holds_without_reentry() -> None:
    state: dict = {
        "gma_threshold_prev": {"long_ma": True},
        "gma_entry_side": "call",
        "gma_entry_trigger": "ma_spread",
    }
    assert (
        gaussian_ma_crossover(
            _ctx(fast=101.0, slow=100.0, state=state, has_open_position=True)
        )
        == SignalAction.HOLD
    )


def test_long_ma_spread_exit_when_spread_flips() -> None:
    state: dict = {
        "gma_entry_side": "call",
        "gma_entry_trigger": "ma_spread",
        "gma_threshold_prev": {"long_ma": True},
    }
    action = gaussian_ma_crossover(
        _ctx(fast=100.5, slow=100.0, state=state, has_open_position=True)
    )
    assert action == SignalAction.EXIT
    assert "gma_entry_side" not in state


def test_long_close_exit_when_close_flips() -> None:
    state: dict = {
        "gma_entry_side": "call",
        "gma_entry_trigger": "close_vs_slow",
        "gma_threshold_prev": {"long_close": True},
    }
    action = gaussian_ma_crossover(
        _ctx(fast=100.2, slow=100.0, close=101.0, state=state, has_open_position=True)
    )
    assert action == SignalAction.EXIT


def test_put_ma_spread_exit_when_spread_flips() -> None:
    state: dict = {
        "gma_entry_side": "put",
        "gma_entry_trigger": "ma_spread",
        "gma_threshold_prev": {"short_ma": True},
    }
    action = gaussian_ma_crossover(
        _ctx(fast=100.0, slow=100.5, state=state, has_open_position=True)
    )
    assert action == SignalAction.EXIT


def test_missing_indicators_hold() -> None:
    ctx = StrategyEvaluationContext(
        symbol="SPY",
        timeframe="400t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
        state={},
    )
    assert gaussian_ma_crossover(ctx) == SignalAction.HOLD


def test_crossover_alignment_helper() -> None:
    assert option_position_aligned_with_gaussian_crossover(
        "CALL", fast=100.75, slow=100.0, close=100.0
    )
    assert option_position_aligned_with_gaussian_crossover(
        "CALL", fast=100.2, slow=100.0, close=101.5
    )
    assert option_position_aligned_with_gaussian_crossover(
        "PUT", fast=100.0, slow=100.75, close=100.0
    )
    assert option_position_aligned_with_gaussian_crossover(
        "PUT", fast=100.2, slow=100.0, close=98.5
    )
    assert not option_position_aligned_with_gaussian_crossover(
        "CALL", fast=100.2, slow=100.0, close=100.5
    )
