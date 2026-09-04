"""Tests for EMA crossover + volume/ADX filter strategy (ema.pine parity)."""

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
    volume: float = 1000.0,
    state: dict | None = None,
    indicators: dict | None = None,
) -> StrategyEvaluationContext:
    values = {
        "ema_fast": fast,
        "ema_slow": slow,
        "volume_sma": 800.0,
        "adx": 30.0,
        "ema_use_vol_filter": True,
        "ema_vol_multiplier": 0.75,
        "ema_use_adx_filter": True,
        "ema_adx_threshold": 25.0,
    }
    if indicators:
        values.update(indicators)
    return StrategyEvaluationContext(
        symbol="SPY",
        timeframe="200t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        volume=volume,
        indicators=values,
        state=state if state is not None else {},
    )


def test_gaussian_ma_crossover_registered() -> None:
    registry = build_default_registry(strategy_timeframe="200t")
    assert registry.get("gaussian_ma_crossover").timeframe == "200t"


def test_confirmed_cross_up_buys() -> None:
    state: dict = {"ema_relation": "slow_above"}
    action = gaussian_ma_crossover(_ctx(fast=101.0, slow=100.0, state=state))
    assert action == SignalAction.BUY
    assert state["ema_relation"] == "fast_above"
    assert state["ema_last_cross"] == "up"


def test_confirmed_cross_down_sells() -> None:
    state: dict = {"ema_relation": "fast_above"}
    action = gaussian_ma_crossover(_ctx(fast=99.0, slow=100.0, state=state))
    assert action == SignalAction.SELL
    assert state["ema_last_cross"] == "down"


def test_volume_filter_blocks_cross() -> None:
    state: dict = {"ema_relation": "slow_above"}
    action = gaussian_ma_crossover(
        _ctx(fast=101.0, slow=100.0, volume=100.0, state=state)
    )
    assert action == SignalAction.HOLD
    assert state["ema_last_cross"] == "weak_up"
    assert state["ema_relation"] == "fast_above"


def test_adx_filter_blocks_cross() -> None:
    state: dict = {"ema_relation": "slow_above"}
    action = gaussian_ma_crossover(
        _ctx(
            fast=101.0,
            slow=100.0,
            state=state,
            indicators={"adx": 15.0},
        )
    )
    assert action == SignalAction.HOLD
    assert state["ema_last_cross"] == "weak_up"


def test_same_regime_holds_after_seed() -> None:
    state: dict = {}
    assert gaussian_ma_crossover(_ctx(fast=101.0, slow=100.0, state=state)) == SignalAction.HOLD
    assert state["ema_relation"] == "fast_above"
    assert gaussian_ma_crossover(_ctx(fast=102.0, slow=100.0, state=state)) == SignalAction.HOLD


def test_filters_can_be_disabled() -> None:
    state: dict = {"ema_relation": "slow_above"}
    action = gaussian_ma_crossover(
        _ctx(
            fast=101.0,
            slow=100.0,
            volume=1.0,
            state=state,
            indicators={
                "ema_use_vol_filter": False,
                "ema_use_adx_filter": False,
                "adx": 5.0,
            },
        )
    )
    assert action == SignalAction.BUY


def test_gma_fallback_crossover_without_filters() -> None:
    state: dict = {"ema_relation": "slow_above"}
    ctx = StrategyEvaluationContext(
        symbol="SPY",
        timeframe="200t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        volume=1.0,
        indicators={"gaussian_ma_fast": 101.0, "gaussian_ma_slow": 100.0},
        state=state,
    )
    assert gaussian_ma_crossover(ctx) == SignalAction.BUY


def test_missing_indicators_hold() -> None:
    ctx = StrategyEvaluationContext(
        symbol="SPY",
        timeframe="200t",
        timestamp=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
        state={},
    )
    assert gaussian_ma_crossover(ctx) == SignalAction.HOLD


def test_alignment_helper_matches_crossover_regime() -> None:
    assert option_position_aligned_with_gaussian_crossover(
        "CALL", fast=101.0, slow=100.0, close=100.0
    )
    assert option_position_aligned_with_gaussian_crossover(
        "PUT", fast=99.0, slow=100.0, close=100.0
    )
    assert not option_position_aligned_with_gaussian_crossover(
        "CALL", fast=99.0, slow=100.0, close=100.0
    )
