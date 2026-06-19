"""Strategy Registry.

Responsibility: Trading rule repository.

Stores strategy definitions and rule logic, separating what the rules are from
live evaluation. Does not calculate indicators, aggregate bars, or emit final
execution events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from enum import Enum


class SignalAction(Enum):
    """Discrete trading decision returned by a strategy rule."""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass(frozen=True)
class StrategyEvaluationContext:
    """Inputs available to a strategy rule at evaluation time."""

    symbol: str
    timeframe: str
    timestamp: datetime
    close: float
    indicators: dict[str, Any]


StrategyRule = Callable[[StrategyEvaluationContext], SignalAction]


@dataclass(frozen=True)
class StrategyDefinition:
    """Named strategy and its associated rule logic."""

    name: str
    rule: StrategyRule
    timeframe: str
    required_indicators: tuple[str, ...] = ()


class StrategyRegistry:
    """Repository for strategy definitions."""

    def __init__(self) -> None:
        self._strategies: dict[str, StrategyDefinition] = {}

    def register(self, definition: StrategyDefinition) -> None:
        """Add or replace a strategy definition.

        Args:
            definition: Strategy metadata and rule callable.
        """
        self._strategies[definition.name] = definition

    def get(self, name: str) -> StrategyDefinition:
        """Return a strategy definition by name.

        Raises:
            KeyError: If the strategy is not registered.
        """
        strategy = self._strategies.get(name)
        if strategy is None:
            raise KeyError(f"Unknown strategy: {name}")
        return strategy

    def list_strategies(self) -> list[str]:
        """Return registered strategy names."""
        return sorted(self._strategies)

    def unregister(self, name: str) -> None:
        """Remove a strategy definition if it exists."""
        self._strategies.pop(name, None)


def rsi_mean_reversion(ctx: StrategyEvaluationContext) -> SignalAction:
    """Buy oversold, sell overbought based on RSI."""
    rsi = ctx.indicators.get("rsi")
    if rsi is None:
        return SignalAction.HOLD
    if rsi < 30:
        return SignalAction.BUY
    if rsi > 70:
        return SignalAction.SELL
    return SignalAction.HOLD


def supertrend_trend(ctx: StrategyEvaluationContext) -> SignalAction:
    """Buy in uptrend, sell in downtrend based on Supertrend direction."""
    trend = ctx.indicators.get("supertrend_trend")
    if trend is None:
        return SignalAction.HOLD
    if trend > 0:
        return SignalAction.BUY
    if trend < 0:
        return SignalAction.SELL
    return SignalAction.HOLD


def supertrend_signals(ctx: StrategyEvaluationContext) -> SignalAction:
    """Buy/sell only when Supertrend flips direction."""
    if ctx.indicators.get("supertrend_buy_signal"):
        return SignalAction.BUY
    if ctx.indicators.get("supertrend_sell_signal"):
        return SignalAction.SELL
    return SignalAction.HOLD


def dema_trend(ctx: StrategyEvaluationContext) -> SignalAction:
    """Buy above DEMA, sell below DEMA."""
    dema = ctx.indicators.get("dema")
    if dema is None:
        return SignalAction.HOLD
    if ctx.close > dema:
        return SignalAction.BUY
    if ctx.close < dema:
        return SignalAction.SELL
    return SignalAction.HOLD


def macd_crossover(ctx: StrategyEvaluationContext) -> SignalAction:
    """Buy when MACD crosses above signal, sell when it crosses below."""
    macd = ctx.indicators.get("macd")
    signal = ctx.indicators.get("macd_signal")
    if macd is None or signal is None:
        return SignalAction.HOLD
    if macd > signal:
        return SignalAction.BUY
    if macd < signal:
        return SignalAction.SELL
    return SignalAction.HOLD


def build_default_registry(*, strategy_timeframe: str = "5m") -> StrategyRegistry:
    """Create a registry with the built-in example strategies."""
    registry = StrategyRegistry()
    registry.register(
        StrategyDefinition(
            name="dema_trend",
            rule=dema_trend,
            timeframe=strategy_timeframe,
            required_indicators=("dema",),
        )
    )
    registry.register(
        StrategyDefinition(
            name="supertrend_trend",
            rule=supertrend_trend,
            timeframe=strategy_timeframe,
            required_indicators=("supertrend_trend",),
        )
    )
    registry.register(
        StrategyDefinition(
            name="supertrend_signals",
            rule=supertrend_signals,
            timeframe=strategy_timeframe,
            required_indicators=("supertrend_buy_signal", "supertrend_sell_signal"),
        )
    )
    registry.register(
        StrategyDefinition(
            name="supertrend",
            rule=supertrend_signals,
            timeframe=strategy_timeframe,
            required_indicators=("supertrend_buy_signal", "supertrend_sell_signal"),
        )
    )
    registry.register(
        StrategyDefinition(
            name="rsi_mean_reversion",
            rule=rsi_mean_reversion,
            timeframe=strategy_timeframe,
            required_indicators=("rsi",),
        )
    )
    registry.register(
        StrategyDefinition(
            name="macd_crossover",
            rule=macd_crossover,
            timeframe=strategy_timeframe,
            required_indicators=("macd", "macd_signal"),
        )
    )
    return registry


if __name__ == "__main__":
    registry = build_default_registry()
    print(f"Registered strategies: {registry.list_strategies()}")
