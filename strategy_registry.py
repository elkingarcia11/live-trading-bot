"""Strategy Registry.

Responsibility: Trading rule repository.

Stores strategy definitions and rule logic, separating what the rules are from
live evaluation. Does not calculate indicators, aggregate bars, or emit final
execution events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional, TYPE_CHECKING

from enum import Enum

if TYPE_CHECKING:
    from gex_calculator import GexSnapshot


class SignalAction(Enum):
    """Discrete trading decision returned by a strategy rule."""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    EXIT = "exit"


@dataclass(frozen=True)
class StrategyEvaluationContext:
    """Inputs available to a strategy rule at evaluation time."""

    symbol: str
    timeframe: str
    timestamp: datetime
    close: float
    indicators: dict[str, Any]
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: float = 0.0
    gex: Optional["GexSnapshot"] = None
    has_open_position: bool = False
    state: dict[str, Any] = field(default_factory=dict)


StrategyRule = Callable[[StrategyEvaluationContext], SignalAction]


@dataclass(frozen=True)
class StrategyDefinition:
    """Named strategy and its associated rule logic."""

    name: str
    rule: StrategyRule
    timeframe: str
    required_indicators: tuple[str, ...] = ()
    required_gex_fields: tuple[str, ...] = ()


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


def gaussian_bands(ctx: StrategyEvaluationContext) -> SignalAction:
    """Gaussian MA + ATR band crossings.

    Buy a call when price crosses above the upper band, exit it when price falls
    back below the upper band. Buy a put when price crosses below the lower band,
    exit it when price climbs back above the lower band.
    """
    if ctx.indicators.get("gaussian_buy_signal"):
        return SignalAction.BUY
    if ctx.indicators.get("gaussian_sell_signal"):
        return SignalAction.SELL
    if ctx.indicators.get("gaussian_exit_long") or ctx.indicators.get(
        "gaussian_exit_short"
    ):
        return SignalAction.EXIT
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


def gex_scalp(ctx: StrategyEvaluationContext) -> SignalAction:
    """Negative-GEX 0DTE scalping: put-wall breaks and magnet snaps on 1m bars."""
    gex = ctx.gex
    if gex is None:
        return SignalAction.HOLD

    state = ctx.state
    volume_sma = ctx.indicators.get("volume_sma")
    volume_mult = float(ctx.indicators.get("gex_volume_multiplier", 1.5))
    put_wall_break_pct = float(ctx.indicators.get("gex_put_wall_break_pct", 0.001))
    stall_body_ratio = float(ctx.indicators.get("gex_stall_body_ratio", 0.25))
    long_wick_ratio = float(ctx.indicators.get("gex_long_wick_ratio", 0.55))

    volume_spike = (
        volume_sma is not None
        and volume_sma > 0
        and ctx.volume >= volume_sma * volume_mult
    )

    if ctx.has_open_position:
        return _gex_scalp_manage_exit(
            ctx,
            state,
            stall_body_ratio=stall_body_ratio,
            long_wick_ratio=long_wick_ratio,
        )

    if gex.regime != "negative":
        state.clear()
        return SignalAction.HOLD

    if not volume_spike:
        return SignalAction.HOLD

    if gex.put_wall is not None:
        threshold = gex.put_wall * (1.0 - put_wall_break_pct)
        if ctx.close < threshold:
            state["trigger_level"] = gex.put_wall
            state["entry_side"] = "put"
            state["consecutive_directional"] = 0
            return SignalAction.SELL

    if gex.flip_level is not None:
        if ctx.close > gex.flip_level and ctx.open <= gex.flip_level:
            state["trigger_level"] = gex.flip_level
            state["entry_side"] = "call"
            state["consecutive_directional"] = 0
            return SignalAction.BUY
        if ctx.close < gex.flip_level and ctx.open >= gex.flip_level:
            state["trigger_level"] = gex.flip_level
            state["entry_side"] = "put"
            state["consecutive_directional"] = 0
            return SignalAction.SELL

    return SignalAction.HOLD


def _gex_scalp_manage_exit(
    ctx: StrategyEvaluationContext,
    state: dict[str, Any],
    *,
    stall_body_ratio: float,
    long_wick_ratio: float,
) -> SignalAction:
    """Apply GEX take-profit and stop-loss rules for an open scalp."""
    trigger = state.get("trigger_level")
    entry_side = state.get("entry_side")
    if trigger is None or entry_side not in {"call", "put"}:
        return SignalAction.HOLD

    if entry_side == "put" and ctx.close > float(trigger):
        state.clear()
        return SignalAction.EXIT
    if entry_side == "call" and ctx.close < float(trigger):
        state.clear()
        return SignalAction.EXIT

    bar_range = max(ctx.high - ctx.low, 1e-9)
    body = abs(ctx.close - ctx.open)
    bullish = ctx.close >= ctx.open
    bearish = not bullish

    if entry_side == "call":
        upper_wick = ctx.high - max(ctx.open, ctx.close)
        reversed_color = bearish
        directional = bullish
    else:
        upper_wick = min(ctx.open, ctx.close) - ctx.low
        reversed_color = bullish
        directional = bearish

    long_wick = upper_wick / bar_range >= long_wick_ratio
    stalled = body / bar_range <= stall_body_ratio

    consecutive = int(state.get("consecutive_directional", 0))
    if directional:
        consecutive += 1
    else:
        consecutive = 0
    state["consecutive_directional"] = consecutive

    if reversed_color or stalled or long_wick or consecutive >= 3:
        state.clear()
        return SignalAction.EXIT

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
            name="gaussian_bands",
            rule=gaussian_bands,
            timeframe=strategy_timeframe,
            required_indicators=(
                "gaussian_buy_signal",
                "gaussian_sell_signal",
                "gaussian_exit_long",
                "gaussian_exit_short",
            ),
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
    registry.register(
        StrategyDefinition(
            name="gex_scalp",
            rule=gex_scalp,
            timeframe="1m",
            required_indicators=(),
            required_gex_fields=("regime", "put_wall", "flip_level"),
        )
    )
    return registry


if __name__ == "__main__":
    registry = build_default_registry()
    print(f"Registered strategies: {registry.list_strategies()}")
