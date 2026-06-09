"""Signal Evaluator.

Responsibility: Live strategy evaluation.

Feeds incoming real-time bars and indicator values into active strategy rules and
outputs simple BUY, SELL, or HOLD events. Does not calculate indicators,
aggregate bars, or submit broker orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from strategy_registry import (
    SignalAction,
    StrategyEvaluationContext,
    StrategyRegistry,
)


@dataclass(frozen=True)
class StrategySignal:
    """Strategy evaluation output for one symbol and bar."""

    symbol: str
    timeframe: str
    timestamp: datetime
    action: SignalAction
    strategy_name: str
    close: float
    indicators: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the signal for logging or downstream consumers."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp.isoformat(),
            "action": self.action.value,
            "strategy_name": self.strategy_name,
            "close": self.close,
            "indicators": self.indicators,
        }


class SignalEvaluator:
    """Evaluates registered strategy rules against live bars and indicators."""

    def __init__(self, registry: StrategyRegistry) -> None:
        """Initialize the evaluator.

        Args:
            registry: Repository containing strategy rule definitions.
        """
        self._registry = registry

    def evaluate(
        self,
        *,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
        close: float,
        indicators: dict[str, Any],
        strategy_name: str,
    ) -> StrategySignal:
        """Evaluate one strategy for the current bar and indicator snapshot.

        Args:
            symbol: Ticker symbol.
            timeframe: Bar interval being evaluated.
            timestamp: Timestamp of the current bar.
            close: Latest close price.
            indicators: Latest indicator values for the symbol/timeframe.
            strategy_name: Registered strategy to evaluate.

        Returns:
            A BUY, SELL, or HOLD strategy signal.

        Raises:
            KeyError: If the strategy is not registered.
            ValueError: If required indicators are missing.
        """
        strategy = self._registry.get(strategy_name)
        if strategy.timeframe != timeframe:
            return self._hold_signal(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                close=close,
                indicators=indicators,
                strategy_name=strategy_name,
            )

        self._validate_required_indicators(strategy.required_indicators, indicators)

        context = StrategyEvaluationContext(
            symbol=symbol.upper(),
            timeframe=timeframe,
            timestamp=timestamp,
            close=close,
            indicators=indicators,
        )
        action = strategy.rule(context)

        return StrategySignal(
            symbol=symbol.upper(),
            timeframe=timeframe,
            timestamp=timestamp,
            action=action,
            strategy_name=strategy_name,
            close=close,
            indicators=dict(indicators),
        )

    def evaluate_active(
        self,
        *,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
        close: float,
        indicators: dict[str, Any],
        active_strategies: Optional[list[str]] = None,
    ) -> list[StrategySignal]:
        """Evaluate one or more active strategies for the current bar.

        Args:
            symbol: Ticker symbol.
            timeframe: Bar interval being evaluated.
            timestamp: Timestamp of the current bar.
            close: Latest close price.
            indicators: Latest indicator values for the symbol/timeframe.
            active_strategies: Strategy names to evaluate. Defaults to all
                registered strategies for the timeframe.

        Returns:
            One strategy signal per evaluated strategy.
        """
        strategies = active_strategies or self._registry.list_strategies()
        return [
            self.evaluate(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                close=close,
                indicators=indicators,
                strategy_name=name,
            )
            for name in strategies
            if self._registry.get(name).timeframe == timeframe
        ]

    def _validate_required_indicators(
        self,
        required: tuple[str, ...],
        indicators: dict[str, Any],
    ) -> None:
        """Ensure a strategy's required indicator values are present."""
        missing = [name for name in required if name not in indicators]
        if missing:
            raise ValueError(f"Missing required indicators: {missing}")

    def _hold_signal(
        self,
        *,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
        close: float,
        indicators: dict[str, Any],
        strategy_name: str,
    ) -> StrategySignal:
        """Return HOLD when a strategy does not apply to the current timeframe."""
        return StrategySignal(
            symbol=symbol.upper(),
            timeframe=timeframe,
            timestamp=timestamp,
            action=SignalAction.HOLD,
            strategy_name=strategy_name,
            close=close,
            indicators=dict(indicators),
        )


if __name__ == "__main__":
    from strategy_registry import build_default_registry

    registry = build_default_registry()
    evaluator = SignalEvaluator(registry)

    signal = evaluator.evaluate(
        symbol="AAPL",
        timeframe="5m",
        timestamp=datetime.now(),
        close=185.4,
        indicators={"rsi": 28.5},
        strategy_name="rsi_mean_reversion",
    )
    print(signal.to_dict())
