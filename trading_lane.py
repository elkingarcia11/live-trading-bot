"""Multi-lane forward-test trading: one shared feed, isolated paper state per lane."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from typing import Iterator, Optional

from config import (
    AppConfig,
    EmaPairConfig,
    resolve_transactions_csv_path,
)
from forward_test_account import ForwardTestAccount
from indicator_coordinator import IndicatorCoordinator, SymbolIndicatorConfig
from signal_evaluator import SignalEvaluator
from strategy_registry import StrategyRegistry, build_default_registry
from transaction_ledger import TransactionLedger

logger = logging.getLogger(__name__)

_active_lane: ContextVar[Optional["TradingLaneRuntime"]] = ContextVar(
    "active_trading_lane",
    default=None,
)


@dataclass(frozen=True)
class TradingLaneConfig:
    """One strategy lane (timeframe + optional MA legs + dollar budget)."""

    timeframe: str
    fast_gma_length: Optional[int] = None
    fast_gma_sigma: Optional[float] = None
    slow_gma_length: Optional[int] = None
    slow_gma_sigma: Optional[float] = None
    fast_ema_period: Optional[int] = None
    slow_ema_period: Optional[int] = None
    position_size: Optional[float] = None
    stop_loss_pct: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    lane_id: str = ""

    def __post_init__(self) -> None:
        from config import _validate_timeframe

        tf = _validate_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", tf)
        if not self.lane_id.strip():
            object.__setattr__(self, "lane_id", tf)
        if self.position_size is not None and self.position_size <= 0:
            raise ValueError("position_size must be positive when set")
        for name, value in (
            ("fast_gma_length", self.fast_gma_length),
            ("slow_gma_length", self.slow_gma_length),
            ("fast_ema_period", self.fast_ema_period),
            ("slow_ema_period", self.slow_ema_period),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when set")
        _validate_lane_stop_pct(self.stop_loss_pct, field="stop_loss_pct")
        _validate_lane_stop_pct(self.trailing_stop_pct, field="trailing_stop_pct")


@dataclass
class TradingLaneRuntime:
    """Isolated indicator, strategy, paper account, and position state for one lane."""

    lane: TradingLaneConfig
    app_config: AppConfig
    indicator_coordinator: IndicatorCoordinator
    strategy_registry: StrategyRegistry
    signal_evaluator: SignalEvaluator
    position_tracker: "PositionTracker"
    risk_guard: "RiskGuard"
    strategy_state: dict[tuple[str, str], dict[str, object]] = field(
        default_factory=dict
    )
    forward_test_account: Optional[ForwardTestAccount] = None
    transaction_ledger: Optional[TransactionLedger] = None


def parse_trading_lanes(payload: object) -> tuple[TradingLaneConfig, ...]:
    """Parse the ``lanes`` array from config.json."""
    if payload is None:
        return ()
    if not isinstance(payload, (list, tuple)):
        raise ValueError("lanes must be a JSON array")
    lanes: list[TradingLaneConfig] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"lanes[{index}] must be an object")
        timeframe = str(item.get("timeframe", "")).strip()
        if not timeframe:
            raise ValueError(f"lanes[{index}].timeframe is required")
        lanes.append(
            TradingLaneConfig(
                timeframe=timeframe,
                lane_id=str(item.get("lane_id", timeframe)).strip() or timeframe,
                fast_gma_length=_optional_positive_int(item, "fast_gma_length"),
                fast_gma_sigma=_optional_float(item, "fast_gma_sigma"),
                slow_gma_length=_optional_positive_int(item, "slow_gma_length"),
                slow_gma_sigma=_optional_float(item, "slow_gma_sigma"),
                fast_ema_period=_optional_positive_int(item, "fast_ema_period"),
                slow_ema_period=_optional_positive_int(item, "slow_ema_period"),
                position_size=(
                    float(item["position_size"])
                    if item.get("position_size") not in (None, "")
                    else None
                ),
                stop_loss_pct=_parse_lane_stop_pct(item, "stop_loss_pct"),
                trailing_stop_pct=_parse_lane_stop_pct(item, "trailing_stop_pct"),
            )
        )
    if not lanes:
        return ()
    timeframes = [lane.timeframe for lane in lanes]
    if len(timeframes) != len(set(timeframes)):
        raise ValueError("lane timeframes must be unique")
    return tuple(lanes)


def lane_tick_timeframes(lanes: tuple[TradingLaneConfig, ...]) -> tuple[str, ...]:
    """Return sorted unique tick timeframes for Databento fan-out."""
    return tuple(sorted({lane.timeframe for lane in lanes}, key=_timeframe_sort_key))


def build_lane_app_config(base: AppConfig, lane: TradingLaneConfig) -> AppConfig:
    """Return an AppConfig copy with lane-specific MA, risk, and ledger path."""
    market = replace(
        base.market,
        stream_timeframe=lane.timeframe,
        strategy_timeframe=lane.timeframe,
    )
    indicators = base.indicators
    gma = indicators.gaussian_ma
    if gma is not None and any(
        value is not None
        for value in (
            lane.fast_gma_length,
            lane.fast_gma_sigma,
            lane.slow_gma_length,
            lane.slow_gma_sigma,
        )
    ):
        indicators = replace(
            indicators,
            gaussian_ma=replace(
                gma,
                fast=replace(
                    gma.fast,
                    length=lane.fast_gma_length or gma.fast.length,
                    sigma_divisor=lane.fast_gma_sigma or gma.fast.sigma_divisor,
                ),
                slow=replace(
                    gma.slow,
                    length=lane.slow_gma_length or gma.slow.length,
                    sigma_divisor=lane.slow_gma_sigma or gma.slow.sigma_divisor,
                ),
            ),
        )
    ema = indicators.ema
    if lane.fast_ema_period is not None or lane.slow_ema_period is not None:
        base_ema = ema or EmaPairConfig()
        indicators = replace(
            indicators,
            ema=replace(
                base_ema,
                fast_period=lane.fast_ema_period or base_ema.fast_period,
                slow_period=lane.slow_ema_period or base_ema.slow_period,
            ),
        )
    risk = base.risk
    if lane.position_size is not None:
        budget = float(lane.position_size)
        risk = replace(
            risk,
            position_size_max_dollars=budget,
            position_size_pct=1.0,
            max_position_quantity=max(risk.max_position_quantity, budget),
        )
    forward_test = replace(
        base.forward_test,
        transactions_csv_path=resolve_transactions_csv_path(
            base,
            strategy_timeframe=lane.timeframe,
        ),
    )
    options = base.options
    option_overrides = {
        key: value
        for key, value in (
            ("stop_loss_pct", lane.stop_loss_pct),
            ("trailing_stop_pct", lane.trailing_stop_pct),
        )
        if value is not None
    }
    if option_overrides:
        options = replace(options, **option_overrides)
    return replace(
        base,
        market=market,
        indicators=indicators,
        risk=risk,
        forward_test=forward_test,
        options=options,
    )


def build_lane_runtimes(
    base_config: "WorkflowConfig",
    lanes: tuple[TradingLaneConfig, ...],
) -> dict[str, TradingLaneRuntime]:
    """Materialize one runtime per lane keyed by timeframe."""
    from position_tracker import PositionTracker
    from workflow import RiskGuard, WorkflowConfig

    runtimes: dict[str, TradingLaneRuntime] = {}
    for lane in lanes:
        lane_app = build_lane_app_config(base_config.app, lane)
        lane_workflow = WorkflowConfig.from_app_config(lane_app)
        coordinator = IndicatorCoordinator(max_bars=lane_app.indicators.max_bars)
        jobs = lane_app.indicators.build_jobs(lane.timeframe)
        for symbol in base_config.stream_symbols:
            coordinator.register(SymbolIndicatorConfig(symbol=symbol, jobs=jobs))
        registry = build_default_registry(strategy_timeframe=lane.timeframe)
        evaluator = SignalEvaluator(registry)
        risk = RiskGuard(
            max_position_quantity=lane_workflow.max_position_quantity,
            max_trades_per_day=(
                lane_app.gex.max_trades_per_day if lane_app.gex.enabled else None
            ),
            max_daily_loss_dollars=(
                lane_app.gex.max_daily_loss_dollars if lane_app.gex.enabled else None
            ),
        )
        forward_test: Optional[ForwardTestAccount] = None
        ledger: Optional[TransactionLedger] = None
        if lane_workflow.email_forward_test:
            try:
                forward_test = ForwardTestAccount.from_app_config(lane_app)
            except Exception:
                logger.exception(
                    "Forward-test account unavailable for lane %s", lane.lane_id
                )
            tx_path = lane_app.forward_test.transactions_csv_path.strip()
            if tx_path:
                ledger = TransactionLedger(tx_path)
        runtime = TradingLaneRuntime(
            lane=lane,
            app_config=lane_app,
            indicator_coordinator=coordinator,
            strategy_registry=registry,
            signal_evaluator=evaluator,
            position_tracker=PositionTracker(),
            risk_guard=risk,
            forward_test_account=forward_test,
            transaction_ledger=ledger,
        )
        runtimes[lane.timeframe] = runtime
        ema = lane_app.indicators.ema
        if ema is not None:
            ma_desc = f"EMA fast={ema.fast_period} slow={ema.slow_period}"
        else:
            gma = lane_app.indicators.gaussian_ma
            if gma is not None:
                ma_desc = (
                    f"GMA fast={gma.fast.length}/{gma.fast.sigma_divisor:.2f} "
                    f"slow={gma.slow.length}/{gma.slow.sigma_divisor:.2f}"
                )
            else:
                ma_desc = "no MA"
        logger.info(
            "Lane %s: %s %s budget=$%s stop=%s trail=%s ledger=%s",
            lane.lane_id,
            lane.timeframe,
            ma_desc,
            f"{lane.position_size:,.0f}" if lane.position_size else "default",
            _format_stop_pct(lane_app.options.stop_loss_pct),
            _format_stop_pct(lane_app.options.trailing_stop_pct),
            lane_app.forward_test.transactions_csv_path,
        )
        if forward_test is not None:
            logger.info(
                "Lane %s forward-test: %s",
                lane.lane_id,
                forward_test.summary_line(),
            )
    return runtimes


def active_lane() -> Optional[TradingLaneRuntime]:
    """Return the lane currently executing on this thread."""
    return _active_lane.get()


@contextmanager
def lane_context(lane: TradingLaneRuntime) -> Iterator[TradingLaneRuntime]:
    """Bind lane-scoped position/account/config accessors for one trading path."""
    token = _active_lane.set(lane)
    try:
        yield lane
    finally:
        _active_lane.reset(token)


def iter_lane_positions(
    runtimes: dict[str, TradingLaneRuntime],
) -> Iterator[tuple[TradingLaneRuntime, "Position"]]:
    """Yield every open option position across all lanes."""
    from position_tracker import Position

    for runtime in runtimes.values():
        for position in runtime.position_tracker.list_positions():
            if position.asset_type == "OPTION" and abs(position.quantity) > 0:
                yield runtime, position


def _timeframe_sort_key(timeframe: str) -> tuple[int, str]:
    from tick_bar_builder import is_tick_timeframe, parse_tick_timeframe

    if is_tick_timeframe(timeframe):
        return (parse_tick_timeframe(timeframe), timeframe)
    return (10_000, timeframe)


def _optional_positive_int(item: dict[str, object], field: str) -> Optional[int]:
    if field not in item or item[field] in (None, ""):
        return None
    return int(item[field])


def _optional_float(item: dict[str, object], field: str) -> Optional[float]:
    if field not in item or item[field] in (None, ""):
        return None
    return float(item[field])


def _parse_lane_stop_pct(item: dict[str, object], field: str) -> Optional[float]:
    """Parse an optional lane stop/trail pct; omitted keys inherit global options."""
    if field not in item or item[field] in (None, ""):
        return None
    value = float(item[field])
    _validate_lane_stop_pct(value, field=field)
    return value


def _validate_lane_stop_pct(value: Optional[float], *, field: str) -> None:
    if value is None:
        return
    if not 0.0 < value < 1.0:
        raise ValueError(f"lanes.{field} must be between 0 and 1 (exclusive)")


def _format_stop_pct(value: Optional[float]) -> str:
    if value is None:
        return "off"
    return f"{value:.0%}"
