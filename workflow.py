"""Workflow.

Responsibility: Main live-trading pipeline orchestration.

Wires ingest, process, strategy, and execute layers together through the event
bus. Cross-cutting trade logging and health monitoring subscribe passively.
Does not own low-level transport, indicator math, or broker protocol details.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional

from data_aggregator import AggregatedBar, DataAggregator
from event_bus import EventBus, Topics
from health_monitor import HealthMonitor
from indicator_calculator import (
    DEFAULT_DEMA_PERIOD,
    DEFAULT_DEMA_SOURCE,
    DEFAULT_SUPERTREND_ATR_PERIOD,
    DEFAULT_SUPERTREND_CHANGE_ATR,
    DEFAULT_SUPERTREND_MULTIPLIER,
    DEFAULT_SUPERTREND_SOURCE,
)
from indicator_coordinator import (
    IndicatorCoordinator,
    IndicatorSnapshot,
    SymbolIndicatorConfig,
    build_dema_job,
    build_supertrend_job,
)
from order_manager import (
    BrokerGateway,
    FillEvent,
    Order,
    OrderManager,
    OrderSide,
    TradingSignal,
)
from schwab_broker_gateway import build_broker_gateway
from position_tracker import ExitNotification, PositionTracker
from signal_evaluator import SignalEvaluator, StrategySignal
from strategy_registry import SignalAction, StrategyRegistry, build_default_registry
from schwab_account_sync import SchwabAccountSync
from schwab_streamer import SchwabStreamSession, build_schwab_stream_processor
from stream_connection_manager import ConnectionState, StreamConnectionManager
from stream_data_processor import CleanBarEvent, StreamDataProcessor
from trade_logger import RiskDecisionRecord, TradeLogger

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "TSLA", "AMZN", "NVDA")


StreamProvider = Literal["generic", "schwab"]


@dataclass(frozen=True)
class WorkflowConfig:
    """Runtime configuration for the live trading workflow."""

    websocket_url: str = ""
    stream_provider: StreamProvider = "schwab"
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    strategies: tuple[str, ...] = ("dema_trend",)
    strategy_timeframe: str = "5m"
    dema_period: int = DEFAULT_DEMA_PERIOD
    dema_source: str = DEFAULT_DEMA_SOURCE
    supertrend_atr_period: int = DEFAULT_SUPERTREND_ATR_PERIOD
    supertrend_source: str = DEFAULT_SUPERTREND_SOURCE
    supertrend_multiplier: float = DEFAULT_SUPERTREND_MULTIPLIER
    supertrend_change_atr: bool = DEFAULT_SUPERTREND_CHANGE_ATR
    order_quantity: float = 10.0
    audit_log_path: str = "logs/audit.jsonl"
    health_check_interval_seconds: float = 30.0
    max_position_quantity: float = 100.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    subscribe_on_connect: bool = True
    sync_broker_positions_on_start: bool = False
    schwab_account_hash: Optional[str] = None
    schwab_account_number: Optional[str] = None
    broker_use_in_memory: bool = True
    broker_fill_price: float = 100.0
    schwab_preview_orders: bool = False

    def __post_init__(self) -> None:
        normalized = tuple(symbol.upper() for symbol in self.symbols)
        if not normalized:
            raise ValueError("At least one symbol is required")
        object.__setattr__(self, "symbols", normalized)

        if self.stream_provider == "generic" and not self.websocket_url:
            raise ValueError("websocket_url is required when stream_provider='generic'")

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> WorkflowConfig:
        """Build workflow configuration from environment variables."""
        if load_dotenv:
            from schwab_auth import _load_dotenv

            _load_dotenv()

        symbols = _env_symbols(
            os.getenv("WATCHLIST_SYMBOLS"),
            fallback=DEFAULT_SYMBOLS,
        )
        stream_provider = os.getenv("STREAM_PROVIDER", "schwab").strip().lower()
        if stream_provider not in {"generic", "schwab"}:
            raise ValueError("STREAM_PROVIDER must be 'generic' or 'schwab'")

        return cls(
            websocket_url=os.getenv("WEBSOCKET_URL", os.getenv("SCHWAB_STREAMER_URL", "")),
            stream_provider=stream_provider,  # type: ignore[arg-type]
            symbols=symbols,
            strategies=_env_tuple(os.getenv("STRATEGIES"), fallback=("dema_trend",)),
            strategy_timeframe=os.getenv("STRATEGY_TIMEFRAME", "5m"),
            dema_period=int(os.getenv("DEMA_PERIOD", "200")),
            dema_source=os.getenv("DEMA_SOURCE", "close"),
            supertrend_atr_period=int(os.getenv("SUPERTREND_ATR_PERIOD", "12")),
            supertrend_source=os.getenv("SUPERTREND_SOURCE", "hl2"),
            supertrend_multiplier=float(os.getenv("SUPERTREND_MULTIPLIER", "3.0")),
            supertrend_change_atr=_env_bool("SUPERTREND_CHANGE_ATR", True),
            order_quantity=float(os.getenv("ORDER_QUANTITY", "10")),
            audit_log_path=os.getenv("AUDIT_LOG_PATH", "logs/audit.jsonl"),
            health_check_interval_seconds=float(
                os.getenv("HEALTH_CHECK_INTERVAL_SECONDS", "30")
            ),
            max_position_quantity=float(os.getenv("MAX_POSITION_QUANTITY", "100")),
            stop_loss=_env_optional_float("STOP_LOSS"),
            take_profit=_env_optional_float("TAKE_PROFIT"),
            trailing_stop_distance=_env_optional_float("TRAILING_STOP_DISTANCE"),
            subscribe_on_connect=_env_bool("STREAM_SUBSCRIBE_ON_CONNECT", True),
            sync_broker_positions_on_start=_env_bool(
                "SCHWAB_SYNC_POSITIONS_ON_START",
                False,
            ),
            schwab_account_hash=os.getenv("SCHWAB_ACCOUNT_HASH") or None,
            schwab_account_number=os.getenv("SCHWAB_ACCOUNT_NUMBER") or None,
            broker_use_in_memory=_env_bool("BROKER_USE_IN_MEMORY", True),
            broker_fill_price=float(os.getenv("BROKER_SIMULATED_FILL_PRICE", "100")),
            schwab_preview_orders=_env_bool("SCHWAB_PREVIEW_ORDERS", False),
        )


class RiskGuard:
    """Pre-trade validation layer between strategy signals and order submission."""

    def __init__(
        self,
        *,
        max_position_quantity: float = 100.0,
        order_quantity: float = 10.0,
    ) -> None:
        self._max_position_quantity = max_position_quantity
        self._order_quantity = order_quantity

    def evaluate(
        self,
        signal: StrategySignal,
        *,
        current_quantity: float,
    ) -> RiskDecisionRecord:
        """Approve or block a strategy signal before order submission."""
        if signal.action == SignalAction.HOLD:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=True,
                reason="hold signal requires no order",
                strategy_name=signal.strategy_name,
            )

        projected = current_quantity
        if signal.action == SignalAction.BUY:
            projected += self._order_quantity
        elif signal.action == SignalAction.SELL:
            projected -= self._order_quantity

        if abs(projected) > self._max_position_quantity:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=False,
                reason="projected position exceeds max_position_quantity",
                strategy_name=signal.strategy_name,
            )

        if signal.action == SignalAction.SELL and current_quantity <= 0:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=False,
                reason="no long position to sell",
                strategy_name=signal.strategy_name,
            )

        return RiskDecisionRecord(
            symbol=signal.symbol,
            approved=True,
            reason="passed pre-trade checks",
            strategy_name=signal.strategy_name,
        )


class TradingWorkflow:
    """Event-bus-backed orchestrator for the full live trading pipeline."""

    def __init__(
        self,
        config: WorkflowConfig,
        *,
        bus: Optional[EventBus] = None,
        broker: Optional[BrokerGateway] = None,
        registry: Optional[StrategyRegistry] = None,
    ) -> None:
        self._config = config
        self._symbols = config.symbols

        self.bus = bus or EventBus()
        self.trade_logger = TradeLogger(self.bus, log_path=config.audit_log_path)
        self.health_monitor = HealthMonitor(self.bus)

        if config.stream_provider == "schwab":
            stream_service = os.getenv("SCHWAB_STREAM_SERVICE", "CHART_EQUITY")
            if stream_service != "CHART_EQUITY":
                logger.warning(
                    "SCHWAB_STREAM_SERVICE=%s is not fully supported; using CHART_EQUITY bars",
                    stream_service,
                )
            self.stream_processor = build_schwab_stream_processor(
                symbols=self._symbols,
                consumers=[self._on_clean_bar],
            )
            self._schwab_stream = SchwabStreamSession.from_env(
                symbols=self._symbols,
                processor=self.stream_processor,
                subscribe_on_connect=config.subscribe_on_connect,
                on_open_external=self._on_stream_connected,
                on_close_external=self._on_stream_closed,
                on_error_external=self._on_stream_error,
            )
            self._stream_manager = None
        else:
            self._schwab_stream = None
            self.stream_processor = StreamDataProcessor(
                symbols=self._symbols,
                consumers=[self._on_clean_bar],
            )
            self._stream_manager = StreamConnectionManager(
                config.websocket_url,
                on_message=self.stream_processor.process_message,
                on_open=self._on_stream_connected,
                on_close=self._on_stream_closed,
                on_error=self._on_stream_error,
            )
        self.aggregator = DataAggregator()
        self.indicator_coordinator = IndicatorCoordinator()
        self.strategy_registry = registry or build_default_registry()
        self.signal_evaluator = SignalEvaluator(self.strategy_registry)
        self.risk_guard = RiskGuard(
            max_position_quantity=config.max_position_quantity,
            order_quantity=config.order_quantity,
        )

        resolved_broker = broker or build_broker_gateway(
            use_in_memory=config.broker_use_in_memory,
            fill_price=config.broker_fill_price,
        )
        self.order_manager = OrderManager(
            resolved_broker,
            on_update=self._on_order_update,
        )
        self.position_tracker = PositionTracker(exit_handlers=[self._on_position_exit])

        self._health_thread: Optional[threading.Thread] = None
        self._stop_health = threading.Event()
        self._started = False
        self._account_sync = (
            SchwabAccountSync.from_env()
            if config.stream_provider == "schwab"
            else None
        )

        self._register_indicator_jobs()
        self._wire_passive_listeners()

    def start(self) -> None:
        """Start passive listeners, health checks, and the market data stream."""
        if self._started:
            return

        self.trade_logger.start()
        self.health_monitor.start()
        self._start_health_checks()
        if self._config.sync_broker_positions_on_start:
            self._sync_broker_positions()
        if self._schwab_stream is not None:
            self._schwab_stream.refresh_streamer_info()
            self._schwab_stream.connect()
        else:
            self.stream_manager.connect()
        self._started = True
        logger.info("TradingWorkflow started for %s", ", ".join(self._symbols))

    def stop(self) -> None:
        """Stop health checks and disconnect the market data stream."""
        self._stop_health.set()
        if self._health_thread is not None:
            self._health_thread.join(timeout=2.0)
        if self._schwab_stream is not None:
            self._schwab_stream.disconnect()
        else:
            self.stream_manager.disconnect()
        self._started = False
        logger.info("TradingWorkflow stopped for %s", ", ".join(self._symbols))

    def process_clean_bar(self, bar: CleanBarEvent) -> None:
        """Run one clean 1-minute bar through the full pipeline.

        Useful for simulations without a live WebSocket connection.
        """
        self._on_clean_bar(bar)

    @property
    def symbols(self) -> tuple[str, ...]:
        """Return the configured watchlist symbols."""
        return self._symbols

    @property
    def stream_manager(self) -> StreamConnectionManager:
        """Return the active stream transport manager."""
        if self._schwab_stream is not None:
            return self._schwab_stream.connection_manager
        if self._stream_manager is None:
            raise RuntimeError("Stream manager is not configured")
        return self._stream_manager

    @property
    def connection_state(self) -> ConnectionState:
        """Return the current WebSocket connection state."""
        return self.stream_manager.state

    def _wire_passive_listeners(self) -> None:
        """Start cross-cutting listeners that subscribe via the event bus."""
        # TradeLogger and HealthMonitor subscribe during their start() methods.

    def _sync_broker_positions(self) -> None:
        """Load broker positions into the local position tracker."""
        if self._account_sync is None:
            logger.warning("Broker position sync requested but Schwab sync is unavailable")
            return

        try:
            snapshot = self._account_sync.sync_positions(
                self.position_tracker,
                watchlist=self._symbols,
                account_hash=self._config.schwab_account_hash,
                account_number=self._config.schwab_account_number,
            )
            self.bus.publish(
                Topics.POSITION_SYNC,
                {
                    "account_number": snapshot.account_number,
                    "equity": snapshot.balances.equity,
                    "buying_power": snapshot.balances.buying_power,
                    "positions": [
                        {
                            "symbol": position.symbol,
                            "quantity": position.quantity,
                            "average_price": position.average_price,
                        }
                        for position in snapshot.positions
                    ],
                },
                source="schwab_account_sync",
            )
        except Exception as exc:
            logger.exception("Failed to sync Schwab account positions: %s", exc)
            self.bus.publish(
                Topics.STREAM_ERROR,
                {"error": f"account position sync failed: {exc}"},
                source="schwab_account_sync",
            )

    def _register_indicator_jobs(self) -> None:
        """Configure indicator jobs for each watchlist symbol."""
        jobs = (
            build_dema_job(
                self._config.strategy_timeframe,
                period=self._config.dema_period,
                source=self._config.dema_source,
            ),
            build_supertrend_job(
                self._config.strategy_timeframe,
                atr_period=self._config.supertrend_atr_period,
                source=self._config.supertrend_source,
                multiplier=self._config.supertrend_multiplier,
                change_atr=self._config.supertrend_change_atr,
            ),
        )
        for symbol in self._symbols:
            self.indicator_coordinator.register(
                SymbolIndicatorConfig(symbol=symbol, jobs=jobs)
            )

    def _on_clean_bar(self, bar: CleanBarEvent) -> None:
        """Publish and process a validated 1-minute bar."""
        self.bus.publish(Topics.BAR_CLEAN, bar, source="stream_data_processor")
        self._run_process_and_strategy_layers(bar)

    def _run_process_and_strategy_layers(self, bar: CleanBarEvent) -> None:
        """Aggregate bars, calculate indicators, and evaluate strategies."""
        started = time.perf_counter()
        aggregated_bars = self.aggregator.on_bar(bar)

        for aggregated in aggregated_bars:
            self.bus.publish(
                Topics.BAR_AGGREGATED,
                aggregated,
                source="data_aggregator",
            )
            snapshot = self._dispatch_indicator_jobs(aggregated, started)
            if aggregated.is_complete and snapshot is not None:
                self._evaluate_strategies(aggregated, snapshot)

        self.position_tracker.update_price(
            bar.symbol,
            bar.close,
            timestamp=bar.timestamp,
        )

    def _dispatch_indicator_jobs(
        self,
        aggregated: AggregatedBar,
        started: float,
    ) -> Optional[IndicatorSnapshot]:
        """Run indicator jobs and publish the latest snapshot."""
        snapshot = self.indicator_coordinator.on_aggregated_bar(aggregated)
        if snapshot is None:
            return None

        duration_ms = (time.perf_counter() - started) * 1000.0
        self.bus.publish(
            Topics.INDICATORS_SNAPSHOT,
            snapshot,
            source="indicator_coordinator",
            metadata={"duration_ms": duration_ms},
        )
        return snapshot

    def _evaluate_strategies(
        self,
        aggregated: AggregatedBar,
        snapshot: IndicatorSnapshot,
    ) -> None:
        """Evaluate active strategies and route actionable signals to execution."""
        if aggregated.timeframe != self._config.strategy_timeframe:
            return

        for strategy_name in self._config.strategies:
            strategy = self.strategy_registry.get(strategy_name)
            if not self._indicators_ready(strategy.required_indicators, snapshot.values):
                logger.debug(
                    "Skipping %s; indicators not ready for %s",
                    strategy_name,
                    aggregated.symbol,
                )
                continue

            try:
                signal = self.signal_evaluator.evaluate(
                    symbol=aggregated.symbol,
                    timeframe=aggregated.timeframe,
                    timestamp=aggregated.timestamp,
                    close=aggregated.close,
                    indicators=snapshot.values,
                    strategy_name=strategy_name,
                )
            except ValueError as exc:
                logger.debug("Skipping %s evaluation: %s", strategy_name, exc)
                continue

            self.bus.publish(
                Topics.STRATEGY_SIGNAL,
                signal,
                source="signal_evaluator",
            )
            self._handle_strategy_signal(signal)

    def _indicators_ready(
        self,
        required: tuple[str, ...],
        values: dict[str, object],
    ) -> bool:
        """Return whether required indicator values are available."""
        return all(name in values and values[name] is not None for name in required)

    def _handle_strategy_signal(self, signal: StrategySignal) -> None:
        """Run risk checks and submit broker orders for actionable signals."""
        if signal.action == SignalAction.HOLD:
            return

        position = self.position_tracker.get_position(signal.symbol)
        current_quantity = position.quantity if position is not None else 0.0
        decision = self.risk_guard.evaluate(
            signal,
            current_quantity=current_quantity,
        )
        self.bus.publish(Topics.RISK_DECISION, decision, source="risk_guard")

        if not decision.approved:
            logger.info("Risk guard blocked %s for %s", signal.action.value, signal.symbol)
            return

        side = (
            OrderSide.BUY
            if signal.action == SignalAction.BUY
            else OrderSide.SELL
        )
        order = self.order_manager.submit_signal(
            TradingSignal(
                symbol=signal.symbol,
                side=side,
                quantity=self._config.order_quantity,
                signal_id=f"{signal.strategy_name}:{signal.timestamp.isoformat()}",
            )
        )
        self.order_manager.refresh_order(order.id)

    def _on_order_update(self, order: Order) -> None:
        """Publish order lifecycle events and update portfolio state."""
        self.bus.publish(Topics.ORDER_UPDATED, order, source="order_manager")

        fill = self.order_manager.to_fill_event(order)
        if fill is None:
            return

        self.bus.publish(Topics.ORDER_FILL, fill, source="order_manager")
        updated = self.position_tracker.on_fill(fill)
        if updated is not None and self._config.stop_loss is not None:
            self._apply_default_risk_targets(updated.symbol)

    def _apply_default_risk_targets(self, symbol: str) -> None:
        """Attach default stop/target settings to a newly opened position."""
        position = self.position_tracker.get_position(symbol)
        if position is None:
            return

        self.position_tracker.open_position(
            symbol=symbol,
            quantity=position.quantity,
            entry_price=position.average_entry_price,
            opened_at=position.opened_at,
            stop_loss=self._config.stop_loss,
            take_profit=self._config.take_profit,
            trailing_stop_distance=self._config.trailing_stop_distance,
        )

    def _on_position_exit(self, notification: ExitNotification) -> None:
        """Publish stop/target exits and submit flattening orders."""
        self.bus.publish(Topics.POSITION_EXIT, notification, source="position_tracker")

        quantity = abs(notification.position.quantity)
        if quantity <= 0:
            return

        side = OrderSide.SELL if notification.position.quantity > 0 else OrderSide.BUY
        order = self.order_manager.submit_signal(
            TradingSignal(
                symbol=notification.symbol,
                side=side,
                quantity=quantity,
                signal_id=f"exit:{notification.reason.value}",
            )
        )
        self.order_manager.refresh_order(order.id)
        self.position_tracker.close_position(notification.symbol)

    def _on_stream_connected(self) -> None:
        """Publish stream connection events and subscribe to watchlist symbols."""
        stream_url = self.stream_manager.url
        self.bus.publish(
            Topics.STREAM_CONNECTED,
            {
                "url": stream_url,
                "symbols": list(self._symbols),
                "provider": self._config.stream_provider,
            },
            source="stream_connection_manager",
        )
        if self._config.stream_provider == "schwab":
            return
        if self._config.subscribe_on_connect:
            self._subscribe_symbols()

    def _subscribe_symbols(self) -> None:
        """Send a provider-specific subscribe payload for all watchlist symbols."""
        import json

        message = json.dumps(
            {
                "action": "subscribe",
                "symbols": list(self._symbols),
                "timeframe": "1m",
            }
        )
        try:
            self.stream_manager.send(message)
            logger.info("Subscribed to symbols: %s", ", ".join(self._symbols))
        except Exception:
            logger.exception("Failed to subscribe to watchlist symbols")

    def _on_stream_closed(self, code: Optional[int], reason: Optional[str]) -> None:
        """Publish stream disconnect events for observability."""
        self.bus.publish(
            Topics.STREAM_DISCONNECTED,
            {"code": code, "reason": reason},
            source="stream_connection_manager",
        )
        self.bus.publish(
            Topics.STREAM_RECONNECTING,
            {"url": self.stream_manager.url, "provider": self._config.stream_provider},
            source="stream_connection_manager",
        )

    def _on_stream_error(self, error: Exception) -> None:
        """Publish stream errors for observability."""
        self.bus.publish(
            Topics.STREAM_ERROR,
            {"error": str(error)},
            source="stream_connection_manager",
        )

    def _start_health_checks(self) -> None:
        """Run periodic health checks in a background thread."""
        self._stop_health.clear()
        self._health_thread = threading.Thread(
            target=self._health_loop,
            name="workflow-health-check",
            daemon=True,
        )
        self._health_thread.start()

    def _health_loop(self) -> None:
        """Periodically evaluate and publish health snapshots."""
        interval = self._config.health_check_interval_seconds
        while not self._stop_health.wait(interval):
            try:
                self.health_monitor.check()
            except Exception:
                logger.exception("Health check failed")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_tuple(name: Optional[str], *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if not name:
        return fallback
    values = tuple(item.strip() for item in name.split(",") if item.strip())
    return values or fallback


def _env_symbols(name: Optional[str], *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    symbols = _env_tuple(name, fallback=fallback)
    return tuple(symbol.upper() for symbol in symbols)


def _env_optional_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return float(raw)


if __name__ == "__main__":
    import json
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    use_live_schwab = os.getenv("RUN_SCHWAB_STREAM", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if use_live_schwab:
        workflow = TradingWorkflow(WorkflowConfig.from_env())
        workflow.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            workflow.stop()
        raise SystemExit(0)

    workflow = TradingWorkflow(
        WorkflowConfig(
            websocket_url="wss://echo.websocket.events",
            stream_provider="generic",
            symbols=DEFAULT_SYMBOLS,
            strategies=("dema_trend",),
            dema_period=200,
            dema_source="close",
            supertrend_atr_period=12,
            supertrend_source="hl2",
            supertrend_multiplier=3.0,
            order_quantity=10,
            stop_loss=180.0,
            take_profit=190.0,
            trailing_stop_distance=1.5,
            subscribe_on_connect=False,
            broker_use_in_memory=True,
            broker_fill_price=185.0,
        ),
    )

    workflow.trade_logger.start()
    workflow.health_monitor.start()

    base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    for offset in range(30):
        for index, symbol in enumerate(DEFAULT_SYMBOLS):
            workflow.process_clean_bar(
                CleanBarEvent(
                    symbol=symbol,
                    timeframe="1m",
                    timestamp=base + timedelta(minutes=offset),
                    open=180 + offset * 0.1 + index,
                    high=181 + offset * 0.1 + index,
                    low=179 + offset * 0.1 + index,
                    close=180.5 + offset * 0.1 + index,
                    volume=1000 + index * 100,
                )
            )

    snapshot = workflow.health_monitor.check()
    print("Health:", json.dumps(snapshot.to_dict(), indent=2))
    print("Open positions:", [p.symbol for p in workflow.position_tracker.list_positions()])
