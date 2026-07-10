"""Workflow.

Responsibility: Main live-trading pipeline orchestration.

Wires ingest, process, strategy, and execute layers together through the event
bus. Cross-cutting trade logging and health monitoring subscribe passively.
Does not own low-level transport, indicator math, or broker protocol details.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from bar_alignment import aggregation_checkpoint, align_bucket_start, last_completed_minute, to_utc
from cloud_storage_repository import CloudStorageRepository
from data_aggregator import AggregatedBar, DataAggregator
from event_bus import EventBus, Topics
from config import AppConfig, load_config
from health_monitor import HealthMonitor, HealthThresholds
from indicator_coordinator import (
    IndicatorCoordinator,
    IndicatorSnapshot,
    SymbolIndicatorConfig,
)
from order_manager import (
    BrokerGateway,
    FillEvent,
    Order,
    OrderManager,
    OrderSide,
    TradingSignal,
)
from position_reconciliation import option_position_aligned_with_gaussian
from position_sizer import contracts_for_buy, shares_for_buy
from option_selector import (
    SelectedOption,
    contract_mark_from_chain,
    contract_quote_from_chain,
    days_to_expiration_for_occ,
    option_contract_type,
    option_is_expired,
    parse_occ_symbol,
    resolve_option_exit_from_chain,
    select_atm_call_from_chain,
    select_atm_put_from_chain,
    synthetic_atm_call,
    synthetic_atm_put,
)
from option_quote import OptionQuoteSnapshot
from ibkr_tws_account_sync import IbkrTwsAccountSync, IbkrTwsAccountSnapshot
from ibkr_tws_connection import IbkrTwsRuntime
from ibkr_tws_streamer import (
    IbkrTwsStreamSession,
    build_ibkr_tws_stream_processor,
)
from schwab_market_data_client import SchwabMarketDataClient
from schwab_options_chain_client import SchwabOptionsChainClient
from schwab_broker_gateway import build_broker_gateway
from position_tracker import ExitNotification, ExitReason, Position, PositionTracker
from signal_evaluator import SignalEvaluator, StrategySignal
from strategy_registry import (
    SignalAction,
    StrategyEvaluationContext,
    StrategyRegistry,
    build_default_registry,
)
from schwab_account_sync import SchwabAccountSync
from schwab_trader_client import SchwabAccountSnapshot
from schwab_streamer import SchwabStreamSession, build_schwab_stream_processor
from stream_connection_manager import ConnectionState, StreamConnectionManager
from stream_data_processor import CleanBarEvent, StreamDataProcessor
from emailer import EmailerConfig, TradeEmailer, describe_conditions_met
from forward_test_account import ForwardTestAccount, ForwardTestFillResult
from gex_calculator import GexSnapshot
from gex_regime_monitor import GexRegimeMonitor
from gex_scalp_feedback import describe_gex_scalp_status
from market_session_scheduler import (
    EodSchedule,
    flatten_deadline_utc,
    is_at_or_past_flatten_time,
    is_regular_hours_timestamp_local,
    parse_hhmm,
    parse_utc_hhmm,
    should_flatten_positions,
    should_shutdown,
)
from trade_logger import RiskDecisionRecord, TradeLogger
from transaction_ledger import TransactionLedger, TransactionRecord
from zero_dte_contract_selector import (
    ZeroDteSelectionCriteria,
    select_zero_dte_contract,
)
from session_ohlcv_recorder import SessionOhlcvRecorder
from workflow_warmup import (
    build_storage_repository,
    indicator_warmup_needed,
    load_stored_bars,
    warm_start_gex,
    warm_start_pipeline,
)

logger = logging.getLogger(__name__)

_GEX_HOLD_LOG_INTERVAL_SECONDS = 300.0

StreamProvider = Literal["generic", "schwab", "ibkr"]


@dataclass(frozen=True)
class ResolvedTrade:
    """Broker order details after instrument and sizing resolution."""

    symbol: str
    underlying_symbol: str
    asset_type: str
    quantity: float
    mark_price: float
    description: str = ""
    option_quote: Optional[OptionQuoteSnapshot] = None


@dataclass(frozen=True)
class WorkflowConfig:
    """Runtime configuration for the live trading workflow."""

    app: AppConfig = field(default_factory=AppConfig)

    def __post_init__(self) -> None:
        if self.stream_provider == "generic" and not self.websocket_url:
            raise ValueError("websocket_url is required when stream_provider='generic'")

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.app.market.symbols

    @property
    def market_config(self):
        return self.app.market

    @property
    def indicator_config(self):
        return self.app.indicators

    @property
    def stream_provider(self) -> StreamProvider:
        return self.app.workflow.stream_provider

    @property
    def websocket_url(self) -> str:
        return self.app.workflow.websocket_url or self.app.stream.schwab_streamer_url

    @property
    def strategies(self) -> tuple[str, ...]:
        return self.app.strategies

    @property
    def risk(self):
        return self.app.risk

    @property
    def audit_log_path(self) -> str:
        return self.app.workflow.audit_log_path

    @property
    def health_check_interval_seconds(self) -> float:
        return self.app.health.check_interval_seconds

    @property
    def max_position_quantity(self) -> float:
        return self.app.risk.max_position_quantity

    @property
    def stop_loss(self) -> Optional[float]:
        return self.app.risk.stop_loss

    @property
    def take_profit(self) -> Optional[float]:
        return self.app.risk.take_profit

    @property
    def trailing_stop_distance(self) -> Optional[float]:
        return self.app.risk.trailing_stop_distance

    @property
    def managed_exits(self) -> bool:
        return self.app.risk.managed_exits

    @property
    def subscribe_on_connect(self) -> bool:
        return self.app.workflow.subscribe_on_connect

    @property
    def sync_broker_positions_on_start(self) -> bool:
        return self.app.broker.sync_positions_on_start

    @property
    def schwab_account_hash(self) -> Optional[str]:
        value = self.app.broker.account_hash
        return value or None

    @property
    def schwab_account_number(self) -> Optional[str]:
        value = self.app.broker.account_number
        return value or None

    @property
    def broker_use_in_memory(self) -> bool:
        return self.app.broker.use_in_memory

    @property
    def broker_fill_price(self) -> float:
        return self.app.broker.simulated_fill_price

    @property
    def schwab_preview_orders(self) -> bool:
        return self.app.broker.preview_orders

    @property
    def email_forward_test(self) -> bool:
        return self.app.email.forward_test

    @property
    def warmup_from_storage(self) -> bool:
        return self.app.workflow.warmup_from_storage

    @property
    def persist_session_bars(self) -> bool:
        return self.app.workflow.persist_session_bars

    @property
    def eod_schedule(self) -> EodSchedule:
        workflow = self.app.workflow
        historical = self.app.historical
        return EodSchedule(
            enabled=workflow.eod_enabled,
            flatten_time_utc=parse_utc_hhmm(workflow.eod_flatten_time_utc),
            shutdown_time_utc=parse_utc_hhmm(workflow.eod_shutdown_time_utc),
            trading_days_only=historical.trading_days_only,
        )

    @property
    def options_enabled(self) -> bool:
        return self.app.options.enabled

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> WorkflowConfig:
        """Build workflow configuration from config.json."""
        app = load_config(reload=load_dotenv)
        return cls.from_app_config(app)

    @classmethod
    def from_app_config(cls, app: AppConfig) -> WorkflowConfig:
        """Build workflow configuration from a loaded AppConfig."""
        return cls(app=app)


class RiskGuard:
    """Pre-trade validation layer between strategy signals and order submission."""

    def __init__(
        self,
        *,
        max_position_quantity: float = 100.0,
        max_trades_per_day: Optional[int] = None,
        max_daily_loss_dollars: Optional[float] = None,
    ) -> None:
        self._max_position_quantity = max_position_quantity
        self._max_trades_per_day = max_trades_per_day
        self._max_daily_loss_dollars = max_daily_loss_dollars
        self._trades_today = 0
        self._daily_realized_pnl = 0.0
        self._trade_date: Optional[date] = None

    def reset_daily_counters(self, trade_date: date) -> None:
        """Reset per-day trade counters when the session date changes."""
        if self._trade_date == trade_date:
            return
        self._trade_date = trade_date
        self._trades_today = 0
        self._daily_realized_pnl = 0.0

    def record_closed_trade_pnl(self, trade_date: date, pnl: float) -> None:
        """Track realized P&L from a closed trade for the daily loss limit."""
        self.reset_daily_counters(trade_date)
        self._daily_realized_pnl += pnl

    def record_opened_trade(self, trade_date: date) -> None:
        """Increment the daily trade count after a new entry is approved."""
        self.reset_daily_counters(trade_date)
        self._trades_today += 1

    def evaluate(
        self,
        signal: StrategySignal,
        *,
        current_quantity: float,
        order_quantity: float,
        option_entry_on_sell: bool = False,
    ) -> RiskDecisionRecord:
        """Approve or block a strategy signal before order submission."""
        if signal.action == SignalAction.HOLD:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=True,
                reason="hold signal requires no order",
                strategy_name=signal.strategy_name,
            )

        if signal.action in {SignalAction.BUY, SignalAction.SELL} and (
            self._max_trades_per_day is not None or self._max_daily_loss_dollars is not None
        ):
            trade_date = signal.timestamp.date()
            self.reset_daily_counters(trade_date)
            if (
                self._max_trades_per_day is not None
                and self._trades_today >= self._max_trades_per_day
            ):
                return RiskDecisionRecord(
                    symbol=signal.symbol,
                    approved=False,
                    reason="max trades per day reached",
                    strategy_name=signal.strategy_name,
                )
            if (
                self._max_daily_loss_dollars is not None
                and self._daily_realized_pnl <= -abs(self._max_daily_loss_dollars)
            ):
                return RiskDecisionRecord(
                    symbol=signal.symbol,
                    approved=False,
                    reason="daily max loss reached",
                    strategy_name=signal.strategy_name,
                )

        if order_quantity <= 0:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=False,
                reason="order quantity is zero",
                strategy_name=signal.strategy_name,
            )

        projected = current_quantity
        if signal.action == SignalAction.BUY or option_entry_on_sell:
            projected += order_quantity
        elif signal.action == SignalAction.SELL:
            projected -= order_quantity

        if abs(projected) > self._max_position_quantity:
            return RiskDecisionRecord(
                symbol=signal.symbol,
                approved=False,
                reason="projected position exceeds max_position_quantity",
                strategy_name=signal.strategy_name,
            )

        if (
            signal.action == SignalAction.SELL
            and current_quantity <= 0
            and not option_entry_on_sell
        ):
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
        health = config.app.health
        self.health_monitor = HealthMonitor(
            self.bus,
            thresholds=HealthThresholds(
                feed_stale_seconds=health.feed_stale_seconds,
                indicator_stale_seconds=health.indicator_stale_seconds,
                max_reconnects_per_hour=health.max_reconnects_per_hour,
                max_order_round_trip_seconds=health.max_order_round_trip_seconds,
                module_silence_seconds=health.module_silence_seconds,
                startup_grace_seconds=health.startup_grace_seconds,
            ),
        )

        if config.stream_provider == "schwab":
            stream = config.app.stream
            if stream.schwab_stream_service != "CHART_EQUITY":
                logger.warning(
                    "stream.schwab_stream_service=%s is not fully supported; using CHART_EQUITY bars",
                    stream.schwab_stream_service,
                )
            self.stream_processor = build_schwab_stream_processor(
                symbols=self._symbols,
                consumers=[self._on_clean_bar],
                timeframe=config.market_config.stream_timeframe,
                stream_settings=stream,
            )
            self._schwab_stream = SchwabStreamSession.from_env(
                symbols=self._symbols,
                processor=self.stream_processor,
                subscribe_on_connect=config.subscribe_on_connect,
                on_open_external=self._on_stream_connected,
                on_close_external=self._on_stream_closed,
                on_error_external=self._on_stream_error,
                on_option_quote=self._on_option_quote,
            )
            self._ibkr_tws_runtime = None
            self._ibkr_stream = None
            self._stream_manager = None
        elif config.stream_provider == "ibkr":
            self._schwab_stream = None
            self._stream_manager = None
            ibkr = config.app.ibkr
            self._ibkr_tws_runtime = IbkrTwsRuntime.from_config()
            self.stream_processor = build_ibkr_tws_stream_processor(
                symbols=self._symbols,
                consumers=[self._on_clean_bar],
                timeframe=config.market_config.stream_timeframe,
                stream_settings=config.app.stream,
            )
            self._ibkr_stream = IbkrTwsStreamSession(
                self._ibkr_tws_runtime,
                symbols=self._symbols,
                processor=self.stream_processor,
                exchange=ibkr.exchange,
                currency=ibkr.currency,
                tick_by_tick_type=ibkr.tick_by_tick_type,
                on_open_external=self._on_stream_connected,
                on_close_external=self._on_stream_closed,
                on_error_external=self._on_stream_error,
            )
        else:
            self._schwab_stream = None
            self._ibkr_tws_runtime = None
            self._ibkr_stream = None
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
        self.aggregator = DataAggregator(
            target_timeframes=config.market_config.aggregation_timeframes,
        )
        self.indicator_coordinator = IndicatorCoordinator(
            max_bars=config.indicator_config.max_bars,
        )
        self.strategy_registry = registry or build_default_registry(
            strategy_timeframe=config.market_config.strategy_timeframe,
        )
        self.signal_evaluator = SignalEvaluator(self.strategy_registry)
        self.risk_guard = RiskGuard(
            max_position_quantity=config.max_position_quantity,
            max_trades_per_day=(
                config.app.gex.max_trades_per_day if config.app.gex.enabled else None
            ),
            max_daily_loss_dollars=(
                config.app.gex.max_daily_loss_dollars if config.app.gex.enabled else None
            ),
        )

        resolved_broker = broker or build_broker_gateway(
            use_in_memory=config.broker_use_in_memory,
            fill_price=config.broker_fill_price,
            ibkr_runtime=self._ibkr_tws_runtime,
        )
        self.order_manager = OrderManager(
            resolved_broker,
            on_update=self._on_order_update,
        )
        self.position_tracker = PositionTracker(
            exit_handlers=(
                [self._on_position_exit] if config.managed_exits else []
            ),
        )
        self._trade_emailer: Optional[TradeEmailer] = None
        self._forward_test_account: Optional[ForwardTestAccount] = None
        self._transaction_ledger: Optional[TransactionLedger] = None
        transactions_path = config.app.forward_test.transactions_csv_path.strip()
        if transactions_path:
            self._transaction_ledger = TransactionLedger(transactions_path)
        if config.email_forward_test:
            self._trade_emailer = TradeEmailer(EmailerConfig.from_app_config(config.app))
            try:
                self._forward_test_account = ForwardTestAccount.from_app_config(
                    config.app
                )
                self._forward_test_account.restore_positions(self.position_tracker)
            except Exception:
                logger.exception("Forward-test account unavailable; using static balance")
            logger.info(
                "Forward-test mode enabled: approved signals email %s (no broker orders)",
                ", ".join(config.app.email.recipients),
            )
            if self._forward_test_account is not None:
                logger.info(
                    "Forward-test account: %s",
                    self._forward_test_account.summary_line(),
                )

        self._health_thread: Optional[threading.Thread] = None
        self._eod_thread: Optional[threading.Thread] = None
        self._stop_health = threading.Event()
        self._stop_eod = threading.Event()
        self._shutdown_requested = threading.Event()
        self._eod_flattened_on: Optional[date] = None
        self._eod_shutdown_on: Optional[date] = None
        self._zero_dte_flattened_on: Optional[date] = None
        self._started = False
        self._account_sync = (
            SchwabAccountSync.from_env()
            if config.stream_provider == "schwab"
            else (
                IbkrTwsAccountSync.from_runtime(self._ibkr_tws_runtime)
                if self._ibkr_tws_runtime is not None
                else (
                    IbkrTwsAccountSync.from_runtime(IbkrTwsRuntime.from_config())
                    if config.app.broker.provider == "ibkr"
                    else None
                )
            )
        )
        self._account_snapshot: Optional[
            SchwabAccountSnapshot | IbkrTwsAccountSnapshot
        ] = None
        self._market_data_client: Optional[SchwabMarketDataClient] = None
        self._session_recorder: Optional[SessionOhlcvRecorder] = None
        self._logged_first_live_bar = False
        self._live_regular_hours_seen = False
        self._flattening_contracts: set[str] = set()
        self._gex_monitor: Optional[GexRegimeMonitor] = None
        self._volume_history: dict[str, deque[float]] = {
            symbol: deque(maxlen=max(config.app.gex.volume_lookback_bars, 1))
            for symbol in self._symbols
        }
        self._gex_strategy_state: dict[str, dict[str, object]] = {
            symbol: {} for symbol in self._symbols
        }
        self._gex_status_log: dict[str, tuple[str, float]] = {}
        self._gex_waiting_snapshot_logged: set[str] = set()
        if config.app.gex.enabled:
            self._init_gex_monitor()
        if config.persist_session_bars:
            try:
                storage = build_storage_repository(config.app)
                persist_timeframes = (config.market_config.stream_timeframe,)
                if indicator_warmup_needed(config.app, config.strategies):
                    persist_timeframes = tuple(
                        dict.fromkeys(
                            (
                                config.market_config.stream_timeframe,
                                config.app.historical.timeframe,
                            )
                        )
                    )
                self._session_recorder = SessionOhlcvRecorder(
                    storage,
                    timeframes=persist_timeframes,
                    use_daily_partitions=config.app.gcs.use_daily_partitions,
                )
                logger.info(
                    "Session OHLCV recorder enabled for %s (flush on shutdown)",
                    ", ".join(persist_timeframes),
                )
            except Exception:
                logger.exception("Session OHLCV recorder unavailable")

        self._register_indicator_jobs()
        self._wire_passive_listeners()

    def start(self) -> None:
        """Start passive listeners, health checks, and the market data stream."""
        if self._started:
            return

        self.trade_logger.start()
        self.health_monitor.start()
        warm_start_gex(self)
        if self._config.warmup_from_storage and indicator_warmup_needed(
            self._config.app,
            self._config.strategies,
        ):
            warm_start_pipeline(self)
        elif self._config.warmup_from_storage:
            logger.info(
                "Skipping 3m historical warmup (GEX-only workflow with no active indicators)"
            )
        self._reconcile_expired_restored_positions()
        self._reconcile_restored_positions_with_trend()
        self._subscribe_open_option_contracts()
        if self._schwab_stream is not None:
            logger.info(
                "Connecting Schwab live stream for %s (%s bars)",
                ", ".join(self._symbols),
                self._config.market_config.stream_timeframe,
            )
            self._schwab_stream.refresh_streamer_info()
            self._schwab_stream.connect()
        elif self._ibkr_stream is not None:
            self._connect_ibkr_runtime()
            logger.info(
                "Connecting IBKR TWS tick stream for %s (%s bars)",
                ", ".join(self._symbols),
                self._config.market_config.stream_timeframe,
            )
            self._ibkr_stream.connect()
        else:
            logger.info("Connecting market data stream at %s", self._config.websocket_url)
            self.stream_manager.connect()
        if self._config.sync_broker_positions_on_start:
            self._sync_broker_positions()
        self._start_health_checks()
        if self._config.eod_schedule.enabled:
            self._start_eod_scheduler()
        if self._gex_monitor is not None:
            if self._config.app.gex.poll_on_startup:
                try:
                    logger.info("Running startup GEX chain poll before live stream")
                    self._gex_monitor.poll_once()
                except Exception:
                    logger.exception(
                        "Startup GEX poll failed; background monitor will retry"
                    )
            self._gex_monitor.start()
        self._started = True
        logger.info("TradingWorkflow started for %s", ", ".join(self._symbols))

    @property
    def shutdown_requested(self) -> bool:
        """True after the scheduled end-of-day shutdown fires."""
        return self._shutdown_requested.is_set()

    def stop(self) -> None:
        """Stop health checks and disconnect the market data stream."""
        if not self._started:
            return

        self._stop_eod.set()
        if (
            self._eod_thread is not None
            and threading.current_thread() is not self._eod_thread
        ):
            self._eod_thread.join(timeout=2.0)
        self._stop_health.set()
        if (
            self._health_thread is not None
            and threading.current_thread() is not self._health_thread
        ):
            self._health_thread.join(timeout=2.0)
        if self._schwab_stream is not None:
            self._schwab_stream.disconnect()
        elif self._ibkr_stream is not None:
            self._ibkr_stream.disconnect()
            if self._ibkr_tws_runtime is not None:
                self._ibkr_tws_runtime.disconnect_session()
        elif self._stream_manager is not None:
            self.stream_manager.disconnect()
        if self._gex_monitor is not None:
            self._gex_monitor.stop()
        if self._forward_test_account is not None:
            self._forward_test_account.save()
        if self._session_recorder is not None:
            flushed = self.aggregator.flush()
            strategy_timeframe = self._config.market_config.strategy_timeframe
            for aggregated in flushed:
                if aggregated.timeframe != strategy_timeframe:
                    continue
                self._session_recorder.record_aggregated_bar(aggregated)
            buffered = self._session_recorder.buffered_row_count
            if buffered:
                logger.info(
                    "Shutdown: saving %d buffered live bar(s) to GCS",
                    buffered,
                )
            self._session_recorder.flush()
        self._started = False
        logger.info("TradingWorkflow stopped for %s", ", ".join(self._symbols))

    @property
    def config(self) -> WorkflowConfig:
        """Return the workflow runtime configuration."""
        return self._config

    def seed_gex_volume_history(
        self,
        symbol: str,
        volumes: Sequence[float],
    ) -> int:
        """Preload the rolling 1m volume buffer used by gex_scalp."""
        if not volumes:
            return 0
        symbol = symbol.upper()
        maxlen = max(self._config.app.gex.volume_lookback_bars, 1)
        history = self._volume_history.setdefault(
            symbol,
            deque(maxlen=maxlen),
        )
        for volume in volumes[-maxlen:]:
            history.append(float(volume))
        return len(history)

    def replay_warmup_bar(self, bar: CleanBarEvent) -> None:
        """Replay one stored 1m bar through aggregation and indicators only."""
        from ohlc_sanity import repair_ohlc_bar

        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        )
        if (open_price, high_price, low_price, close_price) != (
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        ):
            bar = CleanBarEvent(
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                timestamp=bar.timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=bar.volume,
            )
        aggregated_bars = self.aggregator.on_bar(bar)
        strategy_timeframe = self._config.market_config.strategy_timeframe
        for aggregated in aggregated_bars:
            if (
                aggregated.timeframe == strategy_timeframe
                and aggregated.is_complete
            ):
                self.indicator_coordinator.on_aggregated_bar(aggregated)

    def replay_warmup_aggregated_bar(self, bar: AggregatedBar) -> None:
        """Replay one stored strategy-timeframe bar into indicator buffers."""
        from ohlc_sanity import repair_ohlc_bar

        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        )
        if (open_price, high_price, low_price, close_price) != (
            bar.open,
            bar.high,
            bar.low,
            bar.close,
        ):
            bar = AggregatedBar(
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                timestamp=bar.timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=bar.volume,
                is_complete=bar.is_complete,
            )
        self.indicator_coordinator.on_aggregated_bar(bar)

    def seed_live_aggregation_from_storage(
        self,
        symbol: str,
        last_saved_3m: datetime,
        storage: CloudStorageRepository,
        *,
        end: Optional[datetime] = None,
    ) -> None:
        """Continue live 1m->3m aggregation after the last stored 3m candle."""
        app = self._config.app
        strategy_timeframe = self._config.market_config.strategy_timeframe
        stream_timeframe = self._config.market_config.stream_timeframe
        symbol = symbol.upper()
        last_saved_3m = align_bucket_start(last_saved_3m, strategy_timeframe)
        now = last_completed_minute(end)
        completed_through, seed_start = aggregation_checkpoint(
            last_saved_3m,
            timeframe=strategy_timeframe,
            now=now,
        )
        self.aggregator.set_completed_through(
            symbol,
            strategy_timeframe,
            completed_through,
        )

        if now < seed_start:
            logger.info(
                "Live %s aligned with last saved candle @ %s (next bucket starts %s)",
                strategy_timeframe,
                last_saved_3m.isoformat(),
                seed_start.isoformat(),
            )
            return

        minute_bars = load_stored_bars(
            storage,
            symbol,
            stream_timeframe,
            seed_start,
            now,
            use_daily_partitions=app.gcs.use_daily_partitions,
        )
        seeded = 0
        for row in minute_bars.itertuples(index=False):
            timestamp = to_utc(pd.Timestamp(row.timestamp).to_pydatetime())
            if timestamp < seed_start:
                continue
            self.replay_warmup_bar(
                CleanBarEvent(
                    symbol=symbol,
                    timeframe=stream_timeframe,
                    timestamp=timestamp,
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                )
            )
            seeded += 1

        open_bucket = align_bucket_start(now, strategy_timeframe)
        logger.info(
            "Live %s aligned with last saved candle @ %s; checkpoint through %s; "
            "seeded %d stored 1m bar(s) into open bucket %s",
            strategy_timeframe,
            last_saved_3m.isoformat(),
            completed_through.isoformat(),
            seeded,
            open_bucket.isoformat(),
        )

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
        """Return the current stream connection state."""
        if self._stream_manager is not None:
            return self.stream_manager.state
        if self._ibkr_stream is not None and self._ibkr_tws_runtime is not None:
            return (
                ConnectionState.CONNECTED
                if self._ibkr_tws_runtime.isConnected()
                else ConnectionState.DISCONNECTED
            )
        if self._schwab_stream is not None:
            return ConnectionState.CONNECTED
        return ConnectionState.DISCONNECTED

    def _connect_ibkr_runtime(self) -> None:
        """Connect the shared IBKR TWS runtime if it is not already open."""
        if self._ibkr_tws_runtime is None:
            raise RuntimeError("IBKR TWS runtime is not configured")
        if self._ibkr_tws_runtime.isConnected():
            return
        ibkr = self._config.app.ibkr
        self._ibkr_tws_runtime.connect_session(
            host=ibkr.host,
            port=ibkr.port,
            client_id=ibkr.client_id,
            timeout_seconds=ibkr.connect_timeout_seconds,
        )
        self._ibkr_tws_runtime.set_market_data_type(ibkr.market_data_type)

    def _stream_endpoint(self) -> str:
        if self._config.stream_provider == "ibkr":
            ibkr = self._config.app.ibkr
            return f"ibkr-tws://{ibkr.host}:{ibkr.port}"
        return self.stream_manager.url

    def _wire_passive_listeners(self) -> None:
        """Start cross-cutting listeners that subscribe via the event bus."""
        # TradeLogger and HealthMonitor subscribe during their start() methods.

    def _sync_broker_positions(self) -> None:
        """Load broker positions into the local position tracker."""
        if self._account_sync is None:
            logger.warning("Broker position sync requested but account sync is unavailable")
            return

        try:
            if isinstance(self._account_sync, SchwabAccountSync):
                snapshot = self._account_sync.sync_positions(
                    self.position_tracker,
                    watchlist=self._symbols,
                    account_hash=self._config.schwab_account_hash,
                    account_number=self._config.schwab_account_number,
                )
                source = "schwab_account_sync"
            else:
                if self._ibkr_tws_runtime is not None and not self._ibkr_tws_runtime.isConnected():
                    self._connect_ibkr_runtime()
                snapshot = self._account_sync.sync_positions(
                    self.position_tracker,
                    watchlist=self._symbols,
                )
                source = "ibkr_tws_account_sync"
            self._account_snapshot = snapshot
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
                source=source,
            )
        except Exception as exc:
            logger.exception("Failed to sync broker account positions: %s", exc)
            self.bus.publish(
                Topics.STREAM_ERROR,
                {"error": f"account position sync failed: {exc}"},
                source=source if "source" in locals() else "account_sync",
            )

    def _register_indicator_jobs(self) -> None:
        """Configure indicator jobs for each watchlist symbol."""
        indicators = self._config.indicator_config
        timeframe = self._config.market_config.strategy_timeframe
        jobs = indicators.build_jobs(timeframe)
        for symbol in self._symbols:
            self.indicator_coordinator.register(
                SymbolIndicatorConfig(symbol=symbol, jobs=jobs)
            )

    def _on_clean_bar(self, bar: CleanBarEvent) -> None:
        """Publish and process a validated 1-minute bar."""
        if not self._logged_first_live_bar:
            self._logged_first_live_bar = True
            logger.info(
                "First live bar received: %s %s @ %s close=%.2f "
                "(completed 3m bars will log here every ~3 minutes)",
                bar.symbol,
                bar.timeframe,
                bar.timestamp.isoformat(),
                bar.close,
            )
        self.bus.publish(Topics.BAR_CLEAN, bar, source="stream_data_processor")
        if not self._live_regular_hours_seen and self._is_regular_hours_live_bar(
            bar.timestamp
        ):
            self._live_regular_hours_seen = True
            logger.info(
                "First regular-hours live candle received at %s; trade entries enabled",
                bar.timestamp.isoformat(),
            )
        if self._session_recorder is not None:
            self._session_recorder.record_clean_bar(bar)
        self._run_process_and_strategy_layers(bar)
        self._evaluate_gex_strategies(bar)
        self._enforce_zero_dte_session_close(bar)
        self._check_gex_position_timeouts(bar)
        self._track_open_option_marks(bar)

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
            if self._session_recorder is not None:
                self._session_recorder.record_aggregated_bar(aggregated)
            snapshot = self._dispatch_indicator_jobs(aggregated, started)
            if aggregated.is_complete and snapshot is not None:
                self._evaluate_strategies(aggregated, snapshot)

        self.position_tracker.update_price(
            bar.symbol,
            bar.close,
            timestamp=bar.timestamp,
            evaluate_exits=self._config.managed_exits,
        )

    def _track_open_option_marks(self, bar: CleanBarEvent) -> None:
        """Refresh the open option mark and running max unrealized P&L per 1m bar.

        Acts as a REST fallback when the live LEVELONE_OPTIONS stream is not the
        source of truth; when option streaming is active, marks arrive in real
        time via :meth:`_on_option_quote` and this poll is skipped.
        """
        if not self._config.app.options.enabled:
            return
        if self._option_stream_active():
            return

        position = self.position_tracker.get_position_for_underlying(bar.symbol)
        if position is None or position.asset_type != "OPTION":
            return

        mark = self._fetch_live_option_mark(position)
        if mark is None or mark <= 0:
            return

        self._handle_option_mark(position.symbol, mark, bar.timestamp)

    def _option_stream_active(self) -> bool:
        """Return True when option marks are streamed via LEVELONE_OPTIONS."""
        return (
            self._schwab_stream is not None
            and self._config.app.options.stream_contract_marks
        )

    def _on_option_quote(self, payload: dict[str, object]) -> None:
        """Handle one streamed LEVELONE_OPTIONS mark for a held contract."""
        symbol = str(payload.get("symbol", "")).upper()
        mark = payload.get("mark")
        if not symbol or mark is None:
            return
        try:
            mark_value = float(mark)
        except (TypeError, ValueError):
            return
        if mark_value <= 0:
            return
        self._handle_option_mark(symbol, mark_value, datetime.now(timezone.utc))

    def _handle_option_mark(
        self,
        occ_symbol: str,
        mark: float,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Record an option mark, track max P&L, and fire the trailing stop."""
        timestamp = timestamp or datetime.now(timezone.utc)
        position = self.position_tracker.get_position(occ_symbol)
        if position is None or position.asset_type != "OPTION":
            return

        unrealized = self._option_unrealized_pnl(position, mark)
        unrealized_pct = self._option_unrealized_pnl_pct(position, unrealized)
        notification = self.position_tracker.record_option_mark(
            position.symbol,
            mark,
            unrealized_pnl=unrealized,
            unrealized_pnl_pct=unrealized_pct,
            timestamp=timestamp,
        )
        logger.debug(
            "Option mark %s=%.2f unrealized=%.2f (%s) (max+=%.2f max-=%.2f peak=%.2f)",
            position.symbol,
            mark,
            unrealized,
            f"{unrealized_pct:+.1%}" if unrealized_pct is not None else "n/a",
            position.max_unrealized_profit if position.max_unrealized_profit is not None else 0.0,
            position.max_unrealized_loss if position.max_unrealized_loss is not None else 0.0,
            position.max_mark_price if position.max_mark_price is not None else mark,
        )

        if notification is not None and notification.reason == ExitReason.TRAILING_STOP:
            self._exit_on_trailing_stop(position, mark, timestamp)

    def _exit_on_trailing_stop(
        self,
        position: Position,
        mark: float,
        closed_at: datetime,
    ) -> None:
        """Flatten an option position when its peak-mark trailing stop is hit."""
        key = position.symbol.upper()
        if key in self._flattening_contracts:
            return
        self._flattening_contracts.add(key)
        try:
            underlying = (position.underlying_symbol or position.symbol).upper()
            timeframe = self._config.market_config.strategy_timeframe
            spot = self.indicator_coordinator.latest_close(underlying, timeframe)
            if spot is None:
                spot = position.underlying_entry_price or position.average_entry_price
            peak = position.max_mark_price or mark
            pct = position.trailing_stop_pct or 0.0
            logger.info(
                "Trailing stop hit for %s: mark=%.2f fell %.1f%% from peak=%.2f",
                position.symbol,
                mark,
                (1.0 - (mark / peak)) * 100.0 if peak else 0.0,
                peak,
            )
            self._flatten_open_option_position(
                position=position,
                underlying_symbol=underlying,
                underlying_spot=float(spot),
                closed_at=closed_at,
                strategy_name="trailing_stop",
                conditions_met=(
                    f"trailing stop: mark {mark:.2f} fell {pct:.0%}+ from peak {peak:.2f}"
                ),
                send_email=self._trade_emailer is not None,
            )
        finally:
            self._flattening_contracts.discard(key)

    def _subscribe_open_option_contracts(self) -> None:
        """Stream marks and arm trailing stops for already-open option positions."""
        if not self._config.app.options.enabled:
            return
        for position in self.position_tracker.list_positions():
            if position.asset_type != "OPTION":
                continue
            self._apply_option_trailing_stop(position.symbol)
            self._subscribe_option_contract(position.symbol)

    def _apply_option_trailing_stop(self, symbol: str) -> None:
        """Attach the configured peak-mark trailing stop to an open option."""
        options = self._config.app.options
        pct = options.trailing_stop_pct
        if not options.enabled or pct is None:
            return
        position = self.position_tracker.get_position(symbol)
        if position is None or position.asset_type != "OPTION":
            return
        if position.trailing_stop_pct == pct:
            return
        self.position_tracker.open_position(
            symbol=position.symbol,
            quantity=position.quantity,
            entry_price=position.average_entry_price,
            opened_at=position.opened_at,
            asset_type=position.asset_type,
            underlying_symbol=position.underlying_symbol,
            underlying_entry_price=position.underlying_entry_price,
            entry_quote=position.entry_quote,
            trailing_stop_pct=pct,
        )

    def _subscribe_option_contract(self, occ_symbol: str) -> None:
        """Subscribe a held option contract to the live mark stream."""
        if self._schwab_stream is None:
            return
        if not self._config.app.options.stream_contract_marks:
            return
        try:
            self._schwab_stream.subscribe_option(occ_symbol)
        except Exception:
            logger.exception("Failed to subscribe option stream for %s", occ_symbol)

    def _unsubscribe_option_contract(self, occ_symbol: str) -> None:
        """Stop streaming marks for a closed option contract."""
        if self._schwab_stream is None:
            return
        try:
            self._schwab_stream.unsubscribe_option(occ_symbol)
        except Exception:
            logger.debug(
                "Failed to unsubscribe option stream for %s",
                occ_symbol,
                exc_info=True,
            )

    def _fetch_live_option_mark(self, position: Position) -> Optional[float]:
        """Return the current option mark from the live chain, or None on failure."""
        client = self._get_market_data_client()
        if client is None:
            return None

        options = self._config.app.options
        try:
            chain = client.fetch_option_chain(
                position.underlying_symbol or position.symbol,
                contract_type=option_contract_type(position.symbol),
                strike_count=max(options.strike_count, 10),
                days_to_expiration=days_to_expiration_for_occ(position.symbol),
            )
            return contract_mark_from_chain(chain, position.symbol)
        except Exception:
            logger.debug(
                "Failed to fetch live option mark for %s",
                position.symbol,
                exc_info=True,
            )
            return None

    def _option_unrealized_pnl(self, position: Position, mark: float) -> float:
        """Return unrealized P&L for an option at ``mark`` net of entry commission."""
        quantity = abs(position.quantity)
        gross = (mark - position.average_entry_price) * quantity * 100.0
        commission = self._config.app.options.commission_per_contract * quantity
        return gross - commission

    def _option_cost_basis(self, position: Position) -> float:
        """Return the premium paid to open an option position (the % denominator)."""
        return abs(position.quantity) * position.average_entry_price * 100.0

    def _option_unrealized_pnl_pct(
        self,
        position: Position,
        unrealized_pnl: float,
    ) -> Optional[float]:
        """Return unrealized P&L as a fraction of premium paid (None if unknown)."""
        basis = self._option_cost_basis(position)
        if basis <= 0:
            return None
        return unrealized_pnl / basis

    def _dispatch_indicator_jobs(
        self,
        aggregated: AggregatedBar,
        started: float,
    ) -> Optional[IndicatorSnapshot]:
        """Run indicator jobs and publish the latest snapshot."""
        if not aggregated.is_complete:
            return None

        snapshot = self.indicator_coordinator.on_aggregated_bar(aggregated)
        if snapshot is None:
            return None

        if snapshot.values.get("gaussian_ma") is None:
            logger.debug(
                "Gaussian MA not ready for %s %s @ %s (need more 3m bars in buffer)",
                aggregated.symbol,
                aggregated.timeframe,
                aggregated.timestamp.isoformat(),
            )

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
        if aggregated.timeframe != self._config.market_config.strategy_timeframe:
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
            self._log_strategy_evaluation(
                aggregated,
                snapshot,
                strategy_name=strategy_name,
                signal=signal,
            )
            self._handle_strategy_signal(signal)

    def _init_gex_monitor(self) -> None:
        """Wire the background GEX snapshot poller when enabled in config."""
        gex = self._config.app.gex
        try:
            chain_client = SchwabOptionsChainClient.from_config(self._config.app)
            self._gex_monitor = GexRegimeMonitor(
                chain_client,
                self.bus,
                symbols=self._symbols,
                poll_interval_seconds=gex.poll_interval_seconds,
                strike_count=gex.strike_count,
                days_to_expiration=gex.days_to_expiration,
                risk_free_rate=gex.risk_free_rate,
            )
            logger.info(
                "GEX monitor configured for %s (%sDTE, poll=%.0fs)",
                ", ".join(self._symbols),
                gex.days_to_expiration,
                gex.poll_interval_seconds,
            )
        except Exception:
            logger.exception("GEX monitor unavailable; gex_scalp will not receive snapshots")

    def _evaluate_gex_strategies(self, bar: CleanBarEvent) -> None:
        """Evaluate 1m-native GEX strategies on each clean stream bar."""
        if "gex_scalp" not in self._config.strategies:
            return
        if self._gex_monitor is None:
            if bar.symbol not in self._gex_waiting_snapshot_logged:
                logger.warning(
                    "gex_scalp %s: GEX monitor not running — enable gex.enabled in config",
                    bar.symbol,
                )
                self._gex_waiting_snapshot_logged.add(bar.symbol)
            return

        gex = self._gex_monitor.get_latest(bar.symbol)
        if gex is None:
            if bar.symbol not in self._gex_waiting_snapshot_logged:
                poll_s = self._config.app.gex.poll_interval_seconds
                logger.info(
                    "gex_scalp %s: waiting for first GEX snapshot (chain poll every %.0fs)",
                    bar.symbol,
                    poll_s,
                )
                self._gex_waiting_snapshot_logged.add(bar.symbol)
            return
        self._gex_waiting_snapshot_logged.discard(bar.symbol)

        history = self._volume_history.setdefault(
            bar.symbol,
            deque(maxlen=max(self._config.app.gex.volume_lookback_bars, 1)),
        )
        history.append(bar.volume)
        volume_sma = sum(history) / len(history) if history else 0.0

        position = self.position_tracker.get_position_for_underlying(bar.symbol)
        has_open_position = position is not None and abs(position.quantity) > 0

        gex_settings = self._config.app.gex
        indicators = {
            "volume_sma": volume_sma,
            "gex_volume_multiplier": gex_settings.volume_multiplier,
            "gex_put_wall_break_pct": gex_settings.put_wall_break_pct,
            "gex_stall_body_ratio": gex_settings.stall_body_ratio,
            "gex_long_wick_ratio": gex_settings.long_wick_ratio,
        }
        state = self._gex_strategy_state.setdefault(bar.symbol, {})

        try:
            signal = self.signal_evaluator.evaluate(
                symbol=bar.symbol,
                timeframe="1m",
                timestamp=bar.timestamp,
                close=bar.close,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                volume=bar.volume,
                indicators=indicators,
                strategy_name="gex_scalp",
                gex=gex,
                has_open_position=has_open_position,
                state=state,
            )
        except ValueError as exc:
            logger.debug("Skipping gex_scalp evaluation: %s", exc)
            return

        self.bus.publish(
            Topics.STRATEGY_SIGNAL,
            signal,
            source="signal_evaluator",
        )
        self._log_gex_scalp_evaluation(bar, gex, signal, state)
        self._handle_strategy_signal(signal)

    def _log_gex_scalp_evaluation(
        self,
        bar: CleanBarEvent,
        gex: GexSnapshot,
        signal: StrategySignal,
        state: dict[str, object],
    ) -> None:
        """Emit throttled, human-readable gex_scalp status to the console."""
        gex_settings = self._config.app.gex
        history = self._volume_history.get(bar.symbol, deque())
        volume_sma = sum(history) / len(history) if history else 0.0
        position = self.position_tracker.get_position_for_underlying(bar.symbol)
        has_open_position = position is not None and abs(position.quantity) > 0

        ctx = StrategyEvaluationContext(
            symbol=bar.symbol,
            timeframe="1m",
            timestamp=bar.timestamp,
            close=bar.close,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            volume=bar.volume,
            indicators={
                "volume_sma": volume_sma,
                "gex_volume_multiplier": gex_settings.volume_multiplier,
                "gex_put_wall_break_pct": gex_settings.put_wall_break_pct,
                "gex_stall_body_ratio": gex_settings.stall_body_ratio,
                "gex_long_wick_ratio": gex_settings.long_wick_ratio,
            },
            gex=gex,
            has_open_position=has_open_position,
            state=state,
        )
        status = describe_gex_scalp_status(ctx, action=signal.action)

        snapshot_age_s = (bar.timestamp - gex.timestamp).total_seconds()
        if snapshot_age_s > gex_settings.poll_interval_seconds * 1.5:
            status = f"{status} [GEX snapshot {snapshot_age_s:.0f}s old]"

        now = time.monotonic()
        prev = self._gex_status_log.get(bar.symbol)
        should_log = signal.action != SignalAction.HOLD
        if not should_log:
            should_log = (
                prev is None
                or prev[0] != status
                or (now - prev[1]) >= _GEX_HOLD_LOG_INTERVAL_SECONDS
            )

        if not should_log:
            return

        action_label = (
            signal.action.value.upper()
            if signal.action != SignalAction.HOLD
            else "HOLD"
        )
        logger.info(
            "gex_scalp %s @ %s close=%.2f → %s | %s",
            bar.symbol,
            bar.timestamp.isoformat(),
            bar.close,
            action_label,
            status,
        )
        self._gex_status_log[bar.symbol] = (status, now)

    def _enforce_zero_dte_session_close(self, bar: CleanBarEvent) -> None:
        """Flatten same-day (0DTE) options before the regular session close."""
        schedule = self._config.eod_schedule
        bar_ts = (
            bar.timestamp.replace(tzinfo=timezone.utc)
            if bar.timestamp.tzinfo is None
            else bar.timestamp.astimezone(timezone.utc)
        )
        bar_day = bar_ts.date()
        if self._zero_dte_flattened_on == bar_day:
            return
        if not is_at_or_past_flatten_time(bar.timestamp, schedule=schedule):
            return

        as_of = self._market_today()
        closed_any = False
        for position in list(self.position_tracker.list_positions()):
            if position.asset_type != "OPTION":
                continue
            dte = days_to_expiration_for_occ(position.symbol, as_of=as_of)
            if dte is None or dte != 0:
                continue

            underlying = (position.underlying_symbol or bar.symbol).upper()
            logger.warning(
                "0DTE session close: flattening %s before market close (bar=%s)",
                position.symbol,
                bar.timestamp.isoformat(),
            )
            self._flatten_open_option_position(
                position=position,
                underlying_symbol=underlying,
                underlying_spot=bar.close,
                closed_at=bar.timestamp,
                strategy_name="zero_dte_close",
                conditions_met="0DTE flatten before regular session close",
                send_email=self._trade_emailer is not None,
            )
            closed_any = True

        if closed_any:
            self._zero_dte_flattened_on = bar_day

    def _check_gex_position_timeouts(self, bar: CleanBarEvent) -> None:
        """Force-review or exit 0DTE positions that exceed the max hold timer."""
        if "gex_scalp" not in self._config.strategies:
            return

        position = self.position_tracker.get_position_for_underlying(bar.symbol)
        if position is None or position.asset_type != "OPTION":
            return
        if position.force_review_after is None:
            return
        if bar.timestamp < position.force_review_after:
            return

        logger.warning(
            "GEX position %s exceeded max hold (%s); forcing exit review",
            position.symbol,
            position.force_review_after.isoformat(),
        )
        self._flatten_open_option_position(
            position=position,
            underlying_symbol=bar.symbol,
            underlying_spot=bar.close,
            closed_at=bar.timestamp,
            strategy_name="gex_scalp",
            conditions_met="max hold timer exceeded",
            send_email=self._trade_emailer is not None,
        )

    def _log_strategy_evaluation(
        self,
        aggregated: AggregatedBar,
        snapshot: IndicatorSnapshot,
        *,
        strategy_name: str,
        signal: StrategySignal,
    ) -> None:
        """Log completed strategy-timeframe evaluations for live monitoring."""
        values = snapshot.values
        context_label, context_text = self._strategy_log_context(values)

        if signal.action == SignalAction.HOLD:
            logger.info(
                "3m %s @ %s close=%.2f → HOLD | %s %s%s",
                aggregated.symbol,
                aggregated.timestamp.isoformat(),
                aggregated.close,
                strategy_name,
                context_label,
                context_text,
            )
            return

        logger.info(
            "3m %s @ %s close=%.2f → %s | %s %s%s",
            aggregated.symbol,
            aggregated.timestamp.isoformat(),
            aggregated.close,
            signal.action.value.upper(),
            strategy_name,
            context_label,
            context_text,
        )

    @staticmethod
    def _strategy_log_context(values: dict[str, object]) -> tuple[str, str]:
        """Return a (label, detail) pair describing indicator state for logs."""
        gauss_ma = values.get("gaussian_ma")
        if gauss_ma is not None:
            upper = values.get("gaussian_upper")
            lower = values.get("gaussian_lower")
            squeeze = bool(values.get("gaussian_squeeze"))
            label = "squeeze" if squeeze else "active"
            detail = f" GMA={float(gauss_ma):.2f}"
            if upper is not None and lower is not None:
                detail += f" [{float(lower):.2f}, {float(upper):.2f}]"
            return label, detail

        trend = values.get("supertrend_trend")
        st_value = values.get("supertrend")
        if trend in (1, 1.0, True):
            label = "bullish"
        elif trend in (-1, -1.0):
            label = "bearish"
        elif trend is None:
            label = "warming up"
        else:
            label = str(trend)
        detail = f" ST={float(st_value):.2f}" if st_value is not None else ""
        return label, detail

    def _indicators_ready(
        self,
        required: tuple[str, ...],
        values: dict[str, object],
    ) -> bool:
        """Return whether required indicator values are available."""
        return all(name in values and values[name] is not None for name in required)

    def _is_regular_hours_live_bar(self, timestamp: datetime) -> bool:
        """Return whether a live bar timestamp is inside the regular session."""
        historical = self._config.app.historical
        return is_regular_hours_timestamp_local(
            timestamp,
            session_start_local=parse_hhmm(historical.session_start_local),
            session_end_local=parse_hhmm(historical.session_end_local),
            market_timezone=self._config.app.app.timezone,
            trading_days_only=historical.trading_days_only,
        )

    def _market_today(self) -> date:
        """Return today's date in the configured market timezone."""
        return datetime.now(ZoneInfo(self._config.app.app.timezone)).date()

    def _past_entry_cutoff(self, timestamp: datetime) -> bool:
        """Return whether a bar timestamp is at/after the no-new-trades UTC cutoff."""
        raw = self._config.app.workflow.no_new_trades_after_utc.strip()
        if not raw:
            return False
        cutoff = parse_utc_hhmm(raw)
        if timestamp.tzinfo is None:
            ts_utc = timestamp.replace(tzinfo=timezone.utc)
        else:
            ts_utc = timestamp.astimezone(timezone.utc)
        return ts_utc.timetz().replace(tzinfo=None) >= cutoff

    @staticmethod
    def _signal_opens_new_trade(
        signal: StrategySignal,
        *,
        options_enabled: bool,
        option_entry: bool,
    ) -> bool:
        """Return True when a signal would open a new position."""
        if options_enabled:
            return option_entry
        return signal.action == SignalAction.BUY

    def _handle_strategy_signal(self, signal: StrategySignal) -> None:
        """Run risk checks and submit broker orders for actionable signals."""
        if signal.action == SignalAction.HOLD:
            return

        if signal.action == SignalAction.EXIT:
            self._exit_open_position_on_signal(signal)
            return

        options = self._config.app.options
        if options.enabled and signal.action in {SignalAction.BUY, SignalAction.SELL}:
            self._close_existing_option_on_flip(signal)

        position = self.position_tracker.get_position_for_underlying(signal.symbol)
        current_quantity = position.quantity if position is not None else 0.0
        option_put_entry = options.enabled and signal.action == SignalAction.SELL
        option_entry = option_put_entry or (
            options.enabled and signal.action == SignalAction.BUY
        )
        opens_new_trade = self._signal_opens_new_trade(
            signal,
            options_enabled=options.enabled,
            option_entry=option_entry,
        )
        if opens_new_trade and self._past_entry_cutoff(signal.timestamp):
            logger.info(
                "Blocking %s for %s; past entry cutoff %s UTC (exits still allowed)",
                signal.action.value,
                signal.symbol,
                self._config.app.workflow.no_new_trades_after_utc,
            )
            return
        if not self._live_regular_hours_seen and opens_new_trade:
            logger.info(
                "Blocking %s for %s until first regular-hours live candle",
                signal.action.value,
                signal.symbol,
            )
            return

        resolved = self._resolve_trade(
            signal,
            current_quantity=current_quantity,
            price=signal.close,
        )
        order_quantity = resolved.quantity
        decision = self.risk_guard.evaluate(
            signal,
            current_quantity=0.0 if option_entry else current_quantity,
            order_quantity=order_quantity,
            option_entry_on_sell=option_put_entry,
        )
        self.bus.publish(Topics.RISK_DECISION, decision, source="risk_guard")

        if not decision.approved:
            logger.info("Risk guard blocked %s for %s", signal.action.value, signal.symbol)
            return

        if opens_new_trade:
            self.risk_guard.record_opened_trade(signal.timestamp.date())

        if self._trade_emailer is not None:
            fill_result: Optional[ForwardTestFillResult] = None
            closed_position = None
            if signal.action == SignalAction.SELL and not option_put_entry:
                closed_position = self.position_tracker.get_position(resolved.symbol)
            try:
                fill_result = self._record_forward_test_fill(
                    signal,
                    resolved,
                    order_quantity,
                    option_put_entry=option_put_entry,
                )
                instrument_price = (
                    resolved.mark_price
                    if resolved.asset_type == "OPTION"
                    else signal.close
                )
                if option_put_entry:
                    entry_signal = StrategySignal(
                        symbol=signal.symbol,
                        timeframe=signal.timeframe,
                        timestamp=signal.timestamp,
                        action=SignalAction.BUY,
                        strategy_name=signal.strategy_name,
                        close=signal.close,
                        indicators=signal.indicators,
                    )
                    self._trade_emailer.notify_signal(
                        entry_signal,
                        quantity=order_quantity,
                        instrument_symbol=resolved.symbol,
                        instrument_description=resolved.description,
                        account_summary=(
                            self._forward_test_account.summary_line()
                            if self._forward_test_account is not None
                            else None
                        ),
                        trade_amount=fill_result.amount if fill_result is not None else None,
                        instrument_price=instrument_price,
                        underlying_price=signal.close,
                        quote=resolved.option_quote,
                    )
                else:
                    self._trade_emailer.notify_signal(
                        signal,
                        quantity=order_quantity,
                        instrument_symbol=resolved.symbol,
                        instrument_description=resolved.description,
                        account_summary=(
                            self._forward_test_account.summary_line()
                            if self._forward_test_account is not None
                            else None
                        ),
                        trade_amount=fill_result.amount if fill_result is not None else None,
                        trade_pnl=fill_result.trade_pnl if fill_result is not None else None,
                        instrument_price=instrument_price,
                        underlying_price=signal.close,
                        entry_instrument_price=(
                            closed_position.average_entry_price
                            if closed_position is not None
                            else None
                        ),
                        entry_underlying_price=(
                            closed_position.underlying_entry_price
                            if closed_position is not None
                            else None
                        ),
                        quote=resolved.option_quote,
                        entry_quote=(
                            closed_position.entry_quote
                            if closed_position is not None
                            else None
                        ),
                        time_bought=(
                            closed_position.opened_at
                            if closed_position is not None
                            else None
                        ),
                    )
            except Exception:
                logger.exception(
                    "Failed to send forward-test email for %s %s",
                    signal.action.value,
                    signal.symbol,
                )
            return

        side = (
            OrderSide.BUY
            if signal.action == SignalAction.BUY or option_put_entry
            else OrderSide.SELL
        )
        order = self.order_manager.submit_signal(
            TradingSignal(
                symbol=resolved.symbol,
                side=side,
                quantity=order_quantity,
                signal_id=f"{signal.strategy_name}:{signal.timestamp.isoformat()}",
                asset_type=resolved.asset_type,
                underlying_symbol=resolved.underlying_symbol,
                mark_price=(
                    resolved.mark_price if resolved.asset_type == "OPTION" else None
                ),
            )
        )
        self.order_manager.refresh_order(order.id)

    def _exit_open_position_on_signal(self, signal: StrategySignal) -> None:
        """Flatten any open position for a signal that closes to flat (no flip)."""
        position = self.position_tracker.get_position_for_underlying(signal.symbol)
        if position is None or abs(position.quantity) <= 0:
            return

        if position.asset_type == "OPTION":
            self._flatten_open_option_position(
                position=position,
                underlying_symbol=signal.symbol,
                underlying_spot=signal.close,
                closed_at=signal.timestamp,
                strategy_name=signal.strategy_name,
                conditions_met=describe_conditions_met(signal),
                send_email=True,
            )
            return

        # Equity mode: closing to flat is a plain sell of the open long.
        quantity = abs(position.quantity)
        if self._forward_test_account is not None:
            self._forward_test_account.record_sell(
                symbol=position.symbol,
                underlying_symbol=position.underlying_symbol or signal.symbol,
                quantity=quantity,
                exit_price=signal.close,
                asset_type=position.asset_type,
                closed_at=signal.timestamp,
            )
        elif not self._config.email_forward_test:
            order = self.order_manager.submit_signal(
                TradingSignal(
                    symbol=position.symbol,
                    side=OrderSide.SELL,
                    quantity=quantity,
                    signal_id=f"exit:{signal.strategy_name}:{signal.timestamp.isoformat()}",
                    asset_type=position.asset_type,
                    underlying_symbol=position.underlying_symbol or signal.symbol,
                )
            )
            self.order_manager.refresh_order(order.id)
        self.position_tracker.close_position(position.symbol)
        logger.info(
            "Exited %s to flat (%s) at %.2f",
            position.symbol,
            signal.strategy_name,
            signal.close,
        )

    def _record_forward_test_fill(
        self,
        signal: StrategySignal,
        resolved: ResolvedTrade,
        quantity: float,
        *,
        option_put_entry: bool = False,
    ) -> Optional[ForwardTestFillResult]:
        """Update local position and paper-account state after a forward-test email."""
        if quantity <= 0:
            return None

        fill_result: Optional[ForwardTestFillResult] = None
        is_option_entry = signal.action == SignalAction.BUY or option_put_entry

        if is_option_entry:
            entry_price = (
                resolved.mark_price
                if resolved.asset_type == "OPTION"
                else signal.close
            )
            gex_trigger, entry_iv, force_review_after = self._gex_position_metadata(
                signal,
                resolved.option_quote,
            )
            self.position_tracker.open_position(
                symbol=resolved.symbol,
                quantity=quantity,
                entry_price=entry_price,
                opened_at=signal.timestamp,
                asset_type=resolved.asset_type,
                underlying_symbol=resolved.underlying_symbol,
                underlying_entry_price=signal.close,
                entry_quote=resolved.option_quote,
                trailing_stop_pct=(
                    self._config.app.options.trailing_stop_pct
                    if resolved.asset_type == "OPTION"
                    and signal.strategy_name != "gex_scalp"
                    else None
                ),
                gex_trigger_level=gex_trigger,
                entry_iv=entry_iv,
                force_review_after=force_review_after,
            )
            if resolved.asset_type == "OPTION":
                self._subscribe_option_contract(resolved.symbol)
            if self._forward_test_account is not None:
                fill_result = self._forward_test_account.record_buy(
                    symbol=resolved.symbol,
                    underlying_symbol=resolved.underlying_symbol,
                    quantity=quantity,
                    price=entry_price,
                    asset_type=resolved.asset_type,
                    opened_at=signal.timestamp,
                    underlying_entry_price=signal.close,
                    entry_quote=resolved.option_quote,
                )
            self._record_transaction(
                side="BUY",
                signal=signal,
                resolved=resolved,
                quantity=quantity,
                instrument_price=entry_price,
                underlying_price=signal.close,
                entry_instrument_price=entry_price,
                entry_underlying_price=signal.close,
                trade_amount=fill_result.amount if fill_result is not None else None,
                timestamp=signal.timestamp,
                quote=resolved.option_quote,
            )
            logger.info(
                "Forward-test paper BUY %s qty=%.0f @ %.2f",
                resolved.symbol,
                quantity,
                entry_price,
            )
            return fill_result

        if signal.action == SignalAction.SELL:
            closed = self.position_tracker.close_position(resolved.symbol)
            if closed is not None:
                exit_price = resolved.mark_price
                if self._forward_test_account is not None:
                    fill_result = self._forward_test_account.record_sell(
                        symbol=resolved.symbol,
                        underlying_symbol=resolved.underlying_symbol,
                        quantity=quantity,
                        exit_price=exit_price,
                        asset_type=resolved.asset_type,
                        closed_at=signal.timestamp,
                    )
                self._record_transaction(
                    side="SELL",
                    signal=signal,
                    resolved=resolved,
                    quantity=quantity,
                    instrument_price=exit_price,
                    underlying_price=signal.close,
                    entry_instrument_price=closed.average_entry_price,
                    entry_underlying_price=closed.underlying_entry_price,
                    trade_amount=fill_result.amount if fill_result is not None else None,
                    trade_pnl=fill_result.trade_pnl if fill_result is not None else None,
                    timestamp=signal.timestamp,
                    quote=resolved.option_quote,
                    entry_quote=closed.entry_quote,
                )
                logger.info(
                    "Forward-test paper SELL %s qty=%.0f @ %.2f",
                    resolved.symbol,
                    quantity,
                    exit_price,
                )
            return fill_result

        return None

    def _record_transaction(
        self,
        *,
        side: str,
        signal: StrategySignal,
        resolved: ResolvedTrade,
        quantity: float,
        instrument_price: float,
        underlying_price: float,
        entry_instrument_price: Optional[float] = None,
        entry_underlying_price: Optional[float] = None,
        trade_amount: Optional[float] = None,
        trade_pnl: Optional[float] = None,
        execution_mode: str = "forward_test",
        timestamp: Optional[datetime] = None,
        quote: Optional[OptionQuoteSnapshot] = None,
        entry_quote: Optional[OptionQuoteSnapshot] = None,
        max_unrealized_profit: Optional[float] = None,
        max_unrealized_loss: Optional[float] = None,
        max_unrealized_profit_pct: Optional[float] = None,
        max_unrealized_loss_pct: Optional[float] = None,
    ) -> None:
        """Append one buy/sell leg to the account transactions CSV."""
        if self._transaction_ledger is None:
            return

        try:
            self._transaction_ledger.record(
                TransactionRecord(
                    timestamp=timestamp or datetime.now(timezone.utc),
                    side=side,
                    underlying_symbol=resolved.underlying_symbol,
                    instrument_symbol=resolved.symbol,
                    asset_type=resolved.asset_type,
                    quantity=quantity,
                    instrument_price=instrument_price,
                    underlying_price=underlying_price,
                    entry_instrument_price=entry_instrument_price,
                    entry_underlying_price=entry_underlying_price,
                    trade_amount=trade_amount,
                    trade_pnl=trade_pnl,
                    strategy_name=signal.strategy_name,
                    execution_mode=execution_mode,
                    quote=quote,
                    entry_quote=entry_quote,
                    max_unrealized_profit=max_unrealized_profit,
                    max_unrealized_loss=max_unrealized_loss,
                    max_unrealized_profit_pct=max_unrealized_profit_pct,
                    max_unrealized_loss_pct=max_unrealized_loss_pct,
                )
            )
        except Exception:
            logger.exception(
                "Failed to record transaction for %s %s",
                side,
                resolved.symbol,
            )

    def _record_live_fill_transaction(
        self,
        fill: FillEvent,
        position_before: Optional[Position],
    ) -> None:
        """Append a live broker fill to the transactions CSV."""
        if self._transaction_ledger is None:
            return

        side = "BUY" if fill.side == OrderSide.BUY else "SELL"
        asset_type = fill.asset_type.upper()
        underlying = (fill.underlying_symbol or fill.symbol).upper()
        underlying_price = fill.price if asset_type == "EQUITY" else 0.0

        if side == "SELL" and position_before is None:
            logger.warning(
                "Skipping live SELL transaction for %s: no open position",
                fill.symbol,
            )
            return

        entry_instrument = fill.price if side == "BUY" else position_before.average_entry_price
        entry_underlying = (
            underlying_price
            if side == "BUY"
            else (position_before.underlying_entry_price or underlying_price)
        )

        resolved = ResolvedTrade(
            symbol=fill.symbol.upper(),
            underlying_symbol=underlying,
            asset_type=asset_type,
            quantity=fill.quantity,
            mark_price=fill.price,
        )
        signal = StrategySignal(
            symbol=underlying,
            timeframe=self._config.market_config.strategy_timeframe,
            timestamp=fill.timestamp,
            action=SignalAction.BUY if side == "BUY" else SignalAction.SELL,
            strategy_name="live",
            close=underlying_price,
            indicators={},
        )
        self._record_transaction(
            side=side,
            signal=signal,
            resolved=resolved,
            quantity=fill.quantity,
            instrument_price=fill.price,
            underlying_price=underlying_price,
            entry_instrument_price=entry_instrument,
            entry_underlying_price=entry_underlying,
            execution_mode="live",
            timestamp=fill.timestamp,
        )

    def _resolve_trade(
        self,
        signal: StrategySignal,
        *,
        current_quantity: float,
        price: float,
    ) -> ResolvedTrade:
        """Resolve instrument symbol, asset type, and order quantity."""
        underlying = signal.symbol.upper()
        options = self._config.app.options

        if not options.enabled:
            quantity = self._resolve_equity_quantity(
                signal,
                current_quantity=current_quantity,
                price=price,
            )
            return ResolvedTrade(
                symbol=underlying,
                underlying_symbol=underlying,
                asset_type="EQUITY",
                quantity=quantity,
                mark_price=price,
            )

        if signal.action == SignalAction.SELL:
            if options.enabled:
                return self._resolve_option_entry(
                    underlying,
                    price,
                    contract_side="put",
                )
            quantity = self._resolve_equity_quantity(
                signal,
                current_quantity=current_quantity,
                price=price,
            )
            return ResolvedTrade(
                symbol=underlying,
                underlying_symbol=underlying,
                asset_type="EQUITY",
                quantity=quantity,
                mark_price=price,
            )

        if signal.action != SignalAction.BUY:
            return ResolvedTrade(
                symbol=underlying,
                underlying_symbol=underlying,
                asset_type="OPTION",
                quantity=0.0,
                mark_price=price,
            )

        return self._resolve_option_entry(
            underlying,
            price,
            contract_side="call",
        )

    def _resolve_option_entry(
        self,
        underlying: str,
        price: float,
        *,
        contract_side: Literal["call", "put"],
    ) -> ResolvedTrade:
        """Size and resolve a 0DTE or configured-DTE option entry."""
        options = self._config.app.options
        if options.days_to_expiration == 0:
            selected = self._select_zero_dte_contract(
                underlying,
                price,
                contract_side=contract_side,
            )
        elif contract_side == "call":
            selected = self._select_atm_call(underlying, price)
        else:
            selected = self._select_atm_put(underlying, price)
        right_label = contract_side

        balance = self._tradeable_balance()
        contracts = contracts_for_buy(
            balance,
            selected.mark_price,
            pct=self._config.risk.position_size_pct,
            max_dollars=self._config.risk.position_size_max_dollars,
        )
        if contracts <= 0:
            logger.info(
                "Option position size is zero for %s (balance=%.2f, premium=%.2f)",
                underlying,
                balance,
                selected.mark_price,
            )
            return ResolvedTrade(
                symbol=selected.occ_symbol,
                underlying_symbol=underlying,
                asset_type="OPTION",
                quantity=0.0,
                mark_price=selected.mark_price,
            )

        capped = min(float(contracts), self._config.risk.max_position_quantity)
        description = (
            f"{int(capped)} x {selected.occ_symbol} "
            f"({selected.strike:.2f} {right_label}, {selected.days_to_expiration} DTE)"
        )
        return ResolvedTrade(
            symbol=selected.occ_symbol,
            underlying_symbol=underlying,
            asset_type="OPTION",
            quantity=capped,
            mark_price=selected.mark_price,
            description=description,
            option_quote=selected.quote,
        )

    def _select_zero_dte_contract(
        self,
        underlying: str,
        spot_price: float,
        *,
        contract_side: Literal["call", "put"],
    ) -> SelectedOption:
        """Resolve a 0DTE contract in the configured delta band."""
        options = self._config.app.options
        gex = self._config.app.gex
        criteria = ZeroDteSelectionCriteria(
            target_dte=options.days_to_expiration,
            min_delta=gex.min_delta,
            max_delta=gex.max_delta,
        )
        client = self._get_market_data_client()
        if client is not None:
            try:
                chain = client.fetch_option_chain(
                    underlying,
                    contract_type=contract_side.upper(),
                    strike_count=max(options.strike_count, gex.strike_count),
                    days_to_expiration=options.days_to_expiration,
                )
                return select_zero_dte_contract(
                    chain,
                    underlying,
                    spot_price,
                    side=contract_side,
                    criteria=criteria,
                )
            except Exception:
                logger.exception(
                    "Failed to fetch 0DTE %s chain for %s; using synthetic contract",
                    contract_side,
                    underlying,
                )

        from option_selector import synthetic_atm_option

        return synthetic_atm_option(
            underlying,
            spot_price,
            days_to_expiration=options.days_to_expiration,
            mark_price=options.simulated_premium,
            option_right="C" if contract_side == "call" else "P",
            as_of=self._market_today(),
        )

    def _gex_position_metadata(
        self,
        signal: StrategySignal,
        _quote: Optional[OptionQuoteSnapshot],
    ) -> tuple[Optional[float], Optional[float], Optional[datetime]]:
        """Return GEX trigger level, entry IV, and max-hold deadline for a new option."""
        if signal.strategy_name != "gex_scalp":
            return None, None, None

        state = self._gex_strategy_state.get(signal.symbol, {})
        trigger_level = state.get("trigger_level")
        trigger = float(trigger_level) if trigger_level is not None else None
        entry_iv = None
        bar_day = signal.timestamp.astimezone(timezone.utc).date()
        max_hold_deadline = signal.timestamp + timedelta(
            minutes=self._config.app.gex.max_hold_minutes
        )
        flatten_deadline = flatten_deadline_utc(
            bar_day,
            schedule=self._config.eod_schedule,
        )
        force_review_after = min(max_hold_deadline, flatten_deadline)
        return trigger, entry_iv, force_review_after

    def _reconcile_expired_restored_positions(self) -> None:
        """Drop restored options whose OCC expiration date has already passed."""
        closed_at = datetime.now(timezone.utc)
        as_of = self._market_today()
        for position in list(self.position_tracker.list_positions()):
            if position.asset_type != "OPTION":
                continue
            if not option_is_expired(position.symbol, as_of=as_of):
                continue

            parsed = parse_occ_symbol(position.symbol)
            expiration = (
                parsed.expiration_date.isoformat()
                if parsed is not None
                else "unknown"
            )
            underlying = position.underlying_symbol or position.symbol
            logger.warning(
                "Removing expired restored position %s (expiration %s)",
                position.symbol,
                expiration,
            )

            fill_result: Optional[ForwardTestFillResult] = None
            if self._forward_test_account is not None:
                try:
                    fill_result = self._forward_test_account.expire_open_position(
                        symbol=position.symbol,
                        underlying_symbol=underlying,
                        asset_type=position.asset_type,
                        closed_at=closed_at,
                    )
                except ValueError:
                    logger.exception(
                        "Failed to expire forward-test position %s",
                        position.symbol,
                    )
                    continue

            self.position_tracker.close_position(position.symbol)
            self._unsubscribe_option_contract(position.symbol)
            if fill_result is not None:
                logger.info(
                    "Expired %s worthless; realized P&L %+.2f | %s",
                    position.symbol,
                    fill_result.trade_pnl or 0.0,
                    self._forward_test_account.summary_line()
                    if self._forward_test_account is not None
                    else "",
                )

    def _reconcile_restored_positions_with_trend(self) -> None:
        """Close restored options that no longer match the Gaussian MA bias."""
        if not self._config.app.options.enabled:
            return

        timeframe = self._config.market_config.strategy_timeframe
        for symbol in self._symbols:
            position = self.position_tracker.get_position_for_underlying(symbol)
            if position is None or position.asset_type != "OPTION":
                continue

            snapshot = self.indicator_coordinator.get_latest(symbol, timeframe)
            if snapshot is None:
                logger.info(
                    "Keeping restored %s; indicators not ready for reconciliation",
                    position.symbol,
                )
                continue

            gauss_ma = snapshot.values.get("gaussian_ma")
            if gauss_ma is None:
                logger.info(
                    "Keeping restored %s; Gaussian MA not ready for reconciliation",
                    position.symbol,
                )
                continue

            underlying_spot = self.indicator_coordinator.latest_close(symbol, timeframe)
            if underlying_spot is None:
                underlying_spot = (
                    position.underlying_entry_price or position.average_entry_price
                )

            contract_type = option_contract_type(position.symbol)
            bias_label = "bullish" if float(underlying_spot) >= float(gauss_ma) else "bearish"
            if option_position_aligned_with_gaussian(
                contract_type, float(underlying_spot), float(gauss_ma)
            ):
                logger.info(
                    "Restored %s %s matches Gaussian MA bias (%s); keeping position open",
                    symbol,
                    position.symbol,
                    bias_label,
                )
                continue

            logger.info(
                "Reconciling %s: closing stale %s (Gaussian MA bias now %s)",
                position.symbol,
                contract_type,
                bias_label,
            )
            self._flatten_open_option_position(
                position=position,
                underlying_symbol=symbol,
                underlying_spot=float(underlying_spot),
                closed_at=datetime.now(timezone.utc),
                strategy_name="reconciliation",
                conditions_met=(
                    f"startup reconciliation: {contract_type} open but Gaussian MA {bias_label}"
                ),
                send_email=self._trade_emailer is not None,
            )

    def _flatten_open_option_position(
        self,
        *,
        position: Position,
        underlying_symbol: str,
        underlying_spot: float,
        closed_at: datetime,
        strategy_name: str,
        conditions_met: str,
        send_email: bool = True,
    ) -> None:
        """Exit an open option position in paper or live mode."""
        exit_mark, exit_quote, chain_underlying = self._resolve_option_exit(
            position,
            underlying_spot,
        )
        exit_underlying = self._resolve_exit_underlying_price(
            underlying_symbol,
            chain_underlying=chain_underlying,
            fallback_spot=underlying_spot,
        )
        quantity = abs(position.quantity)
        description = f"{quantity:.0f} contracts of {position.symbol}"
        fill_result: Optional[ForwardTestFillResult] = None

        max_unrealized_profit = position.max_unrealized_profit
        max_unrealized_loss = position.max_unrealized_loss
        max_unrealized_profit_pct = position.max_unrealized_profit_pct
        max_unrealized_loss_pct = position.max_unrealized_loss_pct
        if position.asset_type == "OPTION":
            exit_unrealized = self._option_unrealized_pnl(position, exit_mark)
            exit_unrealized_pct = self._option_unrealized_pnl_pct(
                position, exit_unrealized
            )
            if max_unrealized_profit is None or exit_unrealized > max_unrealized_profit:
                max_unrealized_profit = exit_unrealized
                max_unrealized_profit_pct = exit_unrealized_pct
            if max_unrealized_loss is None or exit_unrealized < max_unrealized_loss:
                max_unrealized_loss = exit_unrealized
                max_unrealized_loss_pct = exit_unrealized_pct

        if self._forward_test_account is not None:
            fill_result = self._forward_test_account.record_sell(
                symbol=position.symbol,
                underlying_symbol=position.underlying_symbol or underlying_symbol,
                quantity=quantity,
                exit_price=exit_mark,
                asset_type=position.asset_type,
                closed_at=closed_at,
            )
        elif not self._config.email_forward_test:
            order = self.order_manager.submit_signal(
                TradingSignal(
                    symbol=position.symbol,
                    side=OrderSide.SELL,
                    quantity=quantity,
                    signal_id=f"flatten:{strategy_name}:{closed_at.isoformat()}",
                    asset_type=position.asset_type,
                    underlying_symbol=position.underlying_symbol or underlying_symbol,
                    mark_price=exit_mark,
                )
            )
            self.order_manager.refresh_order(order.id)

        signal = StrategySignal(
            symbol=underlying_symbol,
            timeframe=self._config.market_config.strategy_timeframe,
            timestamp=closed_at,
            action=SignalAction.SELL,
            strategy_name=strategy_name,
            close=exit_underlying,
            indicators={},
        )
        resolved_exit = ResolvedTrade(
            symbol=position.symbol,
            underlying_symbol=position.underlying_symbol or underlying_symbol,
            asset_type=position.asset_type,
            quantity=quantity,
            mark_price=exit_mark,
            description=description,
            option_quote=exit_quote,
        )
        self._record_transaction(
            side="SELL",
            signal=signal,
            resolved=resolved_exit,
            quantity=quantity,
            instrument_price=exit_mark,
            underlying_price=exit_underlying,
            entry_instrument_price=position.average_entry_price,
            entry_underlying_price=position.underlying_entry_price,
            trade_amount=fill_result.amount if fill_result is not None else None,
            trade_pnl=fill_result.trade_pnl if fill_result is not None else None,
            timestamp=closed_at,
            quote=exit_quote,
            entry_quote=position.entry_quote,
            max_unrealized_profit=max_unrealized_profit,
            max_unrealized_loss=max_unrealized_loss,
            max_unrealized_profit_pct=max_unrealized_profit_pct,
            max_unrealized_loss_pct=max_unrealized_loss_pct,
        )
        self.position_tracker.close_position(position.symbol)
        self._unsubscribe_option_contract(position.symbol)
        logger.info(
            "Closed %s (%s) at mark=%.2f",
            position.symbol,
            strategy_name,
            exit_mark,
        )

        if not send_email or self._trade_emailer is None:
            return

        try:
            self._trade_emailer.send_sell_notification(
                symbol=underlying_symbol,
                strategy_name=strategy_name,
                conditions_met=conditions_met,
                time_triggered=closed_at,
                time_sold=datetime.now(timezone.utc),
                exit_instrument_price=exit_mark,
                exit_underlying_price=exit_underlying,
                entry_instrument_price=position.average_entry_price,
                entry_underlying_price=position.underlying_entry_price,
                profit=fill_result.trade_pnl if fill_result is not None else None,
                quantity=quantity,
                instrument_line=description,
                account_summary=(
                    self._forward_test_account.summary_line()
                    if self._forward_test_account is not None
                    else None
                ),
                trade_amount=fill_result.amount if fill_result is not None else None,
                quote=exit_quote,
                entry_quote=position.entry_quote,
                time_bought=position.opened_at,
                max_unrealized_profit=max_unrealized_profit,
                max_unrealized_loss=max_unrealized_loss,
                max_unrealized_profit_pct=max_unrealized_profit_pct,
                max_unrealized_loss_pct=max_unrealized_loss_pct,
            )
        except Exception:
            logger.exception(
                "Failed to send exit email for %s",
                position.symbol,
            )

    def _close_existing_option_on_flip(self, signal: StrategySignal) -> None:
        """Flatten an open option before entering the new flip direction."""
        position = self.position_tracker.get_position_for_underlying(signal.symbol)
        if position is None or position.asset_type != "OPTION":
            return

        self._flatten_open_option_position(
            position=position,
            underlying_symbol=signal.symbol,
            underlying_spot=signal.close,
            closed_at=signal.timestamp,
            strategy_name=signal.strategy_name,
            conditions_met=describe_conditions_met(signal),
            send_email=True,
        )
        logger.info(
            "Closed %s before %s flip entry",
            position.symbol,
            signal.action.value,
        )

    def _resolve_exit_underlying_price(
        self,
        underlying_symbol: str,
        *,
        chain_underlying: Optional[float],
        fallback_spot: float,
    ) -> float:
        """Prefer live chain/1m spot over a stale strategy-bar close for exits."""
        if chain_underlying is not None and chain_underlying > 0:
            return chain_underlying

        one_minute = self.indicator_coordinator.latest_close(underlying_symbol, "1m")
        if one_minute is not None and one_minute > 0:
            return one_minute

        strategy_timeframe = self._config.market_config.strategy_timeframe
        strategy_close = self.indicator_coordinator.latest_close(
            underlying_symbol,
            strategy_timeframe,
        )
        if strategy_close is not None and strategy_close > 0:
            return strategy_close

        return fallback_spot

    def _resolve_option_exit(
        self,
        position: Position,
        fallback_underlying_spot: float,
    ) -> tuple[float, Optional[OptionQuoteSnapshot], Optional[float]]:
        """Return an option premium estimate, quote snapshot, and underlying spot."""
        quote: Optional[OptionQuoteSnapshot] = None
        chain_underlying: Optional[float] = None
        client = self._get_market_data_client()
        underlying_symbol = position.underlying_symbol or position.symbol
        if client is not None:
            options = self._config.app.options
            contract_type = option_contract_type(position.symbol)
            dte = days_to_expiration_for_occ(position.symbol)
            dte_attempts: list[Optional[int]] = []
            if dte is not None and dte >= 0:
                dte_attempts.append(dte)
            if options.days_to_expiration not in dte_attempts:
                dte_attempts.append(options.days_to_expiration)
            dte_attempts.append(None)

            for days_to_expiration in dte_attempts:
                try:
                    chain = client.fetch_option_chain(
                        underlying_symbol,
                        contract_type=contract_type,
                        strike_count=max(options.strike_count, 10),
                        days_to_expiration=days_to_expiration,
                    )
                    exit_mark = resolve_option_exit_from_chain(chain, position.symbol)
                    if exit_mark is not None:
                        return (
                            exit_mark.premium,
                            exit_mark.quote,
                            exit_mark.underlying_price,
                        )
                except Exception:
                    logger.exception(
                        "Failed to fetch exit quote for %s (dte=%s)",
                        position.symbol,
                        days_to_expiration,
                    )

            logger.warning(
                "Could not resolve live exit quote for %s from option chain",
                position.symbol,
            )

        if position.asset_type == "OPTION":
            logger.warning(
                "Using entry premium fallback for option exit on %s",
                position.symbol,
            )
            return (
                float(position.average_entry_price or 0.0),
                quote,
                chain_underlying,
            )

        if position.last_mark_price is not None and position.last_mark_price > 0:
            return float(position.last_mark_price), quote, chain_underlying

        return (
            float(position.average_entry_price or fallback_underlying_spot),
            quote,
            chain_underlying,
        )

    def _resolve_option_exit_mark(self, position, underlying_spot: float) -> float:
        """Return an option premium estimate for paper exit P&L."""
        mark, _, _ = self._resolve_option_exit(position, underlying_spot)
        return mark

    def _select_atm_call(self, underlying: str, spot_price: float) -> SelectedOption:
        """Resolve a 2-DTE ATM call from Schwab chain data or simulation defaults."""
        options = self._config.app.options
        client = self._get_market_data_client()
        if client is not None:
            try:
                chain = client.fetch_option_chain(
                    underlying,
                    contract_type=options.contract_type,
                    strike_count=options.strike_count,
                    days_to_expiration=options.days_to_expiration,
                )
                return select_atm_call_from_chain(
                    chain,
                    underlying,
                    spot_price,
                    target_dte=options.days_to_expiration,
                    as_of=self._market_today(),
                )
            except Exception:
                logger.exception(
                    "Failed to fetch option chain for %s; using synthetic contract",
                    underlying,
                )

        return synthetic_atm_call(
            underlying,
            spot_price,
            days_to_expiration=options.days_to_expiration,
            mark_price=options.simulated_premium,
            as_of=self._market_today(),
        )

    def _select_atm_put(self, underlying: str, spot_price: float) -> SelectedOption:
        """Resolve a 2-DTE ATM put from Schwab chain data or simulation defaults."""
        options = self._config.app.options
        client = self._get_market_data_client()
        if client is not None:
            try:
                chain = client.fetch_option_chain(
                    underlying,
                    contract_type="PUT",
                    strike_count=options.strike_count,
                    days_to_expiration=options.days_to_expiration,
                )
                return select_atm_put_from_chain(
                    chain,
                    underlying,
                    spot_price,
                    target_dte=options.days_to_expiration,
                    as_of=self._market_today(),
                )
            except Exception:
                logger.exception(
                    "Failed to fetch put option chain for %s; using synthetic contract",
                    underlying,
                )

        return synthetic_atm_put(
            underlying,
            spot_price,
            days_to_expiration=options.days_to_expiration,
            mark_price=options.simulated_premium,
            as_of=self._market_today(),
        )

    def _get_market_data_client(self) -> Optional[SchwabMarketDataClient]:
        """Return a cached Schwab market data client when credentials are available."""
        if self._market_data_client is not None:
            return self._market_data_client
        try:
            self._market_data_client = SchwabMarketDataClient.from_config(
                self._config.app
            )
        except Exception:
            logger.debug("Schwab market data client unavailable for option chains")
            return None
        return self._market_data_client

    def _resolve_equity_quantity(
        self,
        signal: StrategySignal,
        *,
        current_quantity: float,
        price: float,
    ) -> float:
        """Size buys from tradeable balance; sells flatten the open long."""
        if signal.action == SignalAction.SELL:
            return max(current_quantity, 0.0)

        if signal.action != SignalAction.BUY:
            return 0.0

        risk = self._config.risk
        balance = self._tradeable_balance()
        shares = shares_for_buy(
            balance,
            price,
            pct=risk.position_size_pct,
            max_dollars=risk.position_size_max_dollars,
        )
        if shares <= 0:
            logger.info(
                "Position size is zero for %s (balance=%.2f, price=%.2f)",
                signal.symbol,
                balance,
                price,
            )
            return 0.0

        capped = min(float(shares), risk.max_position_quantity)
        return capped

    def _tradeable_balance(self) -> float:
        """Return cash available for trading from Schwab or simulation config."""
        if self._forward_test_account is not None:
            return self._forward_test_account.cash_balance

        risk = self._config.risk
        if self._config.broker_use_in_memory:
            return risk.simulated_tradeable_balance

        if self._account_sync is None:
            return risk.simulated_tradeable_balance

        try:
            if isinstance(self._account_sync, SchwabAccountSync):
                snapshot = self._account_sync.fetch_snapshot(
                    account_hash=self._config.schwab_account_hash,
                    account_number=self._config.schwab_account_number,
                )
                balance = snapshot.balances.cash_available_for_trading
            else:
                if self._ibkr_tws_runtime is not None and not self._ibkr_tws_runtime.isConnected():
                    self._connect_ibkr_runtime()
                snapshot = self._account_sync.fetch_snapshot()
                balance = snapshot.balances.equity
            self._account_snapshot = snapshot
            return balance
        except Exception:
            logger.exception("Failed to fetch broker tradeable balance")
            if self._account_snapshot is not None:
                return self._account_snapshot.balances.cash_available_for_trading
            return 0.0

    def _on_order_update(self, order: Order) -> None:
        """Publish order lifecycle events and update portfolio state."""
        self.bus.publish(Topics.ORDER_UPDATED, order, source="order_manager")

        fill = self.order_manager.to_fill_event(order)
        if fill is None:
            return

        position_before = self.position_tracker.get_position(fill.symbol)
        self.bus.publish(Topics.ORDER_FILL, fill, source="order_manager")
        updated = self.position_tracker.on_fill(fill)
        self._record_live_fill_transaction(fill, position_before)

        if fill.asset_type.upper() == "OPTION":
            if updated is not None and fill.side == OrderSide.BUY:
                self._apply_option_trailing_stop(fill.symbol)
                self._subscribe_option_contract(fill.symbol)
            elif updated is None:
                self._unsubscribe_option_contract(fill.symbol)

        if (
            updated is not None
            and self._config.managed_exits
            and self._config.stop_loss is not None
        ):
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
                asset_type=notification.position.asset_type,
                underlying_symbol=notification.position.underlying_symbol,
                mark_price=notification.position.last_mark_price,
            )
        )
        self.order_manager.refresh_order(order.id)
        self.position_tracker.close_position(notification.symbol)

    def _on_stream_connected(self) -> None:
        """Publish stream connection events and subscribe to watchlist symbols."""
        stream_url = self._stream_endpoint()
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
        if self._config.stream_provider == "ibkr":
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
            {
                "url": self._stream_endpoint(),
                "provider": self._config.stream_provider,
            },
            source="stream_connection_manager",
        )

    def _on_stream_error(self, error: Exception) -> None:
        """Publish stream errors for observability."""
        self.bus.publish(
            Topics.STREAM_ERROR,
            {"error": str(error)},
            source="stream_connection_manager",
        )

    def _start_eod_scheduler(self) -> None:
        """Watch the clock and flatten/shutdown at configured UTC times."""
        schedule = self._config.eod_schedule
        logger.info(
            "EOD scheduler enabled: flatten %s UTC, shutdown %s UTC",
            schedule.flatten_time_utc.strftime("%H:%M"),
            schedule.shutdown_time_utc.strftime("%H:%M"),
        )
        self._eod_thread = threading.Thread(
            target=self._eod_loop,
            name="workflow-eod-scheduler",
            daemon=True,
        )
        self._eod_thread.start()

    def _eod_loop(self) -> None:
        """Flatten open positions near the close, then stop and flush."""
        while not self._stop_eod.wait(15.0):
            now = datetime.now(timezone.utc)
            schedule = self._config.eod_schedule

            if should_flatten_positions(
                now,
                schedule=schedule,
                flattened_on=self._eod_flattened_on,
            ):
                logger.info("EOD: flattening open positions before regular close")
                self._flatten_all_positions_eod(now)
                self._eod_flattened_on = now.date()

            if should_shutdown(
                now,
                schedule=schedule,
                shutdown_on=self._eod_shutdown_on,
            ):
                logger.info("EOD: regular session ended; shutting down and flushing data")
                self._eod_shutdown_on = now.date()
                self.stop()
                self._shutdown_requested.set()
                return

    def _flatten_all_positions_eod(self, closed_at: datetime) -> None:
        """Sell every open position at the end of the regular session."""
        positions = self.position_tracker.list_positions()
        if not positions:
            logger.info("EOD flatten: no open positions")
            return

        timeframe = self._config.market_config.strategy_timeframe
        for position in positions:
            underlying = (position.underlying_symbol or position.symbol).upper()
            spot = self.indicator_coordinator.latest_close(underlying, timeframe)
            if spot is None:
                spot = position.last_mark_price or position.average_entry_price

            if position.asset_type == "OPTION":
                self._flatten_open_option_position(
                    position=position,
                    underlying_symbol=underlying,
                    underlying_spot=float(spot),
                    closed_at=closed_at,
                    strategy_name="eod_close",
                    conditions_met="end-of-day flatten before regular session close",
                    send_email=self._trade_emailer is not None,
                )
                continue

            self._flatten_equity_position(
                position=position,
                underlying_symbol=underlying,
                exit_price=float(spot),
                closed_at=closed_at,
            )

    def _flatten_equity_position(
        self,
        *,
        position: Position,
        underlying_symbol: str,
        exit_price: float,
        closed_at: datetime,
    ) -> None:
        """Exit an open equity position in paper or live mode."""
        quantity = abs(position.quantity)
        fill_result: Optional[ForwardTestFillResult] = None

        if self._forward_test_account is not None:
            fill_result = self._forward_test_account.record_sell(
                symbol=position.symbol,
                underlying_symbol=underlying_symbol,
                quantity=quantity,
                exit_price=exit_price,
                asset_type=position.asset_type,
                closed_at=closed_at,
            )
        elif not self._config.email_forward_test:
            side = OrderSide.SELL if position.quantity > 0 else OrderSide.BUY
            order = self.order_manager.submit_signal(
                TradingSignal(
                    symbol=position.symbol,
                    side=side,
                    quantity=quantity,
                    signal_id=f"flatten:eod_close:{closed_at.isoformat()}",
                    asset_type=position.asset_type,
                    underlying_symbol=underlying_symbol,
                )
            )
            self.order_manager.refresh_order(order.id)

        signal = StrategySignal(
            symbol=underlying_symbol,
            timeframe=self._config.market_config.strategy_timeframe,
            timestamp=closed_at,
            action=SignalAction.SELL,
            strategy_name="eod_close",
            close=exit_price,
            indicators={},
        )
        resolved_exit = ResolvedTrade(
            symbol=position.symbol,
            underlying_symbol=underlying_symbol,
            asset_type=position.asset_type,
            quantity=quantity,
            mark_price=exit_price,
        )
        self._record_transaction(
            side="SELL",
            signal=signal,
            resolved=resolved_exit,
            quantity=quantity,
            instrument_price=exit_price,
            underlying_price=exit_price,
            entry_instrument_price=position.average_entry_price,
            entry_underlying_price=position.underlying_entry_price,
            trade_amount=fill_result.amount if fill_result is not None else None,
            trade_pnl=fill_result.trade_pnl if fill_result is not None else None,
            timestamp=closed_at,
        )
        self.position_tracker.close_position(position.symbol)
        logger.info("EOD closed equity %s at %.2f", position.symbol, exit_price)

        if self._trade_emailer is None:
            return

        try:
            self._trade_emailer.send_sell_notification(
                symbol=underlying_symbol,
                strategy_name="eod_close",
                conditions_met="end-of-day flatten before regular session close",
                time_triggered=closed_at,
                time_sold=datetime.now(timezone.utc),
                exit_instrument_price=exit_price,
                exit_underlying_price=exit_price,
                entry_instrument_price=position.average_entry_price,
                entry_underlying_price=position.underlying_entry_price,
                profit=fill_result.trade_pnl if fill_result is not None else None,
                quantity=quantity,
                instrument_line=position.symbol,
                account_summary=(
                    self._forward_test_account.summary_line()
                    if self._forward_test_account is not None
                    else None
                ),
                trade_amount=fill_result.amount if fill_result is not None else None,
                time_bought=position.opened_at,
            )
        except Exception:
            logger.exception("Failed to send EOD exit email for %s", position.symbol)

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


if __name__ == "__main__":
    import json
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    app = load_config()
    use_live_stream = app.workflow.run_schwab_stream or app.workflow.stream_provider in {
        "schwab",
        "ibkr",
    }

    if use_live_stream:
        workflow = TradingWorkflow(WorkflowConfig.from_env())
        workflow.start()
        try:
            while not workflow.shutdown_requested:
                time.sleep(1)
        except KeyboardInterrupt:
            workflow.stop()
        else:
            if workflow.shutdown_requested:
                logger.info("Exiting after scheduled end-of-day shutdown")
        raise SystemExit(0)

    workflow = TradingWorkflow(
        WorkflowConfig.from_app_config(app),
    )

    workflow.trade_logger.start()
    workflow.health_monitor.start()

    base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    for offset in range(30):
        for index, symbol in enumerate(workflow.symbols):
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
