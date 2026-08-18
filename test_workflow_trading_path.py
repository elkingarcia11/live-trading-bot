"""Tests for feed isolation and hot-restart of trading logic."""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from config import AppConfig
from event_bus import EventBus
from health_monitor import HealthMonitor, HealthThresholds
from position_tracker import ExitReason
from signal_evaluator import SignalEvaluator, StrategySignal
from strategy_registry import SignalAction, build_default_registry
from stream_data_processor import CleanBarEvent, StreamDataProcessor
from workflow import ResolvedTrade, RiskGuard, TradingWorkflow, WorkflowConfig


def _bare_workflow(**overrides: object) -> TradingWorkflow:
    app = AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_timeframe": "5t",
                "strategy_timeframe": "5t",
                "aggregation_timeframes": [],
            },
            "workflow": {"stream_provider": "databento"},
            "strategies": ["gaussian_ma_crossover"],
            "broker": {"provider": "schwab", "use_in_memory": True},
        }
    )
    config = WorkflowConfig.from_app_config(app)
    workflow = object.__new__(TradingWorkflow)
    workflow._config = config
    workflow._symbols = ("SPY",)
    workflow.bus = EventBus()
    workflow.health_monitor = HealthMonitor(
        workflow.bus,
        thresholds=HealthThresholds(startup_grace_seconds=0.0),
        monitored_modules=("stream_data_processor",),
    )
    workflow._trading_lock = threading.Lock()
    workflow._trading_generation = 0
    workflow._logged_first_live_bar = True
    workflow._live_regular_hours_seen = True
    workflow._session_recorder = None
    workflow._trade_persist = None
    workflow._last_bar_at = {}
    workflow._missed_bars = 0
    workflow._last_sampled_dropped_bars = 0
    workflow._started = True
    workflow._databento_stream = object()
    workflow.strategy_registry = build_default_registry(strategy_timeframe="5t")
    workflow.signal_evaluator = SignalEvaluator(workflow.strategy_registry)
    workflow.risk_guard = RiskGuard(max_position_quantity=10)
    workflow.indicator_coordinator = MagicMock()
    workflow.stream_processor = MagicMock()
    workflow.stream_processor.dropped_bar_count = 0
    for key, value in overrides.items():
        setattr(workflow, key, value)
    return workflow


def _bar(ts: datetime) -> CleanBarEvent:
    return CleanBarEvent(
        symbol="SPY",
        timeframe="5t",
        timestamp=ts,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
    )


def test_strategy_exception_does_not_stop_later_bars() -> None:
    workflow = _bare_workflow()
    seen: list[datetime] = []

    def boom(bar: CleanBarEvent) -> None:
        seen.append(bar.timestamp)
        raise RuntimeError("strategy bug")

    workflow._run_trading_path = boom  # type: ignore[method-assign]
    first = _bar(datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc))
    second = _bar(datetime(2026, 8, 17, 14, 1, tzinfo=timezone.utc))

    workflow._on_clean_bar(first)
    workflow._on_clean_bar(second)

    assert seen == [first.timestamp, second.timestamp]
    assert workflow.health_monitor.latest_snapshot() is None
    snapshot = workflow.health_monitor.check()
    assert snapshot.trading_errors_hour == 2


def test_tick_callback_only_enqueues_trading_work() -> None:
    workflow = _bare_workflow()
    submitted: list[tuple[object, tuple[object, ...]]] = []
    runtime = MagicMock()
    runtime.submit_trading.side_effect = (
        lambda fn, *args: submitted.append((fn, args))
    )
    workflow._schwab_http = runtime
    workflow._run_trading_path = MagicMock()
    bar = _bar(datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc))

    workflow._on_clean_bar(bar)

    workflow._run_trading_path.assert_not_called()
    assert len(submitted) == 1
    fn, args = submitted[0]
    assert fn == workflow._run_trading_path_isolated
    assert args == (bar,)


def test_stream_processor_consumer_exception_does_not_abort_publish() -> None:
    received: list[CleanBarEvent] = []

    def boom(_bar: CleanBarEvent) -> None:
        raise RuntimeError("order bug")

    processor = StreamDataProcessor(
        symbols=["SPY"],
        timeframe="1m",
        consumers=[boom, received.append],
        require_minute_alignment=False,
        dedup_window=10,
    )
    event = processor.process_bar(
        {
            "t": "2026-08-17T14:30:00Z",
            "o": 100.0,
            "h": 100.1,
            "l": 99.9,
            "c": 100.05,
            "v": 10,
        },
        symbol="SPY",
        timeframe="1m",
    )

    assert event is not None
    assert len(received) == 1
    assert received[0].symbol == "SPY"


def test_processor_counts_dropped_invalid_bars() -> None:
    processor = StreamDataProcessor(
        symbols=["SPY"],
        timeframe="1m",
        require_minute_alignment=False,
        dedup_window=10,
    )
    dropped = processor.process_message("not-json")
    assert dropped is None
    assert processor.dropped_bar_count == 1


def test_missed_clock_bars_are_counted() -> None:
    workflow = _bare_workflow()
    workflow._run_trading_path = lambda _bar: None  # type: ignore[method-assign]
    first = CleanBarEvent(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
    )
    skipped = CleanBarEvent(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 8, 17, 14, 3, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
    )
    workflow._on_clean_bar(first)
    workflow._on_clean_bar(skipped)
    assert workflow._missed_bars == 2


def test_hot_restart_keeps_databento_session() -> None:
    workflow = _bare_workflow()
    stream = workflow._databento_stream
    workflow.risk_guard.record_opened_trade(datetime(2026, 8, 17, tzinfo=timezone.utc).date())
    old_registry = workflow.strategy_registry
    new_config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_timeframe": "5t",
                    "strategy_timeframe": "5t",
                    "aggregation_timeframes": [],
                },
                "workflow": {"stream_provider": "databento"},
                "strategies": ["gaussian_ma_crossover"],
                "broker": {"provider": "schwab", "use_in_memory": True},
            }
        )
    )

    workflow.restart_trading(config=new_config, reload_config=False)

    assert workflow._databento_stream is stream
    assert workflow.strategy_registry is not old_registry
    assert workflow._trading_generation == 1
    _, trades_today, _ = workflow.risk_guard.snapshot_daily_state()
    assert trades_today == 1


def test_es_signal_resolves_to_spy_option_order() -> None:
    workflow = _bare_workflow()
    workflow._config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_symbols": ["ES.n.0"],
                    "stream_to_trade": {"ES.n.0": "SPY"},
                    "stream_timeframe": "5t",
                    "strategy_timeframe": "5t",
                    "aggregation_timeframes": [],
                },
                "workflow": {
                    "stream_provider": "databento",
                    "entry_start_local": "09:30",
                    "entry_end_local": "16:00",
                    "min_warmup_bars": 1,
                },
                "strategies": ["gaussian_ma_crossover"],
                "options": {"enabled": True},
                "broker": {"provider": "schwab", "use_in_memory": True},
            }
        )
    )
    workflow._stream_symbols = ("ES.n.0",)
    workflow._trade_symbols = ("SPY",)
    workflow._symbols = workflow._stream_symbols
    workflow.position_tracker = MagicMock()
    workflow.position_tracker.get_position_for_underlying.return_value = None
    workflow._close_existing_option_on_flip = MagicMock()
    workflow._has_enough_warmup_bars = MagicMock(return_value=True)
    workflow._has_enough_volume_history = MagicMock(return_value=True)
    workflow.risk_guard = MagicMock()
    workflow.risk_guard.evaluate.return_value = SimpleNamespace(approved=True)
    workflow._trade_emailer = None
    workflow.order_manager = MagicMock()
    workflow.order_manager.submit_signal.return_value = SimpleNamespace(id="order-1")
    resolved = ResolvedTrade(
        symbol="SPY   260819C00500000",
        underlying_symbol="SPY",
        asset_type="OPTION",
        quantity=1.0,
        mark_price=2.5,
    )
    workflow._resolve_trade = MagicMock(return_value=resolved)
    signal = StrategySignal(
        symbol="ES.n.0",
        timeframe="5t",
        timestamp=datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc),
        action=SignalAction.BUY,
        strategy_name="gaussian_ma_crossover",
        close=5_000.0,
        indicators={},
    )

    workflow._execute_strategy_signal(signal)

    resolved_signal = workflow._resolve_trade.call_args.args[0]
    assert resolved_signal.symbol == "SPY"
    assert resolved_signal.close == 500.0
    submitted = workflow.order_manager.submit_signal.call_args.args[0]
    assert submitted.symbol == "SPY   260819C00500000"
    assert submitted.underlying_symbol == "SPY"
    assert submitted.asset_type == "OPTION"
    workflow.order_manager.refresh_order.assert_called_once_with("order-1")


def test_approx_trade_spot_scales_es_to_spy() -> None:
    workflow = _bare_workflow()
    workflow._config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_symbols": ["ES.n.0"],
                    "stream_to_trade": {"ES.n.0": "SPY"},
                }
            }
        )
    )
    assert workflow._approx_trade_spot_from_stream("SPY", stream_close=5_000.0) == 500.0
    assert workflow._approx_trade_spot_from_stream("SPY", stream_close=500.0) == 500.0


def test_gex_eval_compares_scaled_spy_close_not_raw_es() -> None:
    workflow = _bare_workflow()
    workflow._config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_symbols": ["ES.n.0"],
                    "stream_to_trade": {"ES.n.0": "SPY"},
                    "stream_timeframe": "5t",
                    "strategy_timeframe": "5t",
                },
                "strategies": ["gex_scalp"],
                "gex": {"enabled": True},
            }
        )
    )
    captured: dict[str, object] = {}

    def capture_evaluate(**kwargs: object) -> StrategySignal:
        captured.update(kwargs)
        return StrategySignal(
            symbol=str(kwargs["symbol"]),
            timeframe=str(kwargs["timeframe"]),
            timestamp=kwargs["timestamp"],  # type: ignore[arg-type]
            action=SignalAction.HOLD,
            strategy_name="gex_scalp",
            close=float(kwargs["close"]),  # type: ignore[arg-type]
            indicators={},
        )

    workflow.signal_evaluator = MagicMock()
    workflow.signal_evaluator.evaluate.side_effect = capture_evaluate
    workflow._gex_monitor = MagicMock()
    snapshot = MagicMock()
    snapshot.with_live_spot.side_effect = lambda spot: snapshot
    snapshot.timestamp = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    snapshot.put_wall = 535.0
    snapshot.flip_level = 540.0
    snapshot.regime = "negative"
    snapshot.net_gex = -1_000_000.0
    workflow._gex_monitor.get_latest.return_value = snapshot
    workflow.position_tracker = MagicMock()
    workflow.position_tracker.get_position_for_underlying.return_value = None
    workflow._gex_strategy_state = {}
    workflow._volume_history = {"ES.n.0": deque(maxlen=5)}
    workflow._gex_waiting_snapshot_logged = set()
    workflow._gex_status_log = {}
    workflow._handle_strategy_signal = MagicMock()
    bar = CleanBarEvent(
        symbol="ES.n.0",
        timeframe="5t",
        timestamp=datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc),
        open=5_000.0,
        high=5_010.0,
        low=4_990.0,
        close=5_000.0,
        volume=10.0,
    )

    workflow._evaluate_gex_strategies(bar)

    assert captured["symbol"] == "SPY"
    assert captured["close"] == 500.0
    assert captured["open"] == 500.0
    assert captured["high"] == 501.0
    assert captured["low"] == 499.0
    assert "SPY" in workflow._gex_strategy_state


def test_option_risk_stop_looks_up_stream_symbol_spot() -> None:
    workflow = _bare_workflow()
    workflow._config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_symbols": ["ES.n.0"],
                    "stream_to_trade": {"ES.n.0": "SPY"},
                    "stream_timeframe": "5t",
                    "strategy_timeframe": "5t",
                },
                "options": {"enabled": True},
            }
        )
    )
    workflow.indicator_coordinator.latest_close.return_value = 5_000.0
    workflow._flatten_open_option_position = MagicMock()
    workflow._flattening_contracts = set()
    workflow._trade_emailer = None
    position = SimpleNamespace(
        symbol="SPY   260819C00500000",
        underlying_symbol="SPY",
        average_entry_price=2.5,
        underlying_entry_price=500.0,
        max_mark_price=3.0,
        stop_loss_pct=0.3,
    )

    workflow._exit_on_option_risk_stop(
        position,  # type: ignore[arg-type]
        1.5,
        datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc),
        reason=ExitReason.STOP_LOSS,
    )

    workflow.indicator_coordinator.latest_close.assert_called_with("ES.n.0", "5t")
    kwargs = workflow._flatten_open_option_position.call_args.kwargs
    assert kwargs["underlying_symbol"] == "SPY"
    assert kwargs["underlying_spot"] == 500.0


def test_hot_restart_skips_trading_without_blocking_ingest() -> None:
    workflow = _bare_workflow()
    ingested: list[CleanBarEvent] = []
    original_ingest = workflow._ingest_clean_bar

    def tracking_ingest(bar: CleanBarEvent) -> None:
        ingested.append(bar)
        original_ingest(bar)

    workflow._ingest_clean_bar = tracking_ingest  # type: ignore[method-assign]
    workflow._run_trading_path = MagicMock(side_effect=AssertionError("should skip"))
    held = workflow._trading_lock.acquire()
    assert held
    try:
        workflow._on_clean_bar(_bar(datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)))
    finally:
        workflow._trading_lock.release()

    assert len(ingested) == 1
    workflow._run_trading_path.assert_not_called()


def test_sample_runtime_health_reports_queue_and_flush_lag() -> None:
    workflow = _bare_workflow()
    persist = MagicMock()
    persist.approx_queued.return_value = 42
    persist.poll_stats.return_value = [{"kind": "flush", "duration_s": 9.5}]
    workflow._trade_persist = persist
    session = MagicMock()
    session.buffered_row_count = 8
    workflow._session_recorder = session
    workflow.stream_processor.dropped_bar_count = 4

    workflow._sample_runtime_health()
    snapshot = workflow.health_monitor.check()

    assert snapshot.queue_depth == 50
    assert snapshot.flush_lag_seconds == 9.5
    assert snapshot.missed_bars == 4


def test_eod_session_close_flattens_open_positions_at_4pm_et() -> None:
    workflow = _bare_workflow()
    workflow._config = WorkflowConfig.from_app_config(
        AppConfig.from_dict(
            {
                "market": {
                    "symbols": ["SPY"],
                    "stream_symbols": ["ES.n.0"],
                    "stream_to_trade": {"ES.n.0": "SPY"},
                    "stream_timeframe": "5t",
                    "strategy_timeframe": "5t",
                },
                "workflow": {
                    "eod_enabled": True,
                    "eod_flatten_time_local": "16:00",
                    "eod_shutdown_time_local": "20:00",
                },
            }
        )
    )
    workflow._eod_flattened_on = None
    workflow._flatten_all_positions_eod = MagicMock()
    before_close = CleanBarEvent(
        symbol="ES.n.0",
        timeframe="5t",
        timestamp=datetime(2026, 8, 17, 19, 55, tzinfo=timezone.utc),
        open=5_000.0,
        high=5_010.0,
        low=4_990.0,
        close=5_000.0,
        volume=10.0,
    )
    at_close = CleanBarEvent(
        symbol="ES.n.0",
        timeframe="5t",
        timestamp=datetime(2026, 8, 17, 20, 0, tzinfo=timezone.utc),
        open=5_000.0,
        high=5_010.0,
        low=4_990.0,
        close=5_005.0,
        volume=10.0,
    )

    workflow._enforce_eod_session_close(before_close)
    workflow._flatten_all_positions_eod.assert_not_called()

    workflow._enforce_eod_session_close(at_close)
    workflow._flatten_all_positions_eod.assert_called_once_with(
        at_close.timestamp,
        bar=at_close,
    )
    assert workflow._eod_flattened_on == datetime(2026, 8, 17, tzinfo=timezone.utc).date()

    workflow._flatten_all_positions_eod.reset_mock()
    workflow._enforce_eod_session_close(at_close)
    workflow._flatten_all_positions_eod.assert_not_called()
