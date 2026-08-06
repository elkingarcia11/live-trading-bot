"""Tests for entry window, volume-history gate, and EOD schedule wiring."""

from __future__ import annotations

from collections import deque
from datetime import datetime, time, timezone
from unittest.mock import MagicMock

from config import AppConfig
from market_session_scheduler import is_local_session_timestamp, parse_hhmm
from signal_evaluator import StrategySignal
from strategy_registry import SignalAction
from workflow import TradingWorkflow, WorkflowConfig


def _bare_workflow(**overrides: object) -> TradingWorkflow:
    app = AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_timeframe": "50t",
                "strategy_timeframe": "50t",
                "aggregation_timeframes": [],
            },
            "workflow": {
                "stream_provider": "databento",
                "entry_start_local": "09:30",
                "entry_end_local": "16:00",
                "eod_flatten_time_local": "16:00",
                "eod_shutdown_time_local": "20:00",
                "min_warmup_bars": 100,
            },
            "strategies": ["gex_scalp"],
            "gex": {
                "enabled": True,
                "volume_lookback_bars": 5,
            },
            "options": {"enabled": True},
            "broker": {"provider": "schwab", "use_in_memory": True},
        }
    )
    config = WorkflowConfig.from_app_config(app)
    workflow = object.__new__(TradingWorkflow)
    workflow._config = config
    workflow._symbols = ("SPY",)
    workflow._volume_history = {
        "SPY": deque(maxlen=config.app.gex.volume_lookback_bars)
    }
    workflow._warmup_ready_logged = set()
    workflow._live_regular_hours_seen = True
    coordinator = MagicMock()
    coordinator.buffered_bar_count.return_value = 100
    workflow.indicator_coordinator = coordinator
    for key, value in overrides.items():
        setattr(workflow, key, value)
    return workflow


def test_entry_window_is_0930_to_1559_et() -> None:
    # EDT: ET = UTC-4
    before = datetime(2026, 6, 17, 13, 29, tzinfo=timezone.utc)  # 09:29
    open_ = datetime(2026, 6, 17, 13, 30, tzinfo=timezone.utc)  # 09:30
    late = datetime(2026, 6, 17, 19, 59, tzinfo=timezone.utc)  # 15:59
    close = datetime(2026, 6, 17, 20, 0, tzinfo=timezone.utc)  # 16:00

    kwargs = dict(
        session_start_local=parse_hhmm("09:30"),
        session_end_local=parse_hhmm("16:00"),
        market_timezone="America/New_York",
    )
    assert is_local_session_timestamp(before, **kwargs) is False
    assert is_local_session_timestamp(open_, **kwargs) is True
    assert is_local_session_timestamp(late, **kwargs) is True
    assert is_local_session_timestamp(close, **kwargs) is False


def test_workflow_is_entry_window_matches_config() -> None:
    workflow = _bare_workflow()
    assert (
        workflow._is_entry_window(datetime(2026, 6, 17, 13, 30, tzinfo=timezone.utc))
        is True
    )
    assert (
        workflow._is_entry_window(datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc))
        is False
    )
    assert (
        workflow._is_entry_window(datetime(2026, 6, 17, 20, 0, tzinfo=timezone.utc))
        is False
    )


def test_volume_history_gate_blocks_until_lookback_filled() -> None:
    workflow = _bare_workflow()
    assert workflow._has_enough_volume_history("SPY") is False

    seeded = workflow.seed_gex_volume_history("SPY", [1, 2, 3, 4])
    assert seeded == 4
    assert workflow._has_enough_volume_history("SPY") is False

    workflow.seed_gex_volume_history("SPY", [5])
    # seed appends; deque already had 4, +1 => 5
    assert len(workflow._volume_history["SPY"]) == 5
    assert workflow._has_enough_volume_history("SPY") is True


def test_warmup_bar_gate_blocks_until_100_bars() -> None:
    workflow = _bare_workflow()
    workflow.indicator_coordinator.buffered_bar_count.return_value = 40
    assert workflow._has_enough_warmup_bars("SPY") is False

    workflow.indicator_coordinator.buffered_bar_count.return_value = 100
    assert workflow._has_enough_warmup_bars("SPY") is True


def test_handle_strategy_signal_blocks_when_warmup_thin() -> None:
    workflow = _bare_workflow()
    workflow.indicator_coordinator.buffered_bar_count.return_value = 20
    workflow.seed_gex_volume_history("SPY", [1, 2, 3, 4, 5])
    workflow.position_tracker = MagicMock()
    workflow.position_tracker.get_position_for_underlying.return_value = None
    workflow.risk_guard = MagicMock()
    workflow._resolve_trade = MagicMock()
    workflow.bus = MagicMock()
    workflow._trade_emailer = None

    signal = StrategySignal(
        strategy_name="gaussian_ma_crossover",
        symbol="SPY",
        action=SignalAction.BUY,
        timeframe="50t",
        timestamp=datetime(2026, 6, 17, 15, 0, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
    )
    workflow._handle_strategy_signal(signal)
    workflow._resolve_trade.assert_not_called()


def test_handle_strategy_signal_blocks_when_volume_history_thin() -> None:
    workflow = _bare_workflow()
    workflow.seed_gex_volume_history("SPY", [10.0, 11.0])  # 2 < 5
    workflow.position_tracker = MagicMock()
    workflow.position_tracker.get_position_for_underlying.return_value = None
    workflow.risk_guard = MagicMock()
    workflow._resolve_trade = MagicMock()
    workflow.bus = MagicMock()
    workflow._trade_emailer = None

    signal = StrategySignal(
        strategy_name="gex_scalp",
        symbol="SPY",
        action=SignalAction.SELL,
        timeframe="50t",
        timestamp=datetime(2026, 6, 17, 15, 0, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
    )
    workflow._handle_strategy_signal(signal)
    workflow._resolve_trade.assert_not_called()


def test_handle_strategy_signal_blocks_outside_entry_window() -> None:
    workflow = _bare_workflow()
    workflow.seed_gex_volume_history("SPY", [1, 2, 3, 4, 5])
    workflow.position_tracker = MagicMock()
    workflow.position_tracker.get_position_for_underlying.return_value = None
    workflow._resolve_trade = MagicMock()
    workflow.bus = MagicMock()
    workflow._trade_emailer = None
    workflow._close_existing_option_on_flip = MagicMock()

    # 08:00 ET pre-market
    signal = StrategySignal(
        strategy_name="gex_scalp",
        symbol="SPY",
        action=SignalAction.SELL,
        timeframe="50t",
        timestamp=datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc),
        close=500.0,
        indicators={},
    )
    workflow._handle_strategy_signal(signal)
    workflow._resolve_trade.assert_not_called()


def test_eod_schedule_flattens_at_4pm_and_shuts_down_at_8pm_et() -> None:
    app = AppConfig.from_dict(
        {
            "workflow": {
                "stream_provider": "databento",
                "eod_flatten_time_local": "16:00",
                "eod_shutdown_time_local": "20:00",
            },
            "historical": {"trading_days_only": True},
            "app": {"timezone": "America/New_York"},
        }
    )
    schedule = WorkflowConfig.from_app_config(app).eod_schedule
    assert schedule.flatten_time_local == time(16, 0)
    assert schedule.shutdown_time_local == time(20, 0)
    assert schedule.market_timezone == "America/New_York"
