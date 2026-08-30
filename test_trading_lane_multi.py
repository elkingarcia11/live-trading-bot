"""Tests for multi-lane trading and Databento tick fan-out."""

from __future__ import annotations

from datetime import datetime, timezone
import threading

from config import AppConfig
from databento_streamer import (
    DatabentoStreamSession,
    build_databento_stream_processor,
)
from trading_lane import (
    build_lane_app_config,
    build_lane_runtimes,
    lane_tick_timeframes,
    parse_trading_lanes,
)
from workflow import TradingWorkflow, WorkflowConfig


def _base_app(*, lanes: list[dict] | None = None) -> AppConfig:
    payload = {
        "market": {
            "symbols": ["SPY"],
            "stream_symbols": ["ES.n.0"],
            "stream_timeframe": "400t",
            "strategy_timeframe": "400t",
            "aggregation_timeframes": [],
        },
        "workflow": {"stream_provider": "databento"},
        "email": {"forward_test": True, "recipients": ["test@example.com"]},
        "options": {"enabled": True, "stream_contract_marks": True},
        "broker": {"provider": "schwab", "use_in_memory": True},
        "strategies": ["gaussian_ma_crossover"],
        "indicators": {
            "gaussian_ma": {
                "enabled": True,
                "fast": {"length": 4, "sigma_divisor": 7.0, "ema_length": 4},
                "slow": {"length": 10, "sigma_divisor": 9.5, "sma_length": 3},
            }
        },
    }
    if lanes is not None:
        payload["lanes"] = lanes
    return AppConfig.from_dict(payload)


def test_parse_trading_lanes_from_config() -> None:
    app = _base_app(
        lanes=[
            {
                "timeframe": "400t",
                "fast_gma_length": 2,
                "fast_gma_sigma": 5,
                "slow_gma_length": 4,
                "slow_gma_sigma": 3,
                "position_size": 3000,
            },
            {
                "timeframe": "100t",
                "fast_gma_length": 5,
                "fast_gma_sigma": 1.5,
                "slow_gma_length": 10,
                "slow_gma_sigma": 2.5,
                "position_size": 3000,
            },
        ]
    )
    lanes = parse_trading_lanes(app.lanes)
    assert len(lanes) == 2
    assert lanes[0].timeframe == "400t"
    assert lanes[1].fast_gma_length == 5
    assert lane_tick_timeframes(lanes) == ("100t", "400t")


def test_build_lane_app_config_sets_ledger_and_gma() -> None:
    app = _base_app()
    lanes = parse_trading_lanes(
        [
            {
                "timeframe": "400t",
                "fast_gma_length": 2,
                "fast_gma_sigma": 5,
                "slow_gma_length": 4,
                "slow_gma_sigma": 3,
                "position_size": 3000,
            }
        ]
    )
    lane_app = build_lane_app_config(app, lanes[0])
    assert lane_app.market.strategy_timeframe == "400t"
    assert lane_app.indicators.gaussian_ma.fast.length == 2
    assert lane_app.risk.position_size_max_dollars == 3000.0
    assert lane_app.forward_test.transactions_csv_path == "data/transactions_400t.csv"


def test_build_lane_app_config_applies_stop_overrides() -> None:
    app = AppConfig.from_dict(
        {
            "market": {"symbols": ["SPY"], "stream_timeframe": "400t", "strategy_timeframe": "400t"},
            "options": {
                "enabled": True,
                "stop_loss_pct": 0.02,
                "trailing_stop_pct": 0.15,
            },
        }
    )
    lanes = parse_trading_lanes(
        [
            {
                "timeframe": "100t",
                "fast_gma_length": 5,
                "fast_gma_sigma": 1.5,
                "slow_gma_length": 10,
                "slow_gma_sigma": 2.5,
                "stop_loss_pct": 0.03,
                "trailing_stop_pct": 0.08,
            }
        ]
    )
    lane_app = build_lane_app_config(app, lanes[0])
    assert lane_app.options.stop_loss_pct == 0.03
    assert lane_app.options.trailing_stop_pct == 0.08


def test_build_lane_app_config_inherits_global_stops_when_omitted() -> None:
    app = AppConfig.from_dict(
        {
            "market": {"symbols": ["SPY"], "stream_timeframe": "400t", "strategy_timeframe": "400t"},
            "options": {
                "enabled": True,
                "stop_loss_pct": 0.02,
                "trailing_stop_pct": 0.15,
            },
        }
    )
    lanes = parse_trading_lanes(
        [
            {
                "timeframe": "100t",
                "fast_gma_length": 5,
                "fast_gma_sigma": 1.5,
                "slow_gma_length": 10,
                "slow_gma_sigma": 2.5,
            }
        ]
    )
    lane_app = build_lane_app_config(app, lanes[0])
    assert lane_app.options.stop_loss_pct == 0.02
    assert lane_app.options.trailing_stop_pct == 0.15


def test_parse_lane_stop_pct_rejects_invalid_values() -> None:
    import pytest

    with pytest.raises(ValueError, match="stop_loss_pct"):
        parse_trading_lanes(
            [
                {
                    "timeframe": "100t",
                    "fast_gma_length": 5,
                    "fast_gma_sigma": 1.5,
                    "slow_gma_length": 10,
                    "slow_gma_sigma": 2.5,
                    "stop_loss_pct": 1.5,
                }
            ]
        )


def test_build_lane_runtimes_isolates_trackers() -> None:
    app = _base_app(
        lanes=[
            {
                "timeframe": "400t",
                "fast_gma_length": 2,
                "fast_gma_sigma": 5,
                "slow_gma_length": 4,
                "slow_gma_sigma": 3,
            },
            {
                "timeframe": "100t",
                "fast_gma_length": 5,
                "fast_gma_sigma": 1.5,
                "slow_gma_length": 10,
                "slow_gma_sigma": 2.5,
            },
        ]
    )
    config = WorkflowConfig.from_app_config(app)
    runtimes = build_lane_runtimes(config, parse_trading_lanes(app.lanes))
    assert set(runtimes) == {"100t", "400t"}
    assert runtimes["400t"].position_tracker is not runtimes["100t"].position_tracker
    assert runtimes["400t"].forward_test_account is not None
    assert runtimes["100t"].forward_test_account is not None
    assert (
        runtimes["400t"].forward_test_account
        is not runtimes["100t"].forward_test_account
    )


def test_databento_multi_tick_builder_emits_both_timeframes() -> None:
    seen: list[str] = []

    def consumer(bar) -> None:
        seen.append(bar.timeframe)

    processor = build_databento_stream_processor(
        symbols=["ES.n.0"],
        consumers=[consumer],
        timeframe="100t",
        accepted_timeframes=("100t", "400t"),
        require_minute_alignment=False,
    )
    session = DatabentoStreamSession(
        api_key="test-key",
        symbols=["ES.n.0"],
        processor=processor,
        tick_timeframes=("100t", "400t"),
        apply_equity_session_filter=False,
    )
    ts = datetime(2026, 8, 28, 14, 30, tzinfo=timezone.utc)
    for index in range(400):
        payloads = session._update_tick_bars(
            symbol="ES.n.0",
            price=5000.0 + index * 0.01,
            size=1.0,
            timestamp=ts,
        )
        for payload in payloads:
            session._emit_completed_bar(payload, price=5000.0)

    assert "100t" in seen
    assert "400t" in seen
    assert seen.count("100t") == 4
    assert seen.count("400t") == 1


def test_handle_option_mark_updates_every_lane_with_the_contract() -> None:
    """Each lane holding an OCC symbol must receive marks (not just the first)."""
    app = _base_app(
        lanes=[
            {
                "timeframe": "400t",
                "fast_gma_length": 2,
                "fast_gma_sigma": 5,
                "slow_gma_length": 4,
                "slow_gma_sigma": 3,
            },
            {
                "timeframe": "100t",
                "fast_gma_length": 5,
                "fast_gma_sigma": 1.5,
                "slow_gma_length": 10,
                "slow_gma_sigma": 2.5,
            },
        ]
    )
    config = WorkflowConfig.from_app_config(app)
    runtimes = build_lane_runtimes(config, parse_trading_lanes(app.lanes))
    occ = "SPY   260831P00769000"
    opened_at = datetime(2026, 8, 28, 14, 12, tzinfo=timezone.utc)
    for runtime in runtimes.values():
        runtime.position_tracker.open_position(
            symbol=occ,
            quantity=1,
            entry_price=1.82,
            opened_at=opened_at,
            asset_type="OPTION",
            underlying_symbol="SPY",
            underlying_entry_price=774.0,
        )

    workflow = object.__new__(TradingWorkflow)
    workflow._trading_lock = threading.Lock()
    workflow._multi_lane = True
    workflow._lane_bound = False
    workflow._lanes = runtimes
    workflow._flattening_contracts = set()
    primary = runtimes["400t"]
    workflow._config = config
    workflow.position_tracker = primary.position_tracker
    workflow._forward_test_account = None
    workflow._transaction_ledger = None
    workflow.strategy_registry = primary.strategy_registry
    workflow.signal_evaluator = primary.signal_evaluator
    workflow.indicator_coordinator = primary.indicator_coordinator
    workflow.risk_guard = primary.risk_guard
    workflow._strategy_state = {}
    seen: list[str] = []

    def _capture(occ_symbol: str, mark: float, timestamp: datetime) -> None:
        seen.append(workflow._config.market_config.strategy_timeframe)

    workflow._process_option_mark = _capture  # type: ignore[method-assign]
    workflow._bind_lane = TradingWorkflow._bind_lane.__get__(workflow, TradingWorkflow)

    workflow._handle_option_mark(occ, 1.51, opened_at)

    assert seen == ["400t", "100t"]
