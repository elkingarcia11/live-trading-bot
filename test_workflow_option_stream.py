"""Tests for Schwab LEVELONE_OPTIONS streaming alongside Databento."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import AppConfig
from workflow import TradingWorkflow, WorkflowConfig


def _databento_app(*, stream_contract_marks: bool = True) -> AppConfig:
    return AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_symbols": ["ES.n.0"],
                "stream_timeframe": "400t",
                "strategy_timeframe": "400t",
                "aggregation_timeframes": [],
            },
            "workflow": {"stream_provider": "databento"},
            "options": {
                "enabled": True,
                "stream_contract_marks": stream_contract_marks,
            },
            "broker": {"provider": "schwab", "use_in_memory": True},
        }
    )


def test_databento_mode_builds_schwab_option_stream() -> None:
    config = WorkflowConfig.from_app_config(_databento_app())
    session = MagicMock(name="SchwabStreamSession")
    with patch(
        "workflow.SchwabStreamSession.from_env",
        return_value=session,
    ) as from_env:
        built = TradingWorkflow._build_schwab_option_stream(
            object.__new__(TradingWorkflow),
            config,
        )
    assert built is session
    from_env.assert_called_once()
    kwargs = from_env.call_args.kwargs
    assert kwargs["subscribe_on_connect"] is False
    assert kwargs["on_option_quote"] is not None


def test_schwab_bar_provider_skips_secondary_option_stream() -> None:
    app = AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_timeframe": "400t",
                "strategy_timeframe": "400t",
                "aggregation_timeframes": [],
            },
            "workflow": {"stream_provider": "schwab"},
            "options": {"enabled": True, "stream_contract_marks": True},
        }
    )
    config = WorkflowConfig.from_app_config(app)
    workflow = object.__new__(TradingWorkflow)
    assert TradingWorkflow._build_schwab_option_stream(workflow, config) is None


def test_option_stream_active_when_secondary_session_present() -> None:
    workflow = object.__new__(TradingWorkflow)
    workflow._schwab_stream = None
    workflow._schwab_option_stream = MagicMock()
    workflow._config = SimpleNamespace(
        app=SimpleNamespace(
            options=SimpleNamespace(stream_contract_marks=True),
        )
    )
    assert workflow._option_stream_active() is True


def test_option_stream_inactive_without_session() -> None:
    workflow = object.__new__(TradingWorkflow)
    workflow._schwab_stream = None
    workflow._schwab_option_stream = None
    workflow._config = SimpleNamespace(
        app=SimpleNamespace(
            options=SimpleNamespace(stream_contract_marks=True),
        )
    )
    assert workflow._option_stream_active() is False
