"""Tests for CLI workflow config overrides (timeframe, GMA, position size)."""

from __future__ import annotations

import pytest

import config as config_module
from config import (
    AppConfig,
    WorkflowConfigOverrides,
    apply_config_overrides,
    get_config,
    set_config_overrides,
)
from workflow import build_workflow_arg_parser, overrides_from_args


def _base_app() -> AppConfig:
    """Mirror the production config.json shape used for live trading."""
    return AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_symbols": ["ES.n.0"],
                "stream_timeframe": "400t",
                "strategy_timeframe": "400t",
                "aggregation_timeframes": [],
            },
            "indicators": {
                "gaussian_ma": {
                    "enabled": True,
                    "fast": {"length": 4, "sigma_divisor": 7.0, "ema_length": 4},
                    "slow": {"length": 10, "sigma_divisor": 9.5, "sma_length": 3},
                },
            },
            "risk": {
                "position_size_pct": 0.3,
                "max_position_quantity": 10,
            },
        }
    )


# --- apply_config_overrides -------------------------------------------------


def test_none_overrides_return_same_instance() -> None:
    app = _base_app()
    assert apply_config_overrides(app, None) is app


def test_all_default_overrides_return_same_instance() -> None:
    app = _base_app()
    assert apply_config_overrides(app, WorkflowConfigOverrides()) is app


def test_timeframe_override_sets_stream_and_strategy() -> None:
    result = apply_config_overrides(
        _base_app(), WorkflowConfigOverrides(timeframe="25t")
    )
    assert result.market.stream_timeframe == "25t"
    assert result.market.strategy_timeframe == "25t"
    assert result.risk.max_position_quantity == 10


def test_gma_overrides_change_leg_params_only() -> None:
    overrides = WorkflowConfigOverrides(
        fast_gma_length=6,
        fast_gma_sigma=8.5,
        slow_gma_length=12,
        slow_gma_sigma=11.0,
    )
    result = apply_config_overrides(_base_app(), overrides)
    fast = result.indicators.gaussian_ma.fast
    slow = result.indicators.gaussian_ma.slow
    assert (fast.length, fast.sigma_divisor) == (6, 8.5)
    assert (slow.length, slow.sigma_divisor) == (12, 11.0)
    # Non-overridden leg fields are preserved.
    assert fast.ema_length == 4
    assert slow.sma_length == 3


def test_partial_gma_override_preserves_other_values() -> None:
    result = apply_config_overrides(
        _base_app(), WorkflowConfigOverrides(fast_gma_length=9)
    )
    fast = result.indicators.gaussian_ma.fast
    slow = result.indicators.gaussian_ma.slow
    assert fast.length == 9
    assert fast.sigma_divisor == 7.0
    assert slow.length == 10
    assert slow.sigma_divisor == 9.5


def test_position_size_override_sets_max_quantity() -> None:
    result = apply_config_overrides(
        _base_app(), WorkflowConfigOverrides(position_size=3)
    )
    assert result.risk.max_position_quantity == 3.0
    assert result.risk.position_size_pct == 0.3


def test_combined_overrides_apply_together() -> None:
    overrides = WorkflowConfigOverrides(
        timeframe="100t",
        fast_gma_length=5,
        slow_gma_sigma=10.0,
        position_size=7,
    )
    result = apply_config_overrides(_base_app(), overrides)
    assert result.market.stream_timeframe == "100t"
    assert result.market.strategy_timeframe == "100t"
    assert result.indicators.gaussian_ma.fast.length == 5
    assert result.indicators.gaussian_ma.fast.sigma_divisor == 7.0
    assert result.indicators.gaussian_ma.slow.length == 10
    assert result.indicators.gaussian_ma.slow.sigma_divisor == 10.0
    assert result.risk.max_position_quantity == 7.0


def test_original_app_is_not_mutated() -> None:
    fresh = _base_app()
    assert fresh.market.stream_timeframe == "400t"
    assert fresh.risk.max_position_quantity == 10


def test_invalid_timeframes_raise() -> None:
    for bad in ("", "abc", "0t"):
        with pytest.raises(ValueError):
            apply_config_overrides(
                _base_app(), WorkflowConfigOverrides(timeframe=bad)
            )


def test_minute_timeframe_override_is_accepted() -> None:
    result = apply_config_overrides(
        _base_app(), WorkflowConfigOverrides(timeframe="5m")
    )
    assert result.market.stream_timeframe == "5m"
    assert result.market.strategy_timeframe == "5m"


# --- argparse wiring --------------------------------------------------------


def test_parser_defaults_are_none() -> None:
    args = build_workflow_arg_parser().parse_args([])
    for field in (
        "timeframe",
        "fast_gma_length",
        "fast_gma_sigma",
        "slow_gma_length",
        "slow_gma_sigma",
        "position_size",
    ):
        assert getattr(args, field) is None
    assert overrides_from_args(args) is None


def test_parser_parses_all_flags() -> None:
    args = build_workflow_arg_parser().parse_args(
        [
            "--timeframe", "25t",
            "--fast-gma-length", "5",
            "--fast-gma-sigma", "8.0",
            "--slow-gma-length", "12",
            "--slow-gma-sigma", "10.5",
            "--position-size", "4",
        ]
    )
    overrides = overrides_from_args(args)
    assert overrides == WorkflowConfigOverrides(
        timeframe="25t",
        fast_gma_length=5,
        fast_gma_sigma=8.0,
        slow_gma_length=12,
        slow_gma_sigma=10.5,
        position_size=4.0,
    )


def test_partial_flags_produce_partial_overrides() -> None:
    overrides = overrides_from_args(
        build_workflow_arg_parser().parse_args(["--position-size", "2"])
    )
    assert overrides.position_size == 2.0
    assert overrides.timeframe is None
    assert overrides.fast_gma_length is None


def test_overrides_flow_through_get_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        config_module.AppConfig,
        "load",
        classmethod(lambda cls, path=None: _base_app()),
    )
    try:
        set_config_overrides(
            WorkflowConfigOverrides(timeframe="200t", position_size=5)
        )
        loaded = get_config(reload=True)
        assert loaded.market.stream_timeframe == "200t"
        assert loaded.market.strategy_timeframe == "200t"
        assert loaded.risk.max_position_quantity == 5.0

        set_config_overrides(None)
        restored = get_config(reload=True)
        assert restored.market.stream_timeframe == "400t"
        assert restored.risk.max_position_quantity == 10
    finally:
        set_config_overrides(None)
