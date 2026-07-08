"""Tests for GEX scalping strategy rules."""

from __future__ import annotations

from datetime import datetime, timezone

from gex_calculator import GexSnapshot
from gex_scalp_feedback import describe_gex_scalp_status
from signal_evaluator import SignalEvaluator
from strategy_registry import (
    SignalAction,
    StrategyEvaluationContext,
    build_default_registry,
)


def _snapshot(**overrides: object) -> GexSnapshot:
    base = {
        "symbol": "SPY",
        "timestamp": datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc),
        "spot": 538.0,
        "net_gex": -1_000_000.0,
        "regime": "negative",
        "flip_level": 540.0,
        "put_wall": 535.0,
        "call_wall": 542.0,
    }
    base.update(overrides)
    return GexSnapshot(**base)  # type: ignore[arg-type]


def test_gex_scalp_puts_on_put_wall_break_with_volume_spike() -> None:
    evaluator = SignalEvaluator(build_default_registry())
    state: dict[str, object] = {}
    signal = evaluator.evaluate(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 7, 7, 15, 1, tzinfo=timezone.utc),
        close=534.0,
        open=535.5,
        high=536.0,
        low=533.8,
        volume=2_000_000.0,
        indicators={
            "volume_sma": 1_000_000.0,
            "gex_volume_multiplier": 1.5,
            "gex_put_wall_break_pct": 0.001,
            "gex_stall_body_ratio": 0.25,
            "gex_long_wick_ratio": 0.55,
        },
        strategy_name="gex_scalp",
        gex=_snapshot(),
        has_open_position=False,
        state=state,
    )
    assert signal.action == SignalAction.SELL
    assert state["entry_side"] == "put"


def test_gex_scalp_holds_when_regime_is_positive() -> None:
    evaluator = SignalEvaluator(build_default_registry())
    signal = evaluator.evaluate(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 7, 7, 15, 1, tzinfo=timezone.utc),
        close=541.0,
        open=540.5,
        high=541.5,
        low=540.0,
        volume=2_000_000.0,
        indicators={"volume_sma": 1_000_000.0, "gex_volume_multiplier": 1.5},
        strategy_name="gex_scalp",
        gex=_snapshot(regime="positive", spot=541.0),
        has_open_position=False,
        state={},
    )
    assert signal.action == SignalAction.HOLD


def test_gex_scalp_exits_when_price_reclaims_trigger_level() -> None:
    rule = build_default_registry().get("gex_scalp").rule
    action = rule(
        StrategyEvaluationContext(
            symbol="SPY",
            timeframe="1m",
            timestamp=datetime(2026, 7, 7, 15, 2, tzinfo=timezone.utc),
            close=536.0,
            open=535.0,
            high=536.5,
            low=534.8,
            volume=1_000_000.0,
            indicators={},
            gex=_snapshot(),
            has_open_position=True,
            state={"trigger_level": 535.0, "entry_side": "put"},
        )
    )
    assert action == SignalAction.EXIT


def test_gex_scalp_status_describes_regime_blocker() -> None:
    status = describe_gex_scalp_status(
        StrategyEvaluationContext(
            symbol="SPY",
            timeframe="1m",
            timestamp=datetime(2026, 7, 7, 15, 1, tzinfo=timezone.utc),
            close=541.0,
            volume=2_000_000.0,
            indicators={"volume_sma": 1_000_000.0, "gex_volume_multiplier": 1.5},
            gex=_snapshot(regime="positive", spot=541.0),
            has_open_position=False,
            state={},
        ),
        action=SignalAction.HOLD,
    )
    assert "need negative GEX regime" in status


def test_gex_scalp_status_describes_volume_blocker() -> None:
    status = describe_gex_scalp_status(
        StrategyEvaluationContext(
            symbol="SPY",
            timeframe="1m",
            timestamp=datetime(2026, 7, 7, 15, 1, tzinfo=timezone.utc),
            close=538.0,
            volume=500_000.0,
            indicators={
                "volume_sma": 1_000_000.0,
                "gex_volume_multiplier": 1.5,
                "gex_put_wall_break_pct": 0.001,
            },
            gex=_snapshot(),
            has_open_position=False,
            state={},
        ),
        action=SignalAction.HOLD,
    )
    assert "need volume spike" in status
    assert "need>=1,500,000" in status


def test_gex_scalp_status_describes_armed_state() -> None:
    status = describe_gex_scalp_status(
        StrategyEvaluationContext(
            symbol="SPY",
            timeframe="1m",
            timestamp=datetime(2026, 7, 7, 15, 1, tzinfo=timezone.utc),
            close=538.0,
            volume=2_000_000.0,
            indicators={
                "volume_sma": 1_000_000.0,
                "gex_volume_multiplier": 1.5,
                "gex_put_wall_break_pct": 0.001,
            },
            gex=_snapshot(),
            has_open_position=False,
            state={},
        ),
        action=SignalAction.HOLD,
    )
    assert status.startswith("armed:")
    assert "put_wall break" in status
    assert "flip magnet" in status
