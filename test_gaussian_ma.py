"""Tests for Pine-style Gaussian MA (slow/fast) on tick bars."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from config import _parse_indicator_config, _parse_options_settings
from indicator_calculator import IndicatorCalculator
from indicator_coordinator import IndicatorCoordinator, SymbolIndicatorConfig
from stream_data_processor import CleanBarEvent


def _pine_gaussian_ma(closes: list[float], length: int, sigma_div: float) -> float:
    """Exact TradingView Gaussian MA-EZ last-bar value."""
    weights = []
    for i in range(length):
        x = i / (length / sigma_div)
        weights.append(math.exp(-0.5 * x * x))
    total = sum(weights)
    weights = [w / total for w in weights]
    return sum(closes[-(i + 1)] * weights[i] for i in range(length))


def test_gaussian_ma_matches_pine_weights() -> None:
    closes = [100.0 + i * 0.1 for i in range(30)]
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-06-01", periods=30, freq="min", tz="UTC"),
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": 50.0,
        }
    )
    calc = IndicatorCalculator()
    slow = calc.gaussian_ma(frame, length=20, sigma_divisor=7.0, output_key="slow")
    fast = calc.gaussian_ma(frame, length=20, sigma_divisor=10.0, output_key="fast")

    assert math.isclose(
        float(slow["slow"].iloc[-1]),
        _pine_gaussian_ma(closes, 20, 7.0),
        rel_tol=1e-9,
    )
    assert math.isclose(
        float(fast["fast"].iloc[-1]),
        _pine_gaussian_ma(closes, 20, 10.0),
        rel_tol=1e-9,
    )
    assert float(slow["slow"].iloc[-1]) != float(fast["fast"].iloc[-1])


def test_config_builds_slow_and_fast_gaussian_ma_jobs() -> None:
    payload = json.loads(Path("config.json").read_text(encoding="utf-8"))
    indicators = _parse_indicator_config(payload["indicators"])
    options = _parse_options_settings(payload["options"])
    jobs = indicators.build_jobs(payload["market"]["strategy_timeframe"])

    keys = {dict(job.params).get("output_key") for job in jobs if job.name == "gaussian_ma"}
    assert keys == {"gaussian_ma_slow", "gaussian_ma_fast"}
    assert options.trailing_stop_pct == 0.10
    assert options.stop_loss_pct == 0.05

    by_key = {
        dict(job.params)["output_key"]: dict(job.params)
        for job in jobs
        if job.name == "gaussian_ma"
    }
    assert by_key["gaussian_ma_slow"]["length"] == 10
    assert by_key["gaussian_ma_slow"]["sigma_divisor"] == 9.5
    assert by_key["gaussian_ma_fast"]["length"] == 4
    assert by_key["gaussian_ma_fast"]["sigma_divisor"] == 7.0


def test_gaussian_ma_leg_parses_ema_and_sma_lengths() -> None:
    from config import _parse_indicator_config

    indicators = _parse_indicator_config(
        {
            "max_bars": 500,
            "dema": {"enabled": False},
            "supertrend": {"enabled": False},
            "gaussian_bands": {"enabled": False},
            "gaussian_ma": {
                "enabled": True,
                "fast": {"length": 4, "sigma_divisor": 7.0, "ema_length": 6},
                "slow": {"length": 10, "sigma_divisor": 9.5, "sma_length": 5},
            },
        }
    )
    assert indicators.gaussian_ma.fast.ema_length == 6
    assert indicators.gaussian_ma.slow.sma_length == 5
    # Unspecified leg smooths fall back to defaults.
    assert indicators.gaussian_ma.slow.ema_length is None
    assert indicators.gaussian_ma.fast.sma_length == 3


def test_coordinator_updates_gaussian_ma_on_50t_stream_bars() -> None:
    indicators = _parse_indicator_config(
        {
            "max_bars": 500,
            "dema": {"enabled": False},
            "supertrend": {"enabled": False},
            "gaussian_bands": {"enabled": False},
            "gaussian_ma": {
                "enabled": True,
                "fast": {"length": 20, "sigma_divisor": 10.0},
                "slow": {"length": 20, "sigma_divisor": 7.0},
            },
        }
    )
    coordinator = IndicatorCoordinator(max_bars=indicators.max_bars)
    jobs = indicators.build_jobs("50t")
    coordinator.register(SymbolIndicatorConfig(symbol="SPY", jobs=jobs))

    base = datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc)
    snapshot = None
    for i in range(25):
        close = 500.0 + i * 0.05
        snapshot = coordinator.on_stream_bar(
            CleanBarEvent(
                symbol="SPY",
                timeframe="50t",
                timestamp=base + timedelta(seconds=i),
                open=close,
                high=close,
                low=close,
                close=close,
                volume=50.0,
            )
        )

    assert snapshot is not None
    assert snapshot.values.get("gaussian_ma_slow") is not None
    assert snapshot.values.get("gaussian_ma_fast") is not None
