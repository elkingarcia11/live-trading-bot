"""Tests for GEX aggregation."""

from __future__ import annotations

from datetime import date

from gex_calculator import (
    attach_gex,
    build_snapshot,
    call_gex,
    find_put_wall,
    find_zero_gamma_flip,
    put_gex,
)
from options_chain_transformer import StrikeRow


def _row(
    *,
    strike: float,
    option_type: str,
    oi: int,
    gamma: float,
) -> StrikeRow:
    return StrikeRow(
        symbol=f"SPY{strike}{option_type[0].upper()}",
        underlying="SPY",
        strike=strike,
        option_type=option_type,
        open_interest=oi,
        iv=0.20,
        expiration_date=date(2026, 7, 7),
        days_to_expiration=0,
        spot=100.0,
        gamma=gamma,
    )


def test_call_and_put_gex_signs() -> None:
    assert call_gex(0.05, 1000, 100.0) > 0
    assert put_gex(0.05, 1000, 100.0) < 0


def test_find_put_wall_returns_most_negative_put_strike() -> None:
    rows = attach_gex(
        [
            _row(strike=95.0, option_type="put", oi=1000, gamma=0.05),
            _row(strike=96.0, option_type="put", oi=5000, gamma=0.05),
        ]
    )
    assert find_put_wall(rows) == 96.0


def test_build_snapshot_classifies_negative_regime_below_flip() -> None:
    rows = attach_gex(
        [
            _row(strike=99.0, option_type="call", oi=2000, gamma=0.04),
            _row(strike=100.0, option_type="call", oi=1000, gamma=0.04),
            _row(strike=101.0, option_type="put", oi=8000, gamma=0.05),
        ]
    )
    snapshot = build_snapshot("SPY", rows)
    assert snapshot.put_wall == 101.0
    assert find_zero_gamma_flip(rows) is not None
    assert snapshot.regime in {"negative", "positive"}
