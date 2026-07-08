"""Tests for Black-Scholes greeks calculator."""

from __future__ import annotations

from greeks_calculator import black_scholes_delta, black_scholes_gamma, year_fraction


def test_atm_call_delta_near_half_with_time_remaining() -> None:
    delta = black_scholes_delta(
        spot=100.0,
        strike=100.0,
        time_years=year_fraction(30),
        iv=0.20,
        option_type="call",
    )
    assert 0.45 < delta < 0.60


def test_gamma_is_positive_for_atm_option() -> None:
    gamma = black_scholes_gamma(
        spot=100.0,
        strike=100.0,
        time_years=year_fraction(1),
        iv=0.25,
    )
    assert gamma > 0.0
