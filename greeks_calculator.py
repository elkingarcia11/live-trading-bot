"""Greeks calculator.

Responsibility: Stateless Black-Scholes greeks (gamma, delta) per contract.
Does not fetch chains, manage configuration, or evaluate trading rules.
"""

from __future__ import annotations

import math
from dataclasses import replace
from typing import Optional

from options_chain_transformer import StrikeRow

DEFAULT_RISK_FREE_RATE = 0.05


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def year_fraction(days_to_expiration: int) -> float:
    """Convert calendar DTE to a year fraction for Black-Scholes."""
    return max(days_to_expiration, 0) / 365.0


def black_scholes_delta(
    spot: float,
    strike: float,
    time_years: float,
    iv: float,
    *,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    option_type: str = "call",
) -> float:
    """Return Black-Scholes delta for a European option."""
    if spot <= 0 or strike <= 0 or iv <= 0 or time_years <= 0:
        return 0.0
    sqrt_t = math.sqrt(time_years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate + 0.5 * iv * iv) * time_years
    ) / (iv * sqrt_t)
    if option_type.lower() == "put":
        return norm_cdf(d1) - 1.0
    return norm_cdf(d1)


def black_scholes_gamma(
    spot: float,
    strike: float,
    time_years: float,
    iv: float,
    *,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> float:
    """Return Black-Scholes gamma (same for calls and puts)."""
    if spot <= 0 or strike <= 0 or iv <= 0 or time_years <= 0:
        return 0.0
    sqrt_t = math.sqrt(time_years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate + 0.5 * iv * iv) * time_years
    ) / (iv * sqrt_t)
    return norm_pdf(d1) / (spot * iv * sqrt_t)


def enrich_strike_row(
    row: StrikeRow,
    *,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> StrikeRow:
    """Attach computed delta and gamma to one strike row."""
    time_years = year_fraction(row.days_to_expiration)
    # 0DTE: use a small epsilon so gamma math remains finite intraday.
    if time_years <= 0:
        time_years = 1.0 / (365.0 * 24.0 * 60.0)
    delta = black_scholes_delta(
        row.spot,
        row.strike,
        time_years,
        row.iv,
        risk_free_rate=risk_free_rate,
        option_type=row.option_type,
    )
    gamma = black_scholes_gamma(
        row.spot,
        row.strike,
        time_years,
        row.iv,
        risk_free_rate=risk_free_rate,
    )
    return replace(row, delta=delta, gamma=gamma)


def enrich_strikes(
    rows: list[StrikeRow],
    *,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> list[StrikeRow]:
    """Attach computed delta and gamma to every strike row."""
    return [enrich_strike_row(row, risk_free_rate=risk_free_rate) for row in rows]
