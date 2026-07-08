"""Options chain transformer.

Responsibility: Normalize vendor option chain payloads into a standard per-strike
schema. Does not perform HTTP requests, greeks math, or GEX aggregation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from option_selector import underlying_price_from_chain


@dataclass(frozen=True)
class StrikeRow:
    """One option contract row used by greeks and GEX calculators."""

    symbol: str
    underlying: str
    strike: float
    option_type: str  # "call" or "put"
    open_interest: int
    iv: float
    expiration_date: date
    days_to_expiration: int
    spot: float
    gamma: float = 0.0
    delta: float = 0.0
    gex: float = 0.0


def normalize_schwab_chain(
    chain: dict[str, Any],
    *,
    underlying_symbol: str,
    spot: Optional[float] = None,
) -> list[StrikeRow]:
    """Convert a Schwab option chain payload into standard strike rows."""
    underlying = underlying_symbol.upper()
    resolved_spot = spot if spot is not None and spot > 0 else underlying_price_from_chain(chain)
    if resolved_spot is None or resolved_spot <= 0:
        raise ValueError("option chain has no usable underlying spot price")

    rows: list[StrikeRow] = []
    for map_key, option_type in (
        ("callExpDateMap", "call"),
        ("putExpDateMap", "put"),
    ):
        expiration_map = chain.get(map_key)
        if not isinstance(expiration_map, dict):
            continue
        for expiration_key, strikes_map in expiration_map.items():
            if not isinstance(strikes_map, dict):
                continue
            expiration_date, dte = _parse_expiration_key(expiration_key)
            for strike_key, contracts in strikes_map.items():
                if not isinstance(contracts, list) or not contracts:
                    continue
                contract = contracts[0]
                if not isinstance(contract, dict):
                    continue
                strike = _optional_float(contract.get("strikePrice", strike_key))
                if strike is None:
                    continue
                oi = _optional_int(contract.get("openInterest"))
                iv = _optional_float(contract.get("volatility"))
                if iv is None:
                    iv = _optional_float(contract.get("impliedVolatility"))
                if iv is None or iv <= 0:
                    continue
                # Schwab returns IV as a percentage (e.g. 18.5 for 18.5%).
                if iv > 3.0:
                    iv = iv / 100.0
                occ_symbol = str(contract.get("symbol", "")).strip()
                rows.append(
                    StrikeRow(
                        symbol=occ_symbol,
                        underlying=underlying,
                        strike=strike,
                        option_type=option_type,
                        open_interest=max(oi or 0, 0),
                        iv=iv,
                        expiration_date=expiration_date,
                        days_to_expiration=dte,
                        spot=resolved_spot,
                    )
                )
    return rows


def filter_expiration(
    rows: list[StrikeRow],
    *,
    days_to_expiration: int,
) -> list[StrikeRow]:
    """Keep only contracts matching a target DTE."""
    return [row for row in rows if row.days_to_expiration == days_to_expiration]


def _parse_expiration_key(key: str) -> tuple[date, int]:
    date_part, _, dte_part = key.partition(":")
    expiration = datetime.strptime(date_part, "%Y-%m-%d").date()
    return expiration, int(dte_part)


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
