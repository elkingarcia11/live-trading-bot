"""Zero-DTE contract selector.

Responsibility: Pick 0DTE ATM or 1-OTM contracts in a target delta band for
GEX scalping entries. Does not submit orders or track portfolio state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from option_quote import OptionQuoteSnapshot
from option_selector import (
    SelectedOption,
    _contract_mark_price,
    _parse_expiration_key,
    build_occ_symbol,
    normalize_occ_symbol,
)

ContractSide = Literal["call", "put"]


@dataclass(frozen=True)
class ZeroDteSelectionCriteria:
    """Delta and moneyness constraints for 0DTE contract selection."""

    target_dte: int = 0
    min_delta: float = 0.45
    max_delta: float = 0.50
    allow_one_otm: bool = True


def select_zero_dte_contract(
    chain: dict[str, Any],
    underlying_symbol: str,
    spot: float,
    *,
    side: ContractSide,
    criteria: Optional[ZeroDteSelectionCriteria] = None,
) -> SelectedOption:
    """Pick a 0DTE contract in the target delta band from a Schwab chain."""
    criteria = criteria or ZeroDteSelectionCriteria()
    exp_map_key = "callExpDateMap" if side == "call" else "putExpDateMap"
    option_right = "C" if side == "call" else "P"
    expiration_map = chain.get(exp_map_key)
    if not isinstance(expiration_map, dict) or not expiration_map:
        raise ValueError(f"option chain has no {exp_map_key} entries")

    expiration_key = _select_zero_dte_expiration_key(
        expiration_map,
        target_dte=criteria.target_dte,
    )
    expiration_date, chain_dte = _parse_expiration_key(expiration_key)
    strikes_map = expiration_map[expiration_key]
    if not isinstance(strikes_map, dict) or not strikes_map:
        raise ValueError(f"no strikes for expiration {expiration_key}")

    candidates: list[tuple[float, dict[str, Any], float]] = []
    for strike_key, contracts in strikes_map.items():
        if not isinstance(contracts, list) or not contracts:
            continue
        contract = contracts[0]
        if not isinstance(contract, dict):
            continue
        strike = float(contract.get("strikePrice", strike_key))
        if not _strike_allowed(spot, strike, side=side, allow_one_otm=criteria.allow_one_otm):
            continue
        delta = _abs_delta(contract, side=side)
        if delta is None:
            continue
        if criteria.min_delta <= delta <= criteria.max_delta:
            candidates.append((abs(delta - 0.475), contract, strike))

    if not candidates:
        raise ValueError(
            f"no 0DTE {side} in delta band "
            f"[{criteria.min_delta:.2f}, {criteria.max_delta:.2f}]"
        )

    candidates.sort(key=lambda item: (item[0], abs(item[2] - spot)))
    _, best_contract, best_strike = candidates[0]
    occ_symbol = str(best_contract.get("symbol", "")).strip()
    if not occ_symbol:
        occ_symbol = build_occ_symbol(
            underlying_symbol,
            expiration_date,
            option_right=option_right,
            strike=best_strike,
        )
    mark_price = _contract_mark_price(best_contract)
    if mark_price <= 0:
        raise ValueError(f"option {occ_symbol} has no usable mark price")

    return SelectedOption(
        occ_symbol=normalize_occ_symbol(occ_symbol),
        underlying_symbol=underlying_symbol.upper(),
        strike=best_strike,
        expiration_date=expiration_date,
        days_to_expiration=chain_dte,
        mark_price=mark_price,
        quote=OptionQuoteSnapshot.from_contract(best_contract),
    )


def _select_zero_dte_expiration_key(
    expiration_map: dict[str, Any],
    *,
    target_dte: int,
) -> str:
    exact = [
        key
        for key in expiration_map
        if _parse_expiration_key(key)[1] == target_dte
    ]
    if not exact:
        raise ValueError(f"no expiration with DTE={target_dte}")
    exact.sort(key=lambda key: _parse_expiration_key(key)[0])
    return exact[0]


def _strike_allowed(
    spot: float,
    strike: float,
    *,
    side: ContractSide,
    allow_one_otm: bool,
) -> bool:
    """Return True when strike is ATM or one step OTM for the requested side."""
    if side == "call":
        if strike < spot:
            return False
        if not allow_one_otm:
            return abs(strike - spot) < 0.01
        return strike <= spot + _one_strike_step(spot)
    if strike > spot:
        return False
    if not allow_one_otm:
        return abs(strike - spot) < 0.01
    return strike >= spot - _one_strike_step(spot)


def _one_strike_step(spot: float) -> float:
    return 1.0 if spot >= 200 else 0.5


def _abs_delta(contract: dict[str, Any], *, side: ContractSide) -> Optional[float]:
    raw = contract.get("delta")
    if raw is None:
        return None
    try:
        delta = abs(float(raw))
    except (TypeError, ValueError):
        return None
    if side == "put" and float(raw) > 0:
        # Schwab sometimes reports put delta as positive magnitude.
        return delta
    return delta
