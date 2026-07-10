"""Option contract selection for ATM calls and puts at a target DTE.

Responsibility: Resolve OCC option symbols and mark prices from chain data or
synthetic inputs. Does not submit orders or manage portfolio state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Optional

from option_quote import OptionQuoteSnapshot


@dataclass(frozen=True)
class SelectedOption:
    """A resolved option contract ready for sizing and order entry."""

    occ_symbol: str
    underlying_symbol: str
    strike: float
    expiration_date: date
    days_to_expiration: int
    mark_price: float
    quote: OptionQuoteSnapshot = OptionQuoteSnapshot()


@dataclass(frozen=True)
class ParsedOccSymbol:
    """Components parsed from a Schwab-style OCC option symbol."""

    root: str
    expiration_date: date
    option_right: str
    strike: float


@dataclass(frozen=True)
class OptionExitMark:
    """Resolved option exit premium, quote, and underlying spot."""

    premium: float
    quote: Optional[OptionQuoteSnapshot]
    underlying_price: Optional[float]


def normalize_occ_symbol(symbol: str) -> str:
    """Return a compact uppercase OCC symbol without padding spaces."""
    return re.sub(r"\s+", "", symbol.strip().upper())


def parse_occ_symbol(symbol: str) -> Optional[ParsedOccSymbol]:
    """Parse a Schwab-style OCC option symbol."""
    compact = normalize_occ_symbol(symbol)
    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", compact)
    if match is None:
        return None

    root, yymmdd, option_right, strike_millis = match.groups()
    expiration_date = datetime.strptime(yymmdd, "%y%m%d").date()
    return ParsedOccSymbol(
        root=root,
        expiration_date=expiration_date,
        option_right=option_right,
        strike=int(strike_millis) / 1000.0,
    )


def days_to_expiration_for_occ(symbol: str, *, as_of: Optional[date] = None) -> Optional[int]:
    """Return whole days from ``as_of`` until the OCC expiration date."""
    parsed = parse_occ_symbol(symbol)
    if parsed is None:
        return None
    today = as_of or datetime.now(timezone.utc).date()
    return (parsed.expiration_date - today).days


def option_is_expired(symbol: str, *, as_of: Optional[date] = None) -> bool:
    """Return True when the OCC expiration date is before ``as_of``."""
    dte = days_to_expiration_for_occ(symbol, as_of=as_of)
    return dte is not None and dte < 0


def build_occ_symbol(
    underlying: str,
    expiration: date,
    *,
    option_right: str = "C",
    strike: float,
) -> str:
    """Build a Schwab-style OCC option symbol."""
    root = underlying.upper().ljust(6)[:6]
    date_part = expiration.strftime("%y%m%d")
    right = option_right.upper()[0]
    strike_millis = int(round(strike * 1000))
    if strike_millis < 0:
        raise ValueError("strike must be non-negative")
    return f"{root}{date_part}{right}{strike_millis:08d}"


def round_atm_strike(underlying_price: float) -> float:
    """Round spot to the nearest standard equity/ETF strike."""
    if underlying_price <= 0:
        raise ValueError("underlying_price must be positive")
    if underlying_price >= 200:
        return float(round(underlying_price))
    return float(round(underlying_price * 2) / 2)


def synthetic_atm_option(
    underlying_symbol: str,
    underlying_price: float,
    *,
    days_to_expiration: int,
    mark_price: float,
    option_right: str = "C",
    as_of: Optional[date] = None,
) -> SelectedOption:
    """Build a synthetic ATM option when live chain data is unavailable."""
    if days_to_expiration < 0:
        raise ValueError("days_to_expiration must be non-negative")
    if mark_price <= 0:
        raise ValueError("mark_price must be positive")

    today = as_of or datetime.now(timezone.utc).date()
    expiration = today + timedelta(days=days_to_expiration)
    strike = round_atm_strike(underlying_price)
    return SelectedOption(
        occ_symbol=build_occ_symbol(
            underlying_symbol,
            expiration,
            option_right=option_right,
            strike=strike,
        ),
        underlying_symbol=underlying_symbol.upper(),
        strike=strike,
        expiration_date=expiration,
        days_to_expiration=days_to_expiration,
        mark_price=mark_price,
    )


def synthetic_atm_call(
    underlying_symbol: str,
    underlying_price: float,
    *,
    days_to_expiration: int,
    mark_price: float,
    as_of: Optional[date] = None,
) -> SelectedOption:
    """Build a synthetic ATM call when live chain data is unavailable."""
    return synthetic_atm_option(
        underlying_symbol,
        underlying_price,
        days_to_expiration=days_to_expiration,
        mark_price=mark_price,
        option_right="C",
        as_of=as_of,
    )


def synthetic_atm_put(
    underlying_symbol: str,
    underlying_price: float,
    *,
    days_to_expiration: int,
    mark_price: float,
    as_of: Optional[date] = None,
) -> SelectedOption:
    """Build a synthetic ATM put when live chain data is unavailable."""
    return synthetic_atm_option(
        underlying_symbol,
        underlying_price,
        days_to_expiration=days_to_expiration,
        mark_price=mark_price,
        option_right="P",
        as_of=as_of,
    )


def select_atm_call_from_chain(
    chain: dict[str, Any],
    underlying_symbol: str,
    underlying_price: float,
    *,
    target_dte: int,
    as_of: Optional[date] = None,
) -> SelectedOption:
    """Pick the ATM call with expiration closest to target_dte from a Schwab chain."""
    return _select_atm_option_from_chain(
        chain,
        underlying_symbol,
        underlying_price,
        target_dte=target_dte,
        as_of=as_of,
        exp_map_key="callExpDateMap",
        option_right="C",
        option_label="call",
    )


def select_atm_put_from_chain(
    chain: dict[str, Any],
    underlying_symbol: str,
    underlying_price: float,
    *,
    target_dte: int,
    as_of: Optional[date] = None,
) -> SelectedOption:
    """Pick the ATM put with expiration closest to target_dte from a Schwab chain."""
    return _select_atm_option_from_chain(
        chain,
        underlying_symbol,
        underlying_price,
        target_dte=target_dte,
        as_of=as_of,
        exp_map_key="putExpDateMap",
        option_right="P",
        option_label="put",
    )


def _target_expiration_date(as_of: date, target_dte: int) -> date:
    """Return the calendar expiration date for a target DTE."""
    return as_of + timedelta(days=target_dte)


def _select_best_expiration_key(
    expiration_map: dict[str, Any],
    *,
    target_dte: int,
    as_of: date,
) -> str:
    """Pick the chain expiration key that best matches the configured target DTE."""
    candidates: list[tuple[str, date, int]] = []
    for key in expiration_map:
        expiration_date, dte = _parse_expiration_key(key)
        candidates.append((key, expiration_date, dte))

    if not candidates:
        raise ValueError("unable to select expiration from option chain")

    exact = [item for item in candidates if item[2] == target_dte]
    if exact:
        exact.sort(key=lambda item: item[1])
        return exact[0][0]

    target_date = _target_expiration_date(as_of, target_dte)
    by_date = [item for item in candidates if item[1] == target_date]
    if by_date:
        by_date.sort(key=lambda item: item[2])
        return by_date[0][0]

    at_or_beyond = [item for item in candidates if item[2] >= target_dte]
    if at_or_beyond:
        at_or_beyond.sort(key=lambda item: (item[2], item[1]))
        return at_or_beyond[0][0]

    candidates.sort(key=lambda item: (abs(item[2] - target_dte), -item[2], item[1]))
    return candidates[0][0]


def _select_atm_option_from_chain(
    chain: dict[str, Any],
    underlying_symbol: str,
    underlying_price: float,
    *,
    target_dte: int,
    as_of: Optional[date] = None,
    exp_map_key: str,
    option_right: str,
    option_label: str,
) -> SelectedOption:
    expiration_map = chain.get(exp_map_key)
    if not isinstance(expiration_map, dict) or not expiration_map:
        raise ValueError(f"option chain has no {exp_map_key} entries")

    trade_date = as_of or datetime.now(timezone.utc).date()
    best_key = _select_best_expiration_key(
        expiration_map,
        target_dte=target_dte,
        as_of=trade_date,
    )

    expiration_date, chain_dte = _parse_expiration_key(best_key)
    strikes_map = expiration_map[best_key]
    if not isinstance(strikes_map, dict) or not strikes_map:
        raise ValueError(f"no strikes for expiration {best_key}")

    best_contract: Optional[dict[str, Any]] = None
    best_strike: Optional[float] = None
    best_strike_diff = float("inf")
    for strike_key, contracts in strikes_map.items():
        if not isinstance(contracts, list) or not contracts:
            continue
        contract = contracts[0]
        if not isinstance(contract, dict):
            continue
        strike = float(contract.get("strikePrice", strike_key))
        strike_diff = abs(strike - underlying_price)
        if strike_diff < best_strike_diff:
            best_strike_diff = strike_diff
            best_strike = strike
            best_contract = contract

    if best_contract is None or best_strike is None:
        raise ValueError(f"no ATM {option_label} found for expiration {best_key}")

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
        occ_symbol=occ_symbol,
        underlying_symbol=underlying_symbol.upper(),
        strike=best_strike,
        expiration_date=expiration_date,
        days_to_expiration=chain_dte,
        mark_price=mark_price,
        quote=OptionQuoteSnapshot.from_contract(best_contract),
    )


def option_contract_type(occ_symbol: str) -> str:
    """Return CALL or PUT for a Schwab-style OCC option symbol."""
    symbol = occ_symbol.strip().upper()
    for index in range(len(symbol) - 1, 5, -1):
        if symbol[index] in {"C", "P"}:
            return "PUT" if symbol[index] == "P" else "CALL"
    return "CALL"


def contract_quote_from_chain(
    chain: dict[str, Any],
    occ_symbol: str,
) -> Optional[OptionQuoteSnapshot]:
    """Return bid/ask/greeks for one OCC symbol from a Schwab option chain."""
    contract = find_contract_in_chain(chain, occ_symbol)
    if contract is None:
        return None
    quote = OptionQuoteSnapshot.from_contract(contract)
    return quote if quote.has_data() else None


def contract_mark_from_chain(chain: dict[str, Any], occ_symbol: str) -> Optional[float]:
    """Return the mark price for one OCC symbol from a Schwab option chain."""
    contract = find_contract_in_chain(chain, occ_symbol)
    if contract is None:
        return None
    mark = _contract_mark_price(contract)
    return mark if mark > 0 else None


def resolve_option_exit_from_chain(
    chain: dict[str, Any],
    occ_symbol: str,
) -> Optional[OptionExitMark]:
    """Resolve exit premium, quote, and underlying spot from an option chain."""
    contract = find_contract_in_chain(chain, occ_symbol)
    if contract is None:
        return None

    quote = OptionQuoteSnapshot.from_contract(contract)
    premium = _contract_mark_price(contract)
    if premium <= 0 and quote.bid is not None and quote.ask is not None:
        premium = (quote.bid + quote.ask) / 2.0
    if premium <= 0:
        return None

    return OptionExitMark(
        premium=premium,
        quote=quote if quote.has_data() else None,
        underlying_price=underlying_price_from_chain(chain),
    )


def underlying_price_from_chain(chain: dict[str, Any]) -> Optional[float]:
    """Return the underlying spot bundled with a Schwab option chain response."""
    direct = _optional_float(chain.get("underlyingPrice"))
    if direct is not None and direct > 0:
        return direct

    underlying = chain.get("underlying")
    if isinstance(underlying, dict):
        for field in ("mark", "last", "close", "bid", "ask"):
            value = _optional_float(underlying.get(field))
            if value is not None and value > 0:
                return value
    return None


def find_contract_in_chain(
    chain: dict[str, Any],
    occ_symbol: str,
) -> Optional[dict[str, Any]]:
    """Locate one contract dict in a Schwab option chain payload."""
    target = normalize_occ_symbol(occ_symbol)
    if not target:
        return None

    for map_key in ("callExpDateMap", "putExpDateMap"):
        expiration_map = chain.get(map_key)
        if not isinstance(expiration_map, dict):
            continue
        for strikes_map in expiration_map.values():
            if not isinstance(strikes_map, dict):
                continue
            for contracts in strikes_map.values():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    if not isinstance(contract, dict):
                        continue
                    symbol = normalize_occ_symbol(str(contract.get("symbol", "")))
                    if symbol == target:
                        return contract

    parsed = parse_occ_symbol(occ_symbol)
    if parsed is None:
        return None

    exp_key_suffix = parsed.expiration_date.isoformat()
    right_map = "callExpDateMap" if parsed.option_right == "C" else "putExpDateMap"
    expiration_map = chain.get(right_map)
    if not isinstance(expiration_map, dict):
        return None

    for expiration_key, strikes_map in expiration_map.items():
        if not expiration_key.startswith(exp_key_suffix):
            continue
        if not isinstance(strikes_map, dict):
            continue
        for contracts in strikes_map.values():
            if not isinstance(contracts, list):
                continue
            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                strike = _optional_float(contract.get("strikePrice"))
                if strike is not None and abs(strike - parsed.strike) < 0.01:
                    return contract
    return None


def _find_contract_in_chain(
    chain: dict[str, Any],
    occ_symbol: str,
) -> Optional[dict[str, Any]]:
    return find_contract_in_chain(chain, occ_symbol)


def _parse_expiration_key(key: str) -> tuple[date, int]:
    date_part, _, dte_part = key.partition(":")
    expiration = datetime.strptime(date_part, "%Y-%m-%d").date()
    return expiration, int(dte_part)


def _contract_mark_price(contract: dict[str, Any]) -> float:
    for field in ("mark", "ask", "bid", "last", "markPrice"):
        value = contract.get(field)
        if value is None:
            continue
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price

    bid = _optional_float(contract.get("bid"))
    ask = _optional_float(contract.get("ask"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0

    return 0.0


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
