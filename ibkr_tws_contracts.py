"""IBKR TWS contract helpers.

Responsibility: Build ibapi Contract objects for common instrument types.
"""

from __future__ import annotations

from ibapi.contract import Contract


def equity_contract(
    symbol: str,
    *,
    exchange: str = "SMART",
    currency: str = "USD",
    primary_exchange: str = "",
) -> Contract:
    """Build a US equity contract routed through SMART."""
    contract = Contract()
    contract.symbol = symbol.upper()
    contract.secType = "STK"
    contract.exchange = exchange
    contract.currency = currency
    if primary_exchange:
        contract.primaryExchange = primary_exchange
    return contract
