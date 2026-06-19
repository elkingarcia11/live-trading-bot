"""Position sizing for equity and option orders.

Responsibility: Convert account balance and price into order quantities.

Uses a percentage of tradeable cash capped by a maximum dollar allocation.
Does not fetch balances or submit orders.
"""

from __future__ import annotations

import math


def allocation_dollars(
    tradeable_balance: float,
    *,
    pct: float,
    max_dollars: float,
) -> float:
    """Return dollar allocation as min(pct * balance, max_dollars)."""
    if tradeable_balance <= 0 or pct <= 0 or max_dollars <= 0:
        return 0.0
    return min(tradeable_balance * pct, max_dollars)


def shares_for_buy(
    tradeable_balance: float,
    price: float,
    *,
    pct: float,
    max_dollars: float,
) -> int:
    """Return whole shares affordable within the configured dollar allocation."""
    if price <= 0:
        return 0

    dollars = allocation_dollars(
        tradeable_balance,
        pct=pct,
        max_dollars=max_dollars,
    )
    if dollars < price:
        return 0

    return int(math.floor(dollars / price))


def contracts_for_buy(
    tradeable_balance: float,
    premium_per_share: float,
    *,
    pct: float,
    max_dollars: float,
) -> int:
    """Return whole option contracts affordable within the dollar allocation."""
    if premium_per_share <= 0:
        return 0

    cost_per_contract = premium_per_share * 100
    dollars = allocation_dollars(
        tradeable_balance,
        pct=pct,
        max_dollars=max_dollars,
    )
    if dollars < cost_per_contract:
        return 0

    return int(math.floor(dollars / cost_per_contract))
