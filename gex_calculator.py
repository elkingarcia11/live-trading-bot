"""GEX calculator.

Responsibility: Aggregate per-strike GEX, compute net GEX, zero-gamma flip,
put wall, and call wall. Does not fetch chains, compute greeks, or submit orders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from options_chain_transformer import StrikeRow


@dataclass(frozen=True)
class GexSnapshot:
    """Published GEX regime snapshot for one underlying."""

    symbol: str
    timestamp: datetime
    spot: float
    net_gex: float
    regime: str
    flip_level: Optional[float]
    put_wall: Optional[float]
    call_wall: Optional[float]
    per_strike_gex: tuple[StrikeRow, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the snapshot for logging or event bus consumers."""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "spot": self.spot,
            "net_gex": self.net_gex,
            "regime": self.regime,
            "flip_level": self.flip_level,
            "put_wall": self.put_wall,
            "call_wall": self.call_wall,
        }

    def with_live_spot(self, spot: float) -> "GexSnapshot":
        """Return a copy with live spot and regime, keeping anchored levels."""
        regime = classify_regime(spot, self.flip_level, self.net_gex)
        return GexSnapshot(
            symbol=self.symbol,
            timestamp=self.timestamp,
            spot=spot,
            net_gex=self.net_gex,
            regime=regime,
            flip_level=self.flip_level,
            put_wall=self.put_wall,
            call_wall=self.call_wall,
            per_strike_gex=self.per_strike_gex,
        )


def call_gex(
    gamma: float,
    open_interest: int,
    spot: float,
    *,
    multiplier: int = 100,
) -> float:
    """Return positive dealer GEX contribution for a call."""
    return gamma * open_interest * multiplier * spot


def put_gex(
    gamma: float,
    open_interest: int,
    spot: float,
    *,
    multiplier: int = 100,
) -> float:
    """Return negative dealer GEX contribution for a put."""
    return -gamma * open_interest * multiplier * spot


def strike_gex(row: StrikeRow, *, multiplier: int = 100) -> float:
    """Return signed GEX for one strike row."""
    if row.option_type == "call":
        return call_gex(row.gamma, row.open_interest, row.spot, multiplier=multiplier)
    return put_gex(row.gamma, row.open_interest, row.spot, multiplier=multiplier)


def attach_gex(
    rows: list[StrikeRow],
    *,
    multiplier: int = 100,
) -> list[StrikeRow]:
    """Attach per-contract GEX values to strike rows."""
    from dataclasses import replace

    enriched: list[StrikeRow] = []
    for row in rows:
        enriched.append(replace(row, gex=strike_gex(row, multiplier=multiplier)))
    return enriched


def net_gex(rows: list[StrikeRow]) -> float:
    """Sum signed GEX across all strike rows."""
    return sum(row.gex for row in rows)


def find_put_wall(rows: list[StrikeRow]) -> Optional[float]:
    """Return the put strike with the largest-magnitude negative GEX."""
    puts = [row for row in rows if row.option_type == "put" and row.gex < 0]
    if not puts:
        return None
    return min(puts, key=lambda row: row.gex).strike


def find_call_wall(rows: list[StrikeRow]) -> Optional[float]:
    """Return the call strike with the largest positive GEX."""
    calls = [row for row in rows if row.option_type == "call" and row.gex > 0]
    if not calls:
        return None
    return max(calls, key=lambda row: row.gex).strike


def find_zero_gamma_flip(rows: list[StrikeRow]) -> Optional[float]:
    """Return the strike where cumulative GEX crosses from positive to negative."""
    if not rows:
        return None

    by_strike: dict[float, float] = {}
    for row in rows:
        by_strike[row.strike] = by_strike.get(row.strike, 0.0) + row.gex

    strikes = sorted(by_strike)
    cumulative = 0.0
    prev_strike: Optional[float] = None
    prev_cumulative = 0.0

    for strike in strikes:
        cumulative += by_strike[strike]
        if prev_strike is not None and prev_cumulative > 0 and cumulative <= 0:
            if cumulative == prev_cumulative:
                return strike
            weight = prev_cumulative / (prev_cumulative - cumulative)
            return prev_strike + (strike - prev_strike) * weight
        prev_strike = strike
        prev_cumulative = cumulative

    return None


def classify_regime(spot: float, flip_level: Optional[float], net: float) -> str:
    """Classify the GEX regime as positive or negative."""
    if flip_level is not None:
        return "negative" if spot < flip_level else "positive"
    return "negative" if net < 0 else "positive"


def build_snapshot(
    symbol: str,
    rows: list[StrikeRow],
    *,
    timestamp: Optional[datetime] = None,
    multiplier: int = 100,
) -> GexSnapshot:
    """Build a full GEX snapshot from enriched strike rows."""
    if not rows:
        raise ValueError("cannot build GEX snapshot from empty strike list")

    spot = rows[0].spot
    enriched = attach_gex(rows, multiplier=multiplier)
    total = net_gex(enriched)
    flip = find_zero_gamma_flip(enriched)
    regime = classify_regime(spot, flip, total)

    return GexSnapshot(
        symbol=symbol.upper(),
        timestamp=timestamp or datetime.now(timezone.utc),
        spot=spot,
        net_gex=total,
        regime=regime,
        flip_level=flip,
        put_wall=find_put_wall(enriched),
        call_wall=find_call_wall(enriched),
        per_strike_gex=tuple(enriched),
    )
