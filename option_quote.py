"""Option bid/ask and Greeks snapshots for trade records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class OptionQuoteSnapshot:
    """Market quote and Greeks for one option contract at one point in time."""

    bid: Optional[float] = None
    ask: Optional[float] = None
    mark: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None

    @classmethod
    def from_contract(cls, contract: dict[str, Any]) -> "OptionQuoteSnapshot":
        """Build a snapshot from one Schwab option chain contract payload."""
        mark = _extract_mark(contract)
        return cls(
            bid=_maybe_float(contract.get("bid")),
            ask=_maybe_float(contract.get("ask")),
            mark=mark,
            delta=_maybe_float(contract.get("delta")),
            gamma=_maybe_float(contract.get("gamma")),
            theta=_maybe_float(contract.get("theta")),
            vega=_maybe_float(contract.get("vega")),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OptionQuoteSnapshot":
        """Restore a snapshot saved in account JSON or CSV round-trips."""
        return cls(
            bid=_maybe_float(payload.get("bid")),
            ask=_maybe_float(payload.get("ask")),
            mark=_maybe_float(payload.get("mark")),
            delta=_maybe_float(payload.get("delta")),
            gamma=_maybe_float(payload.get("gamma")),
            theta=_maybe_float(payload.get("theta")),
            vega=_maybe_float(payload.get("vega")),
        )

    def to_dict(self) -> dict[str, Optional[float]]:
        return asdict(self)

    def has_data(self) -> bool:
        return any(
            value is not None
            for value in (
                self.bid,
                self.ask,
                self.mark,
                self.delta,
                self.gamma,
                self.theta,
                self.vega,
            )
        )


def _maybe_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_mark(contract: dict[str, Any]) -> Optional[float]:
    for field in ("mark", "ask", "bid", "last", "markPrice"):
        value = contract.get(field)
        price = _maybe_float(value)
        if price is not None and price > 0:
            return price

    bid = _maybe_float(contract.get("bid"))
    ask = _maybe_float(contract.get("ask"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return None


def format_quote_email_lines(
    quote: Optional[OptionQuoteSnapshot],
    *,
    label: str,
) -> list[str]:
    """Return email body lines for one quote snapshot."""
    if quote is None or not quote.has_data():
        return [f"{label} quote: n/a"]

    lines = [f"{label} bid: {_format_price(quote.bid)}"]
    lines.append(f"{label} ask: {_format_price(quote.ask)}")
    if quote.mark is not None:
        lines.append(f"{label} mark: {quote.mark:.4f}")
    lines.extend(
        [
            f"{label} delta: {_format_greek(quote.delta)}",
            f"{label} gamma: {_format_greek(quote.gamma)}",
            f"{label} theta: {_format_greek(quote.theta)}",
            f"{label} vega: {_format_greek(quote.vega)}",
        ]
    )
    return lines


def quote_csv_fields(
    quote: Optional[OptionQuoteSnapshot],
    *,
    prefix: str,
) -> dict[str, str]:
    """Map one quote snapshot to CSV column values."""
    columns = {
        f"{prefix}bid",
        f"{prefix}ask",
        f"{prefix}mark",
        f"{prefix}delta",
        f"{prefix}gamma",
        f"{prefix}theta",
        f"{prefix}vega",
    }
    empty = {column: "" for column in columns}
    if quote is None:
        return empty

    return {
        f"{prefix}bid": _format_price(quote.bid),
        f"{prefix}ask": _format_price(quote.ask),
        f"{prefix}mark": _format_price(quote.mark),
        f"{prefix}delta": _format_greek(quote.delta),
        f"{prefix}gamma": _format_greek(quote.gamma),
        f"{prefix}theta": _format_greek(quote.theta),
        f"{prefix}vega": _format_greek(quote.vega),
    }


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.4f}"


def _format_greek(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.6f}"
