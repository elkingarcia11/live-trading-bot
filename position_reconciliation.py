"""Helpers for aligning restored option positions with Supertrend state."""

from __future__ import annotations


def option_position_aligned_with_supertrend(
    contract_type: str,
    trend: float,
) -> bool:
    """Return True when an open call/put matches the current Supertrend direction."""
    normalized = contract_type.upper()
    if trend in (1, 1.0):
        return normalized == "CALL"
    if trend in (-1, -1.0):
        return normalized == "PUT"
    return False
