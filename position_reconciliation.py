"""Helpers for aligning restored option positions with indicator state."""

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


def option_position_aligned_with_gaussian(
    contract_type: str,
    close: float,
    gaussian_ma: float,
) -> bool:
    """Return True when an open call/put matches the Gaussian MA bias.

    A call is aligned while price trades at or above the Gaussian MA; a put is
    aligned while price trades below it.
    """
    normalized = contract_type.upper()
    if close >= gaussian_ma:
        return normalized == "CALL"
    return normalized == "PUT"
