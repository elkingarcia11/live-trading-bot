"""Tests for restored position reconciliation."""

from __future__ import annotations

from position_reconciliation import option_position_aligned_with_supertrend


def test_call_aligns_with_bullish_trend() -> None:
    assert option_position_aligned_with_supertrend("CALL", 1.0) is True


def test_put_aligns_with_bearish_trend() -> None:
    assert option_position_aligned_with_supertrend("PUT", -1.0) is True


def test_call_conflicts_with_bearish_trend() -> None:
    assert option_position_aligned_with_supertrend("CALL", -1.0) is False


def test_put_conflicts_with_bullish_trend() -> None:
    assert option_position_aligned_with_supertrend("PUT", 1.0) is False
