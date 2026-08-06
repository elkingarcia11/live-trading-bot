"""Tests for N-tick OHLCV aggregation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tick_bar_builder import TickBarBuilder, is_tick_timeframe, parse_tick_timeframe


def test_parse_tick_timeframe() -> None:
    assert parse_tick_timeframe("50t") == 50
    assert is_tick_timeframe("50t")
    assert not is_tick_timeframe("1m")


def test_tick_bar_builder_emits_every_n_trades() -> None:
    builder = TickBarBuilder(ticks_per_bar=5)
    base = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    prices = [100.0, 101.0, 99.5, 102.0, 101.5]

    completed = None
    for index, price in enumerate(prices):
        completed = builder.update(
            symbol="SPY",
            price=price,
            size=10.0 + index,
            timestamp=base + timedelta(seconds=index),
        )
        if index < 4:
            assert completed is None

    assert completed is not None
    assert completed["symbol"] == "SPY"
    assert completed["timeframe"] == "5t"
    assert completed["bar"]["open"] == 100.0
    assert completed["bar"]["high"] == 102.0
    assert completed["bar"]["low"] == 99.5
    assert completed["bar"]["close"] == 101.5
    assert completed["bar"]["volume"] == sum(10.0 + i for i in range(5))
    assert completed["bar"]["datetime"] == base.isoformat()


def test_tick_bar_builder_ignores_non_positive_price() -> None:
    builder = TickBarBuilder(ticks_per_bar=2)
    base = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    assert (
        builder.update(symbol="SPY", price=0.0, size=1.0, timestamp=base) is None
    )
    assert (
        builder.update(symbol="SPY", price=-1.0, size=1.0, timestamp=base) is None
    )
    assert (
        builder.update(symbol="SPY", price=100.0, size=1.0, timestamp=base) is None
    )
    completed = builder.update(
        symbol="SPY",
        price=101.0,
        size=1.0,
        timestamp=base + timedelta(seconds=1),
    )
    assert completed is not None
    assert completed["bar"]["open"] == 100.0


def test_tick_bar_builder_reset_clears_forming_bar() -> None:
    builder = TickBarBuilder(ticks_per_bar=3)
    base = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    builder.update(symbol="SPY", price=100.0, size=1.0, timestamp=base)
    builder.reset("SPY")
    assert (
        builder.update(
            symbol="SPY",
            price=101.0,
            size=1.0,
            timestamp=base + timedelta(seconds=1),
        )
        is None
    )


def test_tick_bar_builder_starts_next_bar_after_completion() -> None:
    builder = TickBarBuilder(ticks_per_bar=2)
    base = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)

    first = builder.update(
        symbol="SPY",
        price=100.0,
        size=1.0,
        timestamp=base,
    )
    assert first is None

    first = builder.update(
        symbol="SPY",
        price=101.0,
        size=2.0,
        timestamp=base + timedelta(seconds=1),
    )
    assert first is not None
    assert first["bar"]["close"] == 101.0

    second = builder.update(
        symbol="SPY",
        price=103.0,
        size=3.0,
        timestamp=base + timedelta(seconds=2),
    )
    assert second is None

    second = builder.update(
        symbol="SPY",
        price=102.0,
        size=4.0,
        timestamp=base + timedelta(seconds=3),
    )
    assert second is not None
    assert second["bar"]["open"] == 103.0
    assert second["bar"]["close"] == 102.0
    assert second["bar"]["datetime"] == (base + timedelta(seconds=2)).isoformat()
