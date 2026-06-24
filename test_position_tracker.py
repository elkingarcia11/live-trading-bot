"""Tests for PositionTracker option mark tracking and peak-mark trailing stop."""

from __future__ import annotations

from datetime import datetime, timezone

from position_tracker import ExitReason, PositionTracker


def _open_option(tracker: PositionTracker, *, pct: float | None = 0.15):
    return tracker.open_position(
        symbol="SPY   260622C00450000",
        quantity=2,
        entry_price=5.0,
        opened_at=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
        asset_type="OPTION",
        underlying_symbol="SPY",
        underlying_entry_price=450.0,
        trailing_stop_pct=pct,
    )


def test_record_option_mark_tracks_peak_and_max_pnl() -> None:
    tracker = PositionTracker()
    position = _open_option(tracker)

    tracker.record_option_mark(position.symbol, 6.0, unrealized_pnl=200.0)
    tracker.record_option_mark(position.symbol, 5.5, unrealized_pnl=100.0)

    updated = tracker.get_position(position.symbol)
    assert updated is not None
    assert updated.max_mark_price == 6.0
    assert updated.max_unrealized_profit == 200.0
    assert updated.max_unrealized_loss == 100.0
    assert updated.last_mark_price == 5.5


def test_record_option_mark_tracks_max_pnl_pct() -> None:
    tracker = PositionTracker()
    position = _open_option(tracker)

    tracker.record_option_mark(
        position.symbol, 6.0, unrealized_pnl=200.0, unrealized_pnl_pct=0.2
    )
    tracker.record_option_mark(
        position.symbol, 4.0, unrealized_pnl=-200.0, unrealized_pnl_pct=-0.2
    )

    updated = tracker.get_position(position.symbol)
    assert updated is not None
    # Peaks are stored paired with the percentage observed at that mark.
    assert updated.max_unrealized_profit == 200.0
    assert updated.max_unrealized_profit_pct == 0.2
    assert updated.max_unrealized_loss == -200.0
    assert updated.max_unrealized_loss_pct == -0.2


def test_trailing_stop_triggers_15pct_below_peak() -> None:
    tracker = PositionTracker()
    position = _open_option(tracker)

    # Rally to a peak of 6.00; 15% below peak is 5.10.
    assert tracker.record_option_mark(position.symbol, 6.0, unrealized_pnl=200.0) is None
    # 5.20 is above the 5.10 threshold -> no exit yet.
    assert tracker.record_option_mark(position.symbol, 5.2, unrealized_pnl=40.0) is None
    # 5.05 is below the 5.10 threshold -> trailing stop fires.
    notification = tracker.record_option_mark(position.symbol, 5.05, unrealized_pnl=10.0)

    assert notification is not None
    assert notification.reason == ExitReason.TRAILING_STOP
    assert notification.mark_price == 5.05


def test_trailing_stop_disabled_when_pct_none() -> None:
    tracker = PositionTracker()
    position = _open_option(tracker, pct=None)

    tracker.record_option_mark(position.symbol, 6.0, unrealized_pnl=200.0)
    assert tracker.record_option_mark(position.symbol, 1.0, unrealized_pnl=-800.0) is None


def test_record_option_mark_no_position_returns_none() -> None:
    tracker = PositionTracker()
    assert tracker.record_option_mark("MISSING", 1.0, unrealized_pnl=0.0) is None
