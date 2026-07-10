"""Tests for GEX level-refresh scheduling."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from gex_calculator import GexSnapshot, classify_regime
from gex_level_schedule import (
    due_refresh_slots,
    next_refresh_at,
    parse_local_hhmm_times,
)


NY = ZoneInfo("America/New_York")


def test_parse_local_hhmm_times_sorts() -> None:
    assert parse_local_hhmm_times(("11:30", "09:35")) == (
        parse_local_hhmm_times(("09:35",))[0],
        parse_local_hhmm_times(("11:30",))[0],
    )


def test_next_refresh_skips_past_slots_same_day() -> None:
    now = datetime(2026, 7, 9, 10, 0, tzinfo=NY)
    nxt = next_refresh_at(
        now,
        refresh_times_local=parse_local_hhmm_times(("09:35", "11:30", "15:00")),
        timezone_name="America/New_York",
    )
    assert nxt == datetime(2026, 7, 9, 11, 30, tzinfo=NY)


def test_next_refresh_includes_power_hour() -> None:
    now = datetime(2026, 7, 9, 12, 0, tzinfo=NY)
    nxt = next_refresh_at(
        now,
        refresh_times_local=parse_local_hhmm_times(("09:35", "11:30", "15:00")),
        timezone_name="America/New_York",
    )
    assert nxt == datetime(2026, 7, 9, 15, 0, tzinfo=NY)


def test_next_refresh_rolls_to_next_day() -> None:
    now = datetime(2026, 7, 9, 15, 1, tzinfo=NY)
    nxt = next_refresh_at(
        now,
        refresh_times_local=parse_local_hhmm_times(("09:35", "11:30", "15:00")),
        timezone_name="America/New_York",
    )
    assert nxt == datetime(2026, 7, 10, 9, 35, tzinfo=NY)


def test_due_refresh_slots_catches_missed_open() -> None:
    now = datetime(2026, 7, 9, 10, 15, tzinfo=NY)
    due = due_refresh_slots(
        now,
        refresh_times_local=parse_local_hhmm_times(("09:35", "11:30", "15:00")),
        timezone_name="America/New_York",
        already_fired=set(),
    )
    assert due == [datetime(2026, 7, 9, 9, 35, tzinfo=NY)]


def test_due_refresh_slots_respects_already_fired() -> None:
    now = datetime(2026, 7, 9, 10, 15, tzinfo=NY)
    fired = {datetime(2026, 7, 9, 9, 35, tzinfo=NY)}
    due = due_refresh_slots(
        now,
        refresh_times_local=parse_local_hhmm_times(("09:35", "11:30", "15:00")),
        timezone_name="America/New_York",
        already_fired=fired,
    )
    assert due == []


def test_with_live_spot_keeps_anchored_levels() -> None:
    anchored = GexSnapshot(
        symbol="SPY",
        timestamp=datetime(2026, 7, 9, 13, 35, tzinfo=ZoneInfo("UTC")),
        spot=540.0,
        net_gex=-1_000_000.0,
        regime="negative",
        flip_level=538.0,
        put_wall=535.0,
        call_wall=545.0,
    )
    live = anchored.with_live_spot(539.0)
    assert live.put_wall == 535.0
    assert live.flip_level == 538.0
    assert live.call_wall == 545.0
    assert live.spot == 539.0
    assert live.regime == classify_regime(539.0, 538.0, -1_000_000.0)
    assert live.regime == "positive"
