"""Tests for Phase-0 capture flush policy and partition verification."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from trade_capture_runtime import (
    CaptureMetrics,
    FlushPolicy,
    PartitionVerifyConfig,
    compare_local_remote_counts,
    verify_trade_frame,
)


def test_flush_policy_row_threshold() -> None:
    policy = FlushPolicy(every_rows=100, interval_seconds=60)
    assert (
        policy.should_flush(
            buffered_rows=100,
            rows_since_flush=100,
            seconds_since_flush=1.0,
        )
        == "every-100-rows"
    )
    assert (
        policy.should_flush(
            buffered_rows=50,
            rows_since_flush=50,
            seconds_since_flush=1.0,
        )
        is None
    )


def test_flush_policy_time_threshold() -> None:
    policy = FlushPolicy(every_rows=5000, interval_seconds=60)
    assert (
        policy.should_flush(
            buffered_rows=10,
            rows_since_flush=10,
            seconds_since_flush=60.0,
        )
        == "every-60s"
    )
    assert (
        policy.should_flush(
            buffered_rows=0,
            rows_since_flush=0,
            seconds_since_flush=120.0,
        )
        is None
    )


def test_flush_policy_never_empty_buffer() -> None:
    policy = FlushPolicy(every_rows=1, interval_seconds=1)
    assert (
        policy.should_flush(
            buffered_rows=0,
            rows_since_flush=5,
            seconds_since_flush=5,
        )
        is None
    )


def test_capture_metrics_ticks_per_sec_window() -> None:
    metrics = CaptureMetrics(started_at=0.0, window_started_at=0.0)
    metrics.note_trade()
    metrics.note_trade()
    rate = metrics.ticks_per_sec(now=2.0, window=True)
    assert abs(rate - 1.0) < 1e-9
    assert metrics.window_trades == 0


def test_verify_trade_frame_detects_duplicates_and_gaps() -> None:
    base = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        [
            {
                "timestamp": base,
                "symbol": "ES.n.0",
                "raw_symbol": "ESU6",
                "price": 100.0,
                "price_raw": 100000000000,
                "size": 1.0,
                "side": "A",
                "action": "T",
                "depth": 0,
                "flags": 0,
                "sequence": 1,
                "instrument_id": 1,
                "publisher_id": 1,
                "rtype": 0,
                "ts_event": 1,
                "ts_recv": 1,
                "ts_in_delta": 0,
                "ts_index": 0,
            },
            {
                "timestamp": base,
                "symbol": "ES.n.0",
                "raw_symbol": "ESU6",
                "price": 100.0,
                "price_raw": 100000000000,
                "size": 1.0,
                "side": "A",
                "action": "T",
                "depth": 0,
                "flags": 0,
                "sequence": 1,
                "instrument_id": 1,
                "publisher_id": 1,
                "rtype": 0,
                "ts_event": 1,
                "ts_recv": 1,
                "ts_in_delta": 0,
                "ts_index": 0,
            },
            {
                "timestamp": base.replace(minute=5),
                "symbol": "ES.n.0",
                "raw_symbol": "ESU6",
                "price": 101.0,
                "price_raw": 101000000000,
                "size": 2.0,
                "side": "B",
                "action": "T",
                "depth": 0,
                "flags": 0,
                "sequence": 2,
                "instrument_id": 1,
                "publisher_id": 1,
                "rtype": 0,
                "ts_event": 2,
                "ts_recv": 2,
                "ts_in_delta": 0,
                "ts_index": 1,
            },
        ]
    )
    result = verify_trade_frame(frame, source="test", config=None)
    # Default gap warn is 60s; 5 minutes should warn (not fail).
    assert result.duplicate_rows >= 2
    assert result.gap_count_over_threshold >= 1
    assert result.warnings
    assert not result.ok  # duplicates are hard errors

    strict = verify_trade_frame(
        frame.drop_duplicates(
            subset=["ts_event", "sequence", "instrument_id"], keep="last"
        ).reset_index(drop=True),
        source="test-strict",
        config=PartitionVerifyConfig(gap_warn_seconds=60.0, fail_on_gaps=True),
    )
    assert strict.duplicate_rows == 0
    assert not strict.ok  # gap becomes an issue when fail_on_gaps=True


def test_compare_local_remote_counts() -> None:
    assert compare_local_remote_counts(10, 10) == []
    assert compare_local_remote_counts(10, None) == ["remote partition missing"]
    assert "mismatch" in compare_local_remote_counts(10, 11)[0]


def test_capture_metrics_persist_drop_and_writer_events() -> None:
    metrics = CaptureMetrics()
    metrics.note_persist_drop()
    metrics.apply_writer_event(
        {"kind": "flush", "rows": 10, "duration_s": 0.5, "ok": True}
    )
    metrics.apply_writer_event(
        {"kind": "compact", "rows": 10, "duration_s": 1.2, "ok": True}
    )
    assert metrics.persist_drops == 1
    assert metrics.flushes == 1
    assert metrics.last_flush_rows == 10
    assert metrics.compacts == 1
    assert metrics.last_compact_rows == 10
    line = metrics.snapshot_line(now=1.0, buffered=3)
    assert "persist_drops=1" in line
    assert "queue_size=3" in line
    assert "compacts=1" in line
