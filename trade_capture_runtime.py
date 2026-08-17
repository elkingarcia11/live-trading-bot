"""Shared helpers for raw-trade capture: flush policy and partition checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from session_trade_recorder import (
    DEDUPE_KEYS_FULL,
    DEDUPE_KEYS_LEGACY,
    _normalize_trades,
)


@dataclass
class CaptureMetrics:
    """In-process counters for capture health (ingest + persist writer)."""

    trades_captured: int = 0
    rejected_ticks: int = 0
    persist_drops: int = 0
    flushes: int = 0
    flush_failures: int = 0
    last_flush_duration_s: float = 0.0
    total_flush_duration_s: float = 0.0
    last_flush_rows: int = 0
    compacts: int = 0
    compact_failures: int = 0
    last_compact_duration_s: float = 0.0
    last_compact_rows: int = 0
    started_at: float = 0.0
    window_started_at: float = 0.0
    window_trades: int = 0

    def note_trade(self) -> None:
        self.trades_captured += 1
        self.window_trades += 1

    def note_rejected(self) -> None:
        self.rejected_ticks += 1

    def note_persist_drop(self, n: int = 1) -> None:
        self.persist_drops += n

    def note_flush(self, *, rows: int, duration_s: float, ok: bool) -> None:
        self.flushes += 1
        self.last_flush_rows = rows
        self.last_flush_duration_s = duration_s
        self.total_flush_duration_s += max(duration_s, 0.0)
        if not ok:
            self.flush_failures += 1

    def note_compact(self, *, rows: int, duration_s: float, ok: bool) -> None:
        self.compacts += 1
        self.last_compact_rows = rows
        self.last_compact_duration_s = duration_s
        if not ok:
            self.compact_failures += 1

    def apply_writer_event(self, event: dict) -> None:
        kind = event.get("kind")
        rows = int(event.get("rows") or 0)
        duration_s = float(event.get("duration_s") or 0.0)
        ok = bool(event.get("ok", True))
        if kind == "flush":
            self.note_flush(rows=rows, duration_s=duration_s, ok=ok)
        elif kind == "compact":
            self.note_compact(rows=rows, duration_s=duration_s, ok=ok)

    def ticks_per_sec(self, *, now: float, window: bool = True) -> float:
        if window:
            elapsed = max(now - self.window_started_at, 1e-9)
            rate = self.window_trades / elapsed
            self.window_started_at = now
            self.window_trades = 0
            return rate
        elapsed = max(now - self.started_at, 1e-9)
        return self.trades_captured / elapsed

    def snapshot_line(self, *, now: float, buffered: int) -> str:
        rate = self.ticks_per_sec(now=now, window=True)
        avg_flush = (
            self.total_flush_duration_s / self.flushes if self.flushes else 0.0
        )
        return (
            "metrics "
            f"ticks_per_sec={rate:.1f} "
            f"queue_size={buffered} "
            f"trades_total={self.trades_captured} "
            f"rejected={self.rejected_ticks} "
            f"persist_drops={self.persist_drops} "
            f"flushes={self.flushes} "
            f"flush_failures={self.flush_failures} "
            f"last_flush_s={self.last_flush_duration_s:.3f} "
            f"avg_flush_s={avg_flush:.3f} "
            f"last_flush_rows={self.last_flush_rows} "
            f"compacts={self.compacts} "
            f"compact_failures={self.compact_failures} "
            f"last_compact_s={self.last_compact_duration_s:.3f} "
            f"last_compact_rows={self.last_compact_rows}"
        )


@dataclass(frozen=True)
class FlushPolicy:
    """Flush when either row or time threshold is hit (never per-tick)."""

    every_rows: int = 5000
    interval_seconds: float = 0.0

    def should_flush(
        self,
        *,
        buffered_rows: int,
        rows_since_flush: int,
        seconds_since_flush: float,
    ) -> Optional[str]:
        if buffered_rows <= 0:
            return None
        if self.every_rows > 0 and rows_since_flush >= self.every_rows:
            return f"every-{self.every_rows}-rows"
        if self.interval_seconds > 0 and seconds_since_flush >= self.interval_seconds:
            return f"every-{self.interval_seconds:g}s"
        return None


@dataclass(frozen=True)
class PartitionVerifyResult:
    source: str
    rows: int
    duplicate_rows: int
    unsorted: bool
    max_gap_seconds: float
    gap_count_over_threshold: int
    first_ts: Optional[str]
    last_ts: Optional[str]
    issues: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues


@dataclass
class PartitionVerifyConfig:
    gap_warn_seconds: float = 60.0
    fail_on_gaps: bool = False


def verify_trade_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    config: Optional[PartitionVerifyConfig] = None,
) -> PartitionVerifyResult:
    """Check one trades parquet frame for dups, sort order, and large gaps.

    Large gaps are warnings by default (ES has a daily maintenance break). Pass
    ``fail_on_gaps=True`` when auditing a continuous open window.
    """
    cfg = config or PartitionVerifyConfig()
    issues: list[str] = []
    warnings: list[str] = []
    if frame is None or frame.empty:
        return PartitionVerifyResult(
            source=source,
            rows=0,
            duplicate_rows=0,
            unsorted=False,
            max_gap_seconds=0.0,
            gap_count_over_threshold=0,
            first_ts=None,
            last_ts=None,
            issues=("empty partition",),
        )

    normalized = _normalize_trades(frame)
    rows = len(normalized)

    if normalized["sequence"].notna().any() and normalized["ts_event"].notna().any():
        subset = list(DEDUPE_KEYS_FULL)
    else:
        subset = list(DEDUPE_KEYS_LEGACY)
    duplicate_rows = int(normalized.duplicated(subset=subset, keep=False).sum())
    if duplicate_rows:
        issues.append(f"{duplicate_rows} duplicate row(s) on keys {subset}")

    ts = pd.to_datetime(normalized["timestamp"], utc=True, errors="coerce")
    unsorted = bool((ts.diff().dt.total_seconds().fillna(0) < -1e-9).any())
    if unsorted:
        issues.append("timestamps not sorted ascending")

    gaps = ts.diff().dt.total_seconds().dropna()
    max_gap = float(gaps.max()) if not gaps.empty else 0.0
    gap_over = int((gaps > cfg.gap_warn_seconds).sum()) if not gaps.empty else 0
    if gap_over:
        msg = (
            f"{gap_over} gap(s) > {cfg.gap_warn_seconds:g}s (max={max_gap:.1f}s)"
        )
        if cfg.fail_on_gaps:
            issues.append(msg)
        else:
            warnings.append(msg)

    first = ts.iloc[0]
    last = ts.iloc[-1]
    return PartitionVerifyResult(
        source=source,
        rows=rows,
        duplicate_rows=duplicate_rows,
        unsorted=unsorted,
        max_gap_seconds=max_gap,
        gap_count_over_threshold=gap_over,
        first_ts=None if pd.isna(first) else first.isoformat(),
        last_ts=None if pd.isna(last) else last.isoformat(),
        issues=tuple(issues),
        warnings=tuple(warnings),
    )


def compare_local_remote_counts(local_rows: int, remote_rows: Optional[int]) -> list[str]:
    """Return issues when local and GCS row counts diverge."""
    if remote_rows is None:
        return ["remote partition missing"]
    if local_rows != remote_rows:
        return [f"row count mismatch local={local_rows} remote={remote_rows}"]
    return []
