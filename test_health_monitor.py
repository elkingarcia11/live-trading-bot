"""Tests for health alerts: queue depth, flush lag, missed bars, API errors."""

from __future__ import annotations

from datetime import datetime, timezone

from event_bus import EventBus, Topics
from health_monitor import HealthMonitor, HealthStatus, HealthThresholds
from stream_data_processor import CleanBarEvent


def _monitor(**threshold_overrides: object) -> HealthMonitor:
    thresholds = HealthThresholds(
        startup_grace_seconds=0.0,
        max_queue_depth=10,
        max_flush_lag_seconds=5.0,
        max_missed_bars=2,
        max_api_errors_per_hour=2,
        max_trading_errors_per_hour=2,
        **threshold_overrides,
    )
    monitor = HealthMonitor(
        EventBus(),
        thresholds=thresholds,
        monitored_modules=("stream_data_processor",),
    )
    monitor.start()
    monitor._started_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    return monitor


def _mark_feed_fresh(monitor: HealthMonitor) -> None:
    monitor._bus.publish(
        Topics.BAR_CLEAN,
        CleanBarEvent(
            symbol="SPY",
            timeframe="1m",
            timestamp=datetime.now(timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1.0,
        ),
        source="stream_data_processor",
    )


def test_queue_depth_alert_degrades_health() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    alerts: list[object] = []
    monitor._bus.subscribe(Topics.HEALTH_ALERT, lambda event: alerts.append(event.payload))

    monitor.update_runtime_metrics(queue_depth=11)
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.DEGRADED
    assert snapshot.queue_depth == 11
    assert any("Queue depth" in note for note in snapshot.notes)
    assert alerts


def test_flush_lag_alert_degrades_health() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    monitor.update_runtime_metrics(queue_depth=0, flush_lag_seconds=12.0)
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.DEGRADED
    assert snapshot.flush_lag_seconds == 12.0
    assert any("Flush lag" in note for note in snapshot.notes)


def test_missed_bars_alert_degrades_health() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    monitor.record_missed_bars(3)
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.DEGRADED
    assert snapshot.missed_bars == 3
    assert any("Missed bars" in note for note in snapshot.notes)


def test_api_error_alert_degrades_health() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    monitor._bus.publish(Topics.STREAM_ERROR, {"error": "databento down"}, source="stream")
    monitor.record_api_error()
    monitor.record_api_error()
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.DEGRADED
    assert snapshot.api_errors_hour >= 3
    assert any("API errors" in note for note in snapshot.notes)


def test_trading_error_alert_degrades_health() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    monitor.record_trading_error()
    monitor.record_trading_error()
    monitor.record_trading_error()
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.DEGRADED
    assert snapshot.trading_errors_hour == 3
    assert any("Trading-path errors" in note for note in snapshot.notes)


def test_healthy_when_runtime_metrics_are_inside_thresholds() -> None:
    monitor = _monitor()
    _mark_feed_fresh(monitor)
    monitor.update_runtime_metrics(queue_depth=1, flush_lag_seconds=0.2)
    monitor.record_missed_bars(1)
    snapshot = monitor.check()

    assert snapshot.status == HealthStatus.HEALTHY
    assert snapshot.queue_depth == 1
    assert snapshot.missed_bars == 1
