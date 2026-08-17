"""Health Monitor.

Responsibility: Runtime observability and health checks.

Tracks feed latency, reconnect frequency, indicator timing, order round-trip
latency, persist queue depth, flush lag, missed bars, API errors, and silent
modules. Publishes health alerts when thresholds are breached. Does not
execute trades, persist audit logs, or own pub/sub routing.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

from event_bus import Event, EventBus, Topics

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Overall runtime health state."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class HealthThresholds:
    """Threshold configuration for health evaluation."""

    feed_stale_seconds: float = 120.0
    indicator_stale_seconds: float = 180.0
    max_reconnects_per_hour: int = 5
    max_order_round_trip_seconds: float = 10.0
    module_silence_seconds: float = 300.0
    startup_grace_seconds: float = 180.0
    max_queue_depth: int = 50_000
    max_flush_lag_seconds: float = 120.0
    max_missed_bars: int = 5
    max_api_errors_per_hour: int = 10
    max_trading_errors_per_hour: int = 10


PIPELINE_MODULES = (
    "stream_data_processor",
    "data_aggregator",
    "indicator_coordinator",
)


@dataclass
class HealthSnapshot:
    """Point-in-time observability snapshot."""

    status: HealthStatus
    checked_at: datetime
    feed_latency_seconds: Optional[float] = None
    last_bar_at: Optional[datetime] = None
    reconnect_count_hour: int = 0
    last_reconnect_at: Optional[datetime] = None
    avg_indicator_latency_seconds: Optional[float] = None
    avg_order_round_trip_seconds: Optional[float] = None
    queue_depth: int = 0
    flush_lag_seconds: Optional[float] = None
    missed_bars: int = 0
    api_errors_hour: int = 0
    trading_errors_hour: int = 0
    stale_modules: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the snapshot for logging or dashboards."""
        return {
            "status": self.status.value,
            "checked_at": self.checked_at.isoformat(),
            "feed_latency_seconds": self.feed_latency_seconds,
            "last_bar_at": self.last_bar_at.isoformat() if self.last_bar_at else None,
            "reconnect_count_hour": self.reconnect_count_hour,
            "last_reconnect_at": (
                self.last_reconnect_at.isoformat() if self.last_reconnect_at else None
            ),
            "avg_indicator_latency_seconds": self.avg_indicator_latency_seconds,
            "avg_order_round_trip_seconds": self.avg_order_round_trip_seconds,
            "queue_depth": self.queue_depth,
            "flush_lag_seconds": self.flush_lag_seconds,
            "missed_bars": self.missed_bars,
            "api_errors_hour": self.api_errors_hour,
            "trading_errors_hour": self.trading_errors_hour,
            "stale_modules": self.stale_modules,
            "notes": self.notes,
        }


@dataclass
class _PendingOrder:
    """Tracks order submission time until completion."""

    submitted_at: datetime


class HealthMonitor:
    """Observes bus activity and publishes health alerts when thresholds fail."""

    MONITORED_TOPICS = (
        Topics.BAR_CLEAN,
        Topics.BAR_AGGREGATED,
        Topics.MARKET_TRADE,
        Topics.INDICATORS_SNAPSHOT,
        Topics.ORDER_UPDATED,
        Topics.ORDER_FILL,
        Topics.STREAM_CONNECTED,
        Topics.STREAM_DISCONNECTED,
        Topics.STREAM_RECONNECTING,
        Topics.STREAM_ERROR,
    )

    def __init__(
        self,
        bus: EventBus,
        *,
        thresholds: Optional[HealthThresholds] = None,
        monitored_modules: Optional[tuple[str, ...]] = None,
    ) -> None:
        """Initialize the health monitor.

        Args:
            bus: Event bus to observe.
            thresholds: Health threshold configuration.
            monitored_modules: Producer modules expected to emit events.
        """
        self._bus = bus
        self._thresholds = thresholds or HealthThresholds()
        self._monitored_modules = monitored_modules or PIPELINE_MODULES

        self._started_at: Optional[datetime] = None
        self._last_bar_at: Optional[datetime] = None
        self._last_feed_at: Optional[datetime] = None
        self._last_indicator_at: Optional[datetime] = None
        self._last_module_event_at: dict[str, datetime] = {}
        self._reconnect_times: deque[datetime] = deque()
        self._indicator_latencies: deque[float] = deque(maxlen=100)
        self._order_round_trips: deque[float] = deque(maxlen=100)
        self._pending_orders: dict[str, _PendingOrder] = {}
        self._queue_depth = 0
        self._flush_lag_seconds: Optional[float] = None
        self._missed_bars = 0
        self._api_error_times: deque[datetime] = deque()
        self._trading_error_times: deque[datetime] = deque()
        self._last_snapshot: Optional[HealthSnapshot] = None
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Subscribe to runtime events and begin observability tracking."""
        if self._started:
            return

        for topic in self.MONITORED_TOPICS:
            self._bus.subscribe(topic, self._on_event)
        self._bus.subscribe("*", self._track_module_activity)
        self._started_at = datetime.now(timezone.utc)
        self._started = True
        logger.info("HealthMonitor started")

    def set_thresholds(self, thresholds: HealthThresholds) -> None:
        """Replace health thresholds (used by trading hot-restart)."""
        self._thresholds = thresholds

    def update_runtime_metrics(
        self,
        *,
        queue_depth: Optional[int] = None,
        flush_lag_seconds: Optional[float] = None,
    ) -> None:
        """Sample persist-path metrics that are not published on the bus."""
        with self._lock:
            if queue_depth is not None:
                self._queue_depth = max(0, int(queue_depth))
            self._flush_lag_seconds = flush_lag_seconds

    def record_missed_bars(self, count: int = 1) -> None:
        """Count dropped or skipped bars relative to the expected sequence."""
        if count <= 0:
            return
        with self._lock:
            self._missed_bars += int(count)

    def record_api_error(self, *, at: Optional[datetime] = None) -> None:
        """Record a vendor/broker API or stream protocol error."""
        stamped = at or datetime.now(timezone.utc)
        with self._lock:
            self._api_error_times.append(stamped)

    def record_trading_error(self, *, at: Optional[datetime] = None) -> None:
        """Record a caught strategy/order exception that did not kill the feed."""
        stamped = at or datetime.now(timezone.utc)
        with self._lock:
            self._trading_error_times.append(stamped)

    def check(self) -> HealthSnapshot:
        """Evaluate current health and publish an alert if status worsened.

        Returns:
            Latest health snapshot.
        """
        now = datetime.now(timezone.utc)
        notes: list[str] = []
        stale_modules: list[str] = []
        status = HealthStatus.HEALTHY
        in_startup_grace = self._in_startup_grace(now)

        with self._lock:
            # Prefer last trade/bar activity so tick streams stay healthy while
            # an N-tick bar is still forming.
            feed_anchor = self._last_feed_at or self._last_bar_at
            feed_latency = self._seconds_since(feed_anchor, now)
            indicator_age = self._seconds_since(self._last_indicator_at, now)
            reconnect_count = self._reconnect_count_last_hour(now)
            avg_indicator_latency = self._average(self._indicator_latencies)
            avg_order_round_trip = self._average(self._order_round_trips)
            last_reconnect_at = self._reconnect_times[-1] if self._reconnect_times else None
            queue_depth = self._queue_depth
            flush_lag = self._flush_lag_seconds
            missed_bars = self._missed_bars
            api_errors = self._count_last_hour(self._api_error_times, now)
            trading_errors = self._count_last_hour(self._trading_error_times, now)

            for module in self._monitored_modules:
                last_seen = self._last_module_event_at.get(module)
                # Tick bars only emit through stream_data_processor on completion;
                # keep that module fresh while raw trades are still arriving.
                if (
                    module == "stream_data_processor"
                    and feed_latency is not None
                    and feed_latency <= self._thresholds.feed_stale_seconds
                ):
                    continue
                silence = self._seconds_since(last_seen, now)
                if last_seen is None:
                    if not in_startup_grace:
                        stale_modules.append(module)
                    continue
                if silence > self._thresholds.module_silence_seconds:
                    stale_modules.append(module)

        if feed_latency is not None and feed_latency > self._thresholds.feed_stale_seconds:
            status = HealthStatus.DEGRADED
            notes.append(f"Feed stale for {feed_latency:.1f}s")

        if indicator_age is not None and indicator_age > self._thresholds.indicator_stale_seconds:
            status = HealthStatus.DEGRADED
            notes.append(f"Indicators stale for {indicator_age:.1f}s")

        if reconnect_count > self._thresholds.max_reconnects_per_hour:
            status = HealthStatus.DEGRADED
            notes.append(f"Reconnect count {reconnect_count} in last hour")

        if (
            avg_order_round_trip is not None
            and avg_order_round_trip > self._thresholds.max_order_round_trip_seconds
        ):
            status = HealthStatus.DEGRADED
            notes.append(
                f"Average order round-trip {avg_order_round_trip:.2f}s exceeds threshold"
            )

        if queue_depth > self._thresholds.max_queue_depth:
            status = HealthStatus.DEGRADED
            notes.append(
                f"Queue depth {queue_depth} exceeds {self._thresholds.max_queue_depth}"
            )

        if (
            flush_lag is not None
            and flush_lag > self._thresholds.max_flush_lag_seconds
        ):
            status = HealthStatus.DEGRADED
            notes.append(f"Flush lag {flush_lag:.1f}s exceeds threshold")

        if missed_bars > self._thresholds.max_missed_bars:
            status = HealthStatus.DEGRADED
            notes.append(f"Missed bars {missed_bars} exceeds threshold")

        if api_errors > self._thresholds.max_api_errors_per_hour:
            status = HealthStatus.DEGRADED
            notes.append(f"API errors {api_errors} in last hour")

        if trading_errors > self._thresholds.max_trading_errors_per_hour:
            status = HealthStatus.DEGRADED
            notes.append(f"Trading-path errors {trading_errors} in last hour")

        if stale_modules:
            status = HealthStatus.UNHEALTHY
            notes.append(f"Silent modules: {', '.join(stale_modules)}")

        if feed_latency is None and not in_startup_grace:
            status = HealthStatus.UNHEALTHY
            notes.append("No feed activity yet")
        elif feed_latency is None and in_startup_grace:
            notes.append("Waiting for first trade/bar")

        snapshot = HealthSnapshot(
            status=status,
            checked_at=now,
            feed_latency_seconds=feed_latency,
            last_bar_at=self._last_bar_at,
            reconnect_count_hour=reconnect_count,
            last_reconnect_at=last_reconnect_at,
            avg_indicator_latency_seconds=avg_indicator_latency,
            avg_order_round_trip_seconds=avg_order_round_trip,
            queue_depth=queue_depth,
            flush_lag_seconds=flush_lag,
            missed_bars=missed_bars,
            api_errors_hour=api_errors,
            trading_errors_hour=trading_errors,
            stale_modules=stale_modules,
            notes=notes,
        )

        with self._lock:
            previous_status = (
                self._last_snapshot.status if self._last_snapshot is not None else None
            )
            previous_notes = (
                self._last_snapshot.notes if self._last_snapshot is not None else []
            )
            self._last_snapshot = snapshot

        self._bus.publish(
            Topics.HEALTH_SNAPSHOT,
            snapshot,
            source="health_monitor",
        )

        if previous_status != snapshot.status:
            self._bus.publish(
                Topics.HEALTH_ALERT,
                snapshot,
                source="health_monitor",
                metadata={"previous_status": previous_status.value if previous_status else None},
            )
            self._log_health_snapshot(snapshot, in_startup_grace)
        elif snapshot.status != HealthStatus.HEALTHY and notes != previous_notes:
            self._log_health_snapshot(snapshot, in_startup_grace)

        return snapshot

    def _log_health_snapshot(
        self,
        snapshot: HealthSnapshot,
        in_startup_grace: bool,
    ) -> None:
        if in_startup_grace and snapshot.status == HealthStatus.UNHEALTHY:
            logger.info(
                "Health status=%s notes=%s (startup grace)",
                snapshot.status.value,
                snapshot.notes,
            )
            return
        if snapshot.status == HealthStatus.HEALTHY:
            logger.info("Health status=%s notes=%s", snapshot.status.value, snapshot.notes)
            return
        logger.warning("Health status=%s notes=%s", snapshot.status.value, snapshot.notes)

    def latest_snapshot(self) -> Optional[HealthSnapshot]:
        """Return the most recent health snapshot if one exists."""
        with self._lock:
            return self._last_snapshot

    def _on_event(self, event: Event) -> None:
        """Update observability metrics from runtime events."""
        now = event.timestamp

        with self._lock:
            if event.topic in {Topics.BAR_CLEAN, Topics.BAR_AGGREGATED}:
                self._last_bar_at = now
                self._last_feed_at = now

            if event.topic == Topics.MARKET_TRADE:
                self._last_feed_at = now

            if event.topic == Topics.INDICATORS_SNAPSHOT:
                self._last_indicator_at = now
                duration_ms = event.metadata.get("duration_ms")
                if duration_ms is not None:
                    self._indicator_latencies.append(float(duration_ms) / 1000.0)

            if event.topic == Topics.STREAM_RECONNECTING:
                self._reconnect_times.append(now)

            if event.topic == Topics.STREAM_ERROR:
                self._api_error_times.append(now)

            if event.topic == Topics.ORDER_UPDATED:
                self._track_order_update(event)

            if event.topic == Topics.ORDER_FILL:
                self._track_order_fill(event)

    def _track_module_activity(self, event: Event) -> None:
        """Record the last time each producer module emitted an event."""
        if not event.source:
            return

        with self._lock:
            self._last_module_event_at[event.source] = event.timestamp

    def _track_order_update(self, event: Event) -> None:
        """Record order submission time for round-trip measurement."""
        payload = event.payload
        order_id = getattr(payload, "id", None)
        status = getattr(payload, "status", None)
        if order_id is None or status is None:
            return

        status_value = status.value if hasattr(status, "value") else str(status)
        if status_value == "submitted":
            self._pending_orders[order_id] = _PendingOrder(submitted_at=event.timestamp)

    def _track_order_fill(self, event: Event) -> None:
        """Compute order round-trip latency when a fill is observed."""
        payload = event.payload
        order_id = getattr(payload, "order_id", None)
        if order_id is None:
            return

        pending = self._pending_orders.pop(order_id, None)
        if pending is None:
            return

        latency = (event.timestamp - pending.submitted_at).total_seconds()
        self._order_round_trips.append(latency)

    def _reconnect_count_last_hour(self, now: datetime) -> int:
        """Count reconnect events in the trailing hour."""
        return self._count_last_hour(self._reconnect_times, now)

    def _count_last_hour(self, times: deque[datetime], now: datetime) -> int:
        """Count timestamps in the trailing hour, dropping older entries."""
        cutoff = now - timedelta(hours=1)
        while times and times[0] < cutoff:
            times.popleft()
        return len(times)

    def _seconds_since(
        self,
        timestamp: Optional[datetime],
        now: datetime,
    ) -> Optional[float]:
        """Return elapsed seconds since a timestamp."""
        if timestamp is None:
            return None
        return (now - timestamp).total_seconds()

    def _average(self, values: deque[float]) -> Optional[float]:
        """Return the average of a deque of floats."""
        if not values:
            return None
        return sum(values) / len(values)

    def _in_startup_grace(self, now: datetime) -> bool:
        """Return whether the monitor is still in its post-start grace window."""
        if self._started_at is None:
            return True
        elapsed = (now - self._started_at).total_seconds()
        return elapsed < self._thresholds.startup_grace_seconds


if __name__ == "__main__":
    from signal_evaluator import SignalAction, StrategySignal
    from stream_data_processor import CleanBarEvent

    bus = EventBus()
    monitor = HealthMonitor(bus)
    monitor.start()

    bus.publish(
        Topics.BAR_CLEAN,
        CleanBarEvent(
            symbol="AAPL",
            timeframe="1m",
            timestamp=datetime.now(timezone.utc),
            open=185.0,
            high=185.5,
            low=184.8,
            close=185.2,
            volume=1000,
        ),
        source="stream_data_processor",
    )

    bus.publish(
        Topics.INDICATORS_SNAPSHOT,
        {"symbol": "AAPL", "values": {"rsi": 31.2}},
        source="indicator_coordinator",
        metadata={"duration_ms": 12.5},
    )

    bus.publish(
        Topics.STREAM_RECONNECTING,
        {"url": "wss://stream.example.com"},
        source="stream_connection_manager",
    )

    time.sleep(0.01)
    snapshot = monitor.check()
    print(snapshot.to_dict())
