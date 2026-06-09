"""Event Bus.

Responsibility: In-process pub/sub backbone.

Decouples producers from consumers so modules such as the stream processor,
strategy evaluator, trade logger, and health monitor can communicate without
direct references. Does not persist events, evaluate strategy rules, or track
system health metrics.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

EventHandler = Callable[["Event"], None]


class Topics:
    """Canonical event topics used across the trading system."""

    BAR_CLEAN = "bar.clean"
    BAR_AGGREGATED = "bar.aggregated"
    INDICATORS_SNAPSHOT = "indicators.snapshot"
    STRATEGY_SIGNAL = "strategy.signal"
    RISK_DECISION = "risk.decision"
    ORDER_UPDATED = "order.updated"
    ORDER_FILL = "order.fill"
    POSITION_EXIT = "position.exit"
    POSITION_SYNC = "position.sync"
    STREAM_CONNECTED = "stream.connected"
    STREAM_DISCONNECTED = "stream.disconnected"
    STREAM_RECONNECTING = "stream.reconnecting"
    STREAM_ERROR = "stream.error"
    HEALTH_ALERT = "health.alert"
    HEALTH_SNAPSHOT = "health.snapshot"


@dataclass(frozen=True)
class Event:
    """Envelope published through the event bus."""

    topic: str
    payload: Any
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _Subscription:
    """Internal handler registration."""

    pattern: re.Pattern[str]
    handler: EventHandler


class EventBus:
    """Thread-safe in-process publish/subscribe event bus."""

    def __init__(self) -> None:
        self._subscriptions: list[_Subscription] = []
        self._lock = threading.Lock()

    def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Register a handler for a topic or wildcard pattern.

        Args:
            topic_pattern: Exact topic (e.g. `bar.clean`) or wildcard pattern
                such as `bar.*` or `*`.
            handler: Callback invoked for each matching event.
        """
        pattern = self._compile_pattern(topic_pattern)
        with self._lock:
            self._subscriptions.append(_Subscription(pattern=pattern, handler=handler))

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        source: str = "",
        metadata: Optional[dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Publish an event to all matching subscribers.

        Args:
            topic: Event topic name.
            payload: Domain object or payload associated with the event.
            source: Optional producer module name.
            metadata: Optional event metadata such as timing information.
            timestamp: Optional event timestamp. Defaults to current UTC time.
        """
        event = Event(
            topic=topic,
            payload=payload,
            timestamp=timestamp or datetime.now(timezone.utc),
            source=source,
            metadata=dict(metadata or {}),
        )

        with self._lock:
            subscriptions = list(self._subscriptions)

        for subscription in subscriptions:
            if not subscription.pattern.fullmatch(topic):
                continue
            try:
                subscription.handler(event)
            except Exception:
                logger.exception("Event handler failed for topic %s", topic)

    def clear(self) -> None:
        """Remove all subscriptions."""
        with self._lock:
            self._subscriptions.clear()

    def _compile_pattern(self, topic_pattern: str) -> re.Pattern[str]:
        """Convert a wildcard topic pattern into a regex."""
        if topic_pattern == "*":
            return re.compile(r".+")

        escaped = re.escape(topic_pattern).replace(r"\*", "[^.]+")
        return re.compile(f"^{escaped}$")


if __name__ == "__main__":
    bus = EventBus()
    received: list[str] = []

    def on_any_bar(event: Event) -> None:
        received.append(event.topic)

    bus.subscribe("bar.*", on_any_bar)
    bus.publish(Topics.BAR_CLEAN, {"symbol": "AAPL"}, source="stream_data_processor")
    bus.publish(Topics.STRATEGY_SIGNAL, {"action": "buy"}, source="signal_evaluator")
    print(f"Bar events received: {received}")
