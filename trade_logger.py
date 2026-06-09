"""Trade Logger.

Responsibility: Durable audit trail for trading activity.

Passive listener that records signals, risk decisions, order lifecycle events,
fills, and position exits to append-only storage. Does not publish events,
evaluate strategies, or submit broker orders.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from event_bus import Event, EventBus, Topics

logger = logging.getLogger(__name__)

AUDIT_TOPICS = frozenset(
    {
        Topics.STRATEGY_SIGNAL,
        Topics.RISK_DECISION,
        Topics.ORDER_UPDATED,
        Topics.ORDER_FILL,
        Topics.POSITION_EXIT,
        Topics.BAR_CLEAN,
    }
)


@dataclass(frozen=True)
class RiskDecisionRecord:
    """Risk guard outcome attached to a strategy signal."""

    symbol: str
    approved: bool
    reason: str
    strategy_name: Optional[str] = None


class TradeLogger:
    """Append-only audit logger subscribed to internal trading events."""

    def __init__(
        self,
        bus: EventBus,
        *,
        log_path: str | Path = "logs/audit.jsonl",
        topics: Optional[frozenset[str]] = None,
    ) -> None:
        """Initialize the trade logger.

        Args:
            bus: Event bus to subscribe to.
            log_path: Append-only JSON Lines audit file path.
            topics: Optional topic filter. Defaults to trading audit topics.
        """
        self._bus = bus
        self._log_path = Path(log_path)
        self._topics = topics or AUDIT_TOPICS
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        """Subscribe to audit topics and begin passive logging."""
        if self._started:
            return

        for topic in sorted(self._topics):
            self._bus.subscribe(topic, self._on_event)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._started = True
        logger.info("TradeLogger writing audit records to %s", self._log_path)

    def stop(self) -> None:
        """Mark the logger as stopped. Subscriptions remain on the shared bus."""
        self._started = False

    def log_event(self, event: Event) -> None:
        """Write one audit record for an event.

        Args:
            event: Internal event to persist.
        """
        record = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "topic": event.topic,
            "source": event.source,
            "event_timestamp": event.timestamp.isoformat(),
            "metadata": event.metadata,
            "payload": self._serialize_payload(event.payload),
        }
        line = json.dumps(record, default=str)

        with self._lock:
            with self._log_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def _on_event(self, event: Event) -> None:
        """Persist an event received from the bus."""
        try:
            self.log_event(event)
        except Exception:
            logger.exception("Failed to write audit record for topic %s", event.topic)

    def _serialize_payload(self, payload: Any) -> Any:
        """Convert payloads into JSON-safe audit representations."""
        if payload is None:
            return None
        if hasattr(payload, "to_dict") and callable(payload.to_dict):
            return payload.to_dict()
        if is_dataclass(payload):
            return asdict(payload)
        if isinstance(payload, Enum):
            return payload.value
        if isinstance(payload, dict):
            return {
                key: self._serialize_payload(value)
                for key, value in payload.items()
            }
        if isinstance(payload, (list, tuple)):
            return [self._serialize_payload(item) for item in payload]
        return payload


if __name__ == "__main__":
    from signal_evaluator import SignalAction, StrategySignal

    bus = EventBus()
    audit_path = Path("logs/example_audit.jsonl")
    trade_logger = TradeLogger(bus, log_path=audit_path)
    trade_logger.start()

    bus.publish(
        Topics.STRATEGY_SIGNAL,
        StrategySignal(
            symbol="AAPL",
            timeframe="5m",
            timestamp=datetime.now(timezone.utc),
            action=SignalAction.BUY,
            strategy_name="rsi_mean_reversion",
            close=185.2,
            indicators={"rsi": 28.5},
        ),
        source="signal_evaluator",
    )

    bus.publish(
        Topics.RISK_DECISION,
        RiskDecisionRecord(
            symbol="AAPL",
            approved=True,
            reason="within position limits",
            strategy_name="rsi_mean_reversion",
        ),
        source="risk_guard",
    )

    print(f"Audit records written to {audit_path}")
