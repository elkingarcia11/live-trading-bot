"""GEX regime monitor.

Responsibility: Fetch option chains on a schedule, compute GEX snapshots, and
publish them to the event bus. Structural levels (walls / flip) are refreshed
only at configured local times (e.g. open and Europe close), not continuously.
Does not perform greeks math directly, submit orders, or persist snapshots.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, time as dt_time, timezone
from typing import Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from event_bus import EventBus, Topics
from gex_calculator import GexSnapshot, build_snapshot
from gex_level_schedule import due_refresh_slots, next_refresh_at, prune_fired_slots
from greeks_calculator import enrich_strikes
from options_chain_transformer import filter_expiration, normalize_schwab_chain
from schwab_options_chain_client import SchwabOptionsChainClient

logger = logging.getLogger(__name__)

GexHandler = Callable[[GexSnapshot], None]

# Sleep granularity while waiting for the next scheduled refresh.
_SCHEDULE_POLL_SECONDS = 15.0


class GexRegimeMonitor:
    """Background poller that publishes gex.snapshot events on a schedule."""

    def __init__(
        self,
        chain_client: SchwabOptionsChainClient,
        bus: EventBus,
        *,
        symbols: Sequence[str],
        poll_interval_seconds: float = 20.0,
        strike_count: int = 50,
        days_to_expiration: int = 0,
        risk_free_rate: float = 0.05,
        level_refresh_times_local: Sequence[str] = ("09:35", "11:30", "15:00"),
        timezone_name: str = "America/New_York",
        on_snapshot: Optional[GexHandler] = None,
    ) -> None:
        self._chain_client = chain_client
        self._bus = bus
        self._symbols = tuple(symbol.upper() for symbol in symbols)
        # Kept for logging / backward-compatible config; schedule drives refreshes.
        self._poll_interval_seconds = poll_interval_seconds
        self._strike_count = strike_count
        self._days_to_expiration = days_to_expiration
        self._risk_free_rate = risk_free_rate
        self._timezone_name = timezone_name
        self._refresh_times_local = self._parse_refresh_times(level_refresh_times_local)
        self._on_snapshot = on_snapshot
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._latest: dict[str, GexSnapshot] = {}
        self._fired_slots: set[datetime] = set()
        self._lock = threading.Lock()

    @staticmethod
    def _parse_refresh_times(values: Sequence[str]) -> tuple[dt_time, ...]:
        from gex_level_schedule import parse_local_hhmm_times

        times = parse_local_hhmm_times(tuple(values))
        if not times:
            raise ValueError("level_refresh_times_local must include at least one HH:MM")
        return times

    @property
    def latest_snapshots(self) -> dict[str, GexSnapshot]:
        """Return the most recent snapshot per symbol."""
        with self._lock:
            return dict(self._latest)

    def get_latest(self, symbol: str) -> Optional[GexSnapshot]:
        """Return the latest snapshot for one symbol, if available."""
        with self._lock:
            return self._latest.get(symbol.upper())

    def mark_past_slots_fired(self, now: Optional[datetime] = None) -> None:
        """Mark today's already-due refresh slots as fired (e.g. after startup poll)."""
        stamp = now or datetime.now(timezone.utc)
        due = due_refresh_slots(
            stamp,
            refresh_times_local=self._refresh_times_local,
            timezone_name=self._timezone_name,
            already_fired=set(),
        )
        with self._lock:
            self._fired_slots.update(due)

    def start(self) -> None:
        """Start the background schedule thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="gex-regime-monitor",
            daemon=True,
        )
        self._thread.start()
        slots = ", ".join(t.strftime("%H:%M") for t in self._refresh_times_local)
        logger.info(
            "GEX regime monitor started for %s (level refresh at %s %s)",
            ", ".join(self._symbols),
            slots,
            self._timezone_name,
        )

    def stop(self) -> None:
        """Stop the background schedule thread."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self._poll_interval_seconds, 30.0) + 5.0)
            self._thread = None

    def poll_once(self, *, reason: str = "manual") -> list[GexSnapshot]:
        """Fetch and publish snapshots for all configured symbols."""
        poll_started = time.monotonic()
        snapshots: list[GexSnapshot] = []
        for symbol in self._symbols:
            try:
                logger.info(
                    "GEX refreshing levels for %s (%sDTE, %d strikes) [%s]...",
                    symbol,
                    self._days_to_expiration,
                    self._strike_count,
                    reason,
                )
                snapshot = self._fetch_snapshot(symbol)
            except Exception:
                logger.exception("GEX snapshot failed for %s", symbol)
                continue
            snapshots.append(snapshot)
            with self._lock:
                self._latest[symbol] = snapshot
            self._bus.publish(
                Topics.GEX_SNAPSHOT,
                snapshot,
                source="gex_regime_monitor",
            )
            if self._on_snapshot is not None:
                self._on_snapshot(snapshot)
            elapsed = time.monotonic() - poll_started
            logger.info(
                "GEX %s spot=%.2f net=%.0f regime=%s | "
                "put_wall=%s flip=%s call_wall=%s | "
                "refresh took %.1fs [%s]",
                symbol,
                snapshot.spot,
                snapshot.net_gex,
                snapshot.regime,
                f"{snapshot.put_wall:.2f}" if snapshot.put_wall else "n/a",
                f"{snapshot.flip_level:.2f}" if snapshot.flip_level else "n/a",
                f"{snapshot.call_wall:.2f}" if snapshot.call_wall else "n/a",
                elapsed,
                reason,
            )
        return snapshots

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            tz = ZoneInfo(self._timezone_name)
            local_today = now.astimezone(tz).date()
            with self._lock:
                self._fired_slots = prune_fired_slots(
                    self._fired_slots,
                    keep_on_or_after=local_today,
                )
                fired = set(self._fired_slots)

            due = due_refresh_slots(
                now,
                refresh_times_local=self._refresh_times_local,
                timezone_name=self._timezone_name,
                already_fired=fired,
            )
            for slot in due:
                label = slot.astimezone(tz).strftime("%H:%M %Z")
                self.poll_once(reason=f"scheduled {label}")
                with self._lock:
                    self._fired_slots.add(slot)

            with self._lock:
                fired = set(self._fired_slots)
            nxt = next_refresh_at(
                now,
                refresh_times_local=self._refresh_times_local,
                timezone_name=self._timezone_name,
                already_fired=fired,
            )
            wait_s = max(
                1.0,
                min(_SCHEDULE_POLL_SECONDS, (nxt - now).total_seconds()),
            )
            self._stop.wait(wait_s)

    def _fetch_snapshot(self, symbol: str) -> GexSnapshot:
        chain = self._chain_client.fetch_chain(
            symbol,
            contract_type="ALL",
            strike_count=self._strike_count,
            days_to_expiration=self._days_to_expiration,
        )
        rows = normalize_schwab_chain(chain, underlying_symbol=symbol)
        rows = filter_expiration(rows, days_to_expiration=self._days_to_expiration)
        if not rows:
            raise ValueError(f"no {self._days_to_expiration}DTE contracts for {symbol}")
        enriched = enrich_strikes(rows, risk_free_rate=self._risk_free_rate)
        return build_snapshot(
            symbol,
            enriched,
            timestamp=datetime.now(timezone.utc),
        )
