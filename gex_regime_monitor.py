"""GEX regime monitor.

Responsibility: Poll option chains on an interval, compute GEX snapshots, and
publish them to the event bus. Does not perform greeks math directly, submit
orders, or persist snapshots.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from event_bus import EventBus, Topics
from gex_calculator import GexSnapshot, build_snapshot
from greeks_calculator import enrich_strikes
from options_chain_transformer import filter_expiration, normalize_schwab_chain
from schwab_options_chain_client import SchwabOptionsChainClient

logger = logging.getLogger(__name__)

GexHandler = Callable[[GexSnapshot], None]


class GexRegimeMonitor:
    """Background poller that publishes gex.snapshot events."""

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
        on_snapshot: Optional[GexHandler] = None,
    ) -> None:
        self._chain_client = chain_client
        self._bus = bus
        self._symbols = tuple(symbol.upper() for symbol in symbols)
        self._poll_interval_seconds = poll_interval_seconds
        self._strike_count = strike_count
        self._days_to_expiration = days_to_expiration
        self._risk_free_rate = risk_free_rate
        self._on_snapshot = on_snapshot
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._latest: dict[str, GexSnapshot] = {}

    @property
    def latest_snapshots(self) -> dict[str, GexSnapshot]:
        """Return the most recent snapshot per symbol."""
        return dict(self._latest)

    def get_latest(self, symbol: str) -> Optional[GexSnapshot]:
        """Return the latest snapshot for one symbol, if available."""
        return self._latest.get(symbol.upper())

    def start(self) -> None:
        """Start the background polling thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="gex-regime-monitor",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "GEX regime monitor started for %s (poll every %.0fs)",
            ", ".join(self._symbols),
            self._poll_interval_seconds,
        )

    def stop(self) -> None:
        """Stop the background polling thread."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval_seconds + 5.0)
            self._thread = None

    def poll_once(self) -> list[GexSnapshot]:
        """Fetch and publish snapshots for all configured symbols."""
        poll_started = time.monotonic()
        snapshots: list[GexSnapshot] = []
        for symbol in self._symbols:
            try:
                logger.info(
                    "GEX polling %s chain (%sDTE, %d strikes)...",
                    symbol,
                    self._days_to_expiration,
                    self._strike_count,
                )
                snapshot = self._fetch_snapshot(symbol)
            except Exception:
                logger.exception("GEX snapshot failed for %s", symbol)
                continue
            snapshots.append(snapshot)
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
                "poll took %.1fs, next in %.0fs",
                symbol,
                snapshot.spot,
                snapshot.net_gex,
                snapshot.regime,
                f"{snapshot.put_wall:.2f}" if snapshot.put_wall else "n/a",
                f"{snapshot.flip_level:.2f}" if snapshot.flip_level else "n/a",
                f"{snapshot.call_wall:.2f}" if snapshot.call_wall else "n/a",
                elapsed,
                self._poll_interval_seconds,
            )
        return snapshots

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self._poll_interval_seconds)

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
