"""Schwab account and position synchronization.

Responsibility: Align local portfolio state with Schwab account positions.

Fetches account balances and positions from the Schwab Trader API and updates
the position tracker. Does not submit orders or evaluate strategy signals.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Sequence

from position_tracker import Position, PositionTracker
from schwab_trader_client import SchwabAccountSnapshot, SchwabTraderClient, SchwabTraderError

logger = logging.getLogger(__name__)


class SchwabAccountSync:
    """Synchronize Schwab account positions into the position tracker."""

    def __init__(self, trader_client: SchwabTraderClient) -> None:
        self._trader_client = trader_client

    @classmethod
    def from_env(cls) -> SchwabAccountSync:
        """Build an account sync helper from environment variables."""
        return cls(SchwabTraderClient.from_env())

    def fetch_snapshots(self, *, include_positions: bool = True) -> tuple[SchwabAccountSnapshot, ...]:
        """Fetch balances and positions for every linked Schwab account."""
        return self._trader_client.get_all_account_snapshots(
            include_positions=include_positions,
        )

    def fetch_snapshot(
        self,
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> SchwabAccountSnapshot:
        """Fetch balances and positions for one Schwab account."""
        return self._trader_client.get_account_snapshot(
            account_hash=account_hash,
            account_number=account_number,
            include_positions=True,
        )

    def sync_positions(
        self,
        tracker: PositionTracker,
        *,
        watchlist: Optional[Sequence[str]] = None,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
        preserve_risk_levels: bool = True,
    ) -> SchwabAccountSnapshot:
        """Update the position tracker from Schwab account positions.

        Args:
            tracker: Local portfolio tracker to update.
            watchlist: Optional symbol filter. Symbols in the watchlist that are
                flat at the broker are removed locally.
            account_hash: Optional encrypted account hash override.
            account_number: Optional plain account number override.
            preserve_risk_levels: Keep existing stop/target settings per symbol.

        Returns:
            The Schwab account snapshot used for the sync.
        """
        snapshot = self.fetch_snapshot(
            account_hash=account_hash,
            account_number=account_number,
        )
        broker_positions = {
            position.symbol: (position.quantity, position.average_price)
            for position in snapshot.positions
        }
        watchlist_set = (
            {symbol.upper() for symbol in watchlist} if watchlist is not None else None
        )

        tracker.sync_broker_positions(
            broker_positions,
            watchlist=watchlist_set,
            preserve_risk_levels=preserve_risk_levels,
            timestamp=datetime.now(timezone.utc),
        )
        logger.info(
            "Synced %d Schwab positions into position tracker for account %s",
            len(snapshot.positions),
            snapshot.account_number,
        )
        return snapshot

    def list_synced_positions(self, tracker: PositionTracker) -> list[Position]:
        """Return the local positions currently tracked after a sync."""
        return tracker.list_positions()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync = SchwabAccountSync.from_env()
    tracker = PositionTracker()
    snapshot = sync.sync_positions(tracker, watchlist=("SPY", "QQQ"))
    print(f"Account equity: {snapshot.balances.equity}")
    print(f"Synced positions: {[position.symbol for position in tracker.list_positions()]}")
