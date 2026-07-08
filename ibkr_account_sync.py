"""IBKR account and position synchronization.

Responsibility: Align local portfolio state with IBKR account positions.

Fetches account positions from the IBKR Web API and updates the position
tracker. Does not submit orders or evaluate strategy signals.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Sequence

from ibkr_trader_client import IbkrAccountSnapshot, IbkrTraderClient
from position_tracker import Position, PositionTracker

logger = logging.getLogger(__name__)


class IbkrAccountSync:
    """Synchronize IBKR account positions into the position tracker."""

    def __init__(self, trader_client: IbkrTraderClient) -> None:
        self._trader_client = trader_client

    @classmethod
    def from_env(cls) -> IbkrAccountSync:
        """Build an account sync helper from config.json."""
        return cls(IbkrTraderClient.from_env())

    def fetch_snapshot(
        self,
        *,
        account_id: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> IbkrAccountSnapshot:
        """Fetch balances and positions for one IBKR account."""
        self._trader_client.ensure_session()
        return self._trader_client.get_account_snapshot(
            account_id=account_id,
            account_number=account_number,
            include_positions=True,
        )

    def sync_positions(
        self,
        tracker: PositionTracker,
        *,
        watchlist: Optional[Sequence[str]] = None,
        account_id: Optional[str] = None,
        account_number: Optional[str] = None,
        preserve_risk_levels: bool = True,
    ) -> IbkrAccountSnapshot:
        """Update the position tracker from IBKR account positions."""
        snapshot = self.fetch_snapshot(
            account_id=account_id,
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
            "Synced %d IBKR positions into position tracker for account %s",
            len(snapshot.positions),
            snapshot.account_number,
        )
        return snapshot

    def list_synced_positions(self, tracker: PositionTracker) -> list[Position]:
        """Return the local positions currently tracked after a sync."""
        return tracker.list_positions()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync = IbkrAccountSync.from_env()
    tracker = PositionTracker()
    snapshot = sync.sync_positions(tracker, watchlist=("SPY", "QQQ"))
    print(f"Account: {snapshot.account_number}")
    print(f"Synced positions: {[position.symbol for position in tracker.list_positions()]}")
