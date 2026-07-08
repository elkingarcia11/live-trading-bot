"""IBKR TWS account and position synchronization."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence

from ibkr_tws_connection import IbkrTwsError, IbkrTwsRuntime, IbkrTwsPosition
from position_tracker import Position, PositionTracker

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IbkrTwsAccountBalances:
    equity: float
    buying_power: float
    cash_available_for_trading: float
    liquidation_value: float


@dataclass(frozen=True)
class IbkrTwsAccountPosition:
    symbol: str
    quantity: float
    average_price: float
    market_value: float
    current_day_profit_loss: float
    asset_type: str = "EQUITY"


@dataclass(frozen=True)
class IbkrTwsAccountSnapshot:
    account_number: str
    balances: IbkrTwsAccountBalances
    positions: tuple[IbkrTwsAccountPosition, ...]


class IbkrTwsAccountSync:
    """Synchronize TWS positions into the position tracker."""

    def __init__(self, runtime: IbkrTwsRuntime) -> None:
        self._runtime = runtime

    @classmethod
    def from_runtime(cls, runtime: IbkrTwsRuntime) -> IbkrTwsAccountSync:
        return cls(runtime)

    def fetch_snapshot(
        self,
        *,
        account_id: Optional[str] = None,
    ) -> IbkrTwsAccountSnapshot:
        try:
            positions = self._runtime.request_positions()
        except IbkrTwsError as exc:
            raise IbkrTwsError(str(exc)) from exc

        account_number = account_id or _resolve_account_id(self._runtime.managed_accounts())
        normalized = tuple(_normalize_position(position) for position in positions)
        market_value = sum(position.market_value for position in normalized)
        balances = IbkrTwsAccountBalances(
            equity=market_value,
            buying_power=0.0,
            cash_available_for_trading=0.0,
            liquidation_value=market_value,
        )
        return IbkrTwsAccountSnapshot(
            account_number=account_number,
            balances=balances,
            positions=normalized,
        )

    def sync_positions(
        self,
        tracker: PositionTracker,
        *,
        watchlist: Optional[Sequence[str]] = None,
        account_id: Optional[str] = None,
        preserve_risk_levels: bool = True,
    ) -> IbkrTwsAccountSnapshot:
        snapshot = self.fetch_snapshot(account_id=account_id)
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
            "Synced %d IBKR TWS positions into position tracker for account %s",
            len(snapshot.positions),
            snapshot.account_number,
        )
        return snapshot

    def list_synced_positions(self, tracker: PositionTracker) -> list[Position]:
        return tracker.list_positions()


def _resolve_account_id(accounts_csv: str) -> str:
    for account in accounts_csv.split(","):
        cleaned = account.strip()
        if cleaned:
            return cleaned
    return ""


def _normalize_position(position: IbkrTwsPosition) -> IbkrTwsAccountPosition:
    market_value = position.quantity * position.average_price
    return IbkrTwsAccountPosition(
        symbol=position.symbol,
        quantity=position.quantity,
        average_price=position.average_price,
        market_value=market_value,
        current_day_profit_loss=0.0,
    )
