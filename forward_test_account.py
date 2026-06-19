"""Forward-test paper account with optional GCS persistence.

Tracks cash balance, realized P&L, and open positions for email forward-test
mode. Does not submit broker orders.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from google.cloud import storage
from google.cloud.exceptions import NotFound

from option_quote import OptionQuoteSnapshot

if TYPE_CHECKING:
    from config import AppConfig
    from position_tracker import PositionTracker

logger = logging.getLogger(__name__)


@dataclass
class ForwardTestOpenPosition:
    """One open paper position restored across restarts."""

    symbol: str
    underlying_symbol: str
    quantity: float
    entry_price: float
    asset_type: str
    opened_at: str
    underlying_entry_price: Optional[float] = None
    entry_quote: Optional[dict[str, Optional[float]]] = None


@dataclass
class ForwardTestTradeRecord:
    """One buy or sell applied to the paper account."""

    action: str
    symbol: str
    underlying_symbol: str
    quantity: float
    price: float
    amount: float
    realized_pnl: Optional[float]
    cash_after: float
    timestamp: str


@dataclass
class ForwardTestAccountState:
    """Serializable forward-test account snapshot."""

    cash_balance: float
    initial_balance: float
    realized_pnl: float
    buy_count: int = 0
    sell_count: int = 0
    updated_at: str = ""
    open_positions: list[ForwardTestOpenPosition] = field(default_factory=list)
    trades: list[ForwardTestTradeRecord] = field(default_factory=list)


@dataclass(frozen=True)
class ForwardTestFillResult:
    """Outcome of applying one paper fill."""

    cash_balance: float
    realized_pnl: float
    trade_pnl: Optional[float]
    amount: float


class ForwardTestAccountStore:
    """Read and write forward-test account JSON in GCS."""

    def __init__(
        self,
        bucket_name: str,
        *,
        prefix: str = "forward_test",
        client: Optional[storage.Client] = None,
    ) -> None:
        self._bucket_name = bucket_name
        self._blob_path = f"{prefix.rstrip('/')}/account.json"
        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket_name)

    def load(self) -> Optional[dict[str, Any]]:
        """Return stored account JSON, or None when no snapshot exists."""
        blob = self._bucket.blob(self._blob_path)
        try:
            raw = blob.download_as_text(encoding="utf-8")
        except NotFound:
            return None
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("forward-test account state must be a JSON object")
        return payload

    def save(self, state: ForwardTestAccountState) -> str:
        """Persist account state and return the GCS object path."""
        state.updated_at = datetime.now(timezone.utc).isoformat()
        blob = self._bucket.blob(self._blob_path)
        blob.upload_from_string(
            json.dumps(_state_to_dict(state), indent=2),
            content_type="application/json",
        )
        return f"gs://{self._bucket_name}/{self._blob_path}"


class ForwardTestAccount:
    """Paper cash account used during email forward testing."""

    def __init__(
        self,
        *,
        initial_balance: float,
        store: Optional[ForwardTestAccountStore] = None,
        persist_state: bool = True,
        option_commission_per_contract: float = 0.0,
    ) -> None:
        if initial_balance <= 0:
            raise ValueError("initial_balance must be positive")
        if option_commission_per_contract < 0:
            raise ValueError("option_commission_per_contract must be non-negative")
        self._initial_balance = initial_balance
        self._store = store
        self._persist_state = persist_state
        self._option_commission_per_contract = option_commission_per_contract
        self._lock = threading.Lock()
        self._state = ForwardTestAccountState(
            cash_balance=initial_balance,
            initial_balance=initial_balance,
            realized_pnl=0.0,
        )

    @classmethod
    def from_app_config(cls, app: "AppConfig") -> "ForwardTestAccount":
        """Load or create a forward-test account from application config."""
        import os

        settings = app.forward_test
        gcs = app.gcs
        if gcs.credentials_path:
            os.environ.setdefault(
                "GOOGLE_APPLICATION_CREDENTIALS",
                gcs.credentials_path,
            )
        if gcs.project_id:
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcs.project_id)

        store = None
        if settings.persist_state:
            store = ForwardTestAccountStore(
                app.gcs.bucket_name,
                prefix=settings.state_prefix,
            )
        account = cls(
            initial_balance=settings.initial_balance,
            store=store,
            persist_state=settings.persist_state,
            option_commission_per_contract=app.options.commission_per_contract,
        )
        if store is not None:
            account._load_from_store()
        return account

    @property
    def cash_balance(self) -> float:
        with self._lock:
            return self._state.cash_balance

    @property
    def realized_pnl(self) -> float:
        with self._lock:
            return self._state.realized_pnl

    @property
    def initial_balance(self) -> float:
        return self._initial_balance

    @property
    def equity_estimate(self) -> float:
        """Cash plus mark-to-market value of open positions at entry price."""
        with self._lock:
            open_value = sum(
                _position_market_value(position)
                for position in self._state.open_positions
            )
            return self._state.cash_balance + open_value

    def summary_line(self) -> str:
        """One-line account snapshot for logs and emails."""
        with self._lock:
            open_value = sum(
                _position_market_value(position)
                for position in self._state.open_positions
            )
            equity = self._state.cash_balance + open_value
            return (
                f"cash=${self._state.cash_balance:,.2f} | "
                f"realized P&L={self._state.realized_pnl:+,.2f} | "
                f"equity~${equity:,.2f}"
            )

    def restore_positions(self, tracker: "PositionTracker") -> int:
        """Re-open stored paper positions in the position tracker."""
        restored = 0
        with self._lock:
            positions = list(self._state.open_positions)
        for position in positions:
            opened_at = _parse_iso_datetime(position.opened_at)
            tracker.open_position(
                symbol=position.symbol,
                quantity=position.quantity,
                entry_price=position.entry_price,
                opened_at=opened_at,
                asset_type=position.asset_type,
                underlying_symbol=position.underlying_symbol,
                underlying_entry_price=position.underlying_entry_price,
                entry_quote=(
                    OptionQuoteSnapshot.from_dict(position.entry_quote)
                    if position.entry_quote
                    else None
                ),
            )
            restored += 1
        if restored:
            logger.info("Restored %d forward-test paper position(s)", restored)
        return restored

    def record_buy(
        self,
        *,
        symbol: str,
        underlying_symbol: str,
        quantity: float,
        price: float,
        asset_type: str,
        opened_at: datetime,
        underlying_entry_price: Optional[float] = None,
        entry_quote: Optional[OptionQuoteSnapshot] = None,
    ) -> ForwardTestFillResult:
        """Debit cash for a paper buy and track the open position."""
        if quantity <= 0 or price <= 0:
            raise ValueError("buy quantity and price must be positive")

        commission = self._option_commission(quantity, asset_type)
        amount = _trade_notional(quantity, price, asset_type) + commission
        timestamp = _to_utc(opened_at).isoformat()

        with self._lock:
            if amount > self._state.cash_balance:
                raise ValueError(
                    f"insufficient forward-test cash: need {amount:.2f}, "
                    f"have {self._state.cash_balance:.2f}"
                )

            self._state.cash_balance -= amount
            self._state.buy_count += 1
            self._state.open_positions = [
                position
                for position in self._state.open_positions
                if position.underlying_symbol != underlying_symbol.upper()
            ]
            self._state.open_positions.append(
                ForwardTestOpenPosition(
                    symbol=symbol.upper(),
                    underlying_symbol=underlying_symbol.upper(),
                    quantity=quantity,
                    entry_price=price,
                    asset_type=asset_type,
                    opened_at=timestamp,
                    underlying_entry_price=underlying_entry_price,
                    entry_quote=entry_quote.to_dict() if entry_quote is not None else None,
                )
            )
            self._state.trades.append(
                ForwardTestTradeRecord(
                    action="BUY",
                    symbol=symbol.upper(),
                    underlying_symbol=underlying_symbol.upper(),
                    quantity=quantity,
                    price=price,
                    amount=-amount,
                    realized_pnl=None,
                    cash_after=self._state.cash_balance,
                    timestamp=timestamp,
                )
            )
            result = ForwardTestFillResult(
                cash_balance=self._state.cash_balance,
                realized_pnl=self._state.realized_pnl,
                trade_pnl=None,
                amount=amount,
            )
            self._persist_locked()
            return result

    def record_sell(
        self,
        *,
        symbol: str,
        underlying_symbol: str,
        quantity: float,
        exit_price: float,
        asset_type: str,
        closed_at: datetime,
    ) -> ForwardTestFillResult:
        """Credit cash for a paper sell and realize P&L."""
        if quantity <= 0 or exit_price <= 0:
            raise ValueError("sell quantity and price must be positive")

        symbol = symbol.upper()
        underlying_symbol = underlying_symbol.upper()
        timestamp = _to_utc(closed_at).isoformat()

        with self._lock:
            position = _find_open_position(self._state.open_positions, symbol)
            if position is None:
                raise ValueError(f"no forward-test position to sell for {symbol}")

            sell_qty = min(quantity, position.quantity)
            entry_notional = _trade_notional(sell_qty, position.entry_price, asset_type)
            proceeds = _trade_notional(sell_qty, exit_price, asset_type)
            entry_commission = self._option_commission(sell_qty, asset_type)
            trade_pnl = proceeds - entry_notional - entry_commission

            self._state.cash_balance += proceeds
            self._state.realized_pnl += trade_pnl
            self._state.sell_count += 1

            remaining_qty = position.quantity - sell_qty
            if remaining_qty <= 0:
                self._state.open_positions = [
                    open_position
                    for open_position in self._state.open_positions
                    if open_position.symbol != symbol
                ]
            else:
                position.quantity = remaining_qty

            self._state.trades.append(
                ForwardTestTradeRecord(
                    action="SELL",
                    symbol=symbol,
                    underlying_symbol=underlying_symbol,
                    quantity=sell_qty,
                    price=exit_price,
                    amount=proceeds,
                    realized_pnl=trade_pnl,
                    cash_after=self._state.cash_balance,
                    timestamp=timestamp,
                )
            )
            result = ForwardTestFillResult(
                cash_balance=self._state.cash_balance,
                realized_pnl=self._state.realized_pnl,
                trade_pnl=trade_pnl,
                amount=proceeds,
            )
            self._persist_locked()
            return result

    def _option_commission(self, quantity: float, asset_type: str) -> float:
        """Return the per-contract commission for an options trade."""
        if asset_type.upper() != "OPTION":
            return 0.0
        return abs(quantity) * self._option_commission_per_contract

    def save(self) -> Optional[str]:
        """Persist the current account snapshot."""
        with self._lock:
            return self._persist_locked()

    def _load_from_store(self) -> None:
        if self._store is None:
            return
        try:
            payload = self._store.load()
        except Exception:
            logger.exception("Failed to load forward-test account from GCS")
            return
        if payload is None:
            logger.info(
                "No forward-test account snapshot found; starting at $%.2f",
                self._initial_balance,
            )
            return

        loaded = _state_from_dict(payload)
        with self._lock:
            self._state = loaded
        logger.info(
            "Loaded forward-test account from GCS: cash=$%.2f, realized P&L=%+.2f, "
            "%d open position(s), %d trade(s)",
            loaded.cash_balance,
            loaded.realized_pnl,
            len(loaded.open_positions),
            len(loaded.trades),
        )

    def _persist_locked(self) -> Optional[str]:
        if not self._persist_state or self._store is None:
            return None
        try:
            uri = self._store.save(self._state)
            logger.info("Saved forward-test account to %s", uri)
            return uri
        except Exception:
            logger.exception("Failed to save forward-test account to GCS")
            return None


def _trade_notional(quantity: float, price: float, asset_type: str) -> float:
    multiplier = 100.0 if asset_type.upper() == "OPTION" else 1.0
    return quantity * price * multiplier


def _position_market_value(position: ForwardTestOpenPosition) -> float:
    return _trade_notional(position.quantity, position.entry_price, position.asset_type)


def _find_open_position(
    positions: list[ForwardTestOpenPosition],
    symbol: str,
) -> Optional[ForwardTestOpenPosition]:
    symbol = symbol.upper()
    for position in positions:
        if position.symbol == symbol:
            return position
    return None


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_datetime(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _to_utc(timestamp)


def _state_to_dict(state: ForwardTestAccountState) -> dict[str, Any]:
    return {
        "cash_balance": state.cash_balance,
        "initial_balance": state.initial_balance,
        "realized_pnl": state.realized_pnl,
        "buy_count": state.buy_count,
        "sell_count": state.sell_count,
        "updated_at": state.updated_at,
        "open_positions": [asdict(position) for position in state.open_positions],
        "trades": [asdict(trade) for trade in state.trades],
    }


def _state_from_dict(payload: dict[str, Any]) -> ForwardTestAccountState:
    open_positions = [
        ForwardTestOpenPosition(**item)
        for item in payload.get("open_positions", [])
        if isinstance(item, dict)
    ]
    trades = [
        ForwardTestTradeRecord(**item)
        for item in payload.get("trades", [])
        if isinstance(item, dict)
    ]
    return ForwardTestAccountState(
        cash_balance=float(payload["cash_balance"]),
        initial_balance=float(payload.get("initial_balance", payload["cash_balance"])),
        realized_pnl=float(payload.get("realized_pnl", 0.0)),
        buy_count=int(payload.get("buy_count", 0)),
        sell_count=int(payload.get("sell_count", 0)),
        updated_at=str(payload.get("updated_at", "")),
        open_positions=open_positions,
        trades=trades,
    )
