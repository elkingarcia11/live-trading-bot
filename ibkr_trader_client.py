"""IBKR trader REST client.

Responsibility: IBKR Web API endpoints used by live workflows.

Fetches accounts, positions, contract metadata, and order state. Does not
evaluate strategy signals or maintain portfolio state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from ibkr_auth import IbkrAuthError, IbkrSessionClient
from schwab_auth import _load_dotenv

logger = logging.getLogger(__name__)


class IbkrTraderError(Exception):
    """Raised when IBKR trader API operations fail."""


@dataclass(frozen=True)
class IbkrAccountRef:
    """Trading account identifier returned by IBKR."""

    account_id: str
    alias: str = ""
    is_paper: bool = False


@dataclass(frozen=True)
class IbkrAccountPosition:
    """Normalized position from IBKR portfolio payloads."""

    symbol: str
    quantity: float
    average_price: float
    market_value: float
    current_day_profit_loss: float
    asset_type: str = "EQUITY"
    underlying_symbol: Optional[str] = None
    conid: Optional[int] = None


@dataclass(frozen=True)
class IbkrAccountBalances:
    """Selected balance fields from an IBKR account."""

    equity: float
    buying_power: float
    cash_available_for_trading: float
    liquidation_value: float


@dataclass(frozen=True)
class IbkrAccountSnapshot:
    """Account balances and open positions."""

    account_number: str
    balances: IbkrAccountBalances
    positions: tuple[IbkrAccountPosition, ...]


@dataclass(frozen=True)
class IbkrContractRef:
    """Resolved IBKR contract identifier for a symbol."""

    conid: int
    symbol: str
    sec_type: str
    exchange: str
    company_name: str = ""


class IbkrTraderClient:
    """Minimal IBKR trader REST client."""

    def __init__(
        self,
        session_client: IbkrSessionClient,
        *,
        secdef_search_path: str = "iserver/secdef/search",
        portfolio_accounts_path: str = "portfolio/accounts",
        iserver_accounts_path: str = "iserver/accounts",
        positions_path_template: str = "portfolio/{account_id}/positions/{page_id}",
        orders_path_template: str = "iserver/account/{account_id}/orders",
        order_path_template: str = "iserver/account/{account_id}/order/{order_id}",
        live_orders_path: str = "iserver/account/orders",
        reply_path_template: str = "iserver/reply/{reply_id}",
        listing_exchange: str = "SMART",
        manual_indicator: bool = False,
        ext_operator: str = "live-trading-bot",
    ) -> None:
        self._session = session_client
        self._secdef_search_path = secdef_search_path.lstrip("/")
        self._portfolio_accounts_path = portfolio_accounts_path.lstrip("/")
        self._iserver_accounts_path = iserver_accounts_path.lstrip("/")
        self._positions_path_template = positions_path_template
        self._orders_path_template = orders_path_template
        self._order_path_template = order_path_template
        self._live_orders_path = live_orders_path.lstrip("/")
        self._reply_path_template = reply_path_template
        self._listing_exchange = listing_exchange
        self._manual_indicator = manual_indicator
        self._ext_operator = ext_operator
        self._resolved_account_id: Optional[str] = None
        self._conid_cache: dict[str, IbkrContractRef] = {}

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> IbkrTraderClient:
        """Build a trader client from config.json."""
        if load_dotenv:
            _load_dotenv()

        from config import get_config

        app = get_config(reload=True)
        ibkr = app.ibkr
        return cls(
            IbkrSessionClient.from_env(load_dotenv=False),
            secdef_search_path=ibkr.secdef_search_path,
            portfolio_accounts_path=ibkr.portfolio_accounts_path,
            iserver_accounts_path=ibkr.iserver_accounts_path,
            positions_path_template=ibkr.positions_path_template,
            orders_path_template=ibkr.orders_path_template,
            order_path_template=ibkr.order_path_template,
            live_orders_path=ibkr.live_orders_path,
            reply_path_template=ibkr.reply_path_template,
            listing_exchange=ibkr.listing_exchange,
            manual_indicator=ibkr.manual_indicator,
            ext_operator=ibkr.ext_operator,
        )

    def ensure_session(self) -> None:
        """Ensure the gateway session is authenticated for trading."""
        self._session.ensure_session()

    def get_portfolio_accounts(self) -> list[str]:
        """Return account IDs available through the read-only portfolio API."""
        payload = self._request_json(self._portfolio_accounts_path)
        if not isinstance(payload, list):
            raise IbkrTraderError("Portfolio accounts response has an unexpected shape")
        return [str(account_id) for account_id in payload if account_id]

    def get_trading_accounts(self) -> list[IbkrAccountRef]:
        """Return account IDs and aliases from the trading session."""
        payload = self._request_json(self._iserver_accounts_path)
        if not isinstance(payload, dict):
            raise IbkrTraderError("iServer accounts response has an unexpected shape")

        accounts = payload.get("accounts", [])
        aliases = payload.get("aliases", {})
        if not isinstance(accounts, list):
            raise IbkrTraderError("iServer accounts list has an unexpected shape")
        if not isinstance(aliases, dict):
            aliases = {}

        refs: list[IbkrAccountRef] = []
        for account_id in accounts:
            account_str = str(account_id)
            alias = str(aliases.get(account_id, aliases.get(account_str, "")) or "")
            refs.append(
                IbkrAccountRef(
                    account_id=account_str,
                    alias=alias,
                    is_paper=account_str.upper().startswith("DU"),
                )
            )
        return refs

    def resolve_account_id(
        self,
        *,
        account_id: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> str:
        """Resolve the account id used by trading endpoints."""
        candidate = (account_id or account_number or self._resolved_account_id or "").strip()
        if candidate:
            self._resolved_account_id = candidate
            return candidate

        trading_accounts = self.get_trading_accounts()
        if trading_accounts:
            self._resolved_account_id = trading_accounts[0].account_id
            return self._resolved_account_id

        portfolio_accounts = self.get_portfolio_accounts()
        if portfolio_accounts:
            self._resolved_account_id = portfolio_accounts[0]
            return self._resolved_account_id

        raise IbkrTraderError("No IBKR accounts are available for the current session")

    def search_contract(
        self,
        symbol: str,
        *,
        sec_type: str = "STK",
        force_refresh: bool = False,
    ) -> IbkrContractRef:
        """Resolve a symbol to an IBKR contract id."""
        cache_key = f"{symbol.upper()}:{sec_type.upper()}"
        if not force_refresh and cache_key in self._conid_cache:
            return self._conid_cache[cache_key]

        payload = self._request_json(
            self._secdef_search_path,
            params={"symbol": symbol.upper(), "secType": sec_type.upper()},
        )
        if not isinstance(payload, list) or not payload:
            raise IbkrTraderError(f"No IBKR contract found for {symbol} ({sec_type})")

        match = _select_contract(payload, symbol=symbol.upper(), sec_type=sec_type.upper())
        if match is None:
            raise IbkrTraderError(f"No IBKR contract match for {symbol} ({sec_type})")

        contract = IbkrContractRef(
            conid=int(match["conid"]),
            symbol=str(match.get("symbol", symbol)).upper(),
            sec_type=str(match.get("secType", sec_type)).upper(),
            exchange=str(match.get("exchange", self._listing_exchange) or self._listing_exchange),
            company_name=str(match.get("companyName", "") or ""),
        )
        self._conid_cache[cache_key] = contract
        return contract

    def get_account_snapshot(
        self,
        *,
        account_id: Optional[str] = None,
        account_number: Optional[str] = None,
        include_positions: bool = True,
    ) -> IbkrAccountSnapshot:
        """Fetch balances and positions for one account."""
        resolved_account = self.resolve_account_id(
            account_id=account_id,
            account_number=account_number,
        )
        positions: list[IbkrAccountPosition] = []
        if include_positions:
            positions = list(self.get_positions(resolved_account))

        balances = IbkrAccountBalances(
            equity=sum(position.market_value for position in positions),
            buying_power=0.0,
            cash_available_for_trading=0.0,
            liquidation_value=sum(position.market_value for position in positions),
        )
        return IbkrAccountSnapshot(
            account_number=resolved_account,
            balances=balances,
            positions=tuple(positions),
        )

    def get_positions(self, account_id: str) -> list[IbkrAccountPosition]:
        """Fetch all positions for an account, paging when necessary."""
        positions: list[IbkrAccountPosition] = []
        page_id = 0
        while True:
            path = self._positions_path_template.format(
                account_id=account_id,
                page_id=page_id,
            )
            payload = self._request_json(path)
            if not isinstance(payload, list) or not payload:
                break

            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                position = _position_from_payload(entry)
                if position is not None:
                    positions.append(position)

            if len(payload) < 100:
                break
            page_id += 1
        return positions

    def place_order(self, account_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Submit an order and resolve any IBKR confirmation replies."""
        path = self._orders_path_template.format(account_id=account_id)
        response = self._request_json(path, method="POST", json_body=payload)
        return self._resolve_order_response(response)

    def cancel_order(self, account_id: str, order_id: str) -> dict[str, Any]:
        """Cancel a working order."""
        path = self._order_path_template.format(account_id=account_id, order_id=order_id)
        params = {
            "manualIndicator": str(self._manual_indicator).lower(),
            "extOperator": self._ext_operator,
        }
        payload = self._request_json(path, method="DELETE", params=params)
        if isinstance(payload, dict) and payload.get("error"):
            raise IbkrTraderError(str(payload["error"]))
        return payload if isinstance(payload, dict) else {}

    def get_live_orders(self) -> list[dict[str, Any]]:
        """Return live orders for the active trading account."""
        payload = self._request_json(self._live_orders_path, params={"force": "true"})
        if isinstance(payload, dict):
            orders = payload.get("orders", [])
            if isinstance(orders, list):
                return [entry for entry in orders if isinstance(entry, dict)]
        if isinstance(payload, list):
            return [entry for entry in payload if isinstance(entry, dict)]
        return []

    def get_order(self, order_id: str) -> Optional[dict[str, Any]]:
        """Find an order in the current live-order snapshot."""
        for order in self.get_live_orders():
            if str(order.get("orderId", "")) == str(order_id):
                return order
        return None

    def _resolve_order_response(self, response: Any) -> dict[str, Any]:
        current = response
        while isinstance(current, list) and current:
            first = current[0]
            if not isinstance(first, dict):
                break
            if "order_id" in first or "orderId" in first:
                return first
            reply_id = first.get("id")
            if reply_id:
                current = self.confirm_reply(str(reply_id))
                continue
            if "error" in first:
                raise IbkrTraderError(str(first["error"]))
            break

        if isinstance(current, dict):
            if current.get("error"):
                raise IbkrTraderError(str(current["error"]))
            return current

        raise IbkrTraderError("Unexpected IBKR order response shape")

    def confirm_reply(self, reply_id: str) -> Any:
        """Confirm an IBKR order precaution reply."""
        path = self._reply_path_template.format(reply_id=reply_id)
        return self._request_json(
            path,
            method="POST",
            json_body={"confirmed": True},
        )

    def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        try:
            return self._session.request(
                method,
                path,
                params=params,
                json_body=json_body,
            )
        except IbkrAuthError as exc:
            raise IbkrTraderError(str(exc)) from exc


def _select_contract(
    entries: list[dict[str, Any]],
    *,
    symbol: str,
    sec_type: str,
) -> Optional[dict[str, Any]]:
    for entry in entries:
        sections = entry.get("sections")
        if not isinstance(sections, list):
            continue
        for section in sections:
            if not isinstance(section, dict):
                continue
            if str(section.get("secType", "")).upper() != sec_type:
                continue
            contracts = section.get("contracts")
            if not isinstance(contracts, list):
                continue
            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                if str(contract.get("symbol", "")).upper() == symbol:
                    return {
                        "conid": contract.get("conid"),
                        "symbol": contract.get("symbol", symbol),
                        "secType": sec_type,
                        "exchange": contract.get("exchange")
                        or section.get("exchange")
                        or entry.get("exchange"),
                        "companyName": contract.get("companyName") or entry.get("companyName"),
                    }

    first = entries[0]
    sections = first.get("sections")
    if isinstance(sections, list):
        for section in sections:
            contracts = section.get("contracts") if isinstance(section, dict) else None
            if isinstance(contracts, list) and contracts:
                contract = contracts[0]
                if isinstance(contract, dict) and contract.get("conid") is not None:
                    return {
                        "conid": contract.get("conid"),
                        "symbol": contract.get("symbol", symbol),
                        "secType": str(section.get("secType", sec_type)).upper(),
                        "exchange": contract.get("exchange")
                        or section.get("exchange")
                        or first.get("exchange"),
                        "companyName": contract.get("companyName") or first.get("companyName"),
                    }

    if first.get("conid") is not None:
        return {
            "conid": first.get("conid"),
            "symbol": first.get("symbol", symbol),
            "secType": sec_type,
            "exchange": first.get("exchange"),
            "companyName": first.get("companyName"),
        }
    return None


def _position_from_payload(payload: dict[str, Any]) -> Optional[IbkrAccountPosition]:
    quantity = float(payload.get("position", 0.0) or 0.0)
    if quantity == 0:
        return None

    asset_class = str(payload.get("assetClass", "STK") or "STK").upper()
    asset_type = "OPTION" if asset_class == "OPT" else "EQUITY"
    symbol = str(payload.get("contractDesc") or payload.get("ticker") or "").upper()
    if not symbol:
        return None

    average_price = float(payload.get("avgPrice") or payload.get("avgCost") or 0.0)
    market_value = float(payload.get("mktValue", 0.0) or 0.0)
    realized_pnl = float(payload.get("realizedPnl", 0.0) or 0.0)
    underlying_symbol = None
    if asset_type == "OPTION":
        underlying_symbol = str(payload.get("ticker") or "").upper() or None

    conid_raw = payload.get("conid")
    conid = int(conid_raw) if conid_raw is not None else None

    return IbkrAccountPosition(
        symbol=symbol,
        quantity=quantity,
        average_price=average_price,
        market_value=market_value,
        current_day_profit_loss=realized_pnl,
        asset_type=asset_type,
        underlying_symbol=underlying_symbol,
        conid=conid,
    )
