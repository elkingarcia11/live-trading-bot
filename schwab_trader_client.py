"""Schwab Trader API client.

Responsibility: Schwab trader REST endpoints used by live workflows.

Fetches user preference, account snapshots, and order queries. Does not manage
WebSocket sessions, normalize OHLCV data, or submit orders.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import requests

from schwab_auth import SchwabAuthClient, SchwabAuthError, _load_dotenv

logger = logging.getLogger(__name__)

DEFAULT_TRADER_BASE_URL = "https://api.schwabapi.com/trader/v1"
DEFAULT_USER_PREFERENCE_PATH = "userPreference"
DEFAULT_ACCOUNT_NUMBERS_PATH = "accounts/accountNumbers"
DEFAULT_ACCOUNTS_PATH = "accounts"
DEFAULT_ORDERS_PATH = "accounts/{account_hash}/orders"
DEFAULT_PREVIEW_ORDER_PATH = "accounts/{account_hash}/previewOrder"


class SchwabTraderError(Exception):
    """Raised when Schwab trader API operations fail."""


def format_schwab_entered_time(value: datetime) -> str:
    """Format a datetime for Schwab order entered-time query parameters."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    millis = value.microsecond // 1000
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


@dataclass(frozen=True)
class SchwabAccountRef:
    """Linked brokerage account number and encrypted hash value."""

    account_number: str
    hash_value: str


@dataclass(frozen=True)
class SchwabAccountPosition:
    """Normalized equity position from Schwab account payloads."""

    symbol: str
    quantity: float
    average_price: float
    market_value: float
    current_day_profit_loss: float


@dataclass(frozen=True)
class SchwabAccountBalances:
    """Selected balance fields from a Schwab securities account."""

    equity: float
    buying_power: float
    cash_available_for_trading: float
    liquidation_value: float


@dataclass(frozen=True)
class SchwabAccountSnapshot:
    """Account balances and open equity positions."""

    account_number: str
    balances: SchwabAccountBalances
    positions: tuple[SchwabAccountPosition, ...]


@dataclass(frozen=True)
class SchwabOrderValidationMessage:
    """One Schwab preview-order validation message."""

    validation_rule_name: str
    message: str
    activity_message: str


@dataclass(frozen=True)
class SchwabOrderPreviewResult:
    """Normalized response from POST /accounts/{hash}/previewOrder."""

    rejects: tuple[SchwabOrderValidationMessage, ...]
    warns: tuple[SchwabOrderValidationMessage, ...]
    accepts: tuple[SchwabOrderValidationMessage, ...]
    alerts: tuple[SchwabOrderValidationMessage, ...]
    reviews: tuple[SchwabOrderValidationMessage, ...]
    projected_commission: Optional[float]
    projected_buying_power: Optional[float]
    projected_order_value: Optional[float]

    @property
    def is_valid(self) -> bool:
        return not self.rejects

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> SchwabOrderPreviewResult:
        validation = payload.get("orderValidationResult", {})
        if not isinstance(validation, dict):
            validation = {}

        order_strategy = payload.get("orderStrategy", {})
        order_balance = (
            order_strategy.get("orderBalance", {})
            if isinstance(order_strategy, dict)
            else {}
        )
        if not isinstance(order_balance, dict):
            order_balance = {}

        return cls(
            rejects=_parse_validation_messages(validation.get("rejects")),
            warns=_parse_validation_messages(validation.get("warns")),
            accepts=_parse_validation_messages(validation.get("accepts")),
            alerts=_parse_validation_messages(validation.get("alerts")),
            reviews=_parse_validation_messages(validation.get("reviews")),
            projected_commission=_optional_float(order_balance.get("projectedCommission")),
            projected_buying_power=_optional_float(order_balance.get("projectedBuyingPower")),
            projected_order_value=_optional_float(order_balance.get("orderValue")),
        )


@dataclass(frozen=True)
class SchwabStreamerInfo:
    """Streamer connection details from GET /userPreference."""

    streamer_socket_url: str
    schwab_client_customer_id: str
    schwab_client_correl_id: str
    schwab_client_channel: str
    schwab_client_function_id: str


class SchwabTraderClient:
    """Minimal Schwab trader REST client."""

    def __init__(
        self,
        auth_client: SchwabAuthClient,
        *,
        base_url: str = DEFAULT_TRADER_BASE_URL,
        user_preference_path: str = DEFAULT_USER_PREFERENCE_PATH,
        account_numbers_path: str = DEFAULT_ACCOUNT_NUMBERS_PATH,
        accounts_path: str = DEFAULT_ACCOUNTS_PATH,
        orders_path_template: str = DEFAULT_ORDERS_PATH,
        preview_order_path_template: str = DEFAULT_PREVIEW_ORDER_PATH,
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._auth_client = auth_client
        self._base_url = base_url.rstrip("/") + "/"
        self._user_preference_path = user_preference_path.lstrip("/")
        self._account_numbers_path = account_numbers_path.lstrip("/")
        self._accounts_path = accounts_path.lstrip("/")
        self._orders_path_template = orders_path_template
        self._preview_order_path_template = preview_order_path_template
        self._timeout = timeout
        self._session = session or requests.Session()
        self._resolved_account_hash: Optional[str] = None

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> SchwabTraderClient:
        """Build a trader client from environment variables."""
        if load_dotenv:
            _load_dotenv()

        auth_client = SchwabAuthClient.from_env(load_dotenv=False)
        base_url = os.getenv("SCHWAB_TRADER_BASE_URL", DEFAULT_TRADER_BASE_URL)
        preference_path = os.getenv("SCHWAB_USER_PREFERENCE_PATH", DEFAULT_USER_PREFERENCE_PATH)
        if preference_path.startswith("http"):
            base_url = preference_path.rsplit("/", 1)[0]
            preference_path = preference_path.rstrip("/").split("/")[-1]

        accounts_path = os.getenv("SCHWAB_ACCOUNTS_PATH", DEFAULT_ACCOUNTS_PATH)
        if accounts_path.startswith("http"):
            base_url = accounts_path.rsplit("/", 1)[0]
            accounts_path = accounts_path.rstrip("/").split("/")[-1]

        return cls(
            auth_client,
            base_url=base_url,
            user_preference_path=preference_path,
            account_numbers_path=_suffix_path(
                os.getenv("SCHWAB_ACCOUNT_NUMBERS_PATH"),
                DEFAULT_ACCOUNT_NUMBERS_PATH,
            ),
            accounts_path=accounts_path,
            orders_path_template=os.getenv("SCHWAB_ORDERS_PATH", DEFAULT_ORDERS_PATH),
            preview_order_path_template=os.getenv(
                "SCHWAB_PREVIEW_ORDER_PATH",
                DEFAULT_PREVIEW_ORDER_PATH,
            ),
            timeout=float(os.getenv("MARKET_DATA_REQUEST_TIMEOUT_SECONDS", "30")),
        )

    def get_user_preference(self) -> list[dict[str, Any]]:
        """Fetch raw user preference payloads for the authenticated user."""
        return self._request_json(self._user_preference_path)

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid OAuth access token for trader and streamer calls."""
        return self._auth_client.get_access_token(force_refresh=force_refresh)

    def get_account_numbers(self) -> list[SchwabAccountRef]:
        """Fetch linked account numbers and encrypted hash values."""
        payload = self._request_json(self._account_numbers_path)
        if not isinstance(payload, list):
            raise SchwabTraderError("Account numbers response has an unexpected shape")

        accounts: list[SchwabAccountRef] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            account_number = entry.get("accountNumber")
            hash_value = entry.get("hashValue")
            if account_number and hash_value:
                accounts.append(
                    SchwabAccountRef(
                        account_number=str(account_number),
                        hash_value=str(hash_value),
                    )
                )
        return accounts

    def resolve_account_hash(
        self,
        *,
        account_number: Optional[str] = None,
        account_hash: Optional[str] = None,
    ) -> str:
        """Resolve the encrypted account hash used by trading endpoints."""
        if account_hash:
            self._resolved_account_hash = account_hash
            return account_hash

        configured_hash = os.getenv("SCHWAB_ACCOUNT_HASH", "").strip()
        if configured_hash:
            self._resolved_account_hash = configured_hash
            return configured_hash

        if self._resolved_account_hash:
            return self._resolved_account_hash

        accounts = self.get_account_numbers()
        if not accounts:
            raise SchwabTraderError("No linked Schwab accounts were returned")

        target_number = (
            account_number
            or os.getenv("SCHWAB_ACCOUNT_NUMBER", "").strip()
            or None
        )
        if target_number:
            for account in accounts:
                if account.account_number == target_number:
                    self._resolved_account_hash = account.hash_value
                    return account.hash_value
            raise SchwabTraderError(
                f"No account hash found for account number {target_number}"
            )

        self._resolved_account_hash = accounts[0].hash_value
        return accounts[0].hash_value

    def get_orders(
        self,
        *,
        from_entered_time: str,
        to_entered_time: str,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
        max_results: Optional[int] = None,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch orders for one account (GET /accounts/{hash}/orders).

        Schwab requires both ``from_entered_time`` and ``to_entered_time`` in
        ISO-8601 UTC format, e.g. ``2024-03-29T00:00:00.000Z``. The maximum
        date range is one year.
        """
        if not from_entered_time or not to_entered_time:
            raise SchwabTraderError(
                "from_entered_time and to_entered_time are required for order queries"
            )

        resolved_hash = self.resolve_account_hash(
            account_hash=account_hash,
            account_number=account_number,
        )
        params: dict[str, Any] = {
            "fromEnteredTime": from_entered_time,
            "toEnteredTime": to_entered_time,
        }
        if max_results is not None:
            params["maxResults"] = max_results
        if status is not None:
            params["status"] = status

        path = self._orders_path(resolved_hash)
        payload = self._request_json(path, params=params)
        if not isinstance(payload, list):
            raise SchwabTraderError("Orders response has an unexpected shape")
        return [entry for entry in payload if isinstance(entry, dict)]

    def get_order(
        self,
        order_id: str,
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch one order by Schwab order id (GET /accounts/{hash}/orders/{orderId})."""
        resolved_hash = self.resolve_account_hash(
            account_hash=account_hash,
            account_number=account_number,
        )
        path = f"{self._orders_path(resolved_hash)}/{order_id}"
        payload = self._request_json(path)
        if not isinstance(payload, dict):
            raise SchwabTraderError("Order response has an unexpected shape")
        return payload

    def preview_order(
        self,
        order_payload: dict[str, Any],
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> SchwabOrderPreviewResult:
        """Preview one order without placing it (POST /accounts/{hash}/previewOrder)."""
        resolved_hash = self.resolve_account_hash(
            account_hash=account_hash,
            account_number=account_number,
        )
        path = self._preview_order_path(resolved_hash)
        response = self._request(
            "POST",
            path,
            json_body=order_payload,
            expect_json=True,
        )
        payload = response.json() if response.content else {}
        if not isinstance(payload, dict):
            raise SchwabTraderError("Preview order response has an unexpected shape")
        return SchwabOrderPreviewResult.from_payload(payload)

    def cancel_order(
        self,
        order_id: str,
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
    ) -> None:
        """Cancel one order (DELETE /accounts/{hash}/orders/{orderId}).

        Schwab returns HTTP 200 with an empty body when cancellation succeeds.
        """
        resolved_hash = self.resolve_account_hash(
            account_hash=account_hash,
            account_number=account_number,
        )
        path = f"{self._orders_path(resolved_hash)}/{order_id}"
        self._request("DELETE", path, expect_json=False)

    def get_accounts(self, *, fields: Optional[str] = None) -> list[dict[str, Any]]:
        """Fetch all linked accounts (GET /accounts).

        Balances are returned by default. Pass ``fields=\"positions\"`` to include
        open positions for every linked account.
        """
        params = {"fields": fields} if fields else None
        payload = self._request_json(self._accounts_path, params=params)
        if not isinstance(payload, list):
            raise SchwabTraderError("Accounts response has an unexpected shape")
        return [entry for entry in payload if isinstance(entry, dict)]

    def get_all_account_snapshots(
        self,
        *,
        include_positions: bool = True,
    ) -> tuple[SchwabAccountSnapshot, ...]:
        """Fetch normalized balances and positions for every linked account."""
        fields = "positions" if include_positions else None
        return tuple(
            self._parse_account_snapshot(entry)
            for entry in self.get_accounts(fields=fields)
        )

    def get_account(
        self,
        account_hash: str,
        *,
        fields: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch one account payload (GET /accounts/{accountNumber}).

        ``account_hash`` is the encrypted account id from ``accountNumbers``,
        not the plain account number. Balances are returned by default; pass
        ``fields=\"positions\"`` to include open positions.
        """
        path = f"{self._accounts_path}/{account_hash}"
        params = {"fields": fields} if fields else None
        payload = self._request_json(path, params=params)
        if not isinstance(payload, dict):
            raise SchwabTraderError("Account response has an unexpected shape")
        return payload

    def get_account_snapshot(
        self,
        *,
        account_hash: Optional[str] = None,
        account_number: Optional[str] = None,
        include_positions: bool = True,
    ) -> SchwabAccountSnapshot:
        """Fetch balances and equity positions for one account."""
        resolved_hash = self.resolve_account_hash(
            account_number=account_number,
            account_hash=account_hash,
        )
        fields = "positions" if include_positions else None
        payload = self.get_account(resolved_hash, fields=fields)
        return self._parse_account_snapshot(
            payload,
            fallback_account_number=account_number or resolved_hash,
        )

    def get_streamer_info(self) -> SchwabStreamerInfo:
        """Extract streamer connection details from user preference."""
        payload = self.get_user_preference()
        if not isinstance(payload, list) or not payload:
            raise SchwabTraderError("User preference response was empty")

        preferences = payload[0]
        if not isinstance(preferences, dict):
            raise SchwabTraderError("User preference payload has an unexpected shape")

        streamer_entries = preferences.get("streamerInfo", [])
        if not streamer_entries:
            raise SchwabTraderError("User preference did not include streamerInfo")

        entry = streamer_entries[0]
        if not isinstance(entry, dict):
            raise SchwabTraderError("streamerInfo entry has an unexpected shape")

        try:
            return SchwabStreamerInfo(
                streamer_socket_url=str(entry["streamerSocketUrl"]),
                schwab_client_customer_id=str(entry["schwabClientCustomerId"]),
                schwab_client_correl_id=str(entry["schwabClientCorrelId"]),
                schwab_client_channel=str(entry["SchwabClientChannel"]),
                schwab_client_function_id=str(entry["SchwabClientFunctionId"]),
            )
        except KeyError as exc:
            raise SchwabTraderError(
                f"streamerInfo entry missing required field: {exc}"
            ) from exc

    def _orders_path(self, account_hash: str) -> str:
        return self._orders_path_template.format(account_hash=account_hash).lstrip("/")

    def _preview_order_path(self, account_hash: str) -> str:
        return self._preview_order_path_template.format(
            account_hash=account_hash
        ).lstrip("/")

    def _parse_account_snapshot(
        self,
        payload: dict[str, Any],
        *,
        fallback_account_number: Optional[str] = None,
    ) -> SchwabAccountSnapshot:
        securities_account = payload.get("securitiesAccount", {})
        if not isinstance(securities_account, dict):
            raise SchwabTraderError("Account payload missing securitiesAccount")

        balances = self._parse_balances(securities_account)
        positions = self._parse_positions(securities_account.get("positions", []))
        account_number_value = str(
            securities_account.get("accountNumber", fallback_account_number or "")
        )
        return SchwabAccountSnapshot(
            account_number=account_number_value,
            balances=balances,
            positions=positions,
        )

    def _parse_balances(self, securities_account: dict[str, Any]) -> SchwabAccountBalances:
        current = securities_account.get("currentBalances", {})
        initial = securities_account.get("initialBalances", {})
        source = current if isinstance(current, dict) and current else initial
        if not isinstance(source, dict):
            source = {}

        return SchwabAccountBalances(
            equity=float(source.get("equity", 0.0) or 0.0),
            buying_power=float(source.get("buyingPower", 0.0) or 0.0),
            cash_available_for_trading=float(
                source.get("cashAvailableForTrading", source.get("availableFunds", 0.0)) or 0.0
            ),
            liquidation_value=float(source.get("liquidationValue", 0.0) or 0.0),
        )

    def _parse_positions(self, raw_positions: object) -> tuple[SchwabAccountPosition, ...]:
        if not isinstance(raw_positions, list):
            return ()

        parsed: list[SchwabAccountPosition] = []
        for entry in raw_positions:
            position = self._parse_position(entry)
            if position is not None:
                parsed.append(position)
        return tuple(parsed)

    def _parse_position(self, entry: object) -> Optional[SchwabAccountPosition]:
        if not isinstance(entry, dict):
            return None

        instrument = entry.get("instrument", {})
        if not isinstance(instrument, dict):
            return None

        symbol = str(instrument.get("symbol", "")).upper().strip()
        if not symbol:
            return None

        instrument_type = str(instrument.get("assetType", instrument.get("type", "EQUITY"))).upper()
        if instrument_type not in {"EQUITY", "COLLECTIVE_INVESTMENT", "ETF"}:
            return None

        long_quantity = float(entry.get("longQuantity", 0.0) or 0.0)
        short_quantity = float(entry.get("shortQuantity", 0.0) or 0.0)
        quantity = long_quantity - short_quantity
        if quantity == 0:
            return None

        average_price = float(
            entry.get("averagePrice")
            or entry.get("averageLongPrice")
            or entry.get("taxLotAverageLongPrice")
            or 0.0
        )
        if average_price <= 0:
            return None

        return SchwabAccountPosition(
            symbol=symbol,
            quantity=quantity,
            average_price=average_price,
            market_value=float(entry.get("marketValue", 0.0) or 0.0),
            current_day_profit_loss=float(entry.get("currentDayProfitLoss", 0.0) or 0.0),
        )

    def _request_json(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute a GET request and return parsed JSON."""
        response = self._request(
            "GET",
            path,
            params=params,
            expect_json=True,
        )
        if not response.content:
            return {}
        return response.json()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        expect_json: bool = True,
    ) -> requests.Response:
        """Execute an authenticated Schwab trader API request."""
        url = urljoin(self._base_url, path.lstrip("/"))
        refreshed = False

        for _ in range(2):
            headers = {
                "Authorization": f"Bearer {self._auth_client.get_access_token(force_refresh=refreshed)}",
                "accept": "application/json",
            }
            if json_body is not None:
                headers["Content-Type"] = "application/json"
            refreshed = False

            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                raise SchwabTraderError(f"Request to {url} failed: {exc}") from exc

            if response.status_code == 401 and not refreshed:
                try:
                    self._auth_client.get_access_token(force_refresh=True)
                except SchwabAuthError as exc:
                    raise SchwabTraderError(f"Schwab auth refresh failed: {exc}") from exc
                refreshed = True
                continue

            if not response.ok:
                raise SchwabTraderError(
                    f"Request to {url} failed with {response.status_code}: {response.text}"
                )

            if expect_json and response.content:
                try:
                    response.json()
                except ValueError as exc:
                    raise SchwabTraderError(
                        f"Request to {url} returned non-JSON content"
                    ) from exc

            return response

        raise SchwabTraderError(f"Request to {url} failed after auth refresh")


def _parse_validation_messages(
    raw_messages: object,
) -> tuple[SchwabOrderValidationMessage, ...]:
    if not isinstance(raw_messages, list):
        return ()

    parsed: list[SchwabOrderValidationMessage] = []
    for entry in raw_messages:
        if not isinstance(entry, dict):
            continue
        parsed.append(
            SchwabOrderValidationMessage(
                validation_rule_name=str(entry.get("validationRuleName", "") or ""),
                message=str(entry.get("message", "") or ""),
                activity_message=str(entry.get("activityMessage", "") or ""),
            )
        )
    return tuple(parsed)


def _optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _suffix_path(value: Optional[str], default: str) -> str:
    if not value:
        return default
    return value.strip("/").split("/")[-1]


if __name__ == "__main__":
    client = SchwabTraderClient.from_env()
    account_refs = client.get_account_numbers()
    print(f"Linked account refs: {len(account_refs)}")
    snapshots = client.get_all_account_snapshots()
    for snapshot in snapshots:
        print(
            f"Account {snapshot.account_number} equity={snapshot.balances.equity} "
            f"positions={[position.symbol for position in snapshot.positions]}"
        )
