"""Schwab options chain client.

Responsibility: HTTP transport for Schwab GET /chains — strikes, OI, IV per
expiration. Does not compute greeks, GEX, or normalize vendor payloads.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from schwab_market_data_client import SchwabMarketDataClient

if TYPE_CHECKING:
    from config import AppConfig


class SchwabOptionsChainClient:
    """Thin wrapper around SchwabMarketDataClient for option chain fetches."""

    def __init__(self, market_data_client: SchwabMarketDataClient) -> None:
        self._client = market_data_client

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> SchwabOptionsChainClient:
        """Build a chain client from environment and config.json."""
        return cls(SchwabMarketDataClient.from_env(load_dotenv=load_dotenv))

    @classmethod
    def from_config(cls, app: AppConfig) -> SchwabOptionsChainClient:
        """Build a chain client from a loaded AppConfig."""
        return cls(SchwabMarketDataClient.from_config(app))

    def fetch_chain(
        self,
        symbol: str,
        *,
        contract_type: str = "ALL",
        strike_count: int = 50,
        days_to_expiration: Optional[int] = None,
        include_underlying_quote: bool = True,
    ) -> dict[str, Any]:
        """Fetch the raw Schwab option chain for an underlying symbol."""
        params_contract_type = contract_type.upper()
        if params_contract_type == "ALL":
            # Schwab accepts CALL, PUT, or ALL depending on API version; fetch
            # both sides by omitting restrictive filters when possible.
            params_contract_type = "ALL"

        return self._client.fetch_option_chain(
            symbol,
            contract_type=params_contract_type,
            strike_count=strike_count,
            days_to_expiration=days_to_expiration,
            include_underlying_quote=include_underlying_quote,
        )
