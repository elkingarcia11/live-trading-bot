"""Market Data Transformer.

Responsibility: Pure vendor-to-standard data normalization.

Converts external vendor payloads (live or historical) into the system's uniform
OHLCV format. Does not perform HTTP requests, WebSocket I/O, persistence,
live-stream validation, or duplicate detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence

import pandas as pd

from ohlcv_schema import OHLCV_COLUMNS, ensure_standard_ohlcv


@dataclass(frozen=True)
class OhlcvFieldMap:
    """Maps provider-specific JSON field names to OHLCV columns."""

    timestamp: str = "t"
    open: str = "o"
    high: str = "h"
    low: str = "l"
    close: str = "c"
    volume: str = "v"


@dataclass(frozen=True)
class LiveTickFieldMap:
    """Maps provider-specific live tick/quote fields to OHLCV columns."""

    timestamp: str = "t"
    price: str = "p"
    volume: str = "v"


# Common vendor presets.
SHORT_BAR_FIELDS = OhlcvFieldMap()
LONG_BAR_FIELDS = OhlcvFieldMap(
    timestamp="timestamp",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
)
ALPHA_VANTAGE_BAR_FIELDS = OhlcvFieldMap(
    timestamp="timestamp",
    open="1. open",
    high="2. high",
    low="3. low",
    close="4. close",
    volume="5. volume",
)
SCHWAB_PRICE_HISTORY_FIELDS = OhlcvFieldMap(
    timestamp="datetime",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
)
SCHWAB_CHART_EQUITY_FIELDS = OhlcvFieldMap(
    timestamp="datetime",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
)
IBKR_HISTORY_BAR_FIELDS = OhlcvFieldMap(
    timestamp="datetime",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
)
IBKR_STREAM_BAR_FIELDS = OhlcvFieldMap(
    timestamp="datetime",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
)


class MarketDataTransformer:
    """Normalizes vendor market data payloads into standardized OHLCV DataFrames."""

    def from_bars(
        self,
        bars: Sequence[dict[str, Any]],
        field_map: Optional[OhlcvFieldMap] = None,
    ) -> pd.DataFrame:
        """Convert a list of vendor bar objects into OHLCV format.

        Args:
            bars: Sequence of provider bar dictionaries.
            field_map: Provider-specific field mapping for each OHLCV column.

        Returns:
            A DataFrame with standardized OHLCV columns sorted by timestamp.

        Raises:
            ValueError: If a bar object is missing required fields.
        """
        field_map = field_map or OhlcvFieldMap()
        if not bars:
            return self._empty_frame()

        rows: list[dict[str, Any]] = []
        for bar in bars:
            rows.append(self._extract_bar_row(bar, field_map))

        return self._finalize(pd.DataFrame(rows))

    def from_payload(
        self,
        payload: Any,
        *,
        bars_key: str = "bars",
        field_map: Optional[OhlcvFieldMap] = None,
    ) -> pd.DataFrame:
        """Extract bars from a nested vendor payload and normalize them.

        Args:
            payload: Raw provider response, either a bar list or a dict
                containing a bar list at `bars_key`.
            bars_key: JSON key that holds the list of bar objects.
            field_map: Provider-specific field mapping for each OHLCV column.

        Returns:
            A DataFrame with standardized OHLCV columns sorted by timestamp.

        Raises:
            ValueError: If the payload shape is unsupported or fields are missing.
        """
        if isinstance(payload, list):
            return self.from_bars(payload, field_map=field_map)

        if not isinstance(payload, dict):
            raise ValueError(
                f"Unsupported payload type for OHLCV extraction: {type(payload).__name__}"
            )

        bars = payload.get(bars_key, [])
        if not isinstance(bars, list):
            raise ValueError(
                f"Expected a list of bars at '{bars_key}', got {type(bars).__name__}"
            )

        return self.from_bars(bars, field_map=field_map)

    def from_arrays(
        self,
        rows: Sequence[Sequence[Any]],
        *,
        timestamp_index: int = 0,
        open_index: int = 1,
        high_index: int = 2,
        low_index: int = 3,
        close_index: int = 4,
        volume_index: int = 5,
    ) -> pd.DataFrame:
        """Convert array-of-arrays vendor rows into OHLCV format.

        Args:
            rows: Sequence of row arrays such as
                `[timestamp, open, high, low, close, volume]`.
            timestamp_index: Column index for the timestamp value.
            open_index: Column index for the open price.
            high_index: Column index for the high price.
            low_index: Column index for the low price.
            close_index: Column index for the close price.
            volume_index: Column index for the volume.

        Returns:
            A DataFrame with standardized OHLCV columns sorted by timestamp.

        Raises:
            ValueError: If a row does not contain the expected number of fields.
        """
        if not rows:
            return self._empty_frame()

        normalized_rows: list[dict[str, Any]] = []
        required_indices = (
            timestamp_index,
            open_index,
            high_index,
            low_index,
            close_index,
            volume_index,
        )
        max_index = max(required_indices)

        for row in rows:
            if len(row) <= max_index:
                raise ValueError(
                    f"Row has {len(row)} values but needs index {max_index}"
                )

            normalized_rows.append(
                {
                    "timestamp": row[timestamp_index],
                    "open": row[open_index],
                    "high": row[high_index],
                    "low": row[low_index],
                    "close": row[close_index],
                    "volume": row[volume_index],
                }
            )

        return self._finalize(pd.DataFrame(normalized_rows))

    def from_dataframe(
        self,
        df: pd.DataFrame,
        field_map: Optional[OhlcvFieldMap] = None,
    ) -> pd.DataFrame:
        """Rename and coerce an existing DataFrame into OHLCV format.

        Args:
            df: DataFrame that already contains vendor OHLCV columns.
            field_map: Provider-specific field mapping for each OHLCV column.

        Returns:
            A DataFrame with standardized OHLCV columns sorted by timestamp.

        Raises:
            ValueError: If required OHLCV columns cannot be resolved.
        """
        field_map = field_map or OhlcvFieldMap()
        if df.empty:
            return self._empty_frame()

        rename_map = {
            field_map.timestamp: "timestamp",
            field_map.open: "open",
            field_map.high: "high",
            field_map.low: "low",
            field_map.close: "close",
            field_map.volume: "volume",
        }

        missing = [source for source in rename_map if source not in df.columns]
        if missing:
            raise ValueError(
                f"DataFrame missing required vendor columns: {sorted(missing)}"
            )

        normalized = df.rename(columns=rename_map).loc[:, OHLCV_COLUMNS]
        return self._finalize(normalized)

    def from_live_tick(
        self,
        tick: dict[str, Any],
        field_map: Optional[LiveTickFieldMap] = None,
    ) -> pd.DataFrame:
        """Convert a single live tick/quote into a one-row OHLCV bar.

        Live ticks do not include full bar ranges, so open/high/low/close are
        all set to the tick price.

        Args:
            tick: Provider live tick or trade dictionary.
            field_map: Provider-specific field mapping for tick values.

        Returns:
            A one-row OHLCV DataFrame for the tick.

        Raises:
            ValueError: If the tick object is missing required fields.
        """
        field_map = field_map or LiveTickFieldMap()

        try:
            price = tick[field_map.price]
            volume = tick[field_map.volume]
            timestamp = tick[field_map.timestamp]
        except KeyError as exc:
            raise ValueError(
                f"Live tick missing expected field: {exc}"
            ) from exc

        row = {
            "timestamp": timestamp,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
        }
        return self._finalize(pd.DataFrame([row]))

    def _extract_bar_row(
        self,
        bar: dict[str, Any],
        field_map: OhlcvFieldMap,
    ) -> dict[str, Any]:
        """Map one vendor bar object onto the standard OHLCV row shape."""
        try:
            return {
                "timestamp": bar[field_map.timestamp],
                "open": bar[field_map.open],
                "high": bar[field_map.high],
                "low": bar[field_map.low],
                "close": bar[field_map.close],
                "volume": bar[field_map.volume],
            }
        except KeyError as exc:
            raise ValueError(
                f"Bar object missing expected field: {exc}"
            ) from exc

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce mapped vendor rows into the shared standard OHLCV schema."""
        return ensure_standard_ohlcv(df)

    def _empty_frame(self) -> pd.DataFrame:
        """Return an empty DataFrame with the standard OHLCV columns."""
        return pd.DataFrame(columns=list(OHLCV_COLUMNS))


if __name__ == "__main__":
    transformer = MarketDataTransformer()

    # Historical bars with short provider field names.
    historical_payload = {
        "bars": [
            {
                "t": "2024-01-15T09:30:00Z",
                "o": 185.0,
                "h": 185.5,
                "l": 184.8,
                "c": 185.3,
                "v": 1000,
            },
            {
                "t": "2024-01-15T09:31:00Z",
                "o": 185.2,
                "h": 185.6,
                "l": 185.1,
                "c": 185.4,
                "v": 1200,
            },
        ]
    }
    historical_ohlcv = transformer.from_payload(
        historical_payload,
        field_map=SHORT_BAR_FIELDS,
    )
    print("Historical OHLCV:")
    print(historical_ohlcv)

    # Array-based vendor format.
    array_rows = [
        ["2024-01-15T09:30:00Z", 185.0, 185.5, 184.8, 185.3, 1000],
        ["2024-01-15T09:31:00Z", 185.2, 185.6, 185.1, 185.4, 1200],
    ]
    array_ohlcv = transformer.from_arrays(array_rows)
    print("\nArray OHLCV:")
    print(array_ohlcv)

    # Live tick normalized to a one-row OHLCV bar.
    live_tick = {"t": "2024-01-15T09:30:15Z", "p": 185.25, "v": 50}
    live_ohlcv = transformer.from_live_tick(live_tick)
    print("\nLive tick OHLCV:")
    print(live_ohlcv)
