"""Standard OHLCV schema shared across the system.

This module owns the canonical column definition and coercion helpers for data
that already uses standard OHLCV column names. Vendor-specific mapping belongs
in `market_data_transformer`.
"""

from __future__ import annotations

import pandas as pd

OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def ensure_standard_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and coerce a DataFrame that already uses standard OHLCV columns.

    Args:
        df: DataFrame expected to contain standard OHLCV column names.

    Returns:
        A copy with UTC timestamps, numeric OHLCV fields, and sorted rows.

    Raises:
        ValueError: If any required OHLCV column is missing.
    """
    missing = set(OHLCV_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(
            f"OHLCV data missing required columns: {sorted(missing)}"
        )

    result = df.loc[:, OHLCV_COLUMNS].copy()
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
    for column in ("open", "high", "low", "close", "volume"):
        result[column] = pd.to_numeric(result[column])

    return result.sort_values("timestamp").reset_index(drop=True)
