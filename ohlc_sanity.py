"""OHLC sanity checks for streamed and stored market data."""

from __future__ import annotations

import pandas as pd


def repair_ohlc_outliers(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    *,
    tolerance: float = 0.02,
) -> tuple[float, float, float, float]:
    """Clamp open/high/low that diverge wildly from close.

    Schwab CHART_EQUITY updates occasionally map sequence fields into open/low.
    """
    if close_price <= 0:
        return open_price, high_price, low_price, close_price

    def _is_plausible(price: float) -> bool:
        return abs(price - close_price) / close_price <= tolerance

    if not _is_plausible(open_price):
        open_price = close_price
    if not _is_plausible(low_price):
        low_price = min(open_price, close_price)
    if not _is_plausible(high_price):
        high_price = max(open_price, close_price)

    return open_price, high_price, low_price, close_price


def repair_ohlc_bar(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    *,
    tolerance: float = 0.02,
) -> tuple[float, float, float, float]:
    """Repair outliers and enforce high/low envelope around open/close."""
    open_price, high_price, low_price, close_price = repair_ohlc_outliers(
        open_price,
        high_price,
        low_price,
        close_price,
        tolerance=tolerance,
    )
    high_price = max(high_price, open_price, close_price)
    low_price = min(low_price, open_price, close_price)
    return open_price, high_price, low_price, close_price


def repair_ohlcv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of an OHLCV frame with outlier open/high/low repaired."""
    if df.empty:
        return df.copy()

    repaired = df.copy()
    for index, row in repaired.iterrows():
        open_price, high_price, low_price, close_price = repair_ohlc_bar(
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
        )
        repaired.at[index, "open"] = open_price
        repaired.at[index, "high"] = high_price
        repaired.at[index, "low"] = low_price
        repaired.at[index, "close"] = close_price
    return repaired
