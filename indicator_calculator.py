"""Indicator Calculator.

Responsibility: Stateless technical indicator math.

Accepts OHLCV bars, applies formulas such as DEMA, Supertrend, RSI, and MACD,
and returns values.
Does not manage configuration, dispatch jobs, aggregate timeframes, or evaluate
trading rules.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from ohlcv_schema import OHLCV_COLUMNS, ensure_standard_ohlcv

SUPPORTED_INDICATORS = frozenset({"dema", "supertrend", "rsi", "macd", "sma", "ema"})

DEFAULT_DEMA_PERIOD = 200
DEFAULT_DEMA_SOURCE = "close"
DEFAULT_SUPERTREND_ATR_PERIOD = 12
DEFAULT_SUPERTREND_SOURCE = "hl2"
DEFAULT_SUPERTREND_MULTIPLIER = 3.0
DEFAULT_SUPERTREND_CHANGE_ATR = True


class IndicatorCalculator:
    """Stateless calculator for technical analysis indicators."""

    def calculate(
        self,
        name: str,
        bars: pd.DataFrame,
        **params: Any,
    ) -> pd.Series | dict[str, pd.Series]:
        """Calculate an indicator by name.

        Args:
            name: Indicator identifier (`dema`, `supertrend`, `rsi`, `macd`, `sma`, `ema`).
            bars: OHLCV bars with standard column names.
            **params: Indicator-specific parameters.

        Returns:
            A Series for single-value indicators or a dict for multi-value ones.

        Raises:
            ValueError: If the indicator name or parameters are invalid.
        """
        normalized = ensure_standard_ohlcv(bars)
        indicator = name.lower()

        if indicator == "dema":
            period = int(params.get("period", DEFAULT_DEMA_PERIOD))
            column = str(params.get("column", params.get("source", DEFAULT_DEMA_SOURCE)))
            return self.dema(normalized, period=period, column=column)
        if indicator == "supertrend":
            return self.supertrend(
                normalized,
                atr_period=int(params.get("atr_period", DEFAULT_SUPERTREND_ATR_PERIOD)),
                source=str(params.get("source", DEFAULT_SUPERTREND_SOURCE)),
                multiplier=float(params.get("multiplier", DEFAULT_SUPERTREND_MULTIPLIER)),
                change_atr=bool(params.get("change_atr", DEFAULT_SUPERTREND_CHANGE_ATR)),
            )
        if indicator == "rsi":
            period = int(params.get("period", 14))
            return self.rsi(normalized, period=period)
        if indicator == "macd":
            return self.macd(
                normalized,
                fast=int(params.get("fast", 12)),
                slow=int(params.get("slow", 26)),
                signal=int(params.get("signal", 9)),
            )
        if indicator == "sma":
            period = int(params.get("period", 20))
            column = str(params.get("column", "close"))
            return self.sma(normalized, period=period, column=column)
        if indicator == "ema":
            period = int(params.get("period", 20))
            column = str(params.get("column", "close"))
            return self.ema(normalized, period=period, column=column)

        raise ValueError(
            f"Unsupported indicator '{name}'. Supported: {sorted(SUPPORTED_INDICATORS)}"
        )

    def latest_value(
        self,
        name: str,
        bars: pd.DataFrame,
        **params: Any,
    ) -> float | dict[str, float] | None:
        """Return only the latest indicator value(s) for the final bar.

        Args:
            name: Indicator identifier.
            bars: OHLCV bars with standard column names.
            **params: Indicator-specific parameters.

        Returns:
            Latest scalar value, latest multi-value dict, or None if unavailable.
        """
        result = self.calculate(name, bars, **params)
        if isinstance(result, dict):
            latest = {}
            for key, series in result.items():
                value = self._latest_scalar(series)
                if value is not None:
                    latest[key] = value
            return latest or None

        value = self._latest_scalar(result)
        return value

    def rsi(self, bars: pd.DataFrame, *, period: int = 14) -> pd.Series:
        """Calculate the Relative Strength Index."""
        if period <= 0:
            raise ValueError("period must be positive")

        closes = bars["close"].astype(float)
        delta = closes.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)

        avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        return rsi.rename("rsi")

    def macd(
        self,
        bars: pd.DataFrame,
        *,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> dict[str, pd.Series]:
        """Calculate MACD line, signal line, and histogram."""
        if fast <= 0 or slow <= 0 or signal <= 0:
            raise ValueError("macd periods must be positive")
        if fast >= slow:
            raise ValueError("fast period must be less than slow period")

        closes = bars["close"].astype(float)
        ema_fast = closes.ewm(span=fast, adjust=False).mean()
        ema_slow = closes.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line

        return {
            "macd": macd_line.rename("macd"),
            "macd_signal": signal_line.rename("macd_signal"),
            "macd_histogram": histogram.rename("macd_histogram"),
        }

    def sma(
        self,
        bars: pd.DataFrame,
        *,
        period: int = 20,
        column: str = "close",
    ) -> pd.Series:
        """Calculate a simple moving average."""
        if period <= 0:
            raise ValueError("period must be positive")
        if column not in OHLCV_COLUMNS:
            raise ValueError(f"Unsupported column '{column}'")

        return bars[column].astype(float).rolling(window=period).mean().rename(
            f"sma_{period}"
        )

    def ema(
        self,
        bars: pd.DataFrame,
        *,
        period: int = 20,
        column: str = "close",
    ) -> pd.Series:
        """Calculate an exponential moving average."""
        if period <= 0:
            raise ValueError("period must be positive")
        if column not in OHLCV_COLUMNS:
            raise ValueError(f"Unsupported column '{column}'")

        return bars[column].astype(float).ewm(span=period, adjust=False).mean().rename(
            f"ema_{period}"
        )

    def dema(
        self,
        bars: pd.DataFrame,
        *,
        period: int = DEFAULT_DEMA_PERIOD,
        column: str = DEFAULT_DEMA_SOURCE,
    ) -> pd.Series:
        """Calculate Double EMA using the TradingView DEMA formula.

        Matches Pine Script:
            e1 = ta.ema(src, length)
            e2 = ta.ema(e1, length)
            dema = 2 * e1 - e2
        """
        if period <= 0:
            raise ValueError("period must be positive")
        if column not in OHLCV_COLUMNS:
            raise ValueError(f"Unsupported column '{column}'")

        source = bars[column].astype(float)
        e1 = source.ewm(span=period, adjust=False).mean()
        e2 = e1.ewm(span=period, adjust=False).mean()
        dema = 2 * e1 - e2
        return dema.rename("dema")

    def supertrend(
        self,
        bars: pd.DataFrame,
        *,
        atr_period: int = DEFAULT_SUPERTREND_ATR_PERIOD,
        source: str = DEFAULT_SUPERTREND_SOURCE,
        multiplier: float = DEFAULT_SUPERTREND_MULTIPLIER,
        change_atr: bool = DEFAULT_SUPERTREND_CHANGE_ATR,
    ) -> dict[str, pd.Series]:
        """Calculate Supertrend using the TradingView Supertrend formula.

        Matches Pine Script:
            up = src - (multiplier * atr)
            dn = src + (multiplier * atr)
            with band ratcheting and trend flips on close vs prior bands.
        """
        if atr_period <= 0:
            raise ValueError("atr_period must be positive")
        if multiplier <= 0:
            raise ValueError("multiplier must be positive")

        src = self._resolve_source(bars, source)
        tr = self._true_range(bars)
        if change_atr:
            atr = tr.ewm(alpha=1 / atr_period, min_periods=atr_period, adjust=False).mean()
        else:
            atr = tr.rolling(window=atr_period).mean()

        close = bars["close"].astype(float)
        up, dn, trend = self._compute_supertrend_bands(
            src=src.to_numpy(dtype=float),
            close=close.to_numpy(dtype=float),
            atr=atr.to_numpy(dtype=float),
            multiplier=multiplier,
        )

        trend_series = pd.Series(trend, index=bars.index, name="supertrend_trend")
        prev_trend = trend_series.shift(1)
        buy_signal = (trend_series == 1) & (prev_trend == -1)
        sell_signal = (trend_series == -1) & (prev_trend == 1)
        line = np.where(trend_series.to_numpy() == 1, up, dn)

        return {
            "supertrend": pd.Series(line, index=bars.index, name="supertrend"),
            "supertrend_trend": trend_series,
            "supertrend_up": pd.Series(up, index=bars.index, name="supertrend_up"),
            "supertrend_dn": pd.Series(dn, index=bars.index, name="supertrend_dn"),
            "supertrend_buy_signal": buy_signal.rename("supertrend_buy_signal"),
            "supertrend_sell_signal": sell_signal.rename("supertrend_sell_signal"),
        }

    def _resolve_source(self, bars: pd.DataFrame, source: str) -> pd.Series:
        """Resolve a Pine-style or OHLCV column source into a price series."""
        normalized = source.lower()
        if normalized == "hl2":
            return (bars["high"].astype(float) + bars["low"].astype(float)) / 2
        if normalized == "ohlc4":
            return (
                bars["open"].astype(float)
                + bars["high"].astype(float)
                + bars["low"].astype(float)
                + bars["close"].astype(float)
            ) / 4
        if normalized == "hlc3":
            return (
                bars["high"].astype(float)
                + bars["low"].astype(float)
                + bars["close"].astype(float)
            ) / 3
        if source in OHLCV_COLUMNS:
            return bars[source].astype(float)
        raise ValueError(
            f"Unsupported source '{source}'. Use hl2, ohlc4, hlc3, or an OHLCV column."
        )

    def _true_range(self, bars: pd.DataFrame) -> pd.Series:
        """Calculate true range for ATR inputs."""
        high = bars["high"].astype(float)
        low = bars["low"].astype(float)
        close = bars["close"].astype(float)
        prev_close = close.shift(1)
        ranges = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        )
        return ranges.max(axis=1)

    def _compute_supertrend_bands(
        self,
        *,
        src: np.ndarray,
        close: np.ndarray,
        atr: np.ndarray,
        multiplier: float,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Iterate Pine Supertrend band and trend state bar by bar."""
        length = len(src)
        up = np.full(length, np.nan)
        dn = np.full(length, np.nan)
        trend = np.full(length, np.nan)

        for index in range(length):
            if np.isnan(src[index]) or np.isnan(atr[index]):
                continue

            up_raw = src[index] - multiplier * atr[index]
            dn_raw = src[index] + multiplier * atr[index]

            if index == 0:
                up[index] = up_raw
                dn[index] = dn_raw
                trend[index] = 1
                continue

            up1 = up[index - 1] if not np.isnan(up[index - 1]) else up_raw
            dn1 = dn[index - 1] if not np.isnan(dn[index - 1]) else dn_raw
            prev_close = close[index - 1]

            up[index] = max(up_raw, up1) if prev_close > up1 else up_raw
            dn[index] = min(dn_raw, dn1) if prev_close < dn1 else dn_raw

            prev_trend = trend[index - 1] if not np.isnan(trend[index - 1]) else 1
            if prev_trend == -1 and close[index] > dn1:
                trend[index] = 1
            elif prev_trend == 1 and close[index] < up1:
                trend[index] = -1
            else:
                trend[index] = prev_trend

        return up, dn, trend

    def _latest_scalar(self, series: pd.Series) -> float | None:
        """Return the last non-null value from a Series."""
        if series.empty:
            return None
        valid = series.dropna()
        if valid.empty:
            return None
        return float(valid.iloc[-1])


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-15 09:30", periods=40, freq="1min", tz="UTC"),
            "open": [180 + i * 0.125 for i in range(40)],
            "high": [181 + i * 0.125 for i in range(40)],
            "low": [179 + i * 0.125 for i in range(40)],
            "close": [180.5 + i * 0.125 for i in range(40)],
            "volume": [1000] * 40,
        }
    )

    calculator = IndicatorCalculator()
    print(f"Latest DEMA: {calculator.latest_value('dema', sample, period=9, column='close')}")
    print(
        "Latest Supertrend:",
        calculator.latest_value(
            "supertrend",
            sample,
            atr_period=12,
            source="hl2",
            multiplier=3.0,
        ),
    )
    print(f"Latest RSI: {calculator.latest_value('rsi', sample, period=14)}")
