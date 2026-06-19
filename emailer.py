"""Trade Emailer.

Responsibility: Forward-test notifications via email.

Sends buy and sell alert emails when strategy signals fire, replacing live
broker execution during paper/forward testing. Does not submit orders or
evaluate strategy rules.
"""

from __future__ import annotations

import logging
import os
import re
import smtplib
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Optional
from zoneinfo import ZoneInfo

from signal_evaluator import StrategySignal
from strategy_registry import SignalAction
from option_quote import OptionQuoteSnapshot, format_quote_email_lines

logger = logging.getLogger(__name__)

DEFAULT_SMTP_HOST = "smtp.gmail.com"
DEFAULT_SMTP_PORT = 587


@dataclass(frozen=True)
class EmailerConfig:
    """Gmail SMTP settings for trade notification emails."""

    enabled: bool = False
    smtp_host: str = DEFAULT_SMTP_HOST
    smtp_port: int = DEFAULT_SMTP_PORT
    sender: str = ""
    app_password: str = ""
    recipients: tuple[str, ...] = ()
    timezone_name: str = "America/New_York"

    def __post_init__(self) -> None:
        if not self.enabled:
            return
        if not self.sender:
            raise ValueError("GMAIL_SENDER is required when email.forward_test=true")
        if not self.app_password:
            raise ValueError(
                "GMAIL_APP_PASSWORD is required when email.forward_test=true"
            )
        if not self.recipients:
            raise ValueError(
                "email.recipients is required when email.forward_test=true"
            )

    @classmethod
    def from_app_config(cls, app: "AppConfig") -> EmailerConfig:
        """Build emailer configuration from application config and env secrets."""
        from config import AppConfig, secret

        email = app.email
        return cls(
            enabled=email.forward_test,
            smtp_host=email.smtp_host,
            smtp_port=email.smtp_port,
            sender=email.sender,
            app_password=secret("GMAIL_APP_PASSWORD"),
            recipients=email.recipients,
            timezone_name=app.app.timezone,
        )

    @classmethod
    def from_env(cls, *, load_dotenv: bool = True) -> EmailerConfig:
        """Build emailer configuration from config.json."""
        if load_dotenv:
            from schwab_auth import _load_dotenv

            _load_dotenv()

        from config import load_config

        return cls.from_app_config(load_config(reload=False))


class TradeEmailer:
    """Sends buy/sell notification emails for forward testing."""

    def __init__(self, config: EmailerConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._entry_prices: dict[str, float] = {}
        try:
            self._tz = ZoneInfo(config.timezone_name)
        except Exception:
            logger.warning(
                "Invalid APP_TIMEZONE=%s; falling back to UTC",
                config.timezone_name,
            )
            self._tz = timezone.utc

    def notify_signal(
        self,
        signal: StrategySignal,
        *,
        quantity: float,
        instrument_symbol: Optional[str] = None,
        instrument_description: Optional[str] = None,
        account_summary: Optional[str] = None,
        trade_amount: Optional[float] = None,
        trade_pnl: Optional[float] = None,
        instrument_price: Optional[float] = None,
        underlying_price: Optional[float] = None,
        entry_instrument_price: Optional[float] = None,
        entry_underlying_price: Optional[float] = None,
        quote: Optional[OptionQuoteSnapshot] = None,
        entry_quote: Optional[OptionQuoteSnapshot] = None,
        time_bought: Optional[datetime] = None,
    ) -> None:
        """Send a buy or sell email for an approved strategy signal."""
        if signal.action == SignalAction.HOLD:
            return

        conditions_met = describe_conditions_met(signal)
        time_triggered = signal.timestamp
        executed_at = datetime.now(timezone.utc)
        price = instrument_price if instrument_price is not None else signal.close
        spot = underlying_price if underlying_price is not None else signal.close
        trade_symbol = instrument_symbol or signal.symbol
        instrument_line = instrument_description or trade_symbol

        if signal.action == SignalAction.BUY:
            with self._lock:
                self._entry_prices[signal.symbol] = price
            self.send_buy_notification(
                symbol=signal.symbol,
                strategy_name=signal.strategy_name,
                conditions_met=conditions_met,
                time_triggered=time_triggered,
                time_bought=executed_at,
                entry_instrument_price=price,
                underlying_price=spot,
                quantity=quantity,
                instrument_line=instrument_line,
                account_summary=account_summary,
                trade_amount=trade_amount,
                quote=quote,
            )
            return

        stored_entry = self._entry_prices.get(signal.symbol)
        entry_instrument = entry_instrument_price
        if entry_instrument is None:
            entry_instrument = stored_entry
        entry_underlying = entry_underlying_price
        profit = trade_pnl
        if profit is None and entry_instrument is not None:
            profit = (price - entry_instrument) * quantity
        with self._lock:
            self._entry_prices.pop(signal.symbol, None)

        self.send_sell_notification(
            symbol=signal.symbol,
            strategy_name=signal.strategy_name,
            conditions_met=conditions_met,
            time_triggered=time_triggered,
            time_sold=executed_at,
            exit_instrument_price=price,
            exit_underlying_price=spot,
            entry_instrument_price=entry_instrument,
            entry_underlying_price=entry_underlying,
            profit=profit,
            quantity=quantity,
            instrument_line=instrument_line,
            account_summary=account_summary,
            trade_amount=trade_amount,
            quote=quote,
            entry_quote=entry_quote,
            time_bought=time_bought,
        )

    def send_buy_notification(
        self,
        *,
        symbol: str,
        strategy_name: str,
        conditions_met: str,
        time_triggered: datetime,
        time_bought: datetime,
        entry_instrument_price: float,
        underlying_price: float,
        quantity: float,
        instrument_line: Optional[str] = None,
        account_summary: Optional[str] = None,
        trade_amount: Optional[float] = None,
        quote: Optional[OptionQuoteSnapshot] = None,
    ) -> None:
        """Send a buy forward-test notification email."""
        right = _option_right_suffix(instrument_line)
        subject = f"[Forward Test] BUY {symbol}{right} @ {entry_instrument_price:.2f}"
        lines = [
            "Buy conditions met",
            "",
            f"Underlying: {symbol}",
            f"Instrument: {instrument_line or symbol}",
            f"Strategy: {strategy_name}",
            f"Conditions: {conditions_met}",
            f"Time triggered: {self._format_time(time_triggered)}",
            f"Time bought: {self._format_time(time_bought)}",
            f"Entry instrument price: {entry_instrument_price:.4f}",
            f"Underlying price at entry: {underlying_price:.4f}",
            f"Quantity: {quantity:g}",
        ]
        lines.extend(format_quote_email_lines(quote, label="Entry"))
        if trade_amount is not None:
            lines.append(f"Cost: ${trade_amount:,.2f}")
        if account_summary:
            lines.extend(["", f"Account: {account_summary}"])
        self._send_email(subject, "\n".join(lines))

    def send_sell_notification(
        self,
        *,
        symbol: str,
        strategy_name: str,
        conditions_met: str,
        time_triggered: datetime,
        time_sold: datetime,
        exit_instrument_price: float,
        exit_underlying_price: float,
        profit: Optional[float],
        entry_instrument_price: Optional[float] = None,
        entry_underlying_price: Optional[float] = None,
        quantity: float = 0.0,
        instrument_line: Optional[str] = None,
        account_summary: Optional[str] = None,
        trade_amount: Optional[float] = None,
        quote: Optional[OptionQuoteSnapshot] = None,
        entry_quote: Optional[OptionQuoteSnapshot] = None,
        time_bought: Optional[datetime] = None,
        max_unrealized_profit: Optional[float] = None,
        max_unrealized_loss: Optional[float] = None,
    ) -> None:
        """Send a sell forward-test notification email."""
        right = _option_right_suffix(instrument_line)
        subject = f"[Forward Test] SELL {symbol}{right} @ {exit_instrument_price:.2f}"
        profit_line = "Trade P&L: n/a (no prior buy recorded)"
        if profit is not None:
            profit_line = f"Trade P&L: {profit:+.2f} ({quantity:g} contracts/shares)"

        lines = [
            "Sell conditions met",
            "",
            f"Underlying: {symbol}",
            f"Instrument: {instrument_line or symbol}",
            f"Strategy: {strategy_name}",
            f"Conditions: {conditions_met}",
            f"Time triggered: {self._format_time(time_triggered)}",
            f"Time bought: {self._format_time(time_bought) if time_bought is not None else 'n/a'}",
            f"Time sold: {self._format_time(time_sold)}",
            f"Entry instrument price: {_format_price(entry_instrument_price)}",
            f"Entry underlying price: {_format_price(entry_underlying_price)}",
            f"Exit instrument price: {exit_instrument_price:.4f}",
            f"Exit underlying price: {exit_underlying_price:.4f}",
            f"Quantity: {quantity:g}",
            profit_line,
        ]
        if max_unrealized_profit is not None:
            lines.append(
                f"Max unrealized profit while held: {max_unrealized_profit:+,.2f}"
            )
        if max_unrealized_loss is not None:
            lines.append(
                f"Max unrealized loss while held: {max_unrealized_loss:+,.2f}"
            )
        lines.extend(format_quote_email_lines(entry_quote, label="Entry"))
        lines.extend(format_quote_email_lines(quote, label="Exit"))
        if trade_amount is not None:
            lines.append(f"Proceeds: ${trade_amount:,.2f}")
        if account_summary:
            lines.extend(["", f"Account: {account_summary}"])
        self._send_email(subject, "\n".join(lines))

    def _format_time(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        local = value.astimezone(self._tz)
        return local.strftime("%Y-%m-%d %H:%M:%S %Z")

    def _send_email(self, subject: str, body: str) -> None:
        message = EmailMessage()
        message["From"] = self._config.sender
        message["To"] = ", ".join(self._config.recipients)
        message["Subject"] = subject
        message.set_content(body)

        try:
            with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(self._config.sender, self._config.app_password)
                smtp.send_message(message)
            logger.info("Sent trade notification email: %s", subject)
        except Exception:
            logger.exception("Failed to send trade notification email: %s", subject)


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


_OCC_RIGHT_PATTERN = re.compile(r"\d{6}([CP])\d{8}")


def _option_right_suffix(instrument_line: Optional[str]) -> str:
    """Return ' C' or ' P' when an OCC option symbol is present, else ''."""
    if not instrument_line:
        return ""
    match = _OCC_RIGHT_PATTERN.search(instrument_line.upper())
    if match is None:
        return ""
    return f" {match.group(1)}"


def describe_conditions_met(signal: StrategySignal) -> str:
    """Return a human-readable summary of why the strategy fired."""
    close = signal.close
    indicators = signal.indicators

    if signal.strategy_name == "dema_trend":
        dema = indicators.get("dema")
        if dema is None:
            return "DEMA trend signal"
        if close > dema:
            return f"close {close:.4f} > dema {dema:.4f}"
        return f"close {close:.4f} < dema {dema:.4f}"

    if signal.strategy_name == "supertrend_trend":
        trend = indicators.get("supertrend_trend")
        direction = "uptrend" if trend and trend > 0 else "downtrend"
        return f"supertrend {direction} (trend={trend})"

    if signal.strategy_name in {"supertrend_signals", "supertrend"}:
        if indicators.get("supertrend_buy_signal"):
            return "supertrend flipped bullish"
        if indicators.get("supertrend_sell_signal"):
            return "supertrend flipped bearish"
        return "supertrend signal"

    if signal.strategy_name == "rsi_mean_reversion":
        rsi = indicators.get("rsi")
        if rsi is not None and rsi < 30:
            return f"RSI oversold ({rsi:.2f} < 30)"
        if rsi is not None and rsi > 70:
            return f"RSI overbought ({rsi:.2f} > 70)"
        return f"RSI={rsi}"

    if signal.strategy_name == "macd_crossover":
        macd = indicators.get("macd")
        macd_signal = indicators.get("macd_signal")
        if macd is not None and macd_signal is not None:
            relation = "above" if macd > macd_signal else "below"
            return f"MACD {relation} signal ({macd:.4f} vs {macd_signal:.4f})"
        return "MACD crossover"

    indicator_summary = ", ".join(
        f"{name}={value}"
        for name, value in sorted(indicators.items())
        if value is not None
    )
    action = signal.action.value.upper()
    return f"{action} via {signal.strategy_name} at close {close:.4f} ({indicator_summary})"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_tuple(name: Optional[str]) -> tuple[str, ...]:
    if not name:
        return ()
    values = tuple(item.strip() for item in name.split(",") if item.strip())
    return values


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from datetime import timedelta

    from strategy_registry import SignalAction

    config = EmailerConfig(
        enabled=True,
        sender=os.getenv("GMAIL_SENDER", ""),
        app_password=os.getenv("GMAIL_APP_PASSWORD", ""),
        recipients=_env_tuple(os.getenv("GMAIL_RECIPIENTS")),
    )
    emailer = TradeEmailer(config)

    now = datetime.now(timezone.utc)
    buy_signal = StrategySignal(
        symbol="SPY",
        timeframe="5m",
        timestamp=now,
        action=SignalAction.BUY,
        strategy_name="dema_trend",
        close=585.42,
        indicators={"dema": 584.10},
    )
    emailer.notify_signal(buy_signal, quantity=30)

    sell_signal = StrategySignal(
        symbol="SPY",
        timeframe="5m",
        timestamp=now + timedelta(minutes=5),
        action=SignalAction.SELL,
        strategy_name="dema_trend",
        close=586.75,
        indicators={"dema": 587.00},
    )
    emailer.notify_signal(sell_signal, quantity=30)
