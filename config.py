"""Application configuration loader.

Responsibility: Load all non-secret trading bot settings from a single JSON file.

Secrets (API keys, tokens, passwords) remain in environment variables or .env.
Does not perform network I/O, evaluate strategies, or submit orders.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

from indicator_calculator import (
    DEFAULT_DEMA_PERIOD,
    DEFAULT_DEMA_SOURCE,
    DEFAULT_GAUSSIAN_ATR_MULTIPLIER,
    DEFAULT_GAUSSIAN_ATR_PERIOD,
    DEFAULT_GAUSSIAN_LENGTH,
    DEFAULT_GAUSSIAN_SIGMA_DIVISOR,
    DEFAULT_GAUSSIAN_SQUEEZE_FILTER,
    DEFAULT_GAUSSIAN_SQUEEZE_MA_PERIOD,
    DEFAULT_GAUSSIAN_SQUEEZE_RATIO,
    DEFAULT_SUPERTREND_ATR_PERIOD,
    DEFAULT_SUPERTREND_CHANGE_ATR,
    DEFAULT_SUPERTREND_MULTIPLIER,
    DEFAULT_SUPERTREND_SOURCE,
)
from indicator_coordinator import (
    IndicatorJob,
    build_dema_job,
    build_gaussian_bands_job,
    build_supertrend_job,
)

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config.json")
DEFAULT_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "TSLA", "AMZN", "NVDA")

StreamProvider = Literal["generic", "schwab"]

_config: Optional["AppConfig"] = None


@dataclass(frozen=True)
class AppSettings:
    env: str = "development"
    log_level: str = "INFO"
    timezone: str = "America/New_York"


@dataclass(frozen=True)
class MarketConfig:
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    stream_timeframe: str = "1m"
    strategy_timeframe: str = "3m"
    aggregation_timeframes: tuple[str, ...] = ("3m", "1h", "1d")

    def __post_init__(self) -> None:
        normalized = tuple(symbol.upper() for symbol in self.symbols)
        if not normalized:
            raise ValueError("At least one symbol is required")
        object.__setattr__(self, "symbols", normalized)


@dataclass(frozen=True)
class DemaConfig:
    period: int = DEFAULT_DEMA_PERIOD
    source: str = DEFAULT_DEMA_SOURCE


@dataclass(frozen=True)
class SupertrendConfig:
    atr_period: int = DEFAULT_SUPERTREND_ATR_PERIOD
    source: str = DEFAULT_SUPERTREND_SOURCE
    multiplier: float = DEFAULT_SUPERTREND_MULTIPLIER
    change_atr: bool = DEFAULT_SUPERTREND_CHANGE_ATR


@dataclass(frozen=True)
class GaussianBandsConfig:
    length: int = DEFAULT_GAUSSIAN_LENGTH
    sigma_divisor: float = DEFAULT_GAUSSIAN_SIGMA_DIVISOR
    atr_period: int = DEFAULT_GAUSSIAN_ATR_PERIOD
    multiplier: float = DEFAULT_GAUSSIAN_ATR_MULTIPLIER
    squeeze_filter: bool = DEFAULT_GAUSSIAN_SQUEEZE_FILTER
    squeeze_ma_period: int = DEFAULT_GAUSSIAN_SQUEEZE_MA_PERIOD
    squeeze_ratio: float = DEFAULT_GAUSSIAN_SQUEEZE_RATIO


@dataclass(frozen=True)
class IndicatorConfig:
    max_bars: int = 500
    dema: Optional[DemaConfig] = field(default_factory=DemaConfig)
    supertrend: Optional[SupertrendConfig] = field(default_factory=SupertrendConfig)
    gaussian_bands: Optional[GaussianBandsConfig] = None

    def build_jobs(self, timeframe: str) -> tuple[IndicatorJob, ...]:
        jobs: list[IndicatorJob] = []
        if self.dema is not None:
            jobs.append(
                build_dema_job(
                    timeframe,
                    period=self.dema.period,
                    source=self.dema.source,
                )
            )
        if self.supertrend is not None:
            jobs.append(
                build_supertrend_job(
                    timeframe,
                    atr_period=self.supertrend.atr_period,
                    source=self.supertrend.source,
                    multiplier=self.supertrend.multiplier,
                    change_atr=self.supertrend.change_atr,
                )
            )
        if self.gaussian_bands is not None:
            jobs.append(
                build_gaussian_bands_job(
                    timeframe,
                    length=self.gaussian_bands.length,
                    sigma_divisor=self.gaussian_bands.sigma_divisor,
                    atr_period=self.gaussian_bands.atr_period,
                    multiplier=self.gaussian_bands.multiplier,
                    squeeze_filter=self.gaussian_bands.squeeze_filter,
                    squeeze_ma_period=self.gaussian_bands.squeeze_ma_period,
                    squeeze_ratio=self.gaussian_bands.squeeze_ratio,
                )
            )
        return tuple(jobs)


@dataclass(frozen=True)
class WorkflowSettings:
    run_schwab_stream: bool = False
    stream_provider: StreamProvider = "schwab"
    websocket_url: str = ""
    subscribe_on_connect: bool = True
    audit_log_path: str = "logs/audit.jsonl"
    warmup_from_storage: bool = True
    startup_sync_lookback_days: int = 30
    persist_session_bars: bool = True
    eod_enabled: bool = True
    eod_flatten_time_utc: str = "19:59"
    eod_shutdown_time_utc: str = "20:00"
    no_new_trades_after_utc: str = "19:58"


@dataclass(frozen=True)
class StreamSettings:
    schwab_streamer_url: str = ""
    schwab_stream_service: str = "CHART_EQUITY"
    schwab_chart_equity_fields: str = "0,1,2,3,4,5,6,7"
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 10.0
    reconnect_backoff_seconds: float = 1.0
    max_reconnect_backoff_seconds: float = 60.0
    max_reconnect_attempts: Optional[int] = None
    heartbeat_interval_seconds: Optional[float] = None
    heartbeat_message: str = '{"action":"heartbeat"}'
    require_minute_alignment: bool = True
    dedup_window: int = 500


@dataclass(frozen=True)
class HistoricalSettings:
    timeframe: str = "3m"
    frequency_type: str = "minute"
    frequency: int = 3
    need_extended_hours: bool = False
    need_previous_close: bool = False
    sync_start_date: str = "2024-01-01"
    sync_end_date: str = ""
    bootstrap_if_empty: bool = True
    bootstrap_lookback_months: int = 2
    session_start_utc: str = "14:30"
    session_end_utc: str = "21:00"
    session_start_local: str = "09:30"
    session_end_local: str = "16:00"
    extended_session_start_utc: str = "08:00"
    extended_session_end_utc: str = "01:00"
    trading_days_only: bool = True


@dataclass(frozen=True)
class MarketDataSettings:
    requests_per_minute: int = 120
    request_timeout_seconds: float = 30.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0


@dataclass(frozen=True)
class GcsSettings:
    bucket_name: str = "live-trading-bot"
    ohlcv_prefix: str = "ohlcv"
    credentials_path: str = ""
    project_id: str = ""
    use_daily_partitions: bool = True
    schwab_token_path: str = "schwab/tokens.json"
    local_fallback_path: str = "data"


@dataclass(frozen=True)
class RiskSettings:
    position_size_pct: float = 0.30
    position_size_max_dollars: float = 15_000.0
    simulated_tradeable_balance: float = 100_000.0
    max_position_quantity: float = 100.0
    managed_exits: bool = False
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_distance: Optional[float] = None


@dataclass(frozen=True)
class BrokerSettings:
    use_in_memory: bool = True
    simulated_fill_price: float = 100.0
    preview_orders: bool = False
    sync_positions_on_start: bool = False
    account_number: str = ""
    account_hash: str = ""


@dataclass(frozen=True)
class OptionsSettings:
    enabled: bool = True
    days_to_expiration: int = 2
    contract_type: str = "CALL"
    simulated_premium: float = 5.0
    strike_count: int = 5
    commission_per_contract: float = 0.65
    stream_contract_marks: bool = True
    trailing_stop_pct: Optional[float] = 0.15

@dataclass(frozen=True)
class EmailSettings:
    forward_test: bool = False
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender: str = ""
    recipients: tuple[str, ...] = ()


@dataclass(frozen=True)
class ForwardTestSettings:
    initial_balance: float = 3_000.0
    persist_state: bool = True
    state_prefix: str = "forward_test"
    transactions_csv_path: str = "data/transactions.csv"


@dataclass(frozen=True)
class HealthSettings:
    check_interval_seconds: float = 30.0
    feed_stale_seconds: float = 120.0
    indicator_stale_seconds: float = 180.0
    max_reconnects_per_hour: int = 5
    max_order_round_trip_seconds: float = 10.0
    module_silence_seconds: float = 300.0
    startup_grace_seconds: float = 180.0


@dataclass(frozen=True)
class SchwabSettings:
    callback_url: str = "https://127.0.0.1"
    callback_port: int = 443
    token_file: str = ".schwab_tokens.json"
    api_base_url: str = "https://api.schwabapi.com"
    oauth_authorize_path: str = "/v1/oauth/authorize"
    oauth_token_path: str = "/v1/oauth/token"
    market_data_base_url: str = "https://api.schwabapi.com/marketdata/v1"
    price_history_path: str = "/marketdata/v1/pricehistory"
    trader_base_url: str = "https://api.schwabapi.com/trader/v1"
    user_preference_path: str = "userPreference"
    account_numbers_path: str = "accounts/accountNumbers"
    accounts_path: str = "accounts"
    orders_path: str = "accounts/{account_hash}/orders"
    preview_order_path: str = "accounts/{account_hash}/previewOrder"


@dataclass(frozen=True)
class AppConfig:
    app: AppSettings = field(default_factory=AppSettings)
    market: MarketConfig = field(default_factory=MarketConfig)
    indicators: IndicatorConfig = field(default_factory=IndicatorConfig)
    strategies: tuple[str, ...] = ("dema_trend",)
    workflow: WorkflowSettings = field(default_factory=WorkflowSettings)
    stream: StreamSettings = field(default_factory=StreamSettings)
    historical: HistoricalSettings = field(default_factory=HistoricalSettings)
    market_data: MarketDataSettings = field(default_factory=MarketDataSettings)
    gcs: GcsSettings = field(default_factory=GcsSettings)
    risk: RiskSettings = field(default_factory=RiskSettings)
    broker: BrokerSettings = field(default_factory=BrokerSettings)
    options: OptionsSettings = field(default_factory=OptionsSettings)
    email: EmailSettings = field(default_factory=EmailSettings)
    forward_test: ForwardTestSettings = field(default_factory=ForwardTestSettings)
    health: HealthSettings = field(default_factory=HealthSettings)
    schwab: SchwabSettings = field(default_factory=SchwabSettings)

    @classmethod
    def load(cls, path: str | Path | None = None) -> AppConfig:
        config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
        if not config_path.exists():
            logger.warning("Config file %s not found; using defaults", config_path)
            return cls()

        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in config {config_path}") from exc

        if not isinstance(payload, dict):
            raise ValueError(f"Config {config_path} must be a JSON object")

        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AppConfig:
        stream_provider = str(
            _section(payload, "workflow").get("stream_provider", "schwab")
        ).lower()
        if stream_provider not in {"generic", "schwab"}:
            raise ValueError("workflow.stream_provider must be 'generic' or 'schwab'")

        gcs_payload = _section(payload, "gcs")
        credentials_path = str(gcs_payload.get("credentials_path", "")).strip()
        if not credentials_path:
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

        return cls(
            app=_parse_app_settings(_section(payload, "app")),
            market=_parse_market_config(_section(payload, "market")),
            indicators=_parse_indicator_config(_section(payload, "indicators")),
            strategies=_parse_strategies(payload.get("strategies")),
            workflow=_parse_workflow_settings(
                _section(payload, "workflow"),
                stream_provider=stream_provider,  # type: ignore[arg-type]
            ),
            stream=_parse_stream_settings(_section(payload, "stream")),
            historical=_parse_historical_settings(_section(payload, "historical")),
            market_data=_parse_market_data_settings(_section(payload, "market_data")),
            gcs=GcsSettings(
                bucket_name=str(gcs_payload.get("bucket_name", "live-trading-bot")),
                ohlcv_prefix=str(gcs_payload.get("ohlcv_prefix", "ohlcv")),
                credentials_path=credentials_path,
                project_id=str(
                    gcs_payload.get("project_id")
                    or os.getenv("GOOGLE_CLOUD_PROJECT", "")
                ),
                use_daily_partitions=bool(gcs_payload.get("use_daily_partitions", True)),
                schwab_token_path=str(
                    gcs_payload.get("schwab_token_path", "schwab/tokens.json")
                ).strip(),
                local_fallback_path=str(
                    gcs_payload.get("local_fallback_path", "data")
                ).strip(),
            ),
            risk=_parse_risk_settings(_section(payload, "risk")),
            broker=_parse_broker_settings(_section(payload, "broker")),
            options=_parse_options_settings(_section(payload, "options")),
            email=_parse_email_settings(_section(payload, "email")),
            forward_test=_parse_forward_test_settings(_section(payload, "forward_test")),
            health=_parse_health_settings(_section(payload, "health")),
            schwab=_parse_schwab_settings(_section(payload, "schwab")),
        )


def get_config(*, reload: bool = False) -> AppConfig:
    """Return the cached application configuration."""
    global _config
    if _config is None or reload:
        path = os.getenv("CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
        _config = AppConfig.load(path)
    return _config


def load_config(*, reload: bool = False) -> AppConfig:
    """Load .env and return application configuration."""
    from schwab_auth import _load_dotenv

    _load_dotenv()
    return get_config(reload=reload)


def secret(name: str, default: str = "") -> str:
    """Read a secret from the environment."""
    return os.getenv(name, default)


def _section(payload: dict[str, Any], name: str) -> dict[str, Any]:
    section = payload.get(name, {})
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise ValueError(f"config '{name}' must be an object")
    return section


def _parse_app_settings(payload: dict[str, Any]) -> AppSettings:
    return AppSettings(
        env=str(payload.get("env", "development")),
        log_level=str(payload.get("log_level", "INFO")),
        timezone=str(payload.get("timezone", "America/New_York")),
    )


def _parse_market_config(payload: dict[str, Any]) -> MarketConfig:
    stream_timeframe = str(payload.get("stream_timeframe", "1m"))
    strategy_timeframe = str(payload.get("strategy_timeframe", "3m"))
    aggregation_timeframes = _parse_timeframes(
        payload.get("aggregation_timeframes"),
        fallback=("5m", "1h", "1d"),
    )
    aggregation_timeframes = _higher_aggregation_timeframes(
        stream_timeframe,
        aggregation_timeframes,
    )
    if strategy_timeframe not in aggregation_timeframes:
        logger.warning(
            "strategy_timeframe %s is not in aggregation_timeframes %s; "
            "indicators and strategies will not run until it is included",
            strategy_timeframe,
            aggregation_timeframes,
        )

    return MarketConfig(
        symbols=_parse_symbols(payload.get("symbols"), fallback=DEFAULT_SYMBOLS),
        stream_timeframe=stream_timeframe,
        strategy_timeframe=strategy_timeframe,
        aggregation_timeframes=aggregation_timeframes,
    )


def _parse_indicator_config(payload: dict[str, Any]) -> IndicatorConfig:
    return IndicatorConfig(
        max_bars=int(payload.get("max_bars", 500)),
        dema=_parse_dema_config(payload.get("dema")),
        supertrend=_parse_supertrend_config(payload.get("supertrend")),
        gaussian_bands=_parse_gaussian_bands_config(payload.get("gaussian_bands")),
    )


def _parse_dema_config(payload: Any) -> Optional[DemaConfig]:
    if payload is None:
        return DemaConfig()
    if not isinstance(payload, dict):
        raise ValueError("indicators.dema must be an object")
    if not payload.get("enabled", True):
        return None
    return DemaConfig(
        period=int(payload.get("period", DEFAULT_DEMA_PERIOD)),
        source=str(payload.get("source", DEFAULT_DEMA_SOURCE)),
    )


def _parse_supertrend_config(payload: Any) -> Optional[SupertrendConfig]:
    if payload is None:
        return SupertrendConfig()
    if not isinstance(payload, dict):
        raise ValueError("indicators.supertrend must be an object")
    if not payload.get("enabled", True):
        return None
    return SupertrendConfig(
        atr_period=int(payload.get("atr_period", DEFAULT_SUPERTREND_ATR_PERIOD)),
        source=str(payload.get("source", DEFAULT_SUPERTREND_SOURCE)),
        multiplier=float(payload.get("multiplier", DEFAULT_SUPERTREND_MULTIPLIER)),
        change_atr=bool(payload.get("change_atr", DEFAULT_SUPERTREND_CHANGE_ATR)),
    )


def _parse_gaussian_bands_config(payload: Any) -> Optional[GaussianBandsConfig]:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("indicators.gaussian_bands must be an object")
    if not payload.get("enabled", True):
        return None
    return GaussianBandsConfig(
        length=int(payload.get("length", DEFAULT_GAUSSIAN_LENGTH)),
        sigma_divisor=float(payload.get("sigma_divisor", DEFAULT_GAUSSIAN_SIGMA_DIVISOR)),
        atr_period=int(payload.get("atr_period", DEFAULT_GAUSSIAN_ATR_PERIOD)),
        multiplier=float(payload.get("multiplier", DEFAULT_GAUSSIAN_ATR_MULTIPLIER)),
        squeeze_filter=bool(payload.get("squeeze_filter", DEFAULT_GAUSSIAN_SQUEEZE_FILTER)),
        squeeze_ma_period=int(
            payload.get("squeeze_ma_period", DEFAULT_GAUSSIAN_SQUEEZE_MA_PERIOD)
        ),
        squeeze_ratio=float(payload.get("squeeze_ratio", DEFAULT_GAUSSIAN_SQUEEZE_RATIO)),
    )


def _parse_strategies(value: Any) -> tuple[str, ...]:
    if value is None:
        return ("dema_trend",)
    if isinstance(value, str):
        items = tuple(item.strip() for item in value.split(",") if item.strip())
        return items or ("dema_trend",)
    if isinstance(value, list):
        items = tuple(str(item).strip() for item in value if str(item).strip())
        return items or ("dema_trend",)
    raise ValueError("strategies must be a list or comma-separated string")


def _parse_workflow_settings(
    payload: dict[str, Any],
    *,
    stream_provider: StreamProvider,
) -> WorkflowSettings:
    return WorkflowSettings(
        run_schwab_stream=bool(payload.get("run_schwab_stream", False)),
        stream_provider=stream_provider,
        websocket_url=str(payload.get("websocket_url", "")).strip(),
        subscribe_on_connect=bool(payload.get("subscribe_on_connect", True)),
        audit_log_path=str(payload.get("audit_log_path", "logs/audit.jsonl")),
        warmup_from_storage=bool(payload.get("warmup_from_storage", True)),
        startup_sync_lookback_days=int(payload.get("startup_sync_lookback_days", 30)),
        persist_session_bars=bool(payload.get("persist_session_bars", True)),
        eod_enabled=bool(payload.get("eod_enabled", True)),
        eod_flatten_time_utc=str(payload.get("eod_flatten_time_utc", "19:59")),
        eod_shutdown_time_utc=str(payload.get("eod_shutdown_time_utc", "20:00")),
        no_new_trades_after_utc=str(payload.get("no_new_trades_after_utc", "19:58")),
    )


def _parse_stream_settings(payload: dict[str, Any]) -> StreamSettings:
    max_attempts = payload.get("max_reconnect_attempts")
    heartbeat_interval = payload.get("heartbeat_interval_seconds")
    return StreamSettings(
        schwab_streamer_url=str(payload.get("schwab_streamer_url", "")),
        schwab_stream_service=str(payload.get("schwab_stream_service", "CHART_EQUITY")),
        schwab_chart_equity_fields=str(
            payload.get("schwab_chart_equity_fields", "0,1,2,3,4,5,6,7")
        ),
        ping_interval_seconds=float(payload.get("ping_interval_seconds", 20)),
        ping_timeout_seconds=float(payload.get("ping_timeout_seconds", 10)),
        reconnect_backoff_seconds=float(payload.get("reconnect_backoff_seconds", 1)),
        max_reconnect_backoff_seconds=float(
            payload.get("max_reconnect_backoff_seconds", 60)
        ),
        max_reconnect_attempts=int(max_attempts) if max_attempts not in (None, "") else None,
        heartbeat_interval_seconds=(
            float(heartbeat_interval)
            if heartbeat_interval not in (None, "")
            else None
        ),
        heartbeat_message=str(
            payload.get("heartbeat_message", '{"action":"heartbeat"}')
        ),
        require_minute_alignment=bool(payload.get("require_minute_alignment", True)),
        dedup_window=int(payload.get("dedup_window", 500)),
    )


def _parse_historical_settings(payload: dict[str, Any]) -> HistoricalSettings:
    return HistoricalSettings(
        timeframe=str(payload.get("timeframe", "3m")),
        frequency_type=str(payload.get("frequency_type", "minute")),
        frequency=int(payload.get("frequency", 3)),
        need_extended_hours=bool(payload.get("need_extended_hours", False)),
        need_previous_close=bool(payload.get("need_previous_close", False)),
        sync_start_date=str(payload.get("sync_start_date", "2024-01-01")),
        sync_end_date=str(payload.get("sync_end_date", "")),
        bootstrap_if_empty=bool(payload.get("bootstrap_if_empty", True)),
        bootstrap_lookback_months=int(payload.get("bootstrap_lookback_months", 2)),
        session_start_utc=str(payload.get("session_start_utc", "14:30")),
        session_end_utc=str(payload.get("session_end_utc", "21:00")),
        session_start_local=str(payload.get("session_start_local", "09:30")),
        session_end_local=str(payload.get("session_end_local", "16:00")),
        extended_session_start_utc=str(
            payload.get("extended_session_start_utc", "08:00")
        ),
        extended_session_end_utc=str(payload.get("extended_session_end_utc", "01:00")),
        trading_days_only=bool(payload.get("trading_days_only", True)),
    )


def _parse_market_data_settings(payload: dict[str, Any]) -> MarketDataSettings:
    return MarketDataSettings(
        requests_per_minute=int(payload.get("requests_per_minute", 120)),
        request_timeout_seconds=float(payload.get("request_timeout_seconds", 30)),
        max_retries=int(payload.get("max_retries", 3)),
        retry_backoff_seconds=float(payload.get("retry_backoff_seconds", 1.0)),
    )


def _parse_risk_settings(payload: dict[str, Any]) -> RiskSettings:
    return RiskSettings(
        position_size_pct=float(payload.get("position_size_pct", 0.30)),
        position_size_max_dollars=float(
            payload.get("position_size_max_dollars", 15_000)
        ),
        simulated_tradeable_balance=float(
            payload.get("simulated_tradeable_balance", 100_000)
        ),
        max_position_quantity=float(payload.get("max_position_quantity", 100)),
        managed_exits=bool(payload.get("managed_exits", False)),
        stop_loss=_optional_float(payload.get("stop_loss")),
        take_profit=_optional_float(payload.get("take_profit")),
        trailing_stop_distance=_optional_float(payload.get("trailing_stop_distance")),
    )


def _parse_broker_settings(payload: dict[str, Any]) -> BrokerSettings:
    return BrokerSettings(
        use_in_memory=bool(payload.get("use_in_memory", True)),
        simulated_fill_price=float(payload.get("simulated_fill_price", 100)),
        preview_orders=bool(payload.get("preview_orders", False)),
        sync_positions_on_start=bool(payload.get("sync_positions_on_start", False)),
        account_number=str(payload.get("account_number", "")).strip(),
        account_hash=str(payload.get("account_hash", "")).strip(),
    )


def _parse_options_settings(payload: dict[str, Any]) -> OptionsSettings:
    contract_type = str(payload.get("contract_type", "CALL")).upper()
    if contract_type not in {"CALL", "PUT"}:
        raise ValueError("options.contract_type must be CALL or PUT")
    trailing_stop_pct = _optional_float(payload.get("trailing_stop_pct", 0.15))
    if trailing_stop_pct is not None and not 0.0 < trailing_stop_pct < 1.0:
        raise ValueError("options.trailing_stop_pct must be between 0 and 1 (exclusive)")
    return OptionsSettings(
        enabled=bool(payload.get("enabled", True)),
        days_to_expiration=int(payload.get("days_to_expiration", 2)),
        contract_type=contract_type,
        simulated_premium=float(payload.get("simulated_premium", 5.0)),
        strike_count=int(payload.get("strike_count", 5)),
        commission_per_contract=float(payload.get("commission_per_contract", 0.65)),
        stream_contract_marks=bool(payload.get("stream_contract_marks", True)),
        trailing_stop_pct=trailing_stop_pct,
    )


def _parse_email_settings(payload: dict[str, Any]) -> EmailSettings:
    return EmailSettings(
        forward_test=bool(payload.get("forward_test", False)),
        smtp_host=str(payload.get("smtp_host", "smtp.gmail.com")),
        smtp_port=int(payload.get("smtp_port", 587)),
        sender=str(payload.get("sender", "")).strip(),
        recipients=_parse_recipients(payload.get("recipients")),
    )


def _parse_forward_test_settings(payload: dict[str, Any]) -> ForwardTestSettings:
    return ForwardTestSettings(
        initial_balance=float(payload.get("initial_balance", 3_000)),
        persist_state=bool(payload.get("persist_state", True)),
        state_prefix=str(payload.get("state_prefix", "forward_test")),
        transactions_csv_path=str(
            payload.get("transactions_csv_path", "data/transactions.csv")
        ),
    )


def _parse_health_settings(payload: dict[str, Any]) -> HealthSettings:
    return HealthSettings(
        check_interval_seconds=float(payload.get("check_interval_seconds", 30)),
        feed_stale_seconds=float(payload.get("feed_stale_seconds", 120)),
        indicator_stale_seconds=float(payload.get("indicator_stale_seconds", 180)),
        max_reconnects_per_hour=int(payload.get("max_reconnects_per_hour", 5)),
        max_order_round_trip_seconds=float(
            payload.get("max_order_round_trip_seconds", 10)
        ),
        module_silence_seconds=float(payload.get("module_silence_seconds", 300)),
        startup_grace_seconds=float(payload.get("startup_grace_seconds", 180)),
    )


def _parse_schwab_settings(payload: dict[str, Any]) -> SchwabSettings:
    return SchwabSettings(
        callback_url=str(
            payload.get("callback_url", "https://127.0.0.1")
        ),
        callback_port=int(payload.get("callback_port", 443)),
        token_file=str(payload.get("token_file", ".schwab_tokens.json")),
        api_base_url=str(payload.get("api_base_url", "https://api.schwabapi.com")),
        oauth_authorize_path=str(payload.get("oauth_authorize_path", "/v1/oauth/authorize")),
        oauth_token_path=str(payload.get("oauth_token_path", "/v1/oauth/token")),
        market_data_base_url=str(
            payload.get("market_data_base_url", "https://api.schwabapi.com/marketdata/v1")
        ),
        price_history_path=str(
            payload.get("price_history_path", "/marketdata/v1/pricehistory")
        ),
        trader_base_url=str(
            payload.get("trader_base_url", "https://api.schwabapi.com/trader/v1")
        ),
        user_preference_path=str(
            payload.get("user_preference_path", "userPreference")
        ),
        account_numbers_path=str(
            payload.get("account_numbers_path", "accounts/accountNumbers")
        ),
        accounts_path=str(payload.get("accounts_path", "accounts")),
        orders_path=str(payload.get("orders_path", "accounts/{account_hash}/orders")),
        preview_order_path=str(
            payload.get("preview_order_path", "accounts/{account_hash}/previewOrder")
        ),
    )


def _parse_symbols(value: Any, *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return fallback
    if isinstance(value, str):
        items = tuple(item.strip().upper() for item in value.split(",") if item.strip())
        return items or fallback
    if isinstance(value, list):
        items = tuple(str(item).strip().upper() for item in value if str(item).strip())
        return items or fallback
    raise ValueError("market.symbols must be a list or comma-separated string")


def _parse_timeframes(value: Any, *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return fallback
    if isinstance(value, str):
        items = tuple(item.strip() for item in value.split(",") if item.strip())
        return items or fallback
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raise ValueError("aggregation_timeframes must be a list or comma-separated string")


def _timeframe_to_minutes(timeframe: str) -> int:
    if len(timeframe) < 2:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    if unit == "m":
        return value
    if unit == "h":
        return value * 60
    if unit == "d":
        return value * 60 * 24
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _higher_aggregation_timeframes(
    stream_timeframe: str,
    timeframes: tuple[str, ...],
) -> tuple[str, ...]:
    """Drop rollups at or below the stream interval (e.g. 1m when streaming 1m)."""
    stream_minutes = _timeframe_to_minutes(stream_timeframe)
    filtered = tuple(
        timeframe
        for timeframe in timeframes
        if _timeframe_to_minutes(timeframe) > stream_minutes
    )
    removed = [timeframe for timeframe in timeframes if timeframe not in filtered]
    if removed:
        logger.warning(
            "Ignoring aggregation_timeframes %s (must be higher than stream_timeframe %s)",
            removed,
            stream_timeframe,
        )
    return filtered


def _parse_recipients(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raise ValueError("email.recipients must be a list or comma-separated string")


def _optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)


if __name__ == "__main__":
    print(load_config(reload=True))
