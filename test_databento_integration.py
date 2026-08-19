"""Tests for Databento 50t config, stream processor, and tick→OHLCV path."""

from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone

import pytest

from config import AppConfig
from data_aggregator import DataAggregator
from databento_streamer import (
    DatabentoStreamSession,
    build_databento_stream_processor,
)
from market_session_scheduler import is_equity_streaming_session
from strategy_registry import build_default_registry
from stream_data_processor import CleanBarEvent
from tick_bar_builder import TickBarBuilder, parse_tick_timeframe


def test_config_loads_databento_50t_workflow() -> None:
    config = AppConfig.from_dict(
        {
            "market": {
                "symbols": ["SPY"],
                "stream_timeframe": "50t",
                "strategy_timeframe": "50t",
                "aggregation_timeframes": ["1h", "1d"],
            },
            "workflow": {
                "stream_provider": "databento",
                "warmup_from_storage": True,
                "entry_start_local": "09:30",
                "entry_end_local": "16:00",
                "eod_flatten_time_local": "16:00",
                "eod_shutdown_time_local": "20:00",
            },
            "databento": {
                "dataset": "EQUS.MINI",
                "schema": "trades",
                "stype_in": "raw_symbol",
                "ticks_per_bar": 50,
            },
            "historical": {
                "extended_session_start_local": "04:00",
                "extended_session_end_local": "20:00",
                "need_extended_hours": True,
            },
            "broker": {"provider": "schwab"},
        }
    )

    assert config.workflow.stream_provider == "databento"
    assert config.market.stream_timeframe == "50t"
    assert config.market.strategy_timeframe == "50t"
    assert config.market.aggregation_timeframes == ()
    assert config.databento.dataset == "EQUS.MINI"
    assert config.databento.ticks_per_bar == 50
    assert config.workflow.entry_start_local == "09:30"
    assert config.workflow.entry_end_local == "16:00"
    assert config.workflow.eod_flatten_time_local == "16:00"
    assert config.workflow.eod_shutdown_time_local == "20:00"
    assert config.broker.provider == "schwab"
    assert config.workflow.persist_session_bars is True
    assert config.workflow.persist_raw_trades is True
    assert config.gcs.trades_prefix == "trades"
    assert config.gcs.transactions_prefix == "transactions"


def test_config_rejects_unknown_stream_provider() -> None:
    with pytest.raises(ValueError, match="stream_provider"):
        AppConfig.from_dict({"workflow": {"stream_provider": "polygon"}})


def test_gex_scalp_registers_on_strategy_timeframe_50t() -> None:
    registry = build_default_registry(strategy_timeframe="50t")
    assert registry.get("gex_scalp").timeframe == "50t"


def test_build_databento_stream_processor_publishes_completed_tick_bar() -> None:
    events: list[CleanBarEvent] = []
    processor = build_databento_stream_processor(
        symbols=["SPY"],
        consumers=[events.append],
        timeframe="5t",
        require_minute_alignment=False,
        dedup_window=50,
    )
    builder = TickBarBuilder(ticks_per_bar=5)
    base = datetime(2026, 6, 17, 14, 30, tzinfo=timezone.utc)

    for index in range(5):
        payload = builder.update(
            symbol="SPY",
            price=500.0 + index,
            size=10.0,
            timestamp=base + timedelta(seconds=index),
        )
        if payload is not None:
            processor.process_message(json.dumps(payload))

    assert len(events) == 1
    bar = events[0]
    assert bar.symbol == "SPY"
    assert bar.timeframe == "5t"
    assert bar.open == 500.0
    assert bar.high == 504.0
    assert bar.low == 500.0
    assert bar.close == 504.0
    assert bar.volume == 50.0
    assert bar.timestamp == base


def test_data_aggregator_skips_tick_bars() -> None:
    aggregator = DataAggregator(target_timeframes=("1h",))
    bar = CleanBarEvent(
        symbol="SPY",
        timeframe="50t",
        timestamp=datetime(2026, 6, 17, 14, 30, tzinfo=timezone.utc),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1000,
    )
    assert aggregator.on_bar(bar) == []


def test_data_aggregator_empty_targets_returns_empty() -> None:
    aggregator = DataAggregator(target_timeframes=())
    bar = CleanBarEvent(
        symbol="SPY",
        timeframe="1m",
        timestamp=datetime(2026, 6, 17, 14, 30, tzinfo=timezone.utc),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1000,
    )
    assert aggregator.on_bar(bar) == []


def test_databento_session_uses_equity_streaming_window() -> None:
    processor = build_databento_stream_processor(
        symbols=["SPY"],
        timeframe="2t",
        require_minute_alignment=False,
    )
    session = DatabentoStreamSession(
        api_key="db-test-key",
        symbols=["SPY"],
        processor=processor,
        ticks_per_bar=2,
        market_timezone="America/New_York",
        stream_start_local=time(4, 0),
        stream_end_local=time(20, 0),
    )

    outside_ts = datetime(2026, 6, 17, 7, 59, tzinfo=timezone.utc)  # 03:59 ET
    inside_ts = datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    assert (
        is_equity_streaming_session(
            outside_ts,
            session_start_local=session._stream_start_local,
            session_end_local=session._stream_end_local,
            market_timezone=session._market_timezone,
        )
        is False
    )
    assert (
        is_equity_streaming_session(
            inside_ts,
            session_start_local=session._stream_start_local,
            session_end_local=session._stream_end_local,
            market_timezone=session._market_timezone,
        )
        is True
    )


def test_tick_bar_builder_50t_emits_after_fifty_prints() -> None:
    builder = TickBarBuilder(ticks_per_bar=50)
    base = datetime(2026, 6, 17, 13, 30, tzinfo=timezone.utc)
    completed = None
    for index in range(50):
        completed = builder.update(
            symbol="SPY",
            price=400.0 + (index % 3) * 0.01,
            size=1.0,
            timestamp=base + timedelta(milliseconds=index),
        )
        if index < 49:
            assert completed is None
    assert completed is not None
    assert completed["timeframe"] == "50t"
    assert completed["bar"]["volume"] == 50.0


def test_tick_bar_builder_rejects_non_positive_ticks() -> None:
    with pytest.raises(ValueError):
        TickBarBuilder(ticks_per_bar=0)
    with pytest.raises(ValueError):
        parse_tick_timeframe("0t")


def test_repo_config_json_is_databento_400t_schwab_broker() -> None:
    config = AppConfig.load("config.json")
    assert config.workflow.stream_provider == "databento"
    assert config.market.symbols == ("SPY",)
    assert config.market.stream_symbols == ("ES.n.0",)
    assert config.market.stream_to_trade == {"ES.n.0": "SPY"}
    assert config.market.stream_timeframe == "400t"
    assert config.market.strategy_timeframe == "400t"
    assert config.databento.dataset == "GLBX.MDP3"
    assert config.databento.stype_in == "continuous"
    assert config.databento.ticks_per_bar == 400
    assert config.historical.timeframe == "400t"
    assert config.strategies == ("gaussian_ma_crossover",)
    assert config.indicators.gaussian_ma is not None
    assert config.indicators.gaussian_ma.fast.length == 30
    assert config.indicators.gaussian_ma.fast.sigma_divisor == 7.0
    assert config.indicators.gaussian_ma.slow.length == 19
    assert config.indicators.gaussian_ma.slow.sigma_divisor == 4.0
    assert config.gex.enabled is False
    assert config.broker.provider == "schwab"
    assert config.options.days_to_expiration == 2
    assert config.options.otm_strikes == 2
    assert config.gex.days_to_expiration == 2
    assert config.workflow.warmup_from_storage is True
    assert config.workflow.min_warmup_bars == 100
    assert config.workflow.eod_shutdown_enabled is False
    assert config.gcs.transactions_prefix == "transactions"
    assert config.historical.extended_session_start_local == "04:00"
    assert config.historical.extended_session_end_local == "20:00"
