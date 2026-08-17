"""Tests for stream futures / trade equity option underlying mapping."""

from __future__ import annotations

from config import AppConfig, MarketConfig, normalize_market_symbol


def test_normalize_preserves_continuous_roll_letter() -> None:
    assert normalize_market_symbol("ES.n.0") == "ES.n.0"
    assert normalize_market_symbol("es.N.0") == "ES.n.0"
    assert normalize_market_symbol("SPY") == "SPY"


def test_market_config_maps_es_stream_to_spy_trade() -> None:
    market = MarketConfig(
        symbols=("SPY",),
        stream_symbols=("ES.n.0",),
        stream_to_trade={"ES.n.0": "SPY"},
        stream_timeframe="5t",
        strategy_timeframe="5t",
        aggregation_timeframes=(),
    )
    assert market.symbols == ("SPY",)
    assert market.stream_symbols == ("ES.n.0",)
    assert market.trade_underlying_for("ES.n.0") == "SPY"
    assert market.stream_symbol_for("SPY") == "ES.n.0"


def test_market_config_defaults_stream_to_symbols() -> None:
    market = MarketConfig(symbols=("SPY",), stream_timeframe="5t", strategy_timeframe="5t")
    assert market.stream_symbols == ("SPY",)
    assert market.trade_underlying_for("SPY") == "SPY"


def test_repo_config_streams_es_trades_spy_options() -> None:
    config = AppConfig.load("config.json")
    assert config.market.symbols == ("SPY",)
    assert config.market.stream_symbols == ("ES.n.0",)
    assert config.market.trade_underlying_for("ES.n.0") == "SPY"
    assert config.databento.dataset == "GLBX.MDP3"
    assert config.databento.stype_in == "continuous"
    assert config.databento.apply_equity_session_filter is False
