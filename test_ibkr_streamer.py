"""Tests for IBKR websocket stream parsing and bar aggregation."""

from __future__ import annotations

import json

from ibkr_streamer import (
    IbkrMinuteBarBuilder,
    IbkrStreamMessageParser,
    StreamEventType,
    build_ibkr_stream_processor,
)
from stream_data_processor import CleanBarEvent


def test_parser_recognizes_smd_tick() -> None:
    parser = IbkrStreamMessageParser()
    events = parser.parse(
        json.dumps(
            {
                "topic": "smd+756733",
                "conid": 756733,
                "31": "471.16",
                "88": "100",
                "_updated": 1_700_000_000_000,
            }
        )
    )
    assert len(events) == 1
    assert events[0].event_type == StreamEventType.MARKET_TICK


def test_minute_bar_builder_updates_forming_bar() -> None:
    builder = IbkrMinuteBarBuilder({756733: "SPY"})
    payload = builder.update(
        conid=756733,
        last_price=471.0,
        last_size=50,
        updated_at_ms=1_700_000_000_000,
    )
    assert payload is not None
    assert payload["symbol"] == "SPY"
    assert payload["bar"]["open"] == 471.0
    assert payload["bar"]["volume"] == 50.0

    payload = builder.update(
        conid=756733,
        last_price=472.5,
        last_size=25,
        updated_at_ms=1_700_000_000_000,
    )
    assert payload is not None
    assert payload["bar"]["high"] == 472.5
    assert payload["bar"]["close"] == 472.5
    assert payload["bar"]["volume"] == 75.0


def test_build_ibkr_stream_processor_publishes_bar() -> None:
    events: list[CleanBarEvent] = []

    processor = build_ibkr_stream_processor(
        symbols=("SPY",),
        consumers=[events.append],
        require_minute_alignment=False,
    )
    message = json.dumps(
        {
            "symbol": "SPY",
            "timeframe": "1m",
            "bar": {
                "datetime": "2024-01-15T14:30:00+00:00",
                "open": 470.0,
                "high": 471.0,
                "low": 469.5,
                "close": 470.5,
                "volume": 1000.0,
            },
        }
    )
    event = processor.process_message(message)
    assert event is not None
    assert event.symbol == "SPY"
    assert events[0].close == 470.5
