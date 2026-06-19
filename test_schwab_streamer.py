"""Tests for Schwab stream message parsing."""

from __future__ import annotations

import json

from schwab_streamer import (
    SchwabStreamMessageParser,
    StreamEventType,
)


def test_parse_login_success_response() -> None:
    parser = SchwabStreamMessageParser()
    payload = {
        "response": [
            {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": "1",
                "content": {"code": 0, "msg": "server=s0166bdv-1;status=PN"},
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    assert any(event.event_type == StreamEventType.LOGIN_SUCCESS for event in events)


def test_parse_login_denied_response() -> None:
    parser = SchwabStreamMessageParser()
    payload = {
        "response": [
            {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": "1",
                "content": {
                    "code": 3,
                    "msg": "Login Denied.: token is invalid or has expired.",
                },
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    login_events = [event for event in events if event.event_type == StreamEventType.LOGIN_FAILURE]
    assert len(login_events) == 1
    assert login_events[0].response_code == 3


def test_parse_add_subscription_success() -> None:
    parser = SchwabStreamMessageParser()
    payload = {
        "response": [
            {
                "service": "CHART_EQUITY",
                "command": "ADD",
                "requestid": "2",
                "content": {"code": 0, "msg": "ADD command succeeded"},
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    assert any(event.event_type == StreamEventType.SUBSCRIBE_SUCCESS for event in events)


def test_parse_close_connection_response() -> None:
    parser = SchwabStreamMessageParser()
    payload = {
        "response": [
            {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": "1",
                "content": {"code": 12, "msg": "CLOSE_CONNECTION"},
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    assert any(event.event_type == StreamEventType.CONNECTION_CLOSED for event in events)


def test_parse_chart_equity_bar() -> None:
    parser = SchwabStreamMessageParser()
    payload = {
        "data": [
            {
                "service": "CHART_EQUITY",
                "command": "SUBS",
                "content": [
                    {
                        "key": "SPY",
                        "1": 480.1,
                        "2": 480.5,
                        "3": 480.0,
                        "4": 480.3,
                        "5": 12000.0,
                        "6": 42,
                        "7": 1705329000000,
                    }
                ],
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    chart_events = [event for event in events if event.event_type == StreamEventType.CHART_BAR]
    assert len(chart_events) == 1
    assert chart_events[0].payload is not None
    assert chart_events[0].payload["symbol"] == "SPY"
    assert chart_events[0].payload["bar"]["close"] == 480.3


def test_parse_chart_equity_bar_floors_time_and_accepts_forming_bar() -> None:
    from schwab_streamer import build_schwab_stream_processor

    parser = SchwabStreamMessageParser()
    payload = {
        "data": [
            {
                "service": "CHART_EQUITY",
                "content": [
                    {
                        "seq": 365,
                        "key": "SPY",
                        "1": 365,
                        "2": 755.8,
                        "3": 755.97,
                        "4": 755.78,
                        "5": 755.87,
                        "6": 56959,
                        "7": 1781543100000,
                    }
                ],
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    chart_events = [event for event in events if event.event_type == StreamEventType.CHART_BAR]
    assert len(chart_events) == 1
    bar_payload = chart_events[0].payload
    assert bar_payload is not None
    assert bar_payload["bar"]["datetime"] == "2026-06-15T17:05:00+00:00"
    assert bar_payload["bar"]["open"] == 755.8
    assert bar_payload["bar"]["close"] == 755.87
    assert bar_payload["bar"]["volume"] == 56959

    published: list[object] = []
    processor = build_schwab_stream_processor(
        symbols=("SPY",),
        consumers=[published.append],
        require_minute_alignment=False,
    )
    event = processor.process_bar(
        bar_payload["bar"],
        symbol=bar_payload["symbol"],
        timeframe=bar_payload["timeframe"],
    )
    assert event is not None
    assert event.close == 755.87


def test_repair_ohlc_outliers_clamps_sequence_like_open_low() -> None:
    from ohlc_sanity import repair_ohlc_outliers

    open_price, high_price, low_price, close_price = repair_ohlc_outliers(
        344.0,
        753.2,
        344.0,
        752.98,
    )
    assert open_price == 752.98
    assert low_price == 752.98
    assert high_price == 753.2
    assert close_price == 752.98


def test_parse_chart_equity_repairs_corrupt_open_low_before_publish() -> None:
    from schwab_streamer import build_schwab_stream_processor

    parser = SchwabStreamMessageParser()
    payload = {
        "data": [
            {
                "service": "CHART_EQUITY",
                "content": [
                    {
                        "key": "SPY",
                        "1": 344,
                        "2": 753.2,
                        "3": 344,
                        "4": 752.98,
                        "5": 752.6,
                        "6": 12000,
                        "7": 1781631840000,
                    }
                ],
            }
        ]
    }
    events = parser.parse(json.dumps(payload))
    chart_events = [event for event in events if event.event_type == StreamEventType.CHART_BAR]
    assert len(chart_events) == 1
    bar = chart_events[0].payload["bar"]
    assert bar["open"] == 753.2
    assert bar["low"] == 752.6
    assert bar["close"] == 752.6

    published: list[object] = []
    processor = build_schwab_stream_processor(
        symbols=("SPY",),
        consumers=[published.append],
        require_minute_alignment=False,
    )
    event = processor.process_bar(
        bar,
        symbol=chart_events[0].payload["symbol"],
        timeframe=chart_events[0].payload["timeframe"],
    )
    assert event is not None
    assert event.open == 753.2
    assert event.low == 752.6
