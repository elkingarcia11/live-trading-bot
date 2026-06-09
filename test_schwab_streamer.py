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
