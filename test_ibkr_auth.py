"""Tests for IBKR session auth helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ibkr_auth import IbkrAuthError, IbkrAuthStatus, IbkrSessionClient, IbkrSessionConfig


def test_auth_status_from_payload() -> None:
    status = IbkrAuthStatus.from_payload(
        {
            "connected": True,
            "authenticated": True,
            "established": True,
            "competing": False,
            "message": "",
        }
    )
    assert status.connected is True
    assert status.authenticated is True
    assert status.established is True


def test_tickle_sets_api_cookie() -> None:
    session = MagicMock()
    client = IbkrSessionClient(
        IbkrSessionConfig(max_requests_per_second=0),
        session=session,
    )
    client.request = MagicMock(  # type: ignore[method-assign]
        return_value={"session": "abc123", "iserver": {"authStatus": {"authenticated": True}}}
    )

    payload = client.tickle()
    assert payload["session"] == "abc123"
    session.cookies.set.assert_called_once_with("api", "abc123")


def test_ensure_session_raises_when_not_authenticated() -> None:
    client = IbkrSessionClient(
        IbkrSessionConfig(max_requests_per_second=0),
        session=MagicMock(),
    )
    client.tickle = MagicMock(  # type: ignore[method-assign]
        return_value={"iserver": {"authStatus": {"connected": True, "authenticated": False}}}
    )
    client.init_brokerage_session = MagicMock(  # type: ignore[method-assign]
        return_value=IbkrAuthStatus(connected=True, authenticated=False)
    )

    with pytest.raises(IbkrAuthError, match="not authenticated"):
        client.ensure_session()
