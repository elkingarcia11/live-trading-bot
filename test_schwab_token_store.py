"""Tests for Schwab token persistence helpers."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from schwab_auth import (
    LayeredSchwabTokenStore,
    SchwabGcsTokenStore,
    SchwabTokenStore,
    SchwabTokens,
    normalize_authorization_code,
    tokens_from_payload,
    tokens_to_payload,
)


def test_normalize_authorization_code_strips_suffix() -> None:
    assert normalize_authorization_code("abc123@metadata") == "abc123"


def test_tokens_round_trip_payload() -> None:
    tokens = SchwabTokens(
        access_token="access",
        refresh_token="refresh",
        expires_at=123.0,
    )
    restored = tokens_from_payload(tokens_to_payload(tokens))
    assert restored.access_token == "access"
    assert restored.refresh_token == "refresh"
    assert restored.expires_at == 123.0


def test_gcs_token_store_save_and_load() -> None:
    client = MagicMock()
    bucket = MagicMock()
    blob = MagicMock()
    client.bucket.return_value = bucket
    bucket.blob.return_value = blob

    store = SchwabGcsTokenStore(
        "live-trading-bot",
        blob_path="schwab/tokens.json",
        client=client,
    )
    tokens = SchwabTokens("a", "r", 1.0)

    uri = store.save(tokens)
    assert uri == "gs://live-trading-bot/schwab/tokens.json"
    uploaded = json.loads(blob.upload_from_string.call_args.args[0])
    assert uploaded["refresh_token"] == "r"

    blob.download_as_text.return_value = json.dumps(uploaded)
    loaded = store.load()
    assert loaded is not None
    assert loaded.refresh_token == "r"


def test_layered_token_store_saves_local_first_and_best_effort_gcs(tmp_path) -> None:
    local = SchwabTokenStore(tmp_path / "tokens.json")
    gcs = MagicMock()
    gcs.is_gcs = True
    gcs.save.return_value = "gs://bucket/schwab/tokens.json"
    store = LayeredSchwabTokenStore([local, gcs])
    tokens = SchwabTokens("a", "r", 1.0)

    location = store.save(tokens)
    assert location.endswith("tokens.json")
    gcs.save.assert_called_once_with(tokens)


def test_layered_token_store_continues_when_gcs_save_fails(tmp_path) -> None:
    local = SchwabTokenStore(tmp_path / "tokens.json")
    gcs = MagicMock()
    gcs.is_gcs = True
    gcs.save.side_effect = RuntimeError("bucket missing")
    store = LayeredSchwabTokenStore([local, gcs])
    tokens = SchwabTokens("a", "r", 1.0)

    location = store.save(tokens)
    assert location.endswith("tokens.json")
    gcs.save.assert_called_once_with(tokens)


def test_layered_token_store_syncs_local_tokens_to_missing_gcs(tmp_path) -> None:
    local = SchwabTokenStore(tmp_path / "tokens.json")
    tokens = SchwabTokens("a", "refresh", 1.0)
    local.save(tokens)
    gcs = MagicMock()
    gcs.is_gcs = True
    gcs.load.return_value = None
    store = LayeredSchwabTokenStore([local, gcs])

    loaded = store.load()
    assert loaded is not None
    assert loaded.refresh_token == "refresh"
    gcs.save.assert_called_once()


def test_layered_token_store_loads_first_available() -> None:
    first = MagicMock()
    second = MagicMock()
    first.load.return_value = None
    second.load.return_value = SchwabTokens("a", "refresh", 1.0)
    store = LayeredSchwabTokenStore([first, second])

    loaded = store.load()
    assert loaded is not None
    assert loaded.refresh_token == "refresh"
