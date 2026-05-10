"""Schema registry — built-ins, clear() preserves built-ins, hash compute."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from hs_uploader import schema


@pytest.fixture(autouse=True)
def _reset_registry():
    schema.clear()
    yield
    schema.clear()


def test_builtins_register_at_module_load():
    # psk-recorder v2 is the deployed-on-bee1 schema as of 2026-05-10.
    assert schema.version_for_hash("psk.spots", "8ee544049db79fd0") == 2


def test_clear_repopulates_builtins():
    # Test-specific registration adds a row, clear() drops the test
    # row but preserves the built-ins.
    schema.register("test.table", version=99, column_hash="deadbeef0badf00d")
    assert schema.version_for_hash("test.table", "deadbeef0badf00d") == 99

    schema.clear()
    assert schema.version_for_hash("test.table", "deadbeef0badf00d") is None
    assert schema.version_for_hash("psk.spots", "8ee544049db79fd0") == 2


def test_unknown_hash_returns_none():
    assert schema.version_for_hash("psk.spots", "0000000000000000") is None
    assert schema.version_for_hash("not.a.table", "8ee544049db79fd0") is None


def test_register_conflict_raises():
    schema.register("foo.bar", version=1, column_hash="abc1234567890def")
    # Same triple is idempotent.
    schema.register("foo.bar", version=1, column_hash="abc1234567890def")
    # Different version for the same hash → conflict.
    with pytest.raises(ValueError, match="already registered as v1"):
        schema.register("foo.bar", version=2, column_hash="abc1234567890def")


def test_compute_column_hash_matches_psk_spots_v2():
    """The function reproduces the bee1-deployed psk.spots v2 hash from
    the column list — locks the algorithm so future changes either
    preserve the hash or are caught by the registry mismatch."""
    columns = [
        ("time", "DateTime"),
        ("mode", "LowCardinality(String)"),
        ("host_call", "LowCardinality(String)"),
        ("host_grid", "LowCardinality(String)"),
        ("radiod_id", "LowCardinality(String)"),
        ("instance", "LowCardinality(String)"),
        ("processing_version", "LowCardinality(String)"),
        ("score", "Int16"),
        ("dt", "Float32"),
        ("frequency", "Int64"),
        ("frequency_mhz", "Float64"),
        ("message", "String"),
        ("tx_call", "LowCardinality(String)"),
        ("rx_call", "LowCardinality(String)"),
        ("grid", "LowCardinality(String)"),
        ("report", "Nullable(Int16)"),
        ("ingested_at", "DateTime"),
        ("snr_db", "Nullable(Float32)"),
        ("spectral_width_hz", "Nullable(Float32)"),
        ("decoder_kind", "LowCardinality(String)"),
    ]
    client = MagicMock()
    result = MagicMock()
    result.result_rows = columns
    client.query.return_value = result

    h = schema.compute_column_hash(client, "psk", "spots")
    assert h == "8ee544049db79fd0"


def test_compute_column_hash_returns_empty_for_unknown_table():
    client = MagicMock()
    result = MagicMock()
    result.result_rows = []
    client.query.return_value = result

    assert schema.compute_column_hash(client, "nope", "missing") == ""
