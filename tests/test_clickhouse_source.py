"""ClickHouseSource: cursor query, schema check, no-op when unconfigured.

Stubs the ``clickhouse_connect`` client by passing a ``client_factory``
that returns a fake.  Mirrors the pattern in
``sigmond/tests/test_hamsci_ch.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from hs_uploader import schema
from hs_uploader.sources.clickhouse import (
    HEALTH_NOOP,
    HEALTH_OK,
    HEALTH_STALE_SCHEMA,
    HEALTH_UNREACHABLE,
    ClickHouseSource,
    _ConnectionConfig,
    _Cursor,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    schema.clear()
    yield
    schema.clear()


def _make_client(rows: list[tuple], col_names: list[str], *, columns_rows=None) -> Any:
    """Build a fake CH client with two query() responses:

    * The system.columns DESCRIBE call returns ``columns_rows``.
    * The data SELECT returns ``rows`` with ``col_names``.

    The client_factory captures which call this is by inspecting the
    SQL.
    """
    client = MagicMock()

    def query(sql, parameters=None):
        result = MagicMock()
        if "system.columns" in sql:
            result.result_rows = columns_rows or [
                ("time", "DateTime"),
                ("call", "String"),
                ("freq_hz", "UInt32"),
            ]
            result.column_names = ["name", "type"]
        else:
            result.result_rows = rows
            result.column_names = col_names
        return result

    client.query = MagicMock(side_effect=query)
    return client


def test_no_op_when_no_config():
    src = ClickHouseSource(
        database="wspr",
        table="spots",
        accepted_schema_versions=[1],
        primary_key_columns=["id"],
        config=None,
    )
    assert src.health() == HEALTH_NOOP
    batches = list(src.iter_batches(b"", limit=10))
    assert batches == []


def test_strict_schema_mismatch_blocks_yield():
    cfg = _ConnectionConfig(url="http://localhost:8123")
    client = _make_client(
        rows=[],
        col_names=[],
        columns_rows=[
            ("time", "DateTime"),
            ("unexpected_column", "String"),
        ],
    )
    src = ClickHouseSource(
        database="wspr",
        table="spots",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        config=cfg,
        client_factory=lambda c: client,
    )
    # Registry has nothing — every hash is "unknown" → mismatch.
    batches = list(src.iter_batches(b"", limit=10))
    assert batches == []
    assert src.health() == HEALTH_STALE_SCHEMA


def test_strict_schema_known_version_yields():
    cfg = _ConnectionConfig(url="http://localhost:8123")
    columns_rows = [("time", "DateTime"), ("call", "String")]
    # Pre-compute the hash the source will see.
    import hashlib
    h = hashlib.sha256()
    for n, t in columns_rows:
        h.update(f"{n}\x00{t}\x00".encode("utf-8"))
    expected_hash = h.hexdigest()[:16]
    schema.register("wspr.spots", version=1, column_hash=expected_hash)

    rows = [
        (
            "K1ABC", 14_095_600,
            datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc),
            42,
        ),
    ]
    client = _make_client(
        rows=rows,
        col_names=["call", "freq_hz", "__time__", "__tiebreak__"],
        columns_rows=columns_rows,
    )
    src = ClickHouseSource(
        database="wspr",
        table="spots",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        select_columns=["call", "freq_hz"],
        config=cfg,
        client_factory=lambda c: client,
    )
    batches = list(src.iter_batches(b"", limit=10))
    assert len(batches) == 1
    assert len(batches[0].records) == 1
    rec = batches[0].records[0]
    assert rec.table == "wspr.spots"
    assert rec.columns["call"] == "K1ABC"
    assert rec.columns["freq_hz"] == 14_095_600
    # Cursor advanced.
    assert batches[0].cursor_after != b""


def test_unreachable_when_client_raises():
    cfg = _ConnectionConfig(url="http://localhost:8123")
    def raising(_cfg):
        raise ConnectionError("connection refused")

    src = ClickHouseSource(
        database="wspr",
        table="spots",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        config=cfg,
        client_factory=raising,
    )
    batches = list(src.iter_batches(b"", limit=10))
    assert batches == []
    assert src.health() == HEALTH_UNREACHABLE


def test_cursor_round_trip():
    c = _Cursor(time_iso="2026-05-08 12:00:00.000", tiebreak=42)
    blob = c.to_bytes()
    back = _Cursor.from_bytes(blob)
    assert back.time_iso == c.time_iso
    assert back.tiebreak == c.tiebreak


def test_cursor_empty_means_beginning():
    c = _Cursor.from_bytes(b"")
    assert c.time_iso == "1970-01-01 00:00:00.000"
    assert c.tiebreak == 0


def test_source_id_format():
    src = ClickHouseSource(
        database="wspr", table="spots",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        config=None,
    )
    assert src.source_id() == "ch:wspr.spots"
