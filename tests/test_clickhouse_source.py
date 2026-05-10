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
        col_names=["call", "freq_hz", "__cursor__", "__tiebreak__"],
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


# ---- cursor_column ----

def test_cursor_column_defaults_to_time():
    """Default behavior preserved — clients that don't pass
    ``cursor_column`` get the original ``ORDER BY time`` query shape."""
    cfg = _ConnectionConfig(url="http://localhost:8123")
    columns = [("time", "DateTime"), ("call", "String")]
    import hashlib
    h = hashlib.sha256()
    for n, t in columns:
        h.update(f"{n}\x00{t}\x00".encode("utf-8"))
    schema.register("foo.bar", version=1, column_hash=h.hexdigest()[:16])

    captured: dict = {}
    def fake_query(sql, parameters=None):
        captured["sql"] = sql
        captured["params"] = parameters
        result = MagicMock()
        if "system.columns" in sql:
            result.result_rows = columns
            result.column_names = ["name", "type"]
        else:
            result.result_rows = []
            result.column_names = []
        return result
    client = MagicMock()
    client.query = MagicMock(side_effect=fake_query)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    assert "time AS __cursor__" in captured["sql"]
    assert "ORDER BY time," in captured["sql"]


def test_cursor_column_ingested_at_drives_query():
    """psk-recorder's lesson: when decode time is non-monotonic across
    writers, watermark on ``ingested_at`` instead.  The SQL should
    reflect the chosen column in SELECT, WHERE, and ORDER BY."""
    cfg = _ConnectionConfig(url="http://localhost:8123")
    columns = [
        ("time", "DateTime"), ("ingested_at", "DateTime"), ("call", "String"),
    ]
    import hashlib
    h = hashlib.sha256()
    for n, t in columns:
        h.update(f"{n}\x00{t}\x00".encode("utf-8"))
    schema.register("psk.spots", version=99, column_hash=h.hexdigest()[:16])

    captured: dict = {}
    def fake_query(sql, parameters=None):
        captured["sql"] = sql
        captured["params"] = parameters
        result = MagicMock()
        if "system.columns" in sql:
            result.result_rows = columns
            result.column_names = ["name", "type"]
        else:
            result.result_rows = []
            result.column_names = []
        return result
    client = MagicMock()
    client.query = MagicMock(side_effect=fake_query)
    src = ClickHouseSource(
        database="psk", table="spots",
        accepted_schema_versions=[99],
        primary_key_columns=["host_call", "time", "frequency"],
        cursor_column="ingested_at",
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    sql = captured["sql"]
    assert "ingested_at AS __cursor__" in sql
    assert "WHERE (ingested_at, cityHash64" in sql
    assert "ORDER BY ingested_at, cityHash64" in sql
    # Old behaviour must NOT leak — `time AS __cursor__` would mean the
    # cursor advances on decode time, which is exactly the bug we're
    # avoiding here.
    assert "time AS __cursor__" not in sql


# ---- extra_where ----

def test_extra_where_filters_render_in_query():
    cfg = _ConnectionConfig(url="http://localhost:8123")
    columns = [("time", "DateTime"), ("radiod_id", "String"), ("mode", "String")]
    import hashlib
    h = hashlib.sha256()
    for n, t in columns:
        h.update(f"{n}\x00{t}\x00".encode("utf-8"))
    schema.register("psk.spots", version=99, column_hash=h.hexdigest()[:16])

    captured: dict = {}
    def fake_query(sql, parameters=None):
        captured["sql"] = sql
        captured["params"] = parameters
        result = MagicMock()
        if "system.columns" in sql:
            result.result_rows = columns
            result.column_names = ["name", "type"]
        else:
            result.result_rows = []
            result.column_names = []
        return result
    client = MagicMock()
    client.query = MagicMock(side_effect=fake_query)
    src = ClickHouseSource(
        database="psk", table="spots",
        accepted_schema_versions=[99],
        primary_key_columns=["time"],
        extra_where=[
            ("radiod_id", "=", "my-rx888"),
            ("tx_call", "!=", ""),
            ("mode", "IN", ["ft8", "ft4"]),
        ],
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    sql = captured["sql"]
    params = captured["params"]
    # Each filter renders one AND clause; values flow through params.
    assert " AND radiod_id = %(extra_0)s" in sql
    assert " AND tx_call != %(extra_1)s" in sql
    assert " AND mode IN %(extra_2)s" in sql
    assert params["extra_0"] == "my-rx888"
    assert params["extra_1"] == ""
    assert params["extra_2"] == ["ft8", "ft4"]


def test_extra_where_rejects_disallowed_op():
    with pytest.raises(ValueError, match="extra_where operator"):
        ClickHouseSource(
            database="x", table="y",
            accepted_schema_versions=[1],
            primary_key_columns=["t"],
            extra_where=[("col", "LIKE", "%foo%")],
            config=None,
        )


def test_extra_where_rejects_unsafe_column_name():
    with pytest.raises(ValueError, match="alphanumeric"):
        ClickHouseSource(
            database="x", table="y",
            accepted_schema_versions=[1],
            primary_key_columns=["t"],
            extra_where=[("col; DROP TABLE", "=", "x")],
            config=None,
        )


# ---- start_at ----


def _stub_schema_and_client(columns, captured):
    """Helper: register schema, return a client that captures SQL."""
    cfg = _ConnectionConfig(url="http://localhost:8123")
    import hashlib
    h = hashlib.sha256()
    for n, t in columns:
        h.update(f"{n}\x00{t}\x00".encode("utf-8"))
    schema.register("foo.bar", version=1, column_hash=h.hexdigest()[:16])

    def fake_query(sql, parameters=None):
        captured.setdefault("sql_history", []).append(sql)
        captured["params"] = parameters
        result = MagicMock()
        if "system.columns" in sql:
            result.result_rows = columns
            result.column_names = ["name", "type"]
        else:
            result.result_rows = []
            result.column_names = []
        return result
    client = MagicMock()
    client.query = MagicMock(side_effect=fake_query)
    return cfg, client


def test_start_at_now_skips_history_on_empty_watermark():
    """An empty watermark + start_at='now' should produce a cursor at
    'now' rather than epoch.  Bootstrap dup fix: prevents the new
    uploader from re-shipping every historical row a previous
    uploader has already shipped."""
    columns = [("time", "DateTime"), ("call", "String")]
    captured: dict = {}
    cfg, client = _stub_schema_and_client(columns, captured)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        start_at="now",
        config=cfg, client_factory=lambda c: client,
    )
    before = datetime.now(timezone.utc)
    list(src.iter_batches(b"", limit=10))
    after = datetime.now(timezone.utc)

    cursor_iso = captured["params"]["cursor_value"]
    parsed = datetime.strptime(
        cursor_iso, "%Y-%m-%d %H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)
    # The cursor format truncates microseconds to milliseconds; allow
    # a 2 ms slack on either side so the comparison is meaningful.
    from datetime import timedelta
    assert before - timedelta(milliseconds=2) <= parsed <= after
    # Tiebreak is MAX_UINT64 so rows AT exactly cursor_iso are excluded.
    assert captured["params"]["tiebreak"] == (1 << 64) - 1


def test_start_at_specific_datetime_used_verbatim():
    columns = [("time", "DateTime"), ("call", "String")]
    captured: dict = {}
    cfg, client = _stub_schema_and_client(columns, captured)
    when = datetime(2026, 5, 10, 17, 0, 0, tzinfo=timezone.utc)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        start_at=when,
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    assert captured["params"]["cursor_value"] == "2026-05-10 17:00:00.000"
    assert captured["params"]["tiebreak"] == (1 << 64) - 1


def test_start_at_iso_string_used_verbatim():
    columns = [("time", "DateTime"), ("call", "String")]
    captured: dict = {}
    cfg, client = _stub_schema_and_client(columns, captured)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        start_at="2026-05-10 17:00:00.000",
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    assert captured["params"]["cursor_value"] == "2026-05-10 17:00:00.000"


def test_start_at_ignored_when_watermark_non_empty():
    """The persisted cursor wins.  start_at is only consulted when the
    incoming cursor is empty — once a watermark exists, restarts should
    resume from where they left off, not jump forward."""
    columns = [("time", "DateTime"), ("call", "String")]
    captured: dict = {}
    cfg, client = _stub_schema_and_client(columns, captured)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        start_at="now",
        config=cfg, client_factory=lambda c: client,
    )
    persisted = _Cursor(
        time_iso="2026-05-10 12:00:00.000", tiebreak=999,
    ).to_bytes()
    list(src.iter_batches(persisted, limit=10))
    assert captured["params"]["cursor_value"] == "2026-05-10 12:00:00.000"
    assert captured["params"]["tiebreak"] == 999


def test_start_at_default_preserves_epoch_behaviour():
    """Without start_at, empty watermark still means epoch.
    Back-compat: existing callers' semantics unchanged."""
    columns = [("time", "DateTime"), ("call", "String")]
    captured: dict = {}
    cfg, client = _stub_schema_and_client(columns, captured)
    src = ClickHouseSource(
        database="foo", table="bar",
        accepted_schema_versions=[1],
        primary_key_columns=["time"],
        config=cfg, client_factory=lambda c: client,
    )
    list(src.iter_batches(b"", limit=10))
    assert captured["params"]["cursor_value"] == "1970-01-01 00:00:00.000"
    assert captured["params"]["tiebreak"] == 0
