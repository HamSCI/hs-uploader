"""SqliteSource: queue read, cursor advance, commit-delete, schema check.

Tests use a real in-process sqlite3 connection against either a tmp
file or an in-memory database — no mocking needed, the writer's
schema is small and well-defined.  Producer side is simulated by
direct INSERTs into `pending_uploads` matching what
`sigmond.hamsci_sink.Writer` would emit (target_db, target_table,
schema_version, payload_json, queued_at).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from hs_uploader.sources.sqlite import (
    HEALTH_NOOP,
    HEALTH_OK,
    HEALTH_STALE_SCHEMA,
    HEALTH_UNREACHABLE,
    SqliteSource,
    _ConnectionConfig,
    _Cursor,
)


# ---- fixtures -------------------------------------------------------------

_QUEUE_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
)
"""


def _seed_table(conn: sqlite3.Connection) -> None:
    conn.execute(_QUEUE_DDL)
    conn.execute(
        "CREATE INDEX idx_pending_uploads_target ON pending_uploads "
        "(target_db, target_table, id)"
    )
    conn.commit()


def _insert(
    conn: sqlite3.Connection,
    *,
    target_db: str,
    target_table: str,
    payload: dict,
    schema_version: int = 2,
    queued_at: Optional[str] = None,
) -> int:
    queued_at = queued_at or datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO pending_uploads "
        "(target_db, target_table, schema_version, payload_json, queued_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (target_db, target_table, schema_version, json.dumps(payload), queued_at),
    )
    conn.commit()
    return cur.lastrowid


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "sink.db"


@pytest.fixture
def seeded(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0)
    _seed_table(conn)
    return conn


def _src(db_path: Path, *, schema_versions=(2,), **kwargs) -> SqliteSource:
    """Build a source pointing at `db_path`, defaulted for the
    typical psk.spots case."""
    return SqliteSource(
        database="psk",
        table="spots",
        accepted_schema_versions=list(schema_versions),
        config=_ConnectionConfig(path=str(db_path)),
        **kwargs,
    )


# ---- no-op / unconfigured -------------------------------------------------


def test_noop_when_no_config() -> None:
    src = SqliteSource(
        database="psk", table="spots",
        accepted_schema_versions=[2],
        config=None,
    )
    assert src.health() == HEALTH_NOOP
    assert list(src.iter_batches(b"", 10)) == []
    # commit on no-op is silent / no-op
    src.commit(b"123")


def test_from_env_unset_returns_noop_when_no_default_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGMOND_SQLITE_PATH", raising=False)
    # Patch the default path resolution to a guaranteed-absent file
    # so we don't depend on host state.
    import hs_uploader.sources.sqlite as mod
    monkeypatch.setattr(
        mod._ConnectionConfig, "from_env",
        classmethod(lambda cls, env=None: None),
    )
    src = SqliteSource.from_env(
        database="psk", table="spots",
        accepted_schema_versions=[2],
        env={},
    )
    assert src.health() == HEALTH_NOOP


def test_from_env_sets_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "x.db"
    src = SqliteSource.from_env(
        database="psk", table="spots",
        accepted_schema_versions=[2],
        env={"SIGMOND_SQLITE_PATH": str(path)},
    )
    assert src._config is not None  # noqa: SLF001
    assert src._config.path == str(path)


# ---- happy path -----------------------------------------------------------


def test_iter_batches_yields_records_in_id_order(seeded: sqlite3.Connection, db_path: Path) -> None:
    base = datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(3):
        _insert(
            seeded,
            target_db="psk", target_table="spots",
            payload={
                "tx_call": f"K{i}AA",
                "mode": "ft8",
                "frequency": 14_074_000,
                "time": (base + timedelta(seconds=i)).isoformat(),
                "message": f"msg{i}",
                "radiod_id": "rx888",
            },
        )

    src = _src(db_path)
    batches = list(src.iter_batches(b"", 10))
    assert len(batches) == 1
    batch = batches[0]
    assert len(batch.records) == 3
    assert [r.columns["tx_call"] for r in batch.records] == ["K0AA", "K1AA", "K2AA"]
    assert all(r.table == "psk.spots" for r in batch.records)
    # cursor advances to the last id (3 since the autoincrement starts at 1)
    assert batch.cursor_after == b"3"
    assert batch.commit_token == b"3"
    # Record.time is parsed back from the payload's ISO string
    assert batch.records[0].time == base
    assert src.health() == HEALTH_OK


def test_cursor_advance_skips_already_emitted(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots", payload={"x": 1})
    _insert(seeded, target_db="psk", target_table="spots", payload={"x": 2})

    src = _src(db_path)
    first = list(src.iter_batches(b"", 10))
    assert len(first[0].records) == 2
    cursor = first[0].cursor_after

    # Second call with persisted cursor → no new records (and no new batch)
    second = list(src.iter_batches(cursor, 10))
    assert second == []

    # New row inserted after the cursor → only the new one comes back
    _insert(seeded, target_db="psk", target_table="spots", payload={"x": 3})
    third = list(src.iter_batches(cursor, 10))
    assert len(third) == 1
    assert len(third[0].records) == 1
    assert third[0].records[0].columns["x"] == 3


def test_limit_respected(seeded: sqlite3.Connection, db_path: Path) -> None:
    for i in range(5):
        _insert(seeded, target_db="psk", target_table="spots", payload={"i": i})
    src = _src(db_path)
    batches = list(src.iter_batches(b"", 3))
    assert len(batches[0].records) == 3
    assert batches[0].cursor_after == b"3"


# ---- routing / filtering --------------------------------------------------


def test_other_target_db_table_not_returned(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots", payload={"x": 1})
    _insert(seeded, target_db="wspr", target_table="spots", payload={"x": 2})  # different db
    _insert(seeded, target_db="psk", target_table="noise", payload={"x": 3})    # different table

    src = _src(db_path)
    batches = list(src.iter_batches(b"", 10))
    assert len(batches[0].records) == 1
    assert batches[0].records[0].columns["x"] == 1


def test_extra_where_eq_filter(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"radiod_id": "rx888", "x": 1})
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"radiod_id": "other",  "x": 2})

    src = _src(db_path, extra_where=[("radiod_id", "=", "rx888")])
    batches = list(src.iter_batches(b"", 10))
    assert len(batches[0].records) == 1
    assert batches[0].records[0].columns["x"] == 1


def test_extra_where_in_filter(seeded: sqlite3.Connection, db_path: Path) -> None:
    for mode in ("ft8", "ft4", "jt9"):
        _insert(seeded, target_db="psk", target_table="spots",
                payload={"mode": mode, "x": mode})
    src = _src(db_path, extra_where=[("mode", "IN", ["ft8", "ft4"])])
    rows = list(src.iter_batches(b"", 10))[0].records
    assert sorted(r.columns["x"] for r in rows) == ["ft4", "ft8"]


def test_extra_where_invalid_op_raises() -> None:
    with pytest.raises(ValueError, match="not allowed"):
        SqliteSource(
            database="psk", table="spots",
            accepted_schema_versions=[2],
            extra_where=[("x", "LIKE", "%")],
        )


def test_extra_where_invalid_col_raises() -> None:
    with pytest.raises(ValueError, match="must be alphanumeric"):
        SqliteSource(
            database="psk", table="spots",
            accepted_schema_versions=[2],
            extra_where=[("evil; DROP", "=", "x")],
        )


def test_select_columns_projects_payload(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"a": 1, "b": 2, "c": 3, "tx_call": "ZZ"})
    src = _src(db_path, select_columns=["a", "tx_call"])
    rows = list(src.iter_batches(b"", 10))[0].records
    assert rows[0].columns == {"a": 1, "tx_call": "ZZ"}


# ---- schema handling ------------------------------------------------------


def test_accepted_schema_version_filters(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"x": 1}, schema_version=2)
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"x": 99}, schema_version=99)
    src = _src(db_path, schema_versions=(2,))
    rows = list(src.iter_batches(b"", 10))[0].records
    assert [r.columns["x"] for r in rows] == [1]


def test_stale_schema_promoted_on_unexpected_version(seeded: sqlite3.Connection, db_path: Path) -> None:
    # Only an unaccepted version present → empty result + stale-schema
    _insert(seeded, target_db="psk", target_table="spots",
            payload={"x": 1}, schema_version=99)
    src = _src(db_path, schema_versions=(2,))
    batches = list(src.iter_batches(b"", 10))
    assert batches == []
    assert src.health() == HEALTH_STALE_SCHEMA


def test_missing_table_is_unreachable(db_path: Path) -> None:
    # Don't seed — table doesn't exist yet
    src = _src(db_path)
    batches = list(src.iter_batches(b"", 10))
    assert batches == []
    assert src.health() == HEALTH_UNREACHABLE


# ---- commit deletes rows --------------------------------------------------


def test_commit_deletes_acked_rows(seeded: sqlite3.Connection, db_path: Path) -> None:
    for i in range(3):
        _insert(seeded, target_db="psk", target_table="spots", payload={"i": i})
    _insert(seeded, target_db="wspr", target_table="spots", payload={"i": 999})  # other-tag survivor

    src = _src(db_path)
    batches = list(src.iter_batches(b"", 10))
    token = batches[0].commit_token

    src.commit(token)

    rows = seeded.execute(
        "SELECT id, target_db FROM pending_uploads ORDER BY id"
    ).fetchall()
    # The 3 psk.spots rows are gone, the wspr.spots row remains.
    assert rows == [(4, "wspr")]


def test_commit_empty_token_is_noop(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots", payload={"i": 1})
    src = _src(db_path)
    src.commit(b"")
    n = seeded.execute("SELECT COUNT(*) FROM pending_uploads").fetchone()[0]
    assert n == 1


def test_commit_malformed_token_warns_no_delete(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots", payload={"i": 1})
    src = _src(db_path)
    src.commit(b"not-an-int")
    n = seeded.execute("SELECT COUNT(*) FROM pending_uploads").fetchone()[0]
    assert n == 1


def test_commit_with_delete_disabled_leaves_rows_in_place(
    seeded: sqlite3.Connection, db_path: Path,
) -> None:
    """When two pipelines consume the same logical (database, table)
    queue — wsprdaemon.org + wsprnet.org both reading wspr.spots —
    DELETE-on-ack races them.  delete_on_commit=False makes commit() a
    no-op so a separate retention janitor handles cleanup."""
    for i in range(3):
        _insert(seeded, target_db="psk", target_table="spots", payload={"i": i})
    src = _src(db_path, delete_on_commit=False)
    batches = list(src.iter_batches(b"", 10))
    token = batches[0].commit_token
    assert token  # batch produced a non-empty commit_token

    src.commit(token)

    rows = seeded.execute("SELECT COUNT(*) FROM pending_uploads").fetchone()[0]
    # All 3 rows still present — the watermark cursor advances but
    # the queue is untouched.
    assert rows == 3


# ---- start_at -------------------------------------------------------------


def test_start_at_now_skips_existing_queue(seeded: sqlite3.Connection, db_path: Path) -> None:
    for i in range(3):
        _insert(seeded, target_db="psk", target_table="spots", payload={"i": i})

    src = _src(db_path, start_at="now")
    # Empty cursor + start_at="now" → cursor becomes max(id), first
    # iter_batches yields nothing.
    first = list(src.iter_batches(b"", 10))
    assert first == []

    # A new row inserted after start_at IS shipped.
    _insert(seeded, target_db="psk", target_table="spots", payload={"i": 99})
    second = list(src.iter_batches(b"", 10))
    assert len(second[0].records) == 1
    assert second[0].records[0].columns["i"] == 99


def test_start_at_with_non_empty_cursor_is_ignored(seeded: sqlite3.Connection, db_path: Path) -> None:
    _insert(seeded, target_db="psk", target_table="spots", payload={"i": 1})
    _insert(seeded, target_db="psk", target_table="spots", payload={"i": 2})

    src = _src(db_path, start_at="now")
    rows = list(src.iter_batches(b"1", 10))[0].records
    # cursor=b"1" means "skip id<=1", start_at is ignored — should get id=2 only.
    assert len(rows) == 1
    assert rows[0].columns["i"] == 2


# ---- cursor (de)serialisation ---------------------------------------------


def test_cursor_roundtrip() -> None:
    c = _Cursor(last_id=12345)
    assert _Cursor.from_bytes(c.to_bytes()).last_id == 12345
    assert _Cursor.from_bytes(b"").last_id == 0


def test_cursor_rejects_malformed_bytes() -> None:
    with pytest.raises(ValueError):
        _Cursor.from_bytes(b"not a number")


# ---------------------------------------------------------------------------
# Max-key-wins dedup (multi-RX888 plan, phase 5 follow-up — task #43)
# ---------------------------------------------------------------------------

def test_dedup_collapses_partition_to_max_order_value(
    seeded: sqlite3.Connection, db_path: Path,
) -> None:
    """Two records with the same (time, callsign, frequency_hz) should
    yield only the one with the highest snr_db."""
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z",
        "callsign": "W1AW", "frequency_hz": 14097100,
        "snr_db": -15, "rx_source": "radiod:host-a",
    })
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z",
        "callsign": "W1AW", "frequency_hz": 14097100,
        "snr_db": -3,  "rx_source": "radiod:host-b",
    })
    # And a non-duplicate
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z",
        "callsign": "K9XX", "frequency_hz": 14097200,
        "snr_db": -8, "rx_source": "radiod:host-a",
    })
    src = SqliteSource(
        database="wspr", table="spots", accepted_schema_versions=[2],
        config=_ConnectionConfig(path=str(db_path)),
        dedup_partition_by=("time", "callsign", "frequency_hz"),
        dedup_order_by_desc="snr_db",
    )
    batches = list(src.iter_batches(b"", 10))
    assert len(batches) == 1
    cols = [r.columns for r in batches[0].records]
    # W1AW collapses to one (the -3 winner); K9XX passes through
    snrs = sorted(c["snr_db"] for c in cols)
    calls = sorted(c["callsign"] for c in cols)
    assert calls == ["K9XX", "W1AW"]
    assert -3 in snrs and -8 in snrs
    assert -15 not in snrs


def test_dedup_does_not_yield_loser_rows_after_winner_committed(
    seeded: sqlite3.Connection, db_path: Path,
) -> None:
    """Once a partition's winner has been shipped, the loser rows
    stay behind in pending_uploads (we don't delete them — sibling
    pipelines may need them) but the dedup query must NEVER yield
    them on subsequent polls."""
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -3,
    })
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -15,
    })
    src = SqliteSource(
        database="wspr", table="spots", accepted_schema_versions=[2],
        config=_ConnectionConfig(path=str(db_path)),
        delete_on_commit=False,    # shared with sibling pipeline
        dedup_partition_by=("time", "callsign", "frequency_hz"),
        dedup_order_by_desc="snr_db",
    )
    batches = list(src.iter_batches(b"", 10))
    assert len(batches[0].records) == 1
    # Commit the winner
    src.commit(batches[0].commit_token)
    # Next poll: nothing new (loser is rn=2 in its partition)
    batches2 = list(src.iter_batches(batches[0].cursor_after, 10))
    assert all(len(b.records) == 0 for b in batches2)


def test_dedup_off_yields_every_row(
    seeded: sqlite3.Connection, db_path: Path,
) -> None:
    """Sanity: with dedup params unset, the source yields every row
    (matches the wsprdaemon-tar diversity feed's behaviour on the
    same shared queue)."""
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -3,
    })
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -15,
    })
    src = SqliteSource(
        database="wspr", table="spots", accepted_schema_versions=[2],
        config=_ConnectionConfig(path=str(db_path)),
    )
    batches = list(src.iter_batches(b"", 10))
    assert len(batches[0].records) == 2


def test_dedup_tiebreak_keeps_earliest_id_on_equal_snr(
    seeded: sqlite3.Connection, db_path: Path,
) -> None:
    """Two records with identical SNR — the earlier id wins (first
    write wins, deterministic).  Matches the wsprnet_audit table's
    INSERT OR IGNORE first-write-wins semantics so the audit and
    transport agree on which receiver 'owned' the spot."""
    id_a = _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -7,
        "rx_source": "first",
    })
    _insert(seeded, target_db="wspr", target_table="spots", payload={
        "time": "2026-05-19T22:00:00Z", "callsign": "W1AW",
        "frequency_hz": 14097100, "snr_db": -7,
        "rx_source": "second",
    })
    src = SqliteSource(
        database="wspr", table="spots", accepted_schema_versions=[2],
        config=_ConnectionConfig(path=str(db_path)),
        dedup_partition_by=("time", "callsign", "frequency_hz"),
        dedup_order_by_desc="snr_db",
    )
    batches = list(src.iter_batches(b"", 10))
    assert len(batches[0].records) == 1
    # Winner is the first-inserted row
    assert batches[0].records[0].columns["rx_source"] == "first"


def test_dedup_partition_and_order_must_both_be_set() -> None:
    """Programmer-error guard: setting one without the other is
    meaningless; reject at construction time."""
    with pytest.raises(ValueError, match="set together"):
        SqliteSource(
            database="wspr", table="spots", accepted_schema_versions=[2],
            dedup_partition_by=("time",),
            # dedup_order_by_desc missing
        )


def test_dedup_field_names_rejected_when_unsafe() -> None:
    """The dedup fields go into SQL directly — reject anything that
    isn't [A-Za-z0-9_] to defend against trivial typos / injection."""
    with pytest.raises(ValueError, match="alphanumeric"):
        SqliteSource(
            database="wspr", table="spots", accepted_schema_versions=[2],
            dedup_partition_by=("time", "call sign"),
            dedup_order_by_desc="snr_db",
        )
    with pytest.raises(ValueError, match="alphanumeric"):
        SqliteSource(
            database="wspr", table="spots", accepted_schema_versions=[2],
            dedup_partition_by=("time",),
            dedup_order_by_desc="snr; drop table--",
        )
