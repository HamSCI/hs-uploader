"""Every source skips rows marked `local = 1` (spec-sink-2b-v377 §5.2, test 16).

No writer sets `local = 1` yet, so the fixtures write it with plain SQL.
Each fixture holds pairs of rows that differ only in `local`.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hs_uploader.sink import PENDING_UPLOADS_DDL
from hs_uploader.sources.sqlite import SqliteSource, _ConnectionConfig
from hs_uploader.sources.wspr_cycle import WsprCycleSource

_OLD_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
)
"""


def _db(path: Path, *, column: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(PENDING_UPLOADS_DDL if column else _OLD_DDL)
    return conn


def _add(conn, db, table, payload, *, local=0, producer="") -> int:
    if conn.execute("SELECT 1 FROM pragma_table_info('pending_uploads') "
                    "WHERE name='local'").fetchone():
        cur = conn.execute(
            "INSERT INTO pending_uploads (target_db, target_table, "
            "schema_version, payload_json, queued_at, producer, local) "
            "VALUES (?,?,2,?,'2026-10-21T00:00:00Z',?,?)",
            (db, table, json.dumps(payload), producer, local))
    else:
        cur = conn.execute(
            "INSERT INTO pending_uploads (target_db, target_table, "
            "schema_version, payload_json, queued_at) "
            "VALUES (?,?,2,?,'2026-10-21T00:00:00Z')",
            (db, table, json.dumps(payload)))
    conn.commit()
    return cur.lastrowid


def _src(path: Path, **kw) -> SqliteSource:
    return SqliteSource(
        database="psk", table="spots", accepted_schema_versions=[2],
        config=_ConnectionConfig(path=str(path)), **kw)


def _ids(src, cursor=b"") -> list[int]:
    out = []
    for b in src.iter_batches(cursor, 100):
        out += [r.columns["_id"] for r in b.records]
    return out


def _pairs(conn) -> tuple[list[int], list[int]]:
    """Rows 1..4, as (kept, local): each pair differs only in `local`."""
    kept, local = [], []
    for n in range(2):
        kept.append(_add(conn, "psk", "spots", {"n": n}, producer="psk-recorder"))
        local.append(_add(conn, "psk", "spots", {"n": n}, producer="psk-recorder",
                          local=1))
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id)")
    conn.commit()
    return kept, local


def test_batch_holds_no_local_row_without_producer(tmp_path):
    conn = _db(tmp_path / "s.db")
    kept, _ = _pairs(conn)
    assert _ids(_src(tmp_path / "s.db")) == kept


def test_batch_holds_no_local_row_with_producer(tmp_path):
    conn = _db(tmp_path / "s.db")
    kept, _ = _pairs(conn)
    assert _ids(_src(tmp_path / "s.db", producer="psk-recorder")) == kept


def test_dedup_local_row_with_higher_snr_never_wins(tmp_path):
    conn = _db(tmp_path / "s.db")
    key = {"time": "t", "callsign": "W1AW", "frequency_hz": 1}
    a = _add(conn, "psk", "spots", {**key, "snr_db": -15})
    _add(conn, "psk", "spots", {**key, "snr_db": 20}, local=1)
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id)")
    conn.commit()
    src = _src(tmp_path / "s.db",
               dedup_partition_by=("time", "callsign", "frequency_hz"),
               dedup_order_by_desc="snr_db")
    assert _ids(src) == [a]


def test_start_at_now_anchor_ignores_local_row(tmp_path):
    conn = _db(tmp_path / "s.db")
    _add(conn, "psk", "spots", {"x": 1})
    _add(conn, "psk", "spots", {"x": 2}, local=1)     # highest id, local
    src = _src(tmp_path / "s.db", start_at="now")
    assert src._cursor_from_start_at().last_id == 1


def test_commit_delete_spares_local_row(tmp_path):
    conn = _db(tmp_path / "s.db")
    kept, local = _pairs(conn)
    src = _src(tmp_path / "s.db", delete_on_commit=True)
    src.commit(str(max(kept + local)).encode())
    left = [r[0] for r in conn.execute("SELECT id FROM pending_uploads")]
    assert left == local


def test_commit_delete_spares_local_row_with_producer(tmp_path):
    conn = _db(tmp_path / "s.db")
    kept, local = _pairs(conn)
    src = _src(tmp_path / "s.db", delete_on_commit=True, producer="psk-recorder")
    src.commit(str(max(kept + local)).encode())
    left = [r[0] for r in conn.execute("SELECT id FROM pending_uploads")]
    assert left == local


def test_sqlite_source_without_local_column_behaves_as_before(tmp_path):
    conn = _db(tmp_path / "s.db", column=False)
    ids = [_add(conn, "psk", "spots", {"n": n}) for n in range(3)]
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id)")
    conn.commit()
    src = _src(tmp_path / "s.db", delete_on_commit=True, start_at="now")
    assert src._cursor_from_start_at().last_id == 3
    src2 = _src(tmp_path / "s.db", delete_on_commit=True)
    assert _ids(src2) == ids
    src2.commit(b"3")
    assert conn.execute("SELECT COUNT(*) FROM pending_uploads").fetchone()[0] == 0


def test_sqlite_source_today_every_row_local_zero_matches_unfiltered(tmp_path):
    conn = _db(tmp_path / "s.db")
    for n in range(5):
        _add(conn, "psk", "spots", {"n": n})
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id)")
    conn.commit()
    new = _ids(_src(tmp_path / "s.db"))
    unfiltered = [r[0] for r in conn.execute(
        "SELECT id FROM pending_uploads WHERE target_db='psk' "
        "AND target_table='spots' AND schema_version IN (2) ORDER BY id")]
    assert new == unfiltered == [1, 2, 3, 4, 5]


def test_probe_runs_once_per_connection(tmp_path):
    conn = _db(tmp_path / "s.db")
    _pairs(conn)
    src = _src(tmp_path / "s.db")
    probes = []
    real = src._connect()
    class Spy:
        def __init__(self, c): self.c = c
        def execute(self, sql, *a):
            if "table_info" in sql:
                probes.append(sql)
            return self.c.execute(sql, *a)
    flt = src._local_rows
    spy = Spy(real)
    for _ in range(3):
        flt.clause(spy)   # same object each time
    assert len(probes) == 1


# ---- WsprCycleSource ------------------------------------------------------

C1 = "2026-10-21T00:00:00Z"
C2 = "2026-10-21T00:02:00Z"   # holds only local rows
C3 = "2026-10-21T00:04:00Z"


def _wspr(conn):
    _add(conn, "wspr", "spots", {"time": C1, "k": "keep-spot"})
    _add(conn, "wspr", "spots", {"time": C1, "k": "local-spot"}, local=1)
    _add(conn, "wspr", "noise", {"time": C1, "k": "local-noise"}, local=1)
    _add(conn, "wspr", "noise", {"time": C1, "k": "keep-noise"})
    _add(conn, "wspr", "spots", {"time": C2, "k": "only-local"}, local=1)
    _add(conn, "wspr", "noise", {"time": C3, "k": "keep-c3"})
    _add(conn, "psk", "spots", {"time": C1, "k": "keep-psk"})
    _add(conn, "psk", "spots", {"time": C1, "k": "local-psk"}, local=1)


def _keys(batches):
    return [[r.columns["k"] for r in b.records] for b in batches]


def test_wspr_cycle_skips_local_rows_in_discovery_fetch_and_rider(tmp_path):
    conn = _db(tmp_path / "s.db")
    _wspr(conn)
    src = WsprCycleSource(db_path=tmp_path / "s.db", start_at="beginning",
                          include_psk=True)
    got = _keys(src.iter_batches(b"", 100))
    # C2 holds only local rows: no batch, so no empty cycle either.
    assert got == [["keep-spot", "keep-noise", "keep-psk"], ["keep-c3"]]


def test_wspr_cycle_discovery_ignores_local_only_cycle(tmp_path):
    conn = _db(tmp_path / "s.db")
    _add(conn, "wspr", "spots", {"time": C1, "k": "a"}, local=1)
    _add(conn, "wspr", "spots", {"time": C3, "k": "b"})
    src = WsprCycleSource(db_path=tmp_path / "s.db", start_at="beginning")
    conn2 = src._connect()
    # Drive the discovery statement through iter_batches, and spy on it.
    seen = []
    conn2.set_trace_callback(seen.append)
    list(src.iter_batches(b"", 100))
    discovery = [s for s in seen if "SELECT DISTINCT" in s]
    assert discovery and all("local = 0" in s for s in discovery)


def test_wspr_cycle_without_local_column_behaves_as_before(tmp_path):
    conn = _db(tmp_path / "s.db", column=False)
    _add(conn, "wspr", "spots", {"time": C1, "k": "a"})
    _add(conn, "psk", "spots", {"time": C1, "k": "p"})
    src = WsprCycleSource(db_path=tmp_path / "s.db", start_at="beginning",
                          include_psk=True)
    assert _keys(src.iter_batches(b"", 100)) == [["a", "p"]]


def test_wspr_cycle_today_all_local_zero_matches_unfiltered(tmp_path):
    conn = _db(tmp_path / "s.db")
    _add(conn, "wspr", "spots", {"time": C1, "k": "a"})
    _add(conn, "wspr", "noise", {"time": C1, "k": "b"})
    _add(conn, "psk", "spots", {"time": C1, "k": "p"})
    src = WsprCycleSource(db_path=tmp_path / "s.db", start_at="beginning",
                          include_psk=True)
    assert _keys(src.iter_batches(b"", 100)) == [["a", "b", "p"]]
