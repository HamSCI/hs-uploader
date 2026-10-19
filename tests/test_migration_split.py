"""Migration 2: the pskreporter send-record split, and the retry pass
(sigmond/tasks/spec-key-split-v373.md §4.4, §6, tests 1-2 of §8).

Migration 2 copies each send record keyed ``sqlite:psk.spots`` with a
pskreporter destination to the two producer keys, keeps the old row and
records each copy in ``superseded_keys``.  The retry pass handles every
deliverable queued under ``psk-pskreporter``: it deletes one whose rows
the sink still holds and moves any other to ``--keep-retry-for``.  It runs
on every migrate, not only the one that applies migration 2.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from hs_uploader.cli import main
from hs_uploader.core import Pipeline, Uploader
from hs_uploader.watermark import SqliteWatermarkStore
from hs_uploader.watermark import schema
from hs_uploader.watermark.schema import MigrateContext
from tests.conftest import MemoryTransport
from tests.test_core_orchestration import _ident
from tests.test_sqlite_source_producer import MS, PSK, _build, _src, _tag_ids

PSK_DEST = "pskreporter-tcp:report.pskreporter.info:4739"
OLD = ("sqlite:psk.spots", PSK_DEST, "psk.spots")
NEW_PSK = ("sqlite:psk.spots@psk-recorder", PSK_DEST, "psk.spots")
NEW_MS = ("sqlite:psk.spots@meteor-scatter", PSK_DEST, "psk.spots")
PSK_PIPE = "psk-recorder-pskreporter"
MS_PIPE = "meteor-scatter-pskreporter"
OLD_ACK = "2026-10-18T12:00:00+00:00"

# Keys migration 2 must never touch.
WSPR_KEY = ("sqlite:wspr.cycle",
            "wsprdaemon-tar-sftp:gw1.wsprdaemon.org,gw2.wsprdaemon.org",
            "wspr.spots")
GRAPE_KEY = ("files:/var/lib/timestd/upload", "psws-grape", "grape.obs")
HEARTBEAT_KEY = ("heartbeat:station", "heartbeat-sftp:wd30", "station.heartbeat")
PSK_TAR_KEY = ("sqlite:psk.spots", "wsprdaemon-tar-sftp:gw1.wsprdaemon.org",
               "psk.spots")


# ---- fixtures -----------------------------------------------------------------


def _version(db: Path) -> int:
    conn = sqlite3.connect(db)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _set_version(db: Path, n: int) -> None:
    conn = sqlite3.connect(db)
    try:
        conn.execute("PRAGMA user_version = %d" % n)
        conn.commit()
    finally:
        conn.close()


def _store_v1(directory: Path, *, cursor: bytes = b"5",
              others: bool = True) -> Path:
    """A store as v3.70-v3.72 leave it: version 1, the shared pskreporter
    record at ``cursor``, and the keys no migration may touch."""
    directory.mkdir(parents=True, exist_ok=True)
    db = directory / "watermarks.db"
    s = SqliteWatermarkStore(db)
    s._write_cursor(*OLD, cursor=cursor, last_ack=OLD_ACK)
    if others:
        s._write_cursor(*WSPR_KEY, cursor=b"2026-10-18T11:58:00Z",
                        last_ack="2026-10-18T12:00:30+00:00")
        s._write_cursor(*GRAPE_KEY, cursor=b"\x00\xff\x10binary",
                        last_ack="2026-10-18T01:10:00+00:00")
        s._write_cursor(*HEARTBEAT_KEY, cursor=b"77",
                        last_ack="2026-10-18T12:01:00+00:00")
        s._write_cursor(*PSK_TAR_KEY, cursor=b"3",
                        last_ack="2026-10-18T11:00:00+00:00")
    s.close()
    _set_version(db, 1)
    return db


def _retry(db: Path, *, pipeline: str = "psk-pskreporter", keyed: bool = True,
           cursor_after: bytes = b"8") -> int:
    s = SqliteWatermarkStore(db)
    try:
        key = dict(source_id=OLD[0], dest_id=OLD[1], table=OLD[2]) if keyed else {}
        return s.enqueue_deliverable(
            pipeline=pipeline, payload_blob=b"IPFIX-" + cursor_after,
            enqueued_at="2026-10-18T12:01:00+00:00",
            next_attempt_at="2026-10-18T12:06:00+00:00",
            cursor_after=cursor_after, commit_token=cursor_after, **key)
    finally:
        s.close()


def _sink(path: Path, rows: list[tuple[int, str, str]]) -> Path:
    """A sink.db holding exactly ``rows`` of (id, target_db, target_table)."""
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE pending_uploads (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "target_db TEXT NOT NULL, target_table TEXT NOT NULL, "
        "schema_version INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL, "
        "queued_at TEXT NOT NULL, producer TEXT NOT NULL DEFAULT '', "
        "local INTEGER NOT NULL DEFAULT 0)")
    conn.executemany(
        "INSERT INTO pending_uploads (id, target_db, target_table, "
        "schema_version, payload_json, queued_at) VALUES (?,?,?,2,'{}','t')",
        rows)
    conn.commit()
    conn.close()
    return path


def _psk_rows(ids) -> list[tuple[int, str, str]]:
    return [(i, "psk", "spots") for i in ids]


def _watermarks(db: Path) -> dict:
    conn = sqlite3.connect(db)
    try:
        return {(r[0], r[1], r[2]): (bytes(r[3]), r[4]) for r in conn.execute(
            "SELECT source_id, dest_id, table_name, cursor, last_ack "
            "FROM watermarks")}
    finally:
        conn.close()


def _deliverables(db: Path) -> list[tuple]:
    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            "SELECT id, pipeline, payload_blob, enqueued_at, attempts, "
            "next_attempt_at, source_id, dest_id, table_name, cursor_after, "
            "commit_token FROM deliverables ORDER BY id").fetchall()
    finally:
        conn.close()


def _table(db: Path, name: str) -> list[tuple]:
    conn = sqlite3.connect(db)
    try:
        return conn.execute(f'SELECT * FROM "{name}" ORDER BY rowid').fetchall()
    finally:
        conn.close()


# ---- migration 2: the records ----------------------------------------------------


def test_schema_version_is_2():
    assert schema.SCHEMA_VERSION == 2
    assert [m.version for m in schema.MIGRATIONS] == [1, 2]


def test_copies_old_record_to_both_new_keys(tmp_path):
    db = _store_v1(tmp_path)

    report = schema.migrate(str(db))

    assert (report.from_version, report.to_version) == (1, 2)
    assert report.applied == [schema.MIGRATIONS[1].name]
    rows = _watermarks(db)
    assert rows[OLD] == (b"5", OLD_ACK)                 # kept, frozen
    assert rows[NEW_PSK] == rows[OLD]
    assert rows[NEW_MS] == rows[OLD]
    sup = _table(db, "superseded_keys")
    assert sorted(r[:6] for r in sup) == sorted([(*OLD, *NEW_MS), (*OLD, *NEW_PSK)])
    assert all(r[6] for r in sup)                       # migrated_at stamped
    text = "\n".join(report.lines)
    for key in (OLD, NEW_PSK, NEW_MS):
        assert key[0] in text
    assert PSK_DEST in text and "5" in text


def test_existing_new_key_untouched(tmp_path):
    db = _store_v1(tmp_path)
    s = SqliteWatermarkStore(db)
    s._write_cursor(*NEW_PSK, cursor=b"9", last_ack="2026-10-18T12:30:00+00:00")
    s.close()

    report = schema.migrate(str(db))

    rows = _watermarks(db)
    assert rows[NEW_PSK] == (b"9", "2026-10-18T12:30:00+00:00")
    assert rows[NEW_MS] == (b"5", OLD_ACK)
    assert rows[OLD] == (b"5", OLD_ACK)
    assert any(NEW_PSK[0] in line and "exist" in line for line in report.lines)


def test_other_keys_untouched(tmp_path):
    db = _store_v1(tmp_path)
    before = _watermarks(db)
    attempts_before = _table(db, "attempts")

    schema.migrate(str(db))

    after = _watermarks(db)
    assert set(after) == set(before) | {NEW_PSK, NEW_MS}
    for key in (WSPR_KEY, GRAPE_KEY, HEARTBEAT_KEY, PSK_TAR_KEY, OLD):
        assert after[key] == before[key]
    assert _table(db, "attempts") == attempts_before


def test_a_second_pskreporter_destination_splits_too(tmp_path):
    db = _store_v1(tmp_path)
    other = ("sqlite:psk.spots", "pskreporter-udp:report.pskreporter.info:4739",
             "psk.spots")
    s = SqliteWatermarkStore(db)
    s._write_cursor(*other, cursor=b"4", last_ack=OLD_ACK)
    s.close()

    schema.migrate(str(db))

    rows = _watermarks(db)
    assert rows[("sqlite:psk.spots@psk-recorder", other[1], other[2])] == (b"4", OLD_ACK)
    assert rows[("sqlite:psk.spots@meteor-scatter", other[1], other[2])] == (b"4", OLD_ACK)
    assert len(_table(db, "superseded_keys")) == 4


def test_a_fresh_store_has_the_superseded_table(tmp_path):
    db = tmp_path / "watermarks.db"
    SqliteWatermarkStore(db).close()
    assert _version(db) == 2
    assert _table(db, "superseded_keys") == []


# ---- the retry pass ----------------------------------------------------------------


def test_retry_deleted_when_sink_holds_rows(tmp_path):
    db = _store_v1(tmp_path)
    rid = _retry(db)
    sink = _sink(tmp_path / "sink.db", _psk_rows(range(5, 11)))

    report = schema.migrate(str(db), ctx=MigrateContext(
        sink_path=sink, keep_retry_for=PSK_PIPE))

    assert _deliverables(db) == []
    assert any("deleted" in line and str(rid) in line for line in report.lines)


def test_retry_moved_when_rows_aged_out(tmp_path):
    db = _store_v1(tmp_path)
    rid = _retry(db)
    before = _deliverables(db)[0]
    # The rows up to the old record (5) were trimmed.  Rows at or before it
    # in another table do not count: only psk.spots rows tell.
    sink = _sink(tmp_path / "sink.db",
                 [(2, "wspr", "spots"), (5, "psk", "msk144"), *_psk_rows(range(6, 11))])

    report = schema.migrate(str(db), ctx=MigrateContext(
        sink_path=sink, keep_retry_for=PSK_PIPE))

    (after,) = _deliverables(db)
    assert after[0] == rid
    assert after[1] == PSK_PIPE
    assert after[6:9] == (NEW_PSK[0], OLD[1], OLD[2])
    # payload, times, attempts, cursor_after and commit_token travel unchanged
    assert after[2:6] == before[2:6]
    assert after[9:] == before[9:]
    assert any("moved" in line and PSK_PIPE in line for line in report.lines)


def test_retry_moved_when_no_sink(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db)

    schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=PSK_PIPE))

    (after,) = _deliverables(db)
    assert after[1] == PSK_PIPE and after[6] == NEW_PSK[0]


def test_retry_moved_to_meteor_scatter_on_an_ms_only_host(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db)

    schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=MS_PIPE))

    (after,) = _deliverables(db)
    assert after[1] == MS_PIPE and after[6] == NEW_MS[0]


def test_sink_without_the_rows_or_the_table_or_the_file_moves(tmp_path):
    for i, sink in enumerate((
        tmp_path / "absent.db",                               # no file
        _sink(tmp_path / "empty.db", []),                     # no rows
    )):
        db = _store_v1(tmp_path / f"s{i}")
        _retry(db)
        schema.migrate(str(db), ctx=MigrateContext(
            sink_path=sink, keep_retry_for=PSK_PIPE))
        assert _deliverables(db)[0][1] == PSK_PIPE
    assert not (tmp_path / "absent.db").exists()              # never created


def test_undecodable_or_empty_old_cursor_moves(tmp_path):
    for i, cursor in enumerate((b"not-a-number", b"")):
        db = _store_v1(tmp_path / f"c{i}", cursor=cursor)
        _retry(db)
        sink = _sink(tmp_path / f"sink{i}.db", _psk_rows(range(1, 11)))
        schema.migrate(str(db), ctx=MigrateContext(
            sink_path=sink, keep_retry_for=PSK_PIPE))
        assert _deliverables(db)[0][1] == PSK_PIPE


def test_retry_stays_without_keep_retry_for(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db)
    before = _deliverables(db)

    report = schema.migrate(str(db))

    assert _deliverables(db) == before
    assert any("stays" in line for line in report.lines)


def test_keyless_retry_handled(tmp_path):
    # Deleted: the one pskreporter record names the old cursor.
    db = _store_v1(tmp_path / "a")
    _retry(db, keyed=False)
    sink = _sink(tmp_path / "sink.db", _psk_rows(range(5, 11)))
    schema.migrate(str(db), ctx=MigrateContext(sink_path=sink,
                                               keep_retry_for=PSK_PIPE))
    assert _deliverables(db) == []

    # Moved: no sink.  It gains the new source part; dest and table stay
    # empty, and the replay falls back to the pipeline's own.
    db = _store_v1(tmp_path / "b")
    _retry(db, keyed=False)
    schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=PSK_PIPE))
    (after,) = _deliverables(db)
    assert after[1] == PSK_PIPE
    assert after[6:9] == (NEW_PSK[0], "", "")


def test_retry_pass_repeats_at_version_2(tmp_path, monkeypatch):
    db = _store_v1(tmp_path)
    schema.migrate(str(db))
    assert _version(db) == 2
    rid = _retry(db)          # a rolled-back daemon queued it

    report = schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=PSK_PIPE))

    assert report.applied == []
    assert (report.from_version, report.to_version) == (2, 2)
    (after,) = _deliverables(db)
    assert (after[0], after[1]) == (rid, PSK_PIPE)

    # Now nothing is left to do: migrate takes no write lock.
    monkeypatch.setattr(schema, "BUSY_TIMEOUT_S", 0.2)
    holder = sqlite3.connect(db, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        again = schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=PSK_PIPE))
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert again.applied == [] and again.lines == []


def test_a_retry_that_stays_takes_no_write_lock(tmp_path, monkeypatch):
    db = _store_v1(tmp_path)
    schema.migrate(str(db))
    _retry(db)
    monkeypatch.setattr(schema, "BUSY_TIMEOUT_S", 0.2)
    holder = sqlite3.connect(db, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        report = schema.migrate(str(db))
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert any("stays" in line for line in report.lines)


def test_other_pipelines_retries_untouched(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db, pipeline="grape-psws", cursor_after=b"g1")
    _retry(db, pipeline=PSK_PIPE, cursor_after=b"p1")
    before = _deliverables(db)

    schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=MS_PIPE))

    assert _deliverables(db) == before


def test_dead_letters_untouched(tmp_path):
    db = _store_v1(tmp_path)
    s = SqliteWatermarkStore(db)
    s.send_to_dead_letter(ts="2026-10-18T11:00:00+00:00",
                          pipeline="psk-pskreporter", payload_blob=b"IPFIX",
                          final_error="refused")
    s.close()
    before = _table(db, "dead_letter")

    schema.migrate(str(db), ctx=MigrateContext(keep_retry_for=PSK_PIPE))

    assert _table(db, "dead_letter") == before


def test_keep_retry_for_must_name_a_new_pskreporter_pipeline(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db)
    before = db.read_bytes()
    with pytest.raises(ValueError, match="keep-retry-for"):
        schema.migrate(str(db), ctx=MigrateContext(keep_retry_for="wspr-wsprnet"))
    assert db.read_bytes() == before


def test_check_reports_without_writing(tmp_path):
    db = _store_v1(tmp_path)
    _retry(db)
    sink = _sink(tmp_path / "sink.db", _psk_rows(range(5, 11)))
    before = db.read_bytes()
    listing = sorted(p.name for p in tmp_path.iterdir())

    report = schema.migrate(str(db), check=True, ctx=MigrateContext(
        sink_path=sink, keep_retry_for=PSK_PIPE))

    assert db.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == listing
    assert report.pending == [schema.MIGRATIONS[1].name]
    text = "\n".join(report.lines)
    assert NEW_PSK[0] in text and NEW_MS[0] in text
    assert "would delete" in text

    # At version 2, --check still reports the retry pass.
    schema.migrate(str(db))
    _retry(db)
    before = db.read_bytes()
    report = schema.migrate(str(db), check=True,
                            ctx=MigrateContext(keep_retry_for=MS_PIPE))
    assert db.read_bytes() == before
    assert report.pending == []
    assert any("would move" in line and MS_PIPE in line for line in report.lines)


def test_cli_passes_sink_and_keep_retry_for(tmp_path, capsys):
    db = _store_v1(tmp_path)
    _retry(db)
    sink = _sink(tmp_path / "sink.db", _psk_rows(range(6, 11)))

    rc = main(["migrate", "--db", str(db), "--sink", str(sink),
               "--keep-retry-for", MS_PIPE])

    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "watermarks.db: version 2"
    assert any("applied 2:" in line for line in out)
    assert any("moved" in line and MS_PIPE in line for line in out)
    assert any(NEW_PSK[0] in line for line in out)
    assert _deliverables(db)[0][1] == MS_PIPE

    rc = main(["migrate", "--db", str(db), "--keep-retry-for", "bogus"])
    assert rc == 1


def test_check_exit_code_says_whether_a_migrate_would_write(tmp_path, capsys):
    # sigmond's manifest step stops the daemon only when this says 3.
    db = _store_v1(tmp_path)
    assert main(["migrate", "--db", str(db), "--check"]) == 3   # migration 2
    assert main(["migrate", "--db", str(db)]) == 0
    assert main(["migrate", "--db", str(db), "--check"]) == 0   # current
    _retry(db)
    # A retry with nowhere to go stays: a migrate would write nothing.
    assert main(["migrate", "--db", str(db), "--check"]) == 0
    # The same retry, with a pipeline to move to: a migrate would move it.
    assert main(["migrate", "--db", str(db), "--check",
                 "--keep-retry-for", MS_PIPE]) == 3
    capsys.readouterr()


# ---- end to end: neither re-send nor skip ------------------------------------------


class _Pskreporter(MemoryTransport):
    """A stand-in pskreporter: records every spot, acks every batch."""

    name = PSK_DEST

    def __init__(self):
        super().__init__("psk.spots", accepts_versions=(2,))


@pytest.mark.parametrize("with_column", [True, False],
                         ids=["producer-column", "no-producer-column"])
def test_end_to_end_no_resend_no_skip(tmp_path, with_column):
    sink = tmp_path / "sink.db"
    truth = _build(sink, with_column=with_column)   # FT8, FT4, MSK144, '' rows
    _tag_ids(sink)
    old_cursor = sorted(truth)[len(truth) // 2]       # the record mid-table
    db = _store_v1(tmp_path / "state", cursor=str(old_cursor).encode())

    schema.migrate(str(db), ctx=MigrateContext(sink_path=sink,
                                               keep_retry_for=PSK_PIPE))

    store = SqliteWatermarkStore(db)
    pipes, transports = [], {}
    for name, producer in ((PSK_PIPE, PSK), (MS_PIPE, MS)):
        transports[producer] = _Pskreporter()
        pipes.append(Pipeline(
            name=name, source=_src(sink, producer=producer, delete_on_commit=False),
            transport=transports[producer], watermark=store, identity=_ident(),
            batch_limit=2))
    uploader = Uploader(pipes)
    try:
        uploader.pump_until_idle()
    finally:
        uploader.close()

    for producer, transport in transports.items():
        shipped = [r.columns["_id"] for b in transport.shipped for r in b.records]
        expected = sorted(i for i, t in truth.items()
                          if i > old_cursor and t["owner"] == producer)
        assert sorted(shipped) == expected            # exactly once each
        assert len(shipped) == len(set(shipped))
    assert transports[PSK].shipped and transports[MS].shipped


# ---- every migrate fills a missing producer key (final review I1) -------------------


def _store_v2_shared(directory: Path, *, cursor: bytes = b"5") -> Path:
    """A store the daemon created (born at version 2, no producer keys),
    which a shared ``psk-pskreporter`` pipeline then used."""
    directory.mkdir(parents=True, exist_ok=True)
    db = directory / "watermarks.db"
    s = SqliteWatermarkStore(db)
    assert s.schema_version() == 2
    s._write_cursor(*OLD, cursor=cursor, last_ack=OLD_ACK)
    s.close()
    return db


def test_every_migrate_copies_the_old_record_to_a_missing_producer_key(tmp_path):
    db = _store_v2_shared(tmp_path)
    s = SqliteWatermarkStore(db)
    s._write_cursor(*NEW_PSK, cursor=b"9", last_ack="2026-10-18T12:30:00+00:00")
    s.close()

    report = schema.migrate(str(db))

    assert report.applied == []
    rows = _watermarks(db)
    assert rows[NEW_PSK] == (b"9", "2026-10-18T12:30:00+00:00")   # never touched
    assert rows[NEW_MS] == (b"5", OLD_ACK)                        # filled
    assert rows[OLD] == (b"5", OLD_ACK)
    assert [r[:6] for r in _table(db, "superseded_keys")] == [(*OLD, *NEW_MS)]
    assert any("copied" in line and NEW_MS[0] in line for line in report.lines)

    # Nothing missing now: the next migrate writes nothing.
    assert schema.migrate(str(db)).lines == []


def test_check_reports_a_missing_producer_key_as_pending(tmp_path, capsys):
    db = _store_v2_shared(tmp_path)
    before = db.read_bytes()

    report = schema.migrate(str(db), check=True)

    assert db.read_bytes() == before
    assert report.pending == [] and report.would_write
    assert any("would copy" in line and NEW_PSK[0] in line for line in report.lines)
    assert main(["migrate", "--db", str(db), "--check"]) == 3
    assert main(["migrate", "--db", str(db)]) == 0
    assert main(["migrate", "--db", str(db), "--check"]) == 0
    capsys.readouterr()


def test_retry_moved_not_deleted_while_a_producer_key_is_missing(tmp_path,
                                                                monkeypatch):
    # The guard on its own: with the copy disabled, the sink's proof alone
    # must not delete the retry.
    monkeypatch.setattr(schema, "_fill_missing_producer_keys",
                        lambda conn, *, write: [])
    db = _store_v2_shared(tmp_path)
    _retry(db)
    sink = _sink(tmp_path / "sink.db", _psk_rows(range(5, 11)))

    report = schema.migrate(str(db), ctx=MigrateContext(
        sink_path=sink, keep_retry_for=PSK_PIPE))

    (after,) = _deliverables(db)
    assert after[1] == PSK_PIPE
    assert any("moved" in line and "producer key" in line
               for line in report.lines)


def test_v2_born_shared_store_splits_without_a_skip(tmp_path):
    # Final review I1, its reproduction: a version-2 store with no producer
    # keys, used by the shared pipeline, then split with a queued retry.
    sink = tmp_path / "sink.db"
    truth = _build(sink, with_column=True)
    _tag_ids(sink)
    ids = sorted(truth)
    L, X = ids[len(ids) // 3], ids[2 * len(ids) // 3]
    db = _store_v2_shared(tmp_path / "state", cursor=str(L).encode())
    s = SqliteWatermarkStore(db)
    s.enqueue_deliverable(
        pipeline="psk-pskreporter", payload_blob=b"RETRY",
        enqueued_at="2026-10-18T12:01:00+00:00",
        next_attempt_at="2000-01-01T00:00:00+00:00",
        cursor_after=str(X).encode(), commit_token=str(X).encode(),
        source_id=OLD[0], dest_id=OLD[1], table=OLD[2])
    s.close()

    schema.migrate(str(db), ctx=MigrateContext(sink_path=sink,
                                               keep_retry_for=PSK_PIPE))

    store = SqliteWatermarkStore(db)
    transports, pipes = {}, []
    for name, producer in ((PSK_PIPE, PSK), (MS_PIPE, MS)):
        transports[producer] = _Pskreporter()
        pipes.append(Pipeline(
            name=name,
            source=_src(sink, producer=producer, delete_on_commit=False,
                        start_at="now"),
            transport=transports[producer], watermark=store, identity=_ident(),
            batch_limit=2))
    uploader = Uploader(pipes)
    try:
        uploader.pump_until_idle()
    finally:
        uploader.close()

    for producer, transport in transports.items():
        shipped = [r.columns["_id"] for b in transport.shipped for r in b.records]
        replayed = len(transport.replayed)
        expected = sorted(i for i, t in truth.items()
                          if i > L and t["owner"] == producer)
        assert sorted(shipped) == expected, producer   # nothing skipped
        assert len(shipped) == len(set(shipped))
        assert replayed == 0


# ---- end to end: a moved retry replays (spec test 2; final review I2) ---------------


@pytest.mark.parametrize("keep", [PSK_PIPE, MS_PIPE])
def test_a_moved_retry_replays_once_and_advances_the_keep_key(tmp_path, keep):
    sink = tmp_path / "sink.db"
    truth = _build(sink, with_column=True)
    _tag_ids(sink)
    ids = sorted(truth)
    L, X = ids[len(ids) // 3], ids[2 * len(ids) // 3]
    # Trim the rows up to the old record, so the pass must move the retry.
    conn = sqlite3.connect(sink)
    conn.execute("DELETE FROM pending_uploads WHERE id <= ?", (L,))
    conn.commit()
    conn.close()
    db = _store_v1(tmp_path / "state", cursor=str(L).encode())
    s = SqliteWatermarkStore(db)
    s.enqueue_deliverable(
        pipeline="psk-pskreporter", payload_blob=b"RETRY",
        enqueued_at="2026-10-18T12:01:00+00:00",
        next_attempt_at="2000-01-01T00:00:00+00:00",
        cursor_after=str(X).encode(), commit_token=str(X).encode(),
        source_id=OLD[0], dest_id=OLD[1], table=OLD[2])
    s.close()

    report = schema.migrate(str(db), ctx=MigrateContext(sink_path=sink,
                                                        keep_retry_for=keep))
    assert any("moved" in line and keep in line for line in report.lines)

    store = SqliteWatermarkStore(db)
    pipes, transports = [], {}
    for name, producer in ((PSK_PIPE, PSK), (MS_PIPE, MS)):
        transports[name] = _Pskreporter()
        pipes.append(Pipeline(
            name=name, source=_src(sink, producer=producer, delete_on_commit=False),
            transport=transports[name], watermark=store, identity=_ident(),
            batch_limit=2))
    uploader = Uploader(pipes)
    try:
        uploader.pump_until_idle()
    finally:
        uploader.close()

    other = MS_PIPE if keep == PSK_PIPE else PSK_PIPE
    owner = {PSK_PIPE: PSK, MS_PIPE: MS}
    assert transports[keep].replayed == [b"RETRY"]       # once, on the keeper
    assert transports[other].replayed == []
    keep_ids = sorted(r.columns["_id"] for b in transports[keep].shipped
                      for r in b.records)
    other_ids = sorted(r.columns["_id"] for b in transports[other].shipped
                       for r in b.records)
    # The keeper's replay covered (L, X], so its source ships only after X.
    assert keep_ids == sorted(i for i, t in truth.items()
                              if i > X and t["owner"] == owner[keep])
    # The other pipeline re-reads its own rows from the old record.
    assert other_ids == sorted(i for i, t in truth.items()
                               if i > L and t["owner"] == owner[other])
    # The replay ack advanced the keep key, not the frozen old one.
    rows = _watermarks(db)
    assert rows[OLD][0] == str(L).encode()
