"""watermarks.db schema version and `hs-uploader migrate`
(sigmond/tasks/plan-sink-control.md §10.4 item 3, D10).

Every fixture store gets built through SqliteWatermarkStore, the code that
writes the files stations hold today, so a "v0 file" here means exactly
what ND and B4 carry: user_version 0, rows in every table.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from hs_uploader.cli import main
from hs_uploader.watermark import SqliteWatermarkStore
from hs_uploader.watermark import schema

SRC = Path(__file__).resolve().parent.parent / "src"

PSK_KEY = ("sqlite:psk.spots", "pskreporter-tcp:report.pskreporter.info:4739",
           "psk.spots")
WSPR_KEY = ("sqlite:wspr.cycle",
            "wsprdaemon-tar-sftp:gw1.wsprdaemon.org,gw2.wsprdaemon.org",
            "wspr.spots")
GRAPE_KEY = ("files:/var/lib/timestd/upload", "psws-grape", "grape.obs")

# SQLite rewrites only these header bytes when a commit changes nothing but
# user_version: the change counter (24-27), user_version (60-63), and the
# version-valid-for number and library version (92-99).
_HEADER_BYTES_A_VERSION_BUMP_TOUCHES = (
    set(range(24, 28)) | set(range(60, 64)) | set(range(92, 100))
)


def _v0_store(directory: Path, name: str = "watermarks.db") -> Path:
    """A store as v3.69 leaves it: version 0, rows in all four tables."""
    directory.mkdir(parents=True, exist_ok=True)
    db = directory / name
    s = SqliteWatermarkStore(db)
    s.advance_cursor(*PSK_KEY, cursor=b"1204",
                     last_ack="2026-10-07T12:00:00+00:00")
    s.advance_cursor(*WSPR_KEY, cursor=b"2026-10-07T11:58:00Z",
                     last_ack="2026-10-07T12:00:30+00:00")
    s.advance_cursor(*GRAPE_KEY, cursor=b"\x00\xff\x10binary",
                     last_ack="2026-10-07T01:10:00+00:00")
    for i, outcome in enumerate(("acked", "retry-later", "acked")):
        s.record_attempt(
            ts=f"2026-10-07T12:0{i}:00+00:00",
            source_id=PSK_KEY[0], dest_id=PSK_KEY[1], table=PSK_KEY[2],
            outcome=outcome, records=None if outcome != "acked" else 40 + i,
            bytes_=None if outcome != "acked" else 900 + i,
            error="timed out" if outcome != "acked" else None,
        )
    s.enqueue_deliverable(
        pipeline="psk-pskreporter", payload_blob=b"\x00\x0aIPFIX-frame",
        enqueued_at="2026-10-07T12:01:00+00:00",
        next_attempt_at="2026-10-07T12:06:00+00:00",
        source_id=PSK_KEY[0], dest_id=PSK_KEY[1], table=PSK_KEY[2],
        cursor_after=b"1205", commit_token=b"commit-1205",
    )
    s.enqueue_deliverable(
        pipeline="grape-psws", payload_blob=b"OBS2026-10-06T00-00",
        enqueued_at="2026-10-07T01:10:00+00:00",
        next_attempt_at="2026-10-07T01:40:00+00:00",
    )
    s.send_to_dead_letter(ts="2026-10-06T23:00:00+00:00",
                          pipeline="mag-psws", payload_blob=b"PK\x03\x04zip",
                          final_error="550 permission denied")
    s.close()
    return db


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


def _rows(db: Path) -> dict:
    """Every row of every table, BLOBs as bytes, plus the schema itself."""
    conn = sqlite3.connect(db)
    try:
        names = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        out = {n: conn.execute(f'SELECT * FROM "{n}" ORDER BY rowid').fetchall()
               for n in names}
        out["sqlite_master"] = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        return out
    finally:
        conn.close()


def _cli(*argv: str) -> subprocess.CompletedProcess:
    """`hs-uploader` as a real process, for its true exit status."""
    env = dict(os.environ, PYTHONPATH=str(SRC))
    return subprocess.run([sys.executable, "-m", "hs_uploader.cli", *argv],
                          capture_output=True, text=True, env=env, timeout=120)


def _crash_copy(directory: Path) -> tuple[Path, dict]:
    """A v0 store that a writer died inside.  Its cache spilled pages into
    the file before it died, so watermarks.db-journal holds the pages it
    overwrote: a hot journal.  Returns the path and every row as it stood
    before the crash."""
    db = _v0_store(directory)
    rows_before = _rows(db)
    size_before = db.stat().st_size
    crash = textwrap.dedent(f"""
        import os, sqlite3
        conn = sqlite3.connect({str(db)!r}, isolation_level=None)
        conn.execute("PRAGMA cache_size = 10")    # spill into the file early
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE watermarks SET cursor = x'00'")
        conn.execute("DELETE FROM deliverables")
        conn.execute("CREATE TABLE spill (x BLOB)")
        conn.executemany("INSERT INTO spill VALUES (?)",
                         [(os.urandom(1000),) for _ in range(500)])
        os._exit(1)                               # die before COMMIT
    """)
    subprocess.run([sys.executable, "-c", crash], timeout=60)
    assert Path(f"{db}-journal").stat().st_size > 0
    assert db.stat().st_size > size_before      # the spill reached the file
    return db, rows_before


# ---- the migration list ----


def test_versions_run_from_1_to_schema_version_without_gaps():
    assert [m.version for m in schema.MIGRATIONS] == list(
        range(1, schema.SCHEMA_VERSION + 1))
    assert schema.SCHEMA_VERSION == 1


def test_busy_timeout_constant_is_at_least_30_s():
    assert schema.BUSY_TIMEOUT_S >= 30


# ---- migrate ----


def test_fresh_store_file_migrates_to_version_1(tmp_path, capsys):
    db = tmp_path / "watermarks.db"
    SqliteWatermarkStore(db).close()
    assert _version(db) == 0

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    assert _version(db) == 1
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "watermarks.db: version 1"
    assert out[1:] == [
        "  applied 1: record the schema version; changes no row and no table"]


def test_migrate_keeps_every_row_of_a_v0_store(tmp_path):
    db = _v0_store(tmp_path)
    rows_before = _rows(db)
    bytes_before = db.read_bytes()

    report = schema.migrate(str(db))

    assert (report.from_version, report.to_version) == (0, 1)
    assert report.applied == [schema.MIGRATIONS[0].name]
    assert report.pending == []
    assert _rows(db) == rows_before
    bytes_after = db.read_bytes()
    assert len(bytes_after) == len(bytes_before)
    changed = {i for i, (a, b) in enumerate(zip(bytes_before, bytes_after))
               if a != b}
    # Every page past the 100-byte file header stays byte-identical.
    assert changed <= _HEADER_BYTES_A_VERSION_BUMP_TOUCHES
    assert int.from_bytes(bytes_after[60:64], "big") == 1


def test_second_run_changes_nothing(tmp_path, capsys):
    db = _v0_store(tmp_path)
    assert main(["migrate", "--db", str(db)]) == 0
    capsys.readouterr()
    bytes_before = db.read_bytes()
    mtime_before = db.stat().st_mtime_ns

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    assert db.read_bytes() == bytes_before
    assert db.stat().st_mtime_ns == mtime_before
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: version 1", "  nothing to do"]
    report = schema.migrate(str(db))
    assert (report.from_version, report.to_version) == (1, 1)
    assert report.applied == [] and report.pending == []


def test_check_writes_nothing(tmp_path, capsys):
    # A space and a '#' in the path prove the read-only URI quotes it.
    db = _v0_store(tmp_path / "state dir#1")
    bytes_before = db.read_bytes()
    mtime_before = db.stat().st_mtime_ns
    listing_before = sorted(p.name for p in db.parent.iterdir())

    rc = main(["migrate", "--db", str(db), "--check"])

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: version 0",
        "  pending 1: record the schema version; changes no row and no table",
    ]
    assert db.read_bytes() == bytes_before
    assert db.stat().st_mtime_ns == mtime_before
    assert sorted(p.name for p in db.parent.iterdir()) == listing_before
    report = schema.migrate(str(db), check=True)
    assert (report.from_version, report.to_version) == (0, 0)
    assert report.applied == []
    assert report.pending == [schema.MIGRATIONS[0].name]


def test_a_crash_copy_migrates_and_keeps_every_row(tmp_path, capsys):
    # migrate opens read-write from its first read, so it rolls the hot
    # journal back exactly as the daemon's own open would, then migrates.
    # A read-only first read would fail: "attempt to write a readonly
    # database".
    db, rows_before = _crash_copy(tmp_path)

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: version 1",
        "  applied 1: record the schema version; changes no row and no table",
    ]
    assert not Path(f"{db}-journal").exists()
    assert _version(db) == 1
    assert _rows(db) == rows_before


def test_check_on_a_crash_copy_says_so_and_writes_nothing(tmp_path, capsys):
    db, _ = _crash_copy(tmp_path)
    journal = Path(f"{db}-journal")
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (db, journal)}
    listing_before = sorted(p.name for p in db.parent.iterdir())

    rc = main(["migrate", "--db", str(db), "--check"])

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: a crash left a journal to recover; run hs-uploader "
        "migrate without --check (or start the daemon) to recover it"]
    assert {p: (p.read_bytes(), p.stat().st_mtime_ns)
            for p in (db, journal)} == before
    assert sorted(p.name for p in db.parent.iterdir()) == listing_before
    with pytest.raises(schema.JournalRecoveryNeeded):
        schema.migrate(str(db), check=True)


def test_migrate_waits_for_a_concurrent_writer(tmp_path):
    db = _v0_store(tmp_path)
    holder = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    holder.execute(
        "INSERT INTO watermarks VALUES('sqlite:hfdl.spots','x','hfdl.spots',"
        "x'01','2026-10-07T12:00:00+00:00')")

    def release():
        time.sleep(1.0)
        holder.execute("COMMIT")

    t = threading.Thread(target=release)
    t.start()
    started = time.monotonic()
    try:
        report = schema.migrate(str(db))
    finally:
        t.join()
        holder.close()
    waited = time.monotonic() - started

    assert waited >= 0.9           # it waited for the lock ...
    assert report.to_version == 1  # ... and then finished, rather than failing
    assert _version(db) == 1
    conn = sqlite3.connect(db)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM watermarks WHERE source_id='sqlite:hfdl.spots'"
        ).fetchone()[0] == 1       # the other writer's commit survived
    finally:
        conn.close()


def test_every_connection_waits_at_least_30_s(tmp_path, monkeypatch):
    db = _v0_store(tmp_path)
    seen = []
    real_connect = sqlite3.connect

    def spy(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        mode = str(args[0]).rsplit("?mode=", 1)[-1]
        seen.append((mode, conn.execute("PRAGMA busy_timeout").fetchone()[0]))
        return conn

    monkeypatch.setattr(schema.sqlite3, "connect", spy)
    schema.migrate(str(db), check=True)
    schema.migrate(str(db))

    # --check reads read-only.  A real run opens one read-write connection
    # from its first read, so a hot journal rolls back before it looks.
    assert [mode for mode, _ in seen] == ["ro", "rw"]
    assert all(ms >= 30_000 for _, ms in seen)


def test_a_lock_held_past_the_timeout_exits_1_and_changes_nothing(
        tmp_path, monkeypatch, capsys):
    db = _v0_store(tmp_path)
    rows_before = _rows(db)
    monkeypatch.setattr(schema, "BUSY_TIMEOUT_S", 0.2)
    holder = sqlite3.connect(db, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        rc = main(["migrate", "--db", str(db)])
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    assert rc == 1
    assert "database is locked" in capsys.readouterr().err
    assert _version(db) == 0
    assert _rows(db) == rows_before


def test_a_failing_migration_rolls_back_every_step(tmp_path, monkeypatch):
    db = _v0_store(tmp_path)
    rows_before = _rows(db)

    def boom(conn):
        conn.execute("DELETE FROM watermarks")
        raise RuntimeError("boom")

    monkeypatch.setattr(schema, "MIGRATIONS", (
        *schema.MIGRATIONS, schema.Migration(2, "fails", boom)))

    with pytest.raises(RuntimeError, match="boom"):
        schema.migrate(str(db))

    assert _version(db) == 0       # step 1 rolled back with step 2
    assert _rows(db) == rows_before


def test_missing_file_exits_0_and_creates_nothing(tmp_path, capsys):
    db = tmp_path / "absent" / "watermarks.db"

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        f"watermarks.db: not found at {db}; nothing to migrate"]
    assert not db.parent.exists()
    with pytest.raises(FileNotFoundError):
        schema.migrate(str(db))


def test_a_database_without_a_watermarks_table_exits_1_untouched(
        tmp_path, capsys):
    # sink.db passed by mistake must not gain a version.
    db = tmp_path / "sink.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE pending_uploads (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    bytes_before = db.read_bytes()

    rc = main(["migrate", "--db", str(db)])

    assert rc == 1
    assert "not an hs-uploader watermark store" in capsys.readouterr().err
    assert db.read_bytes() == bytes_before


def test_not_a_database_exits_1(tmp_path, capsys):
    db = tmp_path / "watermarks.db"
    db.write_bytes(b"this is not sqlite\n" * 10)

    rc = main(["migrate", "--db", str(db)])

    assert rc == 1
    assert "hs-uploader migrate:" in capsys.readouterr().err


def test_a_newer_store_is_left_alone_and_exits_0(tmp_path, capsys):
    # A station rolled back from a release whose migrate went further.
    db = _v0_store(tmp_path)
    _set_version(db, schema.SCHEMA_VERSION + 1)
    bytes_before = db.read_bytes()

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == f"watermarks.db: version {schema.SCHEMA_VERSION + 1}"
    assert "newer than this hs-uploader" in out[1]
    assert db.read_bytes() == bytes_before


def test_db_defaults_to_the_state_option(tmp_path, capsys):
    db = _v0_store(tmp_path)

    rc = main(["--state", str(db), "migrate"])

    assert rc == 0
    assert _version(db) == 1


def test_usage_error_exits_2(tmp_path):
    with pytest.raises(SystemExit) as caught:
        main(["migrate", "--no-such-option"])
    assert caught.value.code == 2


def test_exit_codes_from_a_real_process(tmp_path):
    db = _v0_store(tmp_path)
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"not sqlite\n" * 10)

    first = _cli("migrate", "--db", str(db))
    again = _cli("migrate", "--db", str(db))
    check = _cli("migrate", "--db", str(db), "--check")
    failed = _cli("migrate", "--db", str(junk))
    usage = _cli("migrate", "--bogus")

    assert first.returncode == 0, first.stderr
    assert first.stdout.splitlines()[0] == "watermarks.db: version 1"
    assert again.returncode == 0 and "nothing to do" in again.stdout
    assert check.returncode == 0 and check.stdout.splitlines() == [
        "watermarks.db: version 1", "  nothing to do"]
    assert failed.returncode == 1
    assert usage.returncode == 2 and "unrecognized arguments" in usage.stderr


# ---- the runner's guards: each one runs once, and only when needed ----


def test_nothing_to_do_takes_no_write_lock(tmp_path, monkeypatch, capsys):
    # A store at the current version needs no write, so a writer that holds
    # the lock must not make migrate wait or fail.
    db = _v0_store(tmp_path)
    assert main(["migrate", "--db", str(db)]) == 0
    capsys.readouterr()
    monkeypatch.setattr(schema, "BUSY_TIMEOUT_S", 0.2)
    holder = sqlite3.connect(db, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        rc = main(["migrate", "--db", str(db)])
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: version 1", "  nothing to do"]


def test_check_on_a_locked_store_fails_and_does_not_blame_a_crash(
        tmp_path, monkeypatch, capsys):
    # A lock held past the timeout is a failure.  Only a hot journal earns
    # the "a crash left a journal" line.
    db = _v0_store(tmp_path)
    monkeypatch.setattr(schema, "BUSY_TIMEOUT_S", 0.2)
    holder = sqlite3.connect(db, isolation_level=None)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        rc = main(["migrate", "--db", str(db), "--check"])
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    captured = capsys.readouterr()
    assert rc == 1, captured
    assert "database is locked" in captured.err
    assert "crash" not in captured.out


def test_a_migrator_that_loses_the_race_applies_nothing(tmp_path, monkeypatch):
    # Another migrate finishes between this one's first read of the version
    # and its write lock.  Under the lock it must read the version again and
    # find nothing left to run.
    db = _v0_store(tmp_path)
    ran = []
    monkeypatch.setattr(schema, "MIGRATIONS", (
        schema.Migration(1, "counting", lambda conn: ran.append(1)),))
    real_inspect = schema._inspect

    def stale_inspect(conn, path):
        seen = real_inspect(conn, path)
        _set_version(db, 1)        # the other migrate commits in the window
        return seen                # ... and this one still holds the old 0

    monkeypatch.setattr(schema, "_inspect", stale_inspect)

    report = schema.migrate(str(db))

    assert ran == []
    assert report.applied == []
    assert (report.from_version, report.to_version) == (1, 1)


def test_the_runner_applies_only_what_the_file_has_not_seen(
        tmp_path, monkeypatch):
    db = _v0_store(tmp_path)
    schema.migrate(str(db))
    assert _version(db) == 1
    ran = []
    second = schema.Migration(2, "second", lambda conn: ran.append(2))
    monkeypatch.setattr(schema, "MIGRATIONS", (
        schema.Migration(1, "first", lambda conn: ran.append(1)), second))

    report = schema.migrate(str(db))

    assert ran == [2]
    assert report.applied == [second.name]
    assert (report.from_version, report.to_version) == (1, 2)
    assert _version(db) == 2


def test_hot_journal_check_falls_back_when_errors_lack_sqlite_errorname(
        tmp_path):
    # requires-python is >=3.10, where sqlite3 errors carry no
    # sqlite_errorname.  _hot_journal then reads the message and looks for
    # the journal file beside the store.
    db = tmp_path / "watermarks.db"
    readonly = sqlite3.OperationalError("attempt to write a readonly database")
    unrelated = sqlite3.OperationalError("database is locked")
    assert getattr(readonly, "sqlite_errorname", None) is None

    assert schema._hot_journal(readonly, db) is False     # no journal file
    assert schema._hot_journal(unrelated, db) is False
    Path(f"{db}-journal").write_bytes(b"\x00" * 512)
    assert schema._hot_journal(readonly, db) is True
    assert schema._hot_journal(unrelated, db) is False    # journal, wrong error


# ---- the CLI's exact lines and its choice of file ----


def test_the_failure_line_names_path_type_and_message(tmp_path, capsys):
    db = tmp_path / "watermarks.db"
    db.write_bytes(b"this is not sqlite\n" * 10)

    rc = main(["migrate", "--db", str(db)])

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        f"hs-uploader migrate: {db}: DatabaseError: file is not a database"]


def test_the_newer_store_line_is_exact(tmp_path, capsys):
    db = _v0_store(tmp_path)
    _set_version(db, 2)

    rc = main(["migrate", "--db", str(db)])

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "watermarks.db: version 2",
        "  newer than this hs-uploader, which knows versions up to 1; "
        "left as it stands"]


@pytest.mark.skipif(os.geteuid() == 0,
                    reason="root reads through a mode-000 directory")
def test_a_stat_failure_prints_the_failure_line_and_exits_1(tmp_path, capsys):
    # A parent directory the caller cannot search makes the existence check
    # fail with EACCES.  That is a failure with the contract's line, not a
    # traceback.
    locked = tmp_path / "locked"
    locked.mkdir()
    db = locked / "watermarks.db"
    locked.chmod(0o000)
    try:
        rc = main(["migrate", "--db", str(db)])
    finally:
        locked.chmod(0o700)        # so tmp_path can clean up

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        f"hs-uploader migrate: {db}: PermissionError: "
        f"[Errno 13] Permission denied: {str(db)!r}"]


def test_db_beats_state(tmp_path):
    db = _v0_store(tmp_path / "chosen")
    other = _v0_store(tmp_path / "other")

    rc = main(["--state", str(other), "migrate", "--db", str(db)])

    assert rc == 0
    assert _version(db) == 1
    assert _version(other) == 0


def test_without_db_or_state_migrate_uses_the_default_path(
        tmp_path, monkeypatch):
    # default_path() reads HS_UPLOADER_STATE_DIR each time it runs.
    db = _v0_store(tmp_path / "state")
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(db.parent))

    rc = main(["migrate"])

    assert rc == 0
    assert _version(db) == 1


# ---- the store never migrates; old code reads a migrated file ----


def test_store_constructor_leaves_user_version_alone(tmp_path):
    db = tmp_path / "watermarks.db"
    SqliteWatermarkStore(db).close()
    assert _version(db) == 0       # a new store does not migrate itself

    old = _v0_store(tmp_path / "old")
    s = SqliteWatermarkStore(old)
    s.advance_cursor(*PSK_KEY, cursor=b"1300",
                     last_ack="2026-10-07T13:00:00+00:00")
    s.close()
    assert _version(old) == 0      # nor does reopening a v0 store

    _set_version(old, 1)
    s = SqliteWatermarkStore(old)
    s.advance_cursor(*PSK_KEY, cursor=b"1301",
                     last_ack="2026-10-07T13:00:30+00:00")
    s.close()
    assert _version(old) == 1      # and reopening never resets the number


def test_current_store_opens_a_version_1_file(tmp_path):
    # Rollback safety.  This commit leaves watermark/sqlite.py as v3.69
    # shipped it, so here the store IS the v3.69 store code, and it must
    # carry on against a file that v3.70's migrate has stamped.
    db = _v0_store(tmp_path)
    schema.migrate(str(db))
    assert _version(db) == 1

    s = SqliteWatermarkStore(db)
    try:
        assert s.get_cursor(*PSK_KEY) == b"1204"
        assert s.get_cursor(*GRAPE_KEY) == b"\x00\xff\x10binary"
        d = s.pop_due_deliverable("psk-pskreporter",
                                  now="2026-10-07T13:00:00+00:00")
        assert d is not None
        assert (d.cursor_after, d.commit_token) == (b"1205", b"commit-1205")
        s.requeue_deliverable(d)
        s.advance_cursor(*PSK_KEY, cursor=b"1300",
                         last_ack="2026-10-07T13:00:05+00:00")
        s.record_attempt(ts="2026-10-07T13:00:05+00:00",
                         source_id=PSK_KEY[0], dest_id=PSK_KEY[1],
                         table=PSK_KEY[2], outcome="acked", records=3,
                         bytes_=120, error=None)
        assert s.get_cursor(*PSK_KEY) == b"1300"
        assert s.deliverable_count() == 2
        assert s.dead_letter_count() == 1
        assert len(s.all_cursors()) == 3
    finally:
        s.close()
    assert _version(db) == 1
