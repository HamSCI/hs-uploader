"""Tests for hs_uploader.sink.Writer (CONTRACT §17).

The first half moved here from sigmond/tests/test_hamsci_sink.py when
the writer moved into hs-uploader (v3.70); only the imports changed.
The second half covers what v3.70 added: the `producer` and `local`
columns, `ensure_columns`, `infer_producer` and `PENDING_UPLOADS_DDL`
(tasks/plan-sink-control.md §10.4 item 2, D12).
"""

import json
import logging
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from hs_uploader.sink import (
    PENDING_UPLOADS_DDL,
    BufferFull,
    SqliteConfig,
    Writer,
    ensure_columns,
    infer_producer,
)
from hs_uploader.sink.writer import (
    HEALTH_DEGRADED, HEALTH_NOOP, HEALTH_OK, HEALTH_UNREACHABLE,
    _resolve_db_alias,
)

_SRC = Path(__file__).resolve().parent.parent / "src"


def _temp_db_path() -> str:
    """Caller-owned temp file path; we delete in tearDown."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    Path(f.name).unlink()  # let sqlite create the file fresh
    return f.name


class TestNoOpMode(unittest.TestCase):
    """No path configured and no writable default → noop. Standalone-safe."""

    def test_from_env_no_path_yields_noop(self):
        w = Writer.from_env(table="spots", mode="psk", env={})
        # env={} has no SIGMOND_SQLITE_PATH; from_env may still pick the
        # /var/lib/sigmond default if that dir is writable on the test
        # host.  Force the standalone case by disabling the probe.
        from hs_uploader.sink import writer as writer_mod
        original = writer_mod._default_sqlite_writable
        writer_mod._default_sqlite_writable = lambda _p: False
        try:
            w = Writer.from_env(table="spots", mode="psk", env={})
            self.assertTrue(w.is_noop)
            self.assertEqual(w.health, HEALTH_NOOP)
        finally:
            writer_mod._default_sqlite_writable = original

    def test_noop_insert_does_nothing(self):
        from hs_uploader.sink import writer as writer_mod
        original = writer_mod._default_sqlite_writable
        writer_mod._default_sqlite_writable = lambda _p: False
        try:
            w = Writer.from_env(table="spots", mode="psk", env={})
            w.insert([{"a": 1}, {"a": 2}])
            self.assertEqual(w.buffered, 0)
            w.flush()
            w.close()
        finally:
            writer_mod._default_sqlite_writable = original


class TestConfigAndAlias(unittest.TestCase):

    def test_config_from_env_strips_blank(self):
        self.assertIsNone(SqliteConfig.from_env({"SIGMOND_SQLITE_PATH": ""}))
        self.assertIsNone(SqliteConfig.from_env({"SIGMOND_SQLITE_PATH": "   "}))

    def test_config_from_env_returns_path(self):
        cfg = SqliteConfig.from_env({"SIGMOND_SQLITE_PATH": "/tmp/sink.db"})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.path, "/tmp/sink.db")

    def test_resolve_db_alias_uses_env_then_falls_back(self):
        env = {"SIGMOND_SQLITE_DB_PSK": "psk_local"}
        self.assertEqual(_resolve_db_alias("psk", env), "psk_local")
        self.assertEqual(_resolve_db_alias("hfdl", env), "hfdl")


class TestEnabledWriter(unittest.TestCase):

    def setUp(self):
        self.db_path = _temp_db_path()
        self.env = {"SIGMOND_SQLITE_PATH": self.db_path}

    def tearDown(self):
        p = Path(self.db_path)
        if p.exists():
            p.unlink()
        # WAL/SHM sidecars
        for suffix in ("-wal", "-shm"):
            sidecar = Path(self.db_path + suffix)
            if sidecar.exists():
                sidecar.unlink()

    def _writer(self, **kwargs) -> Writer:
        return Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=3, **kwargs,
        )

    def _queue_rows(self) -> list:
        # Table is created lazily on first flush; treat "not yet" as empty.
        if not Path(self.db_path).exists():
            return []
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='pending_uploads'"
            )
            if cur.fetchone() is None:
                return []
            cur = conn.execute(
                "SELECT target_db, target_table, schema_version, "
                "payload_json, queued_at FROM pending_uploads ORDER BY id"
            )
            return list(cur.fetchall())
        finally:
            conn.close()

    def test_buffers_until_batch_threshold(self):
        w = self._writer()
        w.insert([{"a": 1}, {"a": 2}])
        self.assertEqual(w.buffered, 2)
        self.assertEqual(self._queue_rows(), [])  # not flushed yet
        w.insert([{"a": 3}])  # crosses batch_rows=3
        rows = self._queue_rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(w.buffered, 0)
        self.assertEqual(w.health, HEALTH_OK)

    def test_explicit_flush_drains_buffer(self):
        w = self._writer()
        w.insert([{"a": 1}])
        w.flush()
        self.assertEqual(len(self._queue_rows()), 1)
        self.assertEqual(w.buffered, 0)

    def test_payload_is_json_with_target_metadata(self):
        w = self._writer()
        w.insert([{"frequency": 14074000, "mode": "ft8", "score": 17}])
        w.flush()
        rows = self._queue_rows()
        self.assertEqual(len(rows), 1)
        target_db, target_table, schema_version, payload_json, queued_at = rows[0]
        self.assertEqual(target_db, "psk")
        self.assertEqual(target_table, "spots")
        self.assertEqual(schema_version, 0)
        decoded = json.loads(payload_json)
        self.assertEqual(decoded["frequency"], 14074000)
        self.assertEqual(decoded["mode"], "ft8")
        # queued_at parses as ISO8601 UTC.
        parsed = datetime.fromisoformat(queued_at)
        self.assertIsNotNone(parsed.tzinfo)

    def test_datetime_serializes_to_iso(self):
        w = self._writer()
        t = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        w.insert([{"time": t}])
        w.flush()
        payload = json.loads(self._queue_rows()[0][3])
        self.assertEqual(payload["time"], t.isoformat())

    def test_alias_overrides_database_from_env(self):
        env = {**self.env, "SIGMOND_SQLITE_DB_PSK": "psk_alt"}
        w = Writer.from_env(
            table="spots", mode="psk", env=env, batch_rows=1,
        )
        w.insert([{"x": 1}])
        rows = self._queue_rows()
        self.assertEqual(rows[0][0], "psk_alt")

    def test_close_flushes_and_closes_conn(self):
        w = self._writer()
        w.insert([{"a": 1}])
        w.close()
        self.assertEqual(len(self._queue_rows()), 1)

    def test_context_manager_flushes(self):
        with Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=10,
        ) as w:
            w.insert([{"x": 1}])
        self.assertEqual(len(self._queue_rows()), 1)

    def test_schema_version_persisted(self):
        w = Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=1,
            schema_version=7,
        )
        w.insert([{"x": 1}])
        self.assertEqual(self._queue_rows()[0][2], 7)

    def test_multiple_tables_coexist_in_one_db(self):
        spots = Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=1,
        )
        noise = Writer.from_env(
            table="noise", mode="wspr", env=self.env, batch_rows=1,
        )
        spots.insert([{"freq": 14074000}])
        noise.insert([{"floor": -120}])
        rows = self._queue_rows()
        targets = {(r[0], r[1]) for r in rows}
        self.assertEqual(targets, {("psk", "spots"), ("wspr", "noise")})


class TestUnreachableHandling(unittest.TestCase):
    """SQLite is local, so 'unreachable' means disk-full / readonly /
    locked-too-long.  We simulate with a connect_factory that fails."""

    def test_transient_failure_keeps_buffer_marks_unreachable(self):
        attempts = {"n": 0}

        def factory(cfg):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise sqlite3.OperationalError("simulated disk error")
            return sqlite3.connect(":memory:")

        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": "/nonexistent/dir/sink.db"},
            batch_rows=2, connect_factory=factory,
        )
        w.insert([{"a": 1}, {"a": 2}])  # triggers flush; first attempt fails
        self.assertEqual(w.health, HEALTH_UNREACHABLE)
        self.assertEqual(w.buffered, 2)
        # Next flush succeeds against the in-memory connection.
        w.flush()
        self.assertEqual(w.health, HEALTH_OK)
        self.assertEqual(w.buffered, 0)

    def test_buffer_overflow_raises_buffer_full(self):
        def always_fail(cfg):
            raise sqlite3.OperationalError("simulated disk full")

        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": "/nonexistent/dir/sink.db"},
            batch_rows=3, connect_factory=always_fail,
        )
        with self.assertRaises(BufferFull):
            for i in range(7):
                w.insert([{"i": i}])
        self.assertEqual(w.health, HEALTH_DEGRADED)


class TestWriterFromEnvDispatch(unittest.TestCase):
    """`Writer.from_env` selects a writable sink path: an explicit
    `SIGMOND_SQLITE_PATH`, else the sigmond default, else no-op."""

    def setUp(self):
        self.db_path = _temp_db_path()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if p.exists():
                p.unlink()

    def test_explicit_path_yields_enabled_writer(self):
        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": self.db_path},
        )
        self.assertIsInstance(w, Writer)
        self.assertFalse(w.is_noop)

    def test_neither_set_with_no_default_dir_yields_noop(self):
        # When /var/lib/sigmond doesn't exist and can't be created, the
        # fallback is no-op (preserves standalone-safety).  We force this
        # by monkeypatching the writability probe to return False.
        from hs_uploader.sink import writer as writer_mod
        original = writer_mod._default_sqlite_writable
        writer_mod._default_sqlite_writable = lambda _p: False
        try:
            w = Writer.from_env(table="spots", mode="psk", env={})
            self.assertTrue(w.is_noop)
        finally:
            writer_mod._default_sqlite_writable = original

    def test_neither_set_with_writable_default_yields_enabled(self):
        # The default: SQLite at /var/lib/sigmond/sink.db when the
        # parent dir is writable.  Inject a temp dir so the test doesn't
        # need /var/lib/sigmond on the host.
        tmpdir = tempfile.mkdtemp()
        try:
            from hs_uploader.sink import writer as writer_mod
            original_path = writer_mod._DEFAULT_SQLITE_PATH
            writer_mod._DEFAULT_SQLITE_PATH = str(Path(tmpdir) / "sink.db")
            try:
                w = Writer.from_env(table="spots", mode="psk", env={})
                self.assertFalse(w.is_noop)
            finally:
                writer_mod._DEFAULT_SQLITE_PATH = original_path
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestFromEnvBatchRows(unittest.TestCase):
    """`Writer.from_env` defaults `batch_rows` to the small SQLite
    write-buffer size, and honors an explicit override."""

    def setUp(self):
        self.db_path = _temp_db_path()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if p.exists():
                p.unlink()

    def test_default_batch_rows_is_sqlite_default(self):
        from hs_uploader.sink.writer import DEFAULT_SQLITE_BATCH_ROWS
        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": self.db_path},
            # No batch_rows arg — caller uses Writer.from_env's default.
        )
        self.assertEqual(w.batch_rows, DEFAULT_SQLITE_BATCH_ROWS)

    def test_explicit_batch_rows_honored(self):
        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": self.db_path},
            batch_rows=42,
        )
        self.assertEqual(w.batch_rows, 42)


class TestTimeBasedAutoFlush(unittest.TestCase):
    """auto_flush_seconds bounds in-memory residency.  Without it a slow
    stream's buffer could sit for hours before the first write to disk —
    a data-loss-on-crash trap and the same bug that bit psk-recorder
    on its first SQLite run."""

    def setUp(self):
        self.db_path = _temp_db_path()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if p.exists():
                p.unlink()

    def _row_count(self) -> int:
        if not Path(self.db_path).exists():
            return 0
        conn = sqlite3.connect(self.db_path)
        try:
            r = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='pending_uploads'"
            ).fetchone()
            if r is None:
                return 0
            return conn.execute(
                "SELECT count(*) FROM pending_uploads"
            ).fetchone()[0]
        finally:
            conn.close()

    def test_age_trigger_fires_when_seconds_elapsed(self):
        # batch_rows=1000 (high) so size never trips; auto_flush=0.05s.
        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": self.db_path},
            batch_rows=1000, auto_flush_seconds=0.05,
        )
        w.insert([{"a": 1}])
        self.assertEqual(self._row_count(), 0)  # not flushed yet
        import time as _time
        _time.sleep(0.08)
        w.insert([{"a": 2}])
        # After the sleep, the next insert sees age >= threshold and
        # flushes the whole accumulated buffer.
        self.assertEqual(self._row_count(), 2)

    def test_age_trigger_disabled_when_zero(self):
        w = Writer.from_env(
            table="spots", mode="psk",
            env={"SIGMOND_SQLITE_PATH": self.db_path},
            batch_rows=10, auto_flush_seconds=0,
        )
        w.insert([{"a": 1}])
        import time as _time
        _time.sleep(0.05)
        w.insert([{"a": 2}])
        # With auto_flush_seconds=0, no age trigger; under batch_rows
        # threshold so still buffered.
        self.assertEqual(self._row_count(), 0)
        self.assertEqual(w.buffered, 2)


class TestGroupWritablePerms(unittest.TestCase):
    """Writer must make sink.db (+WAL/SHM) group-writable after schema
    init so OTHER producers in the same supplementary group can write
    to the same sink.  Without this, the first producer to flush owns
    the files at mode 0644 and the rest hit "attempt to write a
    readonly database" — observed on bee1 2026-05-12.
    """

    def setUp(self):
        self.db_path = _temp_db_path()
        self.env = {"SIGMOND_SQLITE_PATH": self.db_path}

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if p.exists():
                p.unlink()

    def test_main_db_is_group_writable_after_first_flush(self):
        import stat
        w = Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=1,
        )
        w.insert([{"x": 1}])  # triggers flush
        mode = Path(self.db_path).stat().st_mode
        self.assertTrue(
            mode & stat.S_IWGRP,
            f"main db mode {oct(mode & 0o7777)} missing group-write bit",
        )

    def test_wal_and_shm_sidecars_are_group_writable_after_first_flush(self):
        import stat
        w = Writer.from_env(
            table="spots", mode="psk", env=self.env, batch_rows=1,
        )
        w.insert([{"x": 1}])
        # journal_mode=WAL creates the -wal file at first write commit;
        # -shm appears alongside.  Both inherit the producer's umask
        # at create time, which is what _chmod_group_writable
        # remediates.
        for suffix in ("-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if not p.exists():
                continue  # SQLite version may not have created one yet
            mode = p.stat().st_mode
            self.assertTrue(
                mode & stat.S_IWGRP,
                f"{p.name} mode {oct(mode & 0o7777)} missing group-write bit",
            )

    def test_chmod_failure_is_silent_no_raise(self):
        """A non-owner caller that lacks chmod permission must not
        crash the flush — every sigmond-group writer would otherwise
        hit a hard error on every flush after the first producer
        creates the file."""
        with patch("os.chmod", side_effect=PermissionError("not owner")):
            w = Writer.from_env(
                table="spots", mode="psk", env=self.env, batch_rows=1,
            )
            # No raise — flush completes despite chmod's failure.
            w.insert([{"x": 1}])
        # And the row landed on disk.
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("SELECT COUNT(*) FROM pending_uploads")
            self.assertEqual(cur.fetchone()[0], 1)
        finally:
            conn.close()


class TestCrossThreadUse(unittest.TestCase):
    """AC0G-ND, 2026-09-24/25: wspr-recorder's batcher thread opened the
    wspr.noise connection; at shutdown the main thread's close() flushed
    the last rows through it, sqlite3 refused ("SQLite objects created in
    a thread can only be used in that same thread"), and the rows were
    lost on every exit. SpotSink also calls insert() from several
    BandRecorder threads, relying on the Writer to serialize itself."""

    def setUp(self):
        self.path = _temp_db_path()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            Path(self.path + suffix).unlink(missing_ok=True)

    def _count(self):
        with sqlite3.connect(self.path) as c:
            return c.execute("SELECT COUNT(*) FROM pending_uploads").fetchone()[0]

    def test_close_on_another_thread_flushes_what_a_worker_buffered(self):
        w = Writer("wspr", "noise", batch_rows=2, auto_flush_seconds=0,
                   config=SqliteConfig(path=self.path))

        def worker():
            w.insert([{"n": 1}, {"n": 2}])   # size-flush opens the connection here
            w.insert([{"n": 3}])             # buffered, not yet flushed

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        with self.assertNoLogs("sigmond.hamsci_sink", level="WARNING"):
            w.close()
        self.assertEqual(self._count(), 3)

    def test_concurrent_inserts_lose_no_rows(self):
        # In a child process with a hard timeout: an unserialized Writer
        # sharing one connection across threads can deadlock inside sqlite
        # while holding the GIL, which no in-process join timeout survives.
        import subprocess
        import textwrap
        script = textwrap.dedent(f"""
            import sys, threading
            sys.path.insert(0, {str(_SRC)!r})
            from hs_uploader.sink import SqliteConfig, Writer
            w = Writer("wspr", "spots", batch_rows=7, auto_flush_seconds=0,
                       config=SqliteConfig(path={self.path!r}))
            w._buffer_max = 10_000
            def worker(k):
                for i in range(250):
                    w.insert([{{"k": k, "i": i}}])
            ts = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
            for t in ts: t.start()
            for t in ts: t.join()
            w.close()
        """)
        try:
            subprocess.run([sys.executable, "-c", script], timeout=60, check=True,
                           capture_output=True, text=True)
        except subprocess.TimeoutExpired:
            self.fail("concurrent inserts hung")
        self.assertEqual(self._count(), 8 * 250)


# ---- v3.70: producer, local, ensure_columns, infer_producer ----------------

# pending_uploads exactly as v3.69's writer created it: no producer, no local.
_V369_DDL = """
CREATE TABLE IF NOT EXISTS pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pending_uploads_target
    ON pending_uploads (target_db, target_table, id);
CREATE INDEX IF NOT EXISTS idx_pending_uploads_cycle_time
    ON pending_uploads (target_db, target_table,
                        json_extract(payload_json, '$.time'));
"""

# The INSERT v3.69's writer runs; a client still running that code keeps
# running it until it restarts.
_V369_INSERT = (
    "INSERT INTO pending_uploads "
    "(target_db, target_table, schema_version, payload_json, queued_at) "
    "VALUES (?, ?, ?, ?, ?)"
)

_QUEUED_AT = "2026-10-07T00:00:00+00:00"


def _old_sink(path: Path, rows=()) -> None:
    """A sink.db as v3.69 left it, holding `rows` of (db, table, payload)."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_V369_DDL)
        conn.executemany(
            _V369_INSERT,
            [(db, table, 2, json.dumps(p), _QUEUED_AT) for db, table, p in rows],
        )
        conn.commit()
    finally:
        conn.close()


def _columns(path: Path) -> list:
    with sqlite3.connect(path) as c:
        return [r[1] for r in c.execute("PRAGMA table_info(pending_uploads)")]


def _stored(path: Path) -> list:
    with sqlite3.connect(path) as c:
        return c.execute(
            "SELECT target_db, json_extract(payload_json, '$.mode'), producer, local "
            "FROM pending_uploads ORDER BY id"
        ).fetchall()


def _writer(path: Path, mode: str = "psk", table: str = "spots", **kw) -> Writer:
    return Writer.from_env(
        table=table, mode=mode, env={"SIGMOND_SQLITE_PATH": str(path)},
        batch_rows=1000, auto_flush_seconds=0, **kw,
    )


def test_package_exports_the_writer_surface():
    import hs_uploader.sink as sink
    assert set(sink.__all__) == {
        "Writer", "BufferFull", "SqliteConfig",
        "PENDING_UPLOADS_DDL", "ensure_columns", "infer_producer",
    }


def test_flush_failure_keeps_the_logger_name_and_prefix_operators_grep(caplog):
    def always_fail(cfg):
        raise sqlite3.OperationalError("simulated disk full")

    w = Writer.from_env(
        table="spots", mode="psk",
        env={"SIGMOND_SQLITE_PATH": "/nonexistent/dir/sink.db"},
        batch_rows=1, connect_factory=always_fail,
    )
    with caplog.at_level(logging.WARNING, logger="sigmond.hamsci_sink"):
        w.insert([{"a": 1}])
    [record] = caplog.records
    assert record.name == "sigmond.hamsci_sink"
    assert record.getMessage().startswith("hamsci_sink: flush failed for psk.spots")


def test_a_fresh_sink_has_both_new_columns_last(tmp_path):
    db = tmp_path / "sink.db"
    w = _writer(db)
    w.insert([{"mode": "ft8"}])
    w.close()
    assert _columns(db) == [
        "id", "target_db", "target_table", "schema_version",
        "payload_json", "queued_at", "producer", "local",
    ]


def test_the_ddl_script_and_ensure_columns_build_the_same_table(tmp_path):
    fresh, old = tmp_path / "fresh.db", tmp_path / "old.db"
    with sqlite3.connect(fresh) as c:
        c.executescript(PENDING_UPLOADS_DDL)
        c.executescript(PENDING_UPLOADS_DDL)      # IF NOT EXISTS: runs twice
    _old_sink(old)
    with sqlite3.connect(old) as c:
        assert ensure_columns(c) == ["producer", "local"]
    assert _columns(fresh) == _columns(old)
    with sqlite3.connect(fresh) as c:
        names = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_pending_uploads_target", "idx_pending_uploads_cycle_time"} <= names


def test_a_writer_adds_the_columns_to_an_old_sink(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db, [("psk", "spots", {"mode": "ft8"})])
    w = _writer(db)
    w.insert([{"mode": "msk144"}])
    w.close()
    assert _columns(db)[-2:] == ["producer", "local"]
    assert _stored(db) == [
        ("psk", "ft8", "", 0),                    # stored by v3.69: untouched
        ("psk", "msk144", "meteor-scatter", 0),   # stored now: inferred
    ]


def test_ensure_columns_twice_adds_nothing_the_second_time(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db)
    with sqlite3.connect(db) as c:
        assert ensure_columns(c) == ["producer", "local"]
        assert ensure_columns(c) == []


def test_ensure_columns_without_the_table_creates_nothing(tmp_path):
    db = tmp_path / "sink.db"
    with sqlite3.connect(db) as c:
        assert ensure_columns(c) == []
        assert c.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] == 0


class _StaleTableInfo:
    """A connection that read `PRAGMA table_info` before a rival client
    added the columns: it still sees the v3.69 column list."""

    def __init__(self, conn: sqlite3.Connection, stale_rows: list):
        self._conn = conn
        self._stale = stale_rows

    def execute(self, sql, *args):
        if sql.startswith("PRAGMA table_info"):
            return iter(self._stale)
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_losing_the_race_to_add_a_column_counts_as_success(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db)
    a = sqlite3.connect(db, timeout=30)
    b = sqlite3.connect(db, timeout=30)
    try:
        stale = list(b.execute("PRAGMA table_info(pending_uploads)"))
        assert ensure_columns(a) == ["producer", "local"]   # a wins
        # b decided from its stale read; both ALTERs meet "duplicate column".
        assert ensure_columns(_StaleTableInfo(b, stale)) == []
    finally:
        a.close()
        b.close()
    assert _columns(db)[-2:] == ["producer", "local"]


def test_ensure_columns_raises_any_other_error(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db)

    class _Locked(_StaleTableInfo):
        def execute(self, sql, *args):
            if sql.startswith("ALTER"):
                raise sqlite3.OperationalError("database is locked")
            return super().execute(sql, *args)

    with sqlite3.connect(db) as c:
        stale = list(c.execute("PRAGMA table_info(pending_uploads)"))
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            ensure_columns(_Locked(c, stale))


def test_many_connections_racing_ensure_columns_add_each_column_once(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db)
    n = 8
    barrier = threading.Barrier(n)
    results, errors = [], []

    def client():
        conn = sqlite3.connect(db, timeout=30)
        try:
            barrier.wait()
            results.append(ensure_columns(conn))
        except Exception as e:
            errors.append(e)
        finally:
            conn.close()

    threads = [threading.Thread(target=client) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert errors == []
    assert sorted(c for added in results for c in added) == ["local", "producer"]
    assert _columns(db)[-2:] == ["producer", "local"]


def test_two_writers_racing_ensure_columns(tmp_path, caplog):
    # Two recorders open one old sink.db together.  The slower one read
    # table_info before the faster one added the columns, so both of its
    # ALTERs meet "duplicate column name".  Both must still store.
    db = tmp_path / "sink.db"
    _old_sink(db, [("psk", "spots", {"mode": "ft8"})])
    with sqlite3.connect(db) as c:
        stale = list(c.execute("PRAGMA table_info(pending_uploads)"))

    def slow_factory(cfg):
        conn = sqlite3.connect(cfg.path, timeout=30.0, check_same_thread=False)
        return _StaleTableInfo(conn, stale)

    fast = _writer(db)
    slow = _writer(db, mode="wspr", connect_factory=slow_factory)
    fast.insert([{"mode": "msk144"}])
    slow.insert([{"mode": "W2"}])
    with caplog.at_level(logging.WARNING, logger="sigmond.hamsci_sink"):
        fast.close()
        slow.close()
    assert caplog.records == []                   # neither flush failed
    assert (fast.health, fast.buffered) == (HEALTH_OK, 0)
    assert (slow.health, slow.buffered) == (HEALTH_OK, 0)
    assert _columns(db)[-2:] == ["producer", "local"]
    assert _stored(db) == [
        ("psk", "ft8", "", 0),
        ("psk", "msk144", "meteor-scatter", 0),
        ("wspr", "W2", "wspr-recorder", 0),
    ]


def _traced_factory(statements: list):
    def factory(cfg):
        conn = sqlite3.connect(cfg.path, timeout=30.0, check_same_thread=False)
        conn.set_trace_callback(statements.append)
        return conn
    return factory


def test_the_writer_updates_no_existing_row(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db, [("psk", "spots", {"mode": "ft8"}),
                   ("psk", "spots", {"mode": "msk144"})])
    statements: list = []
    w = Writer.from_env(
        table="spots", mode="psk", env={"SIGMOND_SQLITE_PATH": str(db)},
        batch_rows=1000, auto_flush_seconds=0,
        connect_factory=_traced_factory(statements),
    )
    w.insert([{"mode": "ft8"}])
    w.close()
    # D12: rows stored before the column existed keep producer ''.
    assert _stored(db) == [
        ("psk", "ft8", "", 0),
        ("psk", "msk144", "", 0),
        ("psk", "ft8", "psk-recorder", 0),
    ]
    run = [s.lstrip().upper() for s in statements]
    assert not [s for s in run if s.startswith(("UPDATE", "VACUUM", "DELETE"))]
    assert not [s for s in run if "INDEX" in s and "PRODUCER" in s]
    assert len([s for s in run if s.startswith("ALTER TABLE")]) == 2


def test_a_v369_insert_still_works_after_the_columns_arrive(tmp_path):
    # A client restarted later keeps running v3.69's five-column INSERT.
    db = tmp_path / "sink.db"
    _old_sink(db)
    w = _writer(db)
    w.insert([{"mode": "ft8"}])
    w.close()
    with sqlite3.connect(db) as c:
        c.execute(_V369_INSERT, ("psk", "spots", 2, '{"mode": "ft4"}', _QUEUED_AT))
    assert _stored(db)[-1] == ("psk", "ft4", "", 0)


@pytest.mark.parametrize("target_db, target_table, payload, producer", [
    ("psk", "spots", {"mode": "ft8"}, "psk-recorder"),
    ("psk", "spots", {"mode": "ft4"}, "psk-recorder"),
    ("psk", "spots", {"mode": ""}, "psk-recorder"),       # psk-recorder's fallback
    ("psk", "spots", {}, "psk-recorder"),
    ("psk", "spots", {"mode": "msk144"}, "meteor-scatter"),
    ("psk", "spots", {"mode": "MSK144"}, "meteor-scatter"),
    ("psk", "spots", {"mode": None}, "psk-recorder"),
    ("psk", "spots", "not a dict", "psk-recorder"),       # never raises
    ("wspr", "spots", {"mode": "W2"}, "wspr-recorder"),
    ("wspr", "noise", {}, "wspr-recorder"),
    ("codar", "spots", {}, "codar-sounder"),
    ("superdarn", "detections", {}, "superdarn-sounder"),
    ("hfdl", "spots", {}, "hfdl-recorder"),
    ("timestd", "events", {}, "hf-timestd"),
    ("psk_local", "spots", {"mode": "ft8"}, ""),          # renamed by an alias
    ("unknown", "spots", {}, ""),
])
def test_infer_producer(target_db, target_table, payload, producer):
    assert infer_producer(target_db, target_table, payload) == producer


@pytest.mark.parametrize("mode, table, rows, producer", [
    ("psk", "spots", [{"mode": "ft8"}], "psk-recorder"),
    ("psk", "spots", [{"mode": "msk144"}], "meteor-scatter"),
    ("wspr", "spots", [{"mode": "W2"}], "wspr-recorder"),
    ("wspr", "noise", [{"floor": -120}], "wspr-recorder"),
    ("codar", "spots", [{"freq": 4.5e6}], "codar-sounder"),
    ("superdarn", "detections", [{"radar": "fhw"}], "superdarn-sounder"),
])
def test_the_writer_stores_each_clients_producer(tmp_path, mode, table, rows, producer):
    # Each client's real from_env call (mode, table), read from its repo.
    db = tmp_path / "sink.db"
    w = _writer(db, mode=mode, table=table)
    w.insert(rows)
    w.close()
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT producer, local FROM pending_uploads").fetchall() == [
            (producer, 0)]


def test_one_psk_flush_names_each_rows_producer(tmp_path):
    db = tmp_path / "sink.db"
    w = _writer(db)
    w.insert([{"mode": "ft8"}, {"mode": "msk144"}, {"mode": "ft4"}])
    w.close()
    assert [r[2] for r in _stored(db)] == ["psk-recorder", "meteor-scatter", "psk-recorder"]


def test_a_caller_given_producer_wins_over_inference(tmp_path):
    db = tmp_path / "sink.db"
    w = _writer(db, producer="psk-recorder")
    w.insert([{"mode": "msk144"}])
    w.close()
    alias = _writer(db, database="psk_local", producer="meteor-scatter")
    alias.insert([{"mode": "msk144"}])
    alias.close()
    assert [r[2] for r in _stored(db)] == ["psk-recorder", "meteor-scatter"]
    assert w.producer == "psk-recorder"


def test_an_empty_producer_falls_back_to_inference(tmp_path):
    db = tmp_path / "sink.db"
    w = _writer(db, producer="")
    w.insert([{"mode": "msk144"}])
    w.close()
    assert _stored(db)[0][2] == "meteor-scatter"


def test_large_old_file_gains_columns_without_rewrite(tmp_path):
    db = tmp_path / "sink.db"
    _old_sink(db, [("psk", "spots", {"mode": "ft8", "i": i}) for i in range(200_000)])
    conn = sqlite3.connect(db, timeout=30)
    try:
        pages_before = conn.execute("PRAGMA page_count").fetchone()[0]
        started = time.monotonic()
        assert ensure_columns(conn) == ["producer", "local"]
        elapsed = time.monotonic() - started
        conn.commit()
        assert conn.execute("PRAGMA page_count").fetchone()[0] == pages_before
        assert conn.execute(
            "SELECT count(*) FROM pending_uploads WHERE producer = '' AND local = 0"
        ).fetchone()[0] == 200_000
    finally:
        conn.close()
    # A table rewrite of 200k rows takes seconds; a schema-only change, ms.
    assert elapsed < 1.0, f"ensure_columns took {elapsed:.3f} s"
