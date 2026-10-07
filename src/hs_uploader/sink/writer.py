"""Local sink writer for HamSCI clients (CONTRACT §17).

Why this exists:
    On a sigmond client the local sink is just a store-and-forward
    buffer for `hs-uploader` to ship rows upstream.  SQLite gives a
    durable promise (rows survive a crash; the uploader reads at its
    own pace) at tens of MB of RAM and no daemon — the right shape for
    a host whose real job is running an SDR pipeline.

Where it lives:
    This module moved here from sigmond (`sigmond/lib/sigmond/
    hamsci_sink/writer.py`) in v3.70.  Clients still import it as
    `sigmond.hamsci_sink`, which prefers this module.  sigmond also
    carries a copy for venvs that hold no hs-uploader (D14), and a drift
    test holds the two together.  Keep this file stdlib-only and free of
    imports from the rest of `hs_uploader`, so that copy can stay a
    plain file copy.  The logger name and the `hamsci_sink:` message
    prefix stay as they were: operators grep them.

Selection (`Writer.from_env`):
    `SIGMOND_SQLITE_PATH` set → writer at that path (explicit override;
        useful for tests or unusual layouts).
    unset → the sigmond default `/var/lib/sigmond/sink.db`, IF that
        directory is writable.  Otherwise no-op — a true standalone
        client (no sigmond install) stays safe instead of erroring on
        every flush.

Storage shape:
    One queue table `pending_uploads` shared across modes:

        id              INTEGER PRIMARY KEY AUTOINCREMENT
        target_db       TEXT     -- e.g. "psk", "wspr", "timestd"
        target_table    TEXT     -- e.g. "spots", "noise", "events"
        schema_version  INTEGER
        payload_json    TEXT     -- the row, JSON-serialized
        queued_at       TEXT     -- ISO8601 UTC (writer wall-clock)
        producer        TEXT     -- the client that stored the row
        local           INTEGER  -- 1 = a local archive row (no caller sets it yet)

    `hs-uploader` reads rows in FIFO order, ships them upstream, and
    deletes on success.  JSON-on-disk means the uploader owns schema
    translation, not the producer — so producers stay decoupled from
    the upstream's column shape.

    `producer` and `local` arrived in v3.70 (tasks/plan-sink-control.md
    §10.4 item 2).  A writer that opens an older file adds both with
    ALTER TABLE ADD COLUMN, which changes only the schema: SQLite
    rewrites no row.  Rows stored before v3.70 keep `producer = ''`, and
    readers name their producer with `infer_producer` (D12).  The writer
    stores `local = 0` on every row; a caller that keeps a local archive
    gains a way to say so in a later release.

Thread-safe: see `Writer`.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

# Default batch trigger.  Sized as a write-buffer, not an OLAP
# bulk-insert: we want rows on disk in seconds, not when a large batch
# happens to fill.  Sized so even a low-rate stream (~30 spots/min from
# hf-timestd) flushes within a couple of minutes while a high-rate one
# (~250 spots/min from psk-recorder) flushes every ~4 cycles.  Operators
# can override via the `batch_rows` constructor arg.
DEFAULT_SQLITE_BATCH_ROWS = 1000

# Time bound on durability: if the buffer is non-empty and this many
# seconds have passed since the last flush, the next `insert()` will
# flush regardless of buffer size.  Without this, a stream that goes
# quiet (mode-change, propagation drop, etc.) could leave queued rows
# in memory until the next active period.
DEFAULT_SQLITE_AUTO_FLUSH_SECONDS = 30.0

# The name operators grep for; it predates the move into hs-uploader.
logger = logging.getLogger("sigmond.hamsci_sink")


# Health values per CONTRACT §17.3 (plus `noop` for the standalone case).
HEALTH_OK = "ok"
HEALTH_UNREACHABLE = "unreachable"
HEALTH_DEGRADED = "degraded"
HEALTH_NOOP = "noop"


# Default sink path used when no path is explicitly configured.  Lives
# under sigmond's state dir so operators can find it for backup,
# disk-budget accounting, and hs-uploader's reader.
_DEFAULT_SQLITE_PATH = "/var/lib/sigmond/sink.db"


def _default_sqlite_writable(path: str) -> bool:
    """True iff the default sink path is usable without explicit config.

    A directory is "usable" when (a) it already exists and is writable
    by the current process, OR (b) its parent exists and is writable
    (so the directory itself can be created on first connect).  This
    keeps standalone clients — no sigmond install, no /var/lib/sigmond
    — falling back to no-op instead of erroring on every flush.
    """
    parent = Path(path).parent
    if parent.exists():
        return os.access(parent, os.W_OK)
    grandparent = parent.parent
    return grandparent.exists() and os.access(grandparent, os.W_OK)


class BufferFull(Exception):
    """Buffer reached `2 * batch_rows` while the sink was unwritable.

    Raised by `Writer.insert()` so the caller cannot silently lose rows.
    Callers handle this however they like (sidecar file, drop-with-metric,
    refuse-new-work) — the contract just forbids silent loss.
    """


def _json_default(obj: Any) -> Any:
    """JSON encoder for datetimes and bytes — common in producer rows."""
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    raise TypeError(f"{type(obj).__name__} not JSON-serializable")


@dataclass
class SqliteConfig:
    """Sink config resolved from env."""

    path: str

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> Optional["SqliteConfig"]:
        e = env if env is not None else os.environ
        path = (e.get("SIGMOND_SQLITE_PATH") or "").strip()
        if not path:
            return None
        return cls(path=path)


# `producer` and `local` sit LAST, so a fresh table and an older table
# that `ensure_columns` extended list their columns in the same order.
_QUEUE_DDL = """
CREATE TABLE IF NOT EXISTS pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL,
    producer        TEXT NOT NULL DEFAULT '',
    local           INTEGER NOT NULL DEFAULT 0
)
"""

_QUEUE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_pending_uploads_target
    ON pending_uploads (target_db, target_table, id)
"""

# Expression index for hs-uploader's WsprCycleSource, whose cycle
# discovery and per-cycle batch queries filter on
# json_extract(payload_json, '$.time').  Without it every 30 s pump
# JSON-parses EVERY wspr row in the queue (millions) — observed as
# 100%-CPU spikes and a ~2.4 min/cycle replay pace.  The expression
# text must match the queries' exactly for SQLite to use it.
_QUEUE_CYCLE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_pending_uploads_cycle_time
    ON pending_uploads (target_db, target_table,
                        json_extract(payload_json, '$.time'))
"""

# The whole queue schema as one script, for `conn.executescript()`:
# tests, fixtures and tools that build a sink.db the way the writer does.
PENDING_UPLOADS_DDL = ";\n".join(
    s.strip() for s in (_QUEUE_DDL, _QUEUE_INDEX_DDL, _QUEUE_CYCLE_INDEX_DDL)
) + ";\n"

# The columns v3.70 added, in the order `ensure_columns` adds them.
_ADDED_COLUMNS = (
    ("producer", "TEXT NOT NULL DEFAULT ''"),
    ("local", "INTEGER NOT NULL DEFAULT 0"),
)


def ensure_columns(conn: sqlite3.Connection) -> list[str]:
    """Add `producer` and `local` to an older `pending_uploads`.

    Returns the names of the columns this call added, in order; an
    empty list when both already exist or the table does not exist yet
    (the CREATE TABLE in `PENDING_UPLOADS_DDL` carries both).

    ALTER TABLE ADD COLUMN with a constant default changes only the
    schema, so it runs in milliseconds on a file of millions of rows
    and rewrites none of them.  This function adds no index, runs no
    VACUUM, and updates no row: a row stored before the column existed
    reads `producer = ''` and `local = 0` (D12).

    Several clients open the same sink.db and may restart together.  Two
    can both see a column missing; the slower one's ALTER then fails
    with "duplicate column name", which means the column now exists, so
    that error counts as success.  Any other error propagates.  Each
    ALTER commits on its own unless the caller holds a transaction open.
    """
    have = {row[1] for row in conn.execute("PRAGMA table_info(pending_uploads)")}
    if not have:
        return []
    added: list[str] = []
    for name, decl in _ADDED_COLUMNS:
        if name in have:
            continue
        try:
            conn.execute(f"ALTER TABLE pending_uploads ADD COLUMN {name} {decl}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
            continue
        added.append(name)
    return added


# D12 (tasks/plan-sink-control.md §2): the one client that writes each
# target_db, read from the clients' own writer calls on 2026-10-07.
#   wspr       wspr-recorder      (wspr.spots, wspr.noise)
#   codar      codar-sounder      (codar.spots)
#   superdarn  superdarn-sounder  (superdarn.detections)
#   hfdl       hfdl-recorder      (hfdl.spots)
#   timestd    hf-timestd         (timestd.events, none written since 2026-05)
# psk-recorder and meteor-scatter both write psk.spots; each row's own
# `mode` tells them apart (see `infer_producer`).  hf-tec writes no row
# today, and the fix that makes it write will name its producer, so it
# needs no entry.
_PRODUCER_BY_DB = {
    "wspr": "wspr-recorder",
    "codar": "codar-sounder",
    "superdarn": "superdarn-sounder",
    "hfdl": "hfdl-recorder",
    "timestd": "hf-timestd",
}
_PSK_DB = "psk"
_METEOR_SCATTER_MODE = "msk144"


def infer_producer(target_db: str, target_table: str, payload: dict) -> str:
    """Name the client that stored a row whose writer named none (D12).

    MSK144 rows in `psk` belong to meteor-scatter and every other `psk`
    row to psk-recorder; each other target_db maps to the one client
    that writes it.  An unknown target_db, including one renamed by
    `SIGMOND_SQLITE_DB_<MODE>`, gives `''`, the column's default.

    The writer applies this to each new row whose caller named no
    producer, and readers apply it to rows stored with `''`, so both
    reach the same answer from the stored columns alone.
    `target_table` rides along for that shared signature; no rule needs
    it today.  Never raises: a flush must not fail over a payload's
    shape.
    """
    if target_db == _PSK_DB:
        mode = payload.get("mode") if isinstance(payload, dict) else None
        if isinstance(mode, str) and mode.strip().lower() == _METEOR_SCATTER_MODE:
            return "meteor-scatter"
        return "psk-recorder"
    return _PRODUCER_BY_DB.get(target_db, "")


class Writer:
    """Writer that buffers rows into a local SQLite queue.

    Use `Writer.from_env(...)` to construct from coordination.env.
    Pass `connect_factory` in tests to inject a fake connection.

    `producer` names the client that stores the rows.  When the caller
    leaves it out, each row's producer comes from `infer_producer`.

    Thread-safe: `insert`, `flush` and `close` hold one re-entrant lock,
    and the default connection is opened with ``check_same_thread=False``
    — safe because every use of it holds that lock.  AC0G-ND 2026-09-25:
    wspr-recorder's batcher thread opened the connection, the main
    thread's close() flushed through it at shutdown, sqlite3 refused, and
    the last rows were lost on every exit.  Unlocked, concurrent inserts
    also wrote rows twice (two flushes of one buffer).
    """

    def __init__(
        self,
        database: str,
        table: str,
        *,
        schema_version: int = 0,
        producer: Optional[str] = None,
        batch_rows: int = DEFAULT_SQLITE_BATCH_ROWS,
        auto_flush_seconds: float = DEFAULT_SQLITE_AUTO_FLUSH_SECONDS,
        config: Optional[SqliteConfig] = None,
        connect_factory: Optional[Any] = None,
    ) -> None:
        self.database = database
        self.table = table
        self.schema_version = schema_version
        self.producer = producer or ""
        self.batch_rows = batch_rows
        self.auto_flush_seconds = auto_flush_seconds
        self._buffer_max = batch_rows * 2
        self._config = config
        self._connect_factory = connect_factory or _default_connect_factory
        self._lock = threading.RLock()
        self._buffer: list = []
        self._conn: Optional[sqlite3.Connection] = None
        self._schema_initialized = False
        self._health = HEALTH_NOOP if config is None else HEALTH_OK
        # Used by the time-based auto-flush check in insert().
        # Initialized to "now" so we don't immediately flush a 1-row
        # buffer on the first insert after a long idle.
        self._last_flush_monotonic: float = time.monotonic()

    @classmethod
    def from_env(
        cls,
        table: str,
        *,
        mode: str,
        database: Optional[str] = None,
        schema_version: int = 0,
        producer: Optional[str] = None,
        batch_rows: int = DEFAULT_SQLITE_BATCH_ROWS,
        auto_flush_seconds: float = DEFAULT_SQLITE_AUTO_FLUSH_SECONDS,
        env: Optional[dict] = None,
        connect_factory: Optional[Any] = None,
    ) -> "Writer":
        """Build a Writer from coordination.env.

        `SIGMOND_SQLITE_PATH` selects the sink path.  When unset, the
        sigmond default `/var/lib/sigmond/sink.db` is used if its
        directory is writable; otherwise the writer is a silent no-op
        (preserves standalone-safety for clients running outside a
        sigmond install).

        `mode` is the per-mode key (`wspr`, `psk`, `hfdl`, `codar`,
        `timestd`).  `database` defaults to the mode name — operators
        can override per host via `SIGMOND_SQLITE_DB_<MODE>`.  Pass
        `database=` to bypass the alias.  `producer` names the client;
        leave it out and the writer infers it per row (`infer_producer`).
        """
        e = env if env is not None else os.environ
        sqlite_path = (e.get("SIGMOND_SQLITE_PATH") or "").strip()
        # Fall back to the sigmond state path when no path is configured,
        # but only when its directory is writable — a true standalone
        # client should not silently start writing to /var/lib/sigmond.
        if not sqlite_path and _default_sqlite_writable(_DEFAULT_SQLITE_PATH):
            sqlite_path = _DEFAULT_SQLITE_PATH
        effective_env = dict(e)
        if sqlite_path:
            effective_env["SIGMOND_SQLITE_PATH"] = sqlite_path
        cfg = SqliteConfig.from_env(effective_env)
        actual_db = database or _resolve_db_alias(mode, env)
        return cls(
            database=actual_db,
            table=table,
            schema_version=schema_version,
            producer=producer,
            batch_rows=batch_rows,
            auto_flush_seconds=auto_flush_seconds,
            config=cfg,
            connect_factory=connect_factory,
        )

    @property
    def health(self) -> str:
        return self._health

    @property
    def is_noop(self) -> bool:
        return self._config is None

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    def insert(self, rows: Sequence) -> None:
        """Buffer rows; auto-flush on size OR age threshold.

        Raises `BufferFull` if the buffer would exceed `2 * batch_rows`
        (SQLite has been unwritable for too long).

        Two flush triggers:
        - Size: buffer reaches `batch_rows`.
        - Age: buffer non-empty and `auto_flush_seconds` elapsed since
          last successful flush.  Bounds the in-memory residency time
          so a low-rate stream's rows still land on disk promptly.
        """
        with self._lock:
            if self.is_noop or not rows:
                return
            self._buffer.extend(rows)
            if len(self._buffer) > self._buffer_max:
                self._health = HEALTH_DEGRADED
                buffered = len(self._buffer)
                self._buffer = self._buffer[: self._buffer_max]
                raise BufferFull(
                    f"hamsci_sink buffer overflow: {buffered} rows pending, "
                    f"max {self._buffer_max} (SQLite unwritable at "
                    f"{self._config.path if self._config else '?'})"
                )
            size_trigger = len(self._buffer) >= self.batch_rows
            age_trigger = (
                self.auto_flush_seconds > 0
                and self._buffer
                and time.monotonic() - self._last_flush_monotonic
                >= self.auto_flush_seconds
            )
            if size_trigger or age_trigger:
                self.flush()

    def flush(self) -> None:
        """Force a flush. Quiet on transient failures (buffer retained)."""
        with self._lock:
            if self.is_noop or not self._buffer:
                return
            try:
                conn = self._connect()
                if not self._schema_initialized:
                    self._init_schema(conn)
                now_iso = datetime.now(timezone.utc).isoformat()
                params = [
                    (
                        self.database,
                        self.table,
                        self.schema_version,
                        json.dumps(row, default=_json_default),
                        now_iso,
                        self.producer
                        or infer_producer(self.database, self.table, row),
                        0,
                    )
                    for row in self._buffer
                ]
                conn.executemany(
                    "INSERT INTO pending_uploads "
                    "(target_db, target_table, schema_version, payload_json, "
                    "queued_at, producer, local) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params,
                )
                conn.commit()
                self._buffer = []
                self._last_flush_monotonic = time.monotonic()
                self._health = HEALTH_OK
            except BufferFull:
                raise
            except Exception as e:
                # Drop the handle so a stale/locked DB gets reopened on retry.
                self._conn = None
                self._schema_initialized = False
                if self._health != HEALTH_DEGRADED:
                    self._health = HEALTH_UNREACHABLE
                logger.warning(
                    "hamsci_sink: flush failed for %s.%s "
                    "(%d rows buffered): %s",
                    self.database, self.table, len(self._buffer), e,
                )

    def close(self) -> None:
        with self._lock:
            try:
                self.flush()
            finally:
                if self._conn is not None:
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                self._conn = None

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None and self._config is not None:
            self._conn = self._connect_factory(self._config)
        assert self._conn is not None
        return self._conn

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        # WAL keeps the uploader's reader from blocking the writer.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(_QUEUE_DDL)
        conn.execute(_QUEUE_INDEX_DDL)
        conn.execute(_QUEUE_CYCLE_INDEX_DDL)
        # An older sink.db gains producer and local here (v3.70).
        added = ensure_columns(conn)
        conn.commit()
        if added:
            logger.info(
                "hamsci_sink: added column(s) %s to pending_uploads in %s",
                ", ".join(added),
                self._config.path if self._config else "?",
            )
        # Ensure the main db + WAL/SHM sidecars are group-writable so
        # OTHER producers in the same supplementary group can write to
        # the same sink.  Multiple HamSCI clients (psk-recorder,
        # hf-timestd, hfdl-recorder, ...) share /var/lib/sigmond/sink.db
        # via the `sigmond` group; whichever client flushes first
        # creates the WAL/SHM files with the producer's umask, which
        # default to 0644 — locking everyone else out with "attempt to
        # write a readonly database".  chmod g+w idempotently fixes
        # that.  Best-effort: a non-owner caller can't chmod, so we
        # swallow PermissionError (the next sigmond-group producer
        # to flush gets the same chance, and the storage_migrate
        # verb pre-creates the main db root-owned with 0o664 anyway).
        if self._config is not None:
            self._chmod_group_writable(self._config.path)
        self._schema_initialized = True

    @staticmethod
    def _chmod_group_writable(path: str) -> None:
        """Add group-write bit to `path` and its SQLite sidecar files.

        SQLite manages the -wal / -shm files alongside the main db
        when journal_mode=WAL is set; their default umask-driven mode
        (0644) blocks group writes.  Idempotent and best-effort: a
        non-owner caller silently no-ops.
        """
        import stat
        for suffix in ("", "-wal", "-shm"):
            target = f"{path}{suffix}"
            try:
                st = os.stat(target)
            except FileNotFoundError:
                continue
            new_mode = st.st_mode | stat.S_IWGRP | stat.S_IRGRP
            if new_mode == st.st_mode:
                continue
            try:
                os.chmod(target, new_mode & 0o7777)
            except (PermissionError, OSError):
                # Not the owner — that's fine; whoever owns the file
                # already did this, or will the next time they flush.
                pass


def _default_connect_factory(config: SqliteConfig) -> sqlite3.Connection:
    # Ensure parent directory exists so first-time install works without
    # operators pre-creating /var/lib/sigmond.  Done here (not in the
    # writer) so tests that inject a factory can bypass filesystem prep.
    parent = Path(config.path).parent
    if str(parent) and parent != Path("."):
        parent.mkdir(parents=True, exist_ok=True)
    # Default isolation_level keeps explicit transactions around each
    # flush so a crash mid-batch loses at most the in-memory buffer,
    # never a partial batch on disk.
    # check_same_thread=False: the Writer serializes every use of this
    # connection under its own lock, and its last flush often runs on a
    # different thread (shutdown) from the one that opened it.
    return sqlite3.connect(config.path, timeout=30.0, check_same_thread=False)


def _resolve_db_alias(mode: str, env: Optional[dict] = None) -> str:
    """Per-mode sink db-name alias, overridable via `SIGMOND_SQLITE_DB_<MODE>`."""
    e = env if env is not None else os.environ
    return e.get(f"SIGMOND_SQLITE_DB_{mode.upper()}", mode)
