"""SQLite-backed watermark store.

Default location: ``/var/lib/hs-uploader/watermarks.db``.  Override via
the ``HS_UPLOADER_STATE_DIR`` env var or by passing an explicit path.

Schema:

* ``watermarks(source_id, dest_id, table_name, cursor, last_ack)`` —
  one row per (source, destination, table) tuple; the cursor is the
  bytes the source emitted with its last successful batch.
* ``attempts(ts, source_id, dest_id, table_name, outcome, records,
  bytes, error)`` — ring-buffered audit log; trimmed to the last 10k
  rows on each insert.
* ``deliverables(id, pipeline, payload_blob, enqueued_at, attempts,
  next_attempt_at)`` — retry queue persisted across restarts.
* ``dead_letter(ts, pipeline, payload_blob, final_error)`` — terminal
  failures; cursor was NOT advanced.
* ``pipeline_state(name, client, refused_reason, updated_at)`` — what the
  daemon decided for each pipeline at its last start.
"""

from __future__ import annotations

import os
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from .base import Advance, Deliverable

_ATTEMPTS_RING_SIZE = 10_000

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS watermarks (
    source_id  TEXT NOT NULL,
    dest_id    TEXT NOT NULL,
    table_name TEXT NOT NULL,
    cursor     BLOB NOT NULL,
    last_ack   TEXT NOT NULL,
    PRIMARY KEY (source_id, dest_id, table_name)
);

CREATE TABLE IF NOT EXISTS attempts (
    rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL,
    source_id TEXT NOT NULL,
    dest_id   TEXT NOT NULL,
    table_name TEXT NOT NULL,
    outcome   TEXT NOT NULL,
    records   INTEGER,
    bytes     INTEGER,
    error     TEXT
);
CREATE INDEX IF NOT EXISTS attempts_ts_idx ON attempts(ts);

CREATE TABLE IF NOT EXISTS deliverables (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline        TEXT NOT NULL,
    payload_blob    BLOB NOT NULL,
    enqueued_at     TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    source_id       TEXT NOT NULL DEFAULT '',
    dest_id         TEXT NOT NULL DEFAULT '',
    table_name      TEXT NOT NULL DEFAULT '',
    cursor_after    BLOB NOT NULL DEFAULT x'',
    commit_token    BLOB NOT NULL DEFAULT x''
);
CREATE INDEX IF NOT EXISTS deliverables_due_idx
    ON deliverables(pipeline, next_attempt_at);

CREATE TABLE IF NOT EXISTS dead_letter (
    rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    pipeline    TEXT NOT NULL,
    payload_blob BLOB NOT NULL,
    final_error TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    name           TEXT PRIMARY KEY,
    client         TEXT NOT NULL,
    refused_reason TEXT NOT NULL DEFAULT '',
    updated_at     TEXT NOT NULL
);
"""


def default_path() -> Path:
    base = os.environ.get("HS_UPLOADER_STATE_DIR", "/var/lib/hs-uploader")
    return Path(base) / "watermarks.db"


logger = logging.getLogger(__name__)

# advance_cursor logs a refused (backward) write, or a failed check, with
# one WARNING per key per this many seconds, and at DEBUG between.  A
# key that runs backward on every acknowledgement would otherwise bury
# every other line in the journal.
BACKWARD_WARN_INTERVAL_S = 3600.0


class SqliteWatermarkStore:
    """SQLite-backed implementation of ``WatermarkStore``.

    All public methods take the connection lock so concurrent callers
    from the same process serialize cleanly.  Two pumps on the same
    key remain an operator config error, but ``advance_cursor`` runs
    its read, comparison and write in one ``BEGIN IMMEDIATE``
    transaction, so a second process cannot move a send record back.
    """

    def __init__(
        self,
        path: Path | str = ":memory:",
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.path = str(path)
        # ``check_same_thread=False`` because the orchestrator may dispatch
        # from a thread that's different from the constructor's; we
        # serialize all access through ``self._lock``.
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        # advance_cursor's warning windows, one per key:
        # key -> [window start on `clock`, writes since the last WARNING].
        # In memory only, so each process keeps its own.
        self._clock = clock
        self._warn_windows: dict[tuple[str, str, str], list] = {}
        # key -> `clock` reading at the last WARNING that a failed check
        # produced (the stored value or the comparator raised).
        self._check_failed_at: dict[tuple[str, str, str], float] = {}
        self._init_schema()
        # Make the db + its WAL/SHM sidecars group-writable so OTHER
        # users in the same supplementary group (sigmond) can write
        # to the same watermark store.  Multiple HamSCI clients
        # (psk-recorder under pskrec, wsprdaemon-client under
        # wsprdaemon, ...) share /var/lib/hs-uploader/watermarks.db;
        # whichever opens it first creates it with the producer's
        # umask (default 0o644), locking everyone else out with
        # "sqlite3.OperationalError: attempt to write a readonly
        # database" — observed on bee1 2026-05-12 during the wspr-
        # uploader cutover.  Same mitigation pattern as sigmond's
        # SqliteWriter._chmod_group_writable (sigmond 19a8ba1):
        # best-effort + silent on PermissionError so a non-owner
        # caller doesn't crash on every construct.
        self._chmod_group_writable()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            fresh = self._conn.execute(
                "SELECT count(*) FROM sqlite_master").fetchone()[0] == 0
            self._conn.executescript(_SCHEMA_SQL)
            if fresh:
                # A file that held no table has nothing to migrate: born
                # current.  A file with tables keeps its number, so
                # `hs-uploader migrate` still runs on it (spec §4.3).
                from .schema import SCHEMA_VERSION
                self._conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)

    def schema_version(self) -> int:
        """The store's ``PRAGMA user_version``."""
        with self._lock:
            return int(self._conn.execute("PRAGMA user_version").fetchone()[0])

    def record_pipeline_states(
        self, states: "list[tuple[str, str, str]]"
    ) -> None:
        """Upsert (name, client, refused_reason) rows in one transaction."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT INTO pipeline_state (name, client, refused_reason, updated_at)"
                " VALUES (?, ?, ?, ?)"
                " ON CONFLICT(name) DO UPDATE SET client=excluded.client,"
                " refused_reason=excluded.refused_reason,"
                " updated_at=excluded.updated_at",
                [(n, c, r, now) for n, c, r in states],
            )

    def record_pipeline_state(
        self, name: str, client: str, refused_reason: str = ""
    ) -> None:
        self.record_pipeline_states([(name, client, refused_reason)])

    def pipeline_states(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(
                "SELECT name, client, refused_reason, updated_at"
                " FROM pipeline_state ORDER BY name"))

    def _chmod_group_writable(self) -> None:
        """Add group-write bit to the watermark db + its WAL/SHM
        sidecars.  No-op for ``:memory:``.  Idempotent and silent on
        PermissionError (non-owner callers leave it to whoever owns
        the file)."""
        if self.path == ":memory:":
            return
        import stat as _stat
        for suffix in ("", "-wal", "-shm"):
            target = self.path + suffix
            try:
                st = os.stat(target)
            except FileNotFoundError:
                continue
            new_mode = st.st_mode | _stat.S_IWGRP | _stat.S_IRGRP
            if new_mode == st.st_mode:
                continue
            try:
                os.chmod(target, new_mode & 0o7777)
            except (PermissionError, OSError):
                pass

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- cursors --

    def get_cursor(self, source_id: str, dest_id: str, table: str) -> bytes:
        with self._lock:
            row = self._conn.execute(
                "SELECT cursor FROM watermarks "
                "WHERE source_id=? AND dest_id=? AND table_name=?",
                (source_id, dest_id, table),
            ).fetchone()
            return bytes(row["cursor"]) if row else b""

    def _write_cursor(
        self,
        source_id: str,
        dest_id: str,
        table: str,
        *,
        cursor: bytes,
        last_ack: str,
    ) -> None:
        """Write the send record without asking the source's order.

        Private: only ``advance_cursor`` (when the comparison raises) and
        tests that seed a store call it.  ``reset_cursor`` remains the
        one public way to move a record backward.
        """
        with self._lock, self._conn:
            self._upsert(source_id, dest_id, table, cursor, last_ack)

    def _upsert(
        self, source_id: str, dest_id: str, table: str, cursor: bytes,
        last_ack: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO watermarks(source_id, dest_id, table_name, "
            "cursor, last_ack) VALUES(?,?,?,?,?) "
            "ON CONFLICT(source_id, dest_id, table_name) DO UPDATE SET "
            "cursor=excluded.cursor, last_ack=excluded.last_ack",
            (source_id, dest_id, table, cursor, last_ack),
        )

    def advance_cursor(
        self,
        source_id: str,
        dest_id: str,
        table: str,
        *,
        cursor: bytes,
        last_ack: str,
        is_after: Callable[[bytes, bytes], bool],
    ) -> Advance:
        """Move the send record forward only (spec §4.6).  Returns what it did.

        One ``BEGIN IMMEDIATE`` transaction holds the read of the stored
        record, the comparison and the write, so another connection (a
        second process on the same file) cannot write between them.  It
        waits on the connection's busy timeout for that lock instead.

        * No stored record: write it; ``WRITTEN``.
        * ``is_after(cursor, stored)``: write; ``WRITTEN``.
        * Neither lies after the other in the source's order: refresh
          ``last_ack`` and keep the stored cursor bytes; ``REFRESHED``.
        * ``cursor`` lies before ``stored``: change nothing and log a
          WARNING with both values (``_log_not_forward``, one per key per
          ``BACKWARD_WARN_INTERVAL_S``); ``REFUSED``.
        * The comparison raises, or the stored value cannot be read as
          bytes: write anyway and log a WARNING with both values and the
          error (``_log_check_failed``, same throttle);
          ``WRITTEN_UNCHECKED``.  A broken comparator must not freeze a
          key, since every pump would then re-send the same batch.

        A logging failure changes no decision.  A database error rolls the
        transaction back and propagates.
        """
        key = (source_id, dest_id, table)
        with self._lock:
            if self._conn.in_transaction:
                # Every writer here commits before it returns, so an open
                # transaction means a caller broke that rule; a nested
                # BEGIN would fail anyway.
                raise RuntimeError(
                    "advance_cursor called inside an open transaction")
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._advance_in_txn(key, cursor, last_ack, is_after)
                self._conn.commit()
            except BaseException:
                try:
                    self._conn.rollback()
                except Exception:  # noqa: BLE001 — keep the original error
                    logger.debug("send record (%s, %s, %s): rollback failed",
                                 *key, exc_info=True)
                raise
            return result

    def _advance_in_txn(
        self,
        key: tuple[str, str, str],
        cursor: bytes,
        last_ack: str,
        is_after: Callable[[bytes, bytes], bool],
    ) -> Advance:
        row = self._conn.execute(
            "SELECT cursor FROM watermarks "
            "WHERE source_id=? AND dest_id=? AND table_name=?", key,
        ).fetchone()
        if row is None:
            self._upsert(*key, cursor, last_ack)
            return Advance.WRITTEN
        raw = row["cursor"]
        try:
            stored = bytes(raw)
        except Exception as exc:  # noqa: BLE001 — e.g. a TEXT cursor from a hand edit
            self._log_check_failed(key, "forward check", exc, raw, cursor)
            self._upsert(*key, cursor, last_ack)
            return Advance.WRITTEN_UNCHECKED
        try:
            after = bool(is_after(cursor, stored))
            before = (not after and cursor != stored
                      and bool(is_after(stored, cursor)))
        except Exception as exc:  # noqa: BLE001 — never freeze the key
            self._log_check_failed(key, "cursor comparator", exc, stored, cursor)
            self._upsert(*key, cursor, last_ack)
            return Advance.WRITTEN_UNCHECKED
        if after:
            self._upsert(*key, cursor, last_ack)
            return Advance.WRITTEN
        if before:
            try:
                self._log_not_forward(key, stored, cursor)
            except Exception:  # noqa: BLE001 — a logging failure changes nothing
                pass
            return Advance.REFUSED
        self._conn.execute(
            "UPDATE watermarks SET last_ack=? "
            "WHERE source_id=? AND dest_id=? AND table_name=?",
            (last_ack, *key),
        )
        return Advance.REFRESHED

    def _log_check_failed(
        self, key: tuple[str, str, str], what: str, exc: BaseException,
        stored: object, cursor: bytes,
    ) -> None:
        """Log that the forward check for ``key`` raised, without raising.

        The first failure on a key logs a WARNING.  Failures within
        ``BACKWARD_WARN_INTERVAL_S`` of that WARNING log at DEBUG, so a
        comparator that raises on every acknowledgement cannot flood the
        journal.  A failure in here (a log filter that raises, a clock
        that raises) gets swallowed.  The caller holds ``self._lock``.
        """
        try:
            now = self._clock()
            last = self._check_failed_at.get(key)
            if last is not None and now - last < BACKWARD_WARN_INTERVAL_S:
                level = logging.DEBUG
            else:
                level = logging.WARNING
                self._check_failed_at[key] = now
            logger.log(
                level,
                "send record (%s, %s, %s): %s raised %s: %s; stored %r, "
                "new %r; writing it unchecked",
                *key, what, type(exc).__name__, exc, stored, cursor,
            )
        except Exception:  # noqa: BLE001 — a logging failure must not block the write
            pass

    def _log_not_forward(
        self, key: tuple[str, str, str], stored: bytes, new: bytes,
    ) -> None:
        """Log a refused write: ``new`` lies before ``key``'s send record.

        The first refusal on a key logs a WARNING naming the key and both
        values, and opens a window of ``BACKWARD_WARN_INTERVAL_S`` on
        ``self._clock``.  Inside the window each further one adds to the
        key's count and logs at DEBUG.  The first one after the window
        logs one WARNING with the key, the count since the last WARNING
        (itself included) and the latest pair of values, then opens a new
        window.  A grep for "does not move forward" finds the WARNINGs
        alone.  The caller holds ``self._lock``.
        """
        now = self._clock()
        window = self._warn_windows.get(key)
        if window is None:
            self._warn_windows[key] = [now, 0]
            logger.warning(
                "send record (%s, %s, %s) does not move forward: stored %r, "
                "new %r; refused, record held (repeats on this key log at "
                "DEBUG for %d s, then one WARNING counts them)",
                *key, stored, new, BACKWARD_WARN_INTERVAL_S,
            )
            return
        window[1] += 1
        if now - window[0] < BACKWARD_WARN_INTERVAL_S:
            logger.debug(
                "send record (%s, %s, %s) again fails to move forward: "
                "stored %r, new %r (%d since the last warning)",
                *key, stored, new, window[1],
            )
            return
        logger.warning(
            "send record (%s, %s, %s) does not move forward: %d %s since the "
            "last warning, the latest stored %r, new %r; refused, record held",
            *key, window[1], "time" if window[1] == 1 else "times", stored, new,
        )
        self._warn_windows[key] = [now, 0]

    # -- attempts (audit log) --

    def record_attempt(
        self,
        *,
        ts: str,
        source_id: str,
        dest_id: str,
        table: str,
        outcome: str,
        records: Optional[int],
        bytes_: Optional[int],
        error: Optional[str],
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO attempts(ts, source_id, dest_id, table_name, "
                "outcome, records, bytes, error) VALUES(?,?,?,?,?,?,?,?)",
                (ts, source_id, dest_id, table, outcome, records, bytes_, error),
            )
            # Trim to last N entries to keep the file small.  Cheap because
            # rowid is the autoincrement key.
            self._conn.execute(
                "DELETE FROM attempts WHERE rowid IN ("
                "  SELECT rowid FROM attempts ORDER BY rowid DESC LIMIT -1 OFFSET ?"
                ")",
                (_ATTEMPTS_RING_SIZE,),
            )

    def recent_attempts(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM attempts ORDER BY rowid DESC LIMIT ?",
                    (limit,),
                )
            )

    # -- deliverables (retry queue) --

    def enqueue_deliverable(
        self,
        *,
        pipeline: str,
        payload_blob: bytes,
        enqueued_at: str,
        next_attempt_at: str,
        source_id: str = "",
        dest_id: str = "",
        table: str = "",
        cursor_after: bytes = b"",
        commit_token: bytes = b"",
    ) -> int:
        with self._lock, self._conn:
            if cursor_after:
                # hs-uploader#4: a deliverable is identified by what it
                # advances the cursor TO.  Two rows for the same
                # (pipeline, cursor_after) are the same work queued twice
                # — the retry storm's raw material — so keep the first.
                # Empty cursors (file-tree sources: each file is its own
                # payload) are never collapsed.
                row = self._conn.execute(
                    "SELECT id FROM deliverables WHERE pipeline=? AND "
                    "cursor_after=? ORDER BY id LIMIT 1",
                    (pipeline, cursor_after),
                ).fetchone()
                if row is not None:
                    logger.warning(
                        "%s: deliverable for cursor %r already queued (id %s) "
                        "— not enqueuing a duplicate", pipeline,
                        cursor_after[:40], row["id"],
                    )
                    return int(row["id"])
            cur = self._conn.execute(
                "INSERT INTO deliverables(pipeline, payload_blob, enqueued_at, "
                "attempts, next_attempt_at, source_id, dest_id, table_name, "
                "cursor_after, commit_token) "
                "VALUES(?,?,?,0,?,?,?,?,?,?)",
                (
                    pipeline, payload_blob, enqueued_at, next_attempt_at,
                    source_id, dest_id, table,
                    cursor_after, commit_token,
                ),
            )
            return int(cur.lastrowid)

    def pop_due_deliverable(
        self, pipeline: str, *, now: str
    ) -> Optional[Deliverable]:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT id, pipeline, payload_blob, enqueued_at, attempts, "
                "next_attempt_at, source_id, dest_id, table_name, "
                "cursor_after, commit_token FROM deliverables "
                "WHERE pipeline=? AND next_attempt_at<=? "
                "ORDER BY next_attempt_at LIMIT 1",
                (pipeline, now),
            ).fetchone()
            if row is None:
                return None
            # Take it off the queue; the caller is responsible for
            # requeue-on-retry-later or dead-lettering.
            self._conn.execute(
                "DELETE FROM deliverables WHERE id=?", (row["id"],)
            )
            return Deliverable(
                id=row["id"],
                pipeline=row["pipeline"],
                payload_blob=bytes(row["payload_blob"]),
                enqueued_at=row["enqueued_at"],
                attempts=row["attempts"],
                next_attempt_at=row["next_attempt_at"],
                source_id=row["source_id"] or "",
                dest_id=row["dest_id"] or "",
                table=row["table_name"] or "",
                cursor_after=bytes(row["cursor_after"] or b""),
                commit_token=bytes(row["commit_token"] or b""),
            )

    def requeue_deliverable(self, deliverable: Deliverable) -> None:
        """Re-insert a previously-popped deliverable with its updated
        attempt count and next-attempt time.

        ``pop_due_deliverable`` removes the row when it claims it; this
        method re-inserts it, preserving the original id so audit logs
        stay linkable across retries.
        """
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO deliverables(id, pipeline, payload_blob, "
                "enqueued_at, attempts, next_attempt_at, source_id, "
                "dest_id, table_name, cursor_after, commit_token) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    deliverable.id,
                    deliverable.pipeline,
                    deliverable.payload_blob,
                    deliverable.enqueued_at,
                    deliverable.attempts,
                    deliverable.next_attempt_at,
                    deliverable.source_id,
                    deliverable.dest_id,
                    deliverable.table,
                    deliverable.cursor_after,
                    deliverable.commit_token,
                ),
            )

    # -- dead letter --

    def send_to_dead_letter(
        self,
        *,
        ts: str,
        pipeline: str,
        payload_blob: bytes,
        final_error: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO dead_letter(ts, pipeline, payload_blob, final_error) "
                "VALUES(?,?,?,?)",
                (ts, pipeline, payload_blob, final_error),
            )

    def dead_letter_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letter"
            ).fetchone()
            return int(row["n"])

    # -- introspection helpers (used by CLI `status`) --

    def all_cursors(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT source_id, dest_id, table_name, last_ack, "
                    "length(cursor) AS cursor_len FROM watermarks "
                    "ORDER BY source_id, dest_id, table_name"
                )
            )

    def deliverable_count(self, pipeline: Optional[str] = None) -> int:
        with self._lock:
            if pipeline is None:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM deliverables"
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM deliverables WHERE pipeline=?",
                    (pipeline,),
                ).fetchone()
            return int(row["n"])

    def reset_cursor(
        self, source_id: str, dest_id: str, table: str
    ) -> bool:
        """Remove the watermark row.  Next pump starts from the beginning.

        Returns True if a row was removed, False if none existed.
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM watermarks "
                "WHERE source_id=? AND dest_id=? AND table_name=?",
                (source_id, dest_id, table),
            )
            return cur.rowcount > 0
