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
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional

from .base import Deliverable

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
"""


def default_path() -> Path:
    base = os.environ.get("HS_UPLOADER_STATE_DIR", "/var/lib/hs-uploader")
    return Path(base) / "watermarks.db"


class SqliteWatermarkStore:
    """SQLite-backed implementation of ``WatermarkStore``.

    All public methods take the connection lock so concurrent callers
    from the same process serialize cleanly.  Cross-process access
    (multiple hs-uploader pumps writing the same db file) is not
    supported and is an operator config error.
    """

    def __init__(self, path: Path | str = ":memory:"):
        self.path = str(path)
        # ``check_same_thread=False`` because the orchestrator may dispatch
        # from a thread that's different from the constructor's; we
        # serialize all access through ``self._lock``.
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA_SQL)

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

    def advance_cursor(
        self,
        source_id: str,
        dest_id: str,
        table: str,
        *,
        cursor: bytes,
        last_ack: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO watermarks(source_id, dest_id, table_name, "
                "cursor, last_ack) VALUES(?,?,?,?,?) "
                "ON CONFLICT(source_id, dest_id, table_name) DO UPDATE SET "
                "cursor=excluded.cursor, last_ack=excluded.last_ack",
                (source_id, dest_id, table, cursor, last_ack),
            )

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
