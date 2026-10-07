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
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Optional

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


logger = logging.getLogger(__name__)

# advance_cursor_checked logs a send record that does not move forward
# with one WARNING per key per this many seconds, and at DEBUG between.
# A shared key can run backward on every acknowledgement, and a WARNING
# each time would bury every other line in the journal.
BACKWARD_WARN_INTERVAL_S = 3600.0


class SqliteWatermarkStore:
    """SQLite-backed implementation of ``WatermarkStore``.

    All public methods take the connection lock so concurrent callers
    from the same process serialize cleanly.  Cross-process access
    (multiple hs-uploader pumps writing the same db file) is not
    supported and is an operator config error.
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
        # advance_cursor_checked's warning windows, one per key:
        # key -> [window start on `clock`, writes since the last WARNING].
        # In memory only, so each process keeps its own.
        self._clock = clock
        self._warn_windows: dict[tuple[str, str, str], list] = {}
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
            self._conn.executescript(_SCHEMA_SQL)

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

    def advance_cursor_checked(
        self,
        source_id: str,
        dest_id: str,
        table: str,
        *,
        cursor: bytes,
        last_ack: str,
        is_after: Callable[[bytes, bytes], bool],
    ) -> bool:
        """Write the send record exactly as ``advance_cursor`` does, after
        asking ``is_after(cursor, stored)`` whether the write moves it
        forward.  Returns that answer.

        v3.70 warns only.  A write that would move the record backward,
        or leave it where it stands, gets logged with the key and both
        values and then goes ahead, so nothing the station sends
        changes.  v3.71 enforces the rule once each sender owns its key.
        ``_log_not_forward`` keeps that log to one WARNING per key per
        ``BACKWARD_WARN_INTERVAL_S``.  A comparator that raises counts as
        True: it logs a WARNING and never blocks the write, because a
        skipped write would re-send the batch on every pump.

        ``self._lock`` covers the read and the write, which serializes
        callers in this process only.  Another process that opens the
        same file (an in-process sender) can still write between them;
        a check that only warns tolerates that.
        """
        with self._lock:
            stored = self.get_cursor(source_id, dest_id, table)
            try:
                forward = bool(is_after(cursor, stored))
            except Exception as exc:  # noqa: BLE001 — never block the write
                logger.warning(
                    "send record (%s, %s, %s): cursor comparator raised %s: %s; "
                    "writing %r anyway",
                    source_id, dest_id, table, type(exc).__name__, exc, cursor,
                )
                forward = True
            if not forward:
                self._log_not_forward((source_id, dest_id, table), stored, cursor)
            self.advance_cursor(
                source_id, dest_id, table, cursor=cursor, last_ack=last_ack,
            )
            return forward

    def _log_not_forward(
        self, key: tuple[str, str, str], stored: bytes, new: bytes,
    ) -> None:
        """Log a write that does not move ``key``'s send record forward.

        The first such write on a key logs a WARNING naming the key and
        both values, and opens a window of ``BACKWARD_WARN_INTERVAL_S``
        on ``self._clock``.  Inside the window each further one adds to
        the key's count and logs at DEBUG.  The first one after the
        window logs one WARNING with the key, the count since the last
        WARNING (itself included) and the latest pair of values, then
        opens a new window.  The caller holds ``self._lock``.
        """
        now = self._clock()
        window = self._warn_windows.get(key)
        if window is None:
            self._warn_windows[key] = [now, 0]
            logger.warning(
                "send record (%s, %s, %s) does not move forward: stored %r, "
                "new %r; writing it anyway (v3.70 warns only; repeats on this "
                "key log at DEBUG for %d s, then one WARNING counts them)",
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
            "last warning, the latest stored %r, new %r; writing it anyway "
            "(v3.70 warns only)",
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
