"""Schema version of the watermark store, and the migrations that raise it.

``PRAGMA user_version`` in ``watermarks.db`` holds the store's schema
version.  A file that has never met ``migrate`` reads 0.  ``migrate()``
runs every migration above the file's version, in order, inside one
``BEGIN IMMEDIATE`` transaction, so a failure part-way leaves the file as
it found it.

``migrate()`` opens the file read-write from its first read, as the
daemon's own open does.  A process that dies in the middle of a write
leaves a hot journal (``watermarks.db-journal``) beside the file, and
SQLite rolls it back on the first read through a connection allowed to
write.  A read-only connection cannot roll it back, so it refuses to read
at all.  Only ``check=True`` reads read-only; on a hot journal it raises
``JournalRecoveryNeeded`` and leaves both files as it found them.

Only ``hs-uploader migrate`` calls ``migrate()``.  Neither
``SqliteWatermarkStore``'s constructor nor the daemon at start migrates
a store (D10 in sigmond/tasks/plan-sink-control.md).  The constructor
stamps ``SCHEMA_VERSION`` only on a file that held no table when it
opened, which has nothing to migrate (spec §4.3).  Sigmond
runs the migration once every component's code sits in place and before
it restarts the daemon, and a store still on an older version keeps
working as it stands.

Version 1 (v3.70) changes no row and no table.  It only records that the
store now carries a version, so a key migration has a number to start
from.  Never edit a migration that has shipped; append a new one and
``SCHEMA_VERSION`` follows.

Version 2 (v3.73) splits the shared pskreporter send record
(sigmond/tasks/spec-key-split-v373.md §4.4).  Each record keyed
``sqlite:psk.spots`` with a pskreporter destination gets copied to the
two producer keys, ``sqlite:psk.spots@psk-recorder`` and
``sqlite:psk.spots@meteor-scatter``, unless a record already exists
there.  The old row stays, frozen, and ``superseded_keys`` records each
old key beside each new one.

Every migrate that writes, at a version from 2 up to this code's, first
copies each old pskreporter record to any producer key that holds no
record yet, and records the pair in ``superseded_keys``.  It never
touches a key that exists.  A store the daemon created stands at version
2 with no producer keys; a shared pipeline may then have used it, and
without the copy each producer pipeline would start at "now" (final
review I1).

The retry pass runs after that, on every migrate that writes, at version
2 as at version 1, inside the same transaction (spec §4.4 item 4, §6).
It handles each deliverable still queued under ``psk-pskreporter``:

* the sink still holds its rows and both producer keys hold a record:
  delete it, and each new pipeline re-reads its own rows;
* otherwise, with ``keep_retry_for`` named: move it whole to that
  pipeline, under that pipeline's source part.  It replays once there,
  and its ack advances that pipeline's key, so only the other producer
  re-sends its own rows of the batch.  A retry queued before v3.70 may
  carry no ``cursor_after``; its ack then advances nothing, and both
  producers re-send their rows of its batch;
* otherwise: leave it where it stands, and say so.

"The sink still holds its rows" means ``sink_path`` was given and its
``pending_uploads`` still holds a ``psk.spots`` row at or before the old
record's position.  The sink trims oldest first, so a surviving row at
or before the old record means no row after it has gone.  The pass never
deletes a retry it cannot prove that for, so repeating it cannot skip a
row.  It leaves dead letters alone.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, NamedTuple, Optional

__all__ = [
    "BUSY_TIMEOUT_S",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "JournalRecoveryNeeded",
    "MigrateContext",
    "MigrateError",
    "MigrateReport",
    "Migration",
    "migrate",
]

# How long migrate waits for another process's write lock.  The daemon and
# the in-process senders each hold it for milliseconds; Python's default of
# 5 s would turn an unlucky moment into a failed update.
BUSY_TIMEOUT_S = 30.0


class MigrateError(RuntimeError):
    """The file opened, but it holds no watermark store."""


class JournalRecoveryNeeded(RuntimeError):
    """A read-only check met a hot journal.  A crash left a write half
    done, and only a connection allowed to write can roll it back."""


@dataclass(frozen=True)
class MigrateContext:
    """What a migration may need beyond the store itself.

    ``sink_path``: the sink (``sink.db``) the pskreporter pipelines read,
    opened read-only to learn whether it still holds a retry's rows.
    ``keep_retry_for``: the pipeline that inherits a ``psk-pskreporter``
    retry the pass cannot delete; one of ``NEW_PSKREPORTER_PIPELINES``.
    """

    sink_path: Optional[Path] = None
    keep_retry_for: Optional[str] = None


# A migration's apply(conn, ctx) runs inside migrate's transaction and
# returns the lines it reports (or None).  preview(conn, ctx), when set,
# reads only and returns the lines apply would report, for --check.
@dataclass(frozen=True)
class Migration:
    version: int
    summary: str
    apply: Callable[[sqlite3.Connection, MigrateContext], Optional[list[str]]]
    preview: Optional[
        Callable[[sqlite3.Connection, MigrateContext], list[str]]] = None

    @property
    def name(self) -> str:
        return f"{self.version}: {self.summary}"


@dataclass
class MigrateReport:
    from_version: int          # the file's version when migrate looked
    to_version: int            # the file's version when migrate returned
    applied: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    # What the migrations and the retry pass copied, deleted, moved or
    # left, one line each; with check=True, what they would do.
    lines: list[str] = field(default_factory=list)
    # check=True only: whether a migrate would write anything, a pending
    # migration or a retry the pass would delete or move.
    would_write: bool = False


def _record_version_only(conn: sqlite3.Connection,
                         ctx: MigrateContext) -> None:
    """Version 1 changes nothing; the runner records the number."""


# ---- version 2: the pskreporter split (spec-key-split-v373 §4.4) ----------

OLD_PSKREPORTER_PIPELINE = "psk-pskreporter"
OLD_PSK_SOURCE = "sqlite:psk.spots"
# Each new pipeline and its source part, psk-recorder's first.
NEW_PSKREPORTER_PIPELINES: dict[str, str] = {
    "psk-recorder-pskreporter": "sqlite:psk.spots@psk-recorder",
    "meteor-scatter-pskreporter": "sqlite:psk.spots@meteor-scatter",
}
# The rows the old source part reads in the sink.
_PSK_TARGET = ("psk", "spots")

# Frozen with migration 2.  The store's own schema creates the same table
# on a new file; this copy stays as shipped even if that one changes.
_SUPERSEDED_KEYS_DDL = """
CREATE TABLE IF NOT EXISTS superseded_keys (
    old_source_id TEXT NOT NULL,
    old_dest_id   TEXT NOT NULL,
    old_table     TEXT NOT NULL,
    new_source_id TEXT NOT NULL,
    new_dest_id   TEXT NOT NULL,
    new_table     TEXT NOT NULL,
    migrated_at   TEXT NOT NULL,
    PRIMARY KEY (old_source_id, old_dest_id, old_table,
                 new_source_id, new_dest_id, new_table)
)
"""


def _show(blob: object) -> str:
    """A cursor for a report line: ASCII as it reads, anything else repr."""
    if isinstance(blob, (bytes, bytearray, memoryview)):
        raw = bytes(blob)
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError:
            return repr(raw)
        return text if text.isprintable() else repr(raw)
    return repr(blob)


def _key(source_id: str, dest_id: str, table: str) -> str:
    return f"({source_id}, {dest_id}, {table})"


def _old_pskreporter_records(conn: sqlite3.Connection) -> list[tuple]:
    """Each send record under the old source part with a pskreporter
    destination: (source_id, dest_id, table_name, cursor, last_ack)."""
    return conn.execute(
        "SELECT source_id, dest_id, table_name, cursor, last_ack "
        "FROM watermarks WHERE source_id = ? AND dest_id LIKE 'pskreporter%' "
        "ORDER BY dest_id, table_name", (OLD_PSK_SOURCE,)).fetchall()


def _has_record(conn: sqlite3.Connection, source_id: str, dest_id: str,
                table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM watermarks "
        "WHERE source_id = ? AND dest_id = ? AND table_name = ?",
        (source_id, dest_id, table)).fetchone() is not None


def _split_pskreporter(conn: sqlite3.Connection, ctx: MigrateContext, *,
                       write: bool = True) -> list[str]:
    """Copy each shared pskreporter send record to both producer keys.

    The old row stays.  A new key that already holds a record keeps it.
    ``superseded_keys`` gets one row per old key and new key, copied or
    kept, so a reader can name the old key's successors either way.  With
    ``write=False`` it only reads and reports what it would do.
    """
    if write:
        conn.execute(_SUPERSEDED_KEYS_DDL)
    now = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []
    olds = _old_pskreporter_records(conn)
    for src, dest, table, cursor, last_ack in olds:
        for new_src in NEW_PSKREPORTER_PIPELINES.values():
            existing = conn.execute(
                "SELECT cursor FROM watermarks "
                "WHERE source_id = ? AND dest_id = ? AND table_name = ?",
                (new_src, dest, table)).fetchone()
            if existing is None:
                if write:
                    conn.execute(
                        "INSERT INTO watermarks (source_id, dest_id, "
                        "table_name, cursor, last_ack) VALUES (?,?,?,?,?)",
                        (new_src, dest, table, cursor, last_ack))
                lines.append(
                    f"{'copied' if write else 'would copy'} send record "
                    f"{_key(src, dest, table)} (cursor {_show(cursor)}, "
                    f"last_ack {last_ack}) to {_key(new_src, dest, table)}")
            else:
                lines.append(
                    f"{'kept' if write else 'would keep'} "
                    f"{_key(new_src, dest, table)}: a record already exists "
                    f"there (cursor {_show(existing[0])}); "
                    f"{_key(src, dest, table)} not copied over it")
            if write:
                conn.execute(
                    "INSERT OR IGNORE INTO superseded_keys (old_source_id, "
                    "old_dest_id, old_table, new_source_id, new_dest_id, "
                    "new_table, migrated_at) VALUES (?,?,?,?,?,?,?)",
                    (src, dest, table, new_src, dest, table, now))
    if not olds:
        lines.append(f"no pskreporter send record under {OLD_PSK_SOURCE}; "
                     "nothing to copy")
    return lines


def _preview_split(conn: sqlite3.Connection, ctx: MigrateContext) -> list[str]:
    return _split_pskreporter(conn, ctx, write=False)


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "record the schema version; changes no row and no table",
              _record_version_only),
    Migration(2, "split the pskreporter send record by producer",
              _split_pskreporter, _preview_split),
)


# ---- every migrate: fill a missing producer key (spec §4.4 item 4) --------


def _fill_missing_producer_keys(conn: sqlite3.Connection, *,
                                write: bool) -> list[str]:
    """Copy each old pskreporter send record to any producer key that
    holds no record yet, and record the pair in ``superseded_keys``.

    Migration 2 runs once.  A store the daemon created stands at version 2
    with no producer keys, and a shared pipeline may then have used it.
    Without this copy, each producer pipeline would start at its own
    anchor ("now") and skip the rows the shared pipeline had not sent
    (final review I1).  The copy fills only a missing key, with a position
    the old pipeline has acked through.  It never touches a key that
    exists, so it never moves a producer's record.  With ``write=False``
    it only reads and reports what it would do.
    """
    if not _has_table(conn, "watermarks"):
        return []
    now = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []
    made_table = False
    for src, dest, table, cursor, last_ack in _old_pskreporter_records(conn):
        for new_src in NEW_PSKREPORTER_PIPELINES.values():
            if _has_record(conn, new_src, dest, table):
                continue
            if write:
                if not made_table:
                    conn.execute(_SUPERSEDED_KEYS_DDL)
                    made_table = True
                conn.execute(
                    "INSERT INTO watermarks (source_id, dest_id, "
                    "table_name, cursor, last_ack) VALUES (?,?,?,?,?)",
                    (new_src, dest, table, cursor, last_ack))
                conn.execute(
                    "INSERT OR IGNORE INTO superseded_keys (old_source_id, "
                    "old_dest_id, old_table, new_source_id, new_dest_id, "
                    "new_table, migrated_at) VALUES (?,?,?,?,?,?,?)",
                    (src, dest, table, new_src, dest, table, now))
            lines.append(
                f"{'copied' if write else 'would copy'} send record "
                f"{_key(src, dest, table)} (cursor {_show(cursor)}, "
                f"last_ack {last_ack}) to {_key(new_src, dest, table)}: "
                "that producer key held no record")
    return lines


def _fills(version: int) -> bool:
    """Whether a store at ``version`` gets the fill: one this code knows,
    from version 2 (the split) up.  A newer store stays as it stands; the
    retry pass's own guard still refuses to delete a retry there while a
    producer key is missing."""
    return 2 <= version <= max(m.version for m in MIGRATIONS)


# ---- the retry pass: every migrate, not once (spec §4.4 item 4, §6) -------


class _RetryAction(NamedTuple):
    id: int
    cursor_after: bytes
    action: str              # "delete", "move" or "stay"
    to_pipeline: str         # for "move"
    to_source: str           # for "move"
    reason: str


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,)).fetchone() is not None


def _old_last_id(conn: sqlite3.Connection, source_id: str, dest_id: str,
                 table: str) -> tuple[Optional[int], str, tuple[str, str]]:
    """The row id the old send record names for this retry, or None and
    why, and the record's (dest_id, table_name).

    A keyless retry (queued before v3.70) names no destination; it counts
    only when exactly one pskreporter record exists under the old source.
    """
    sql = ("SELECT cursor, dest_id, table_name FROM watermarks "
           "WHERE source_id = ? "
           "AND dest_id LIKE 'pskreporter%'")
    params: list = [source_id or OLD_PSK_SOURCE]
    if dest_id:
        sql += " AND dest_id = ?"
        params.append(dest_id)
    if table:
        sql += " AND table_name = ?"
        params.append(table)
    rows = conn.execute(sql, params).fetchall()
    if len(rows) != 1:
        return None, (f"{len(rows)} old pskreporter send records match it, "
                      "so its position cannot be told"), ("", "")
    raw, rdest, rtable = rows[0]
    where = (rdest, rtable)
    if not raw:
        return None, ("the old send record is empty, so its position cannot "
                      "be told"), where
    from ..sources.sqlite import _Cursor
    try:
        return _Cursor.from_bytes(bytes(raw)).last_id, "", where
    except (TypeError, ValueError):
        return None, (f"the old send record's cursor {_show(raw)} is not a "
                      "row id, so its position cannot be told"), where


def _sink_holds(sink_path: Optional[Path], last_id: int) -> tuple[bool, str]:
    """Whether the sink still holds a psk.spots row at or before
    ``last_id``.  Opens it read-only; never creates it."""
    if sink_path is None:
        return False, "no --sink given"
    try:
        conn = _connect(Path(sink_path), "ro")
    except sqlite3.Error as exc:
        return False, f"cannot read the sink at {sink_path}: {exc}"
    try:
        found = conn.execute(
            "SELECT 1 FROM pending_uploads WHERE target_db = ? AND "
            "target_table = ? AND id <= ? LIMIT 1",
            (*_PSK_TARGET, last_id)).fetchone()
    except sqlite3.Error as exc:
        return False, f"cannot read the sink at {sink_path}: {exc}"
    finally:
        conn.close()
    if found is None:
        return False, (f"the sink holds no psk.spots row at or before the old "
                       f"record ({last_id}), so some of its rows may be gone")
    return True, (f"the sink still holds psk.spots rows from the old record "
                  f"({last_id}) on, so each new pipeline re-reads its own")


def _missing_producer_keys(conn: sqlite3.Connection, dest_id: str,
                           table: str) -> list[str]:
    return [src for src in NEW_PSKREPORTER_PIPELINES.values()
            if not _has_record(conn, src, dest_id, table)]


def _plan_retries(conn: sqlite3.Connection, ctx: MigrateContext, *,
                  assume_filled: bool = False) -> list[_RetryAction]:
    """Decide, reading only, what to do with each retry under the old name.

    A retry may be deleted only when both producer keys exist: a pipeline
    without its key starts at its own anchor and would skip the retry's
    rows (final review I1).  ``assume_filled`` reads them as existing, for
    a read before ``_fill_missing_producer_keys`` runs; that copy fills
    them from the very record that gives the retry its position.
    """
    if not _has_table(conn, "deliverables"):
        return []
    plan: list[_RetryAction] = []
    for rid, src, dest, table, cursor_after in conn.execute(
            "SELECT id, source_id, dest_id, table_name, cursor_after "
            "FROM deliverables WHERE pipeline = ? ORDER BY id",
            (OLD_PSKREPORTER_PIPELINE,)).fetchall():
        last_id, why, (rdest, rtable) = _old_last_id(
            conn, src or "", dest or "", table or "")
        holds = False
        if last_id is not None:
            holds, why = _sink_holds(ctx.sink_path, last_id)
        if holds and not assume_filled:
            missing = _missing_producer_keys(conn, rdest, rtable)
            if missing:
                holds = False
                why = (f"no send record under producer key "
                       f"{', '.join(missing)}, so that pipeline would start "
                       "at its own anchor and could skip the retry's rows")
        after = bytes(cursor_after or b"")
        if holds:
            plan.append(_RetryAction(rid, after, "delete", "", "", why))
        elif ctx.keep_retry_for:
            plan.append(_RetryAction(
                rid, after, "move", ctx.keep_retry_for,
                NEW_PSKREPORTER_PIPELINES[ctx.keep_retry_for], why))
        else:
            plan.append(_RetryAction(rid, after, "stay", "", "", why))
    return plan


def _retry_lines(plan: list[_RetryAction], *, done: bool) -> list[str]:
    out = []
    for a in plan:
        head = (f"retry {a.id} ({OLD_PSKREPORTER_PIPELINE}, cursor_after "
                f"{_show(a.cursor_after)})")
        if a.action == "delete":
            verb = "deleted" if done else "would delete"
            out.append(f"{head}: {verb}; {a.reason}")
        elif a.action == "move":
            verb = "moved" if done else "would move"
            line = (f"{head}: {verb} to {a.to_pipeline} under "
                    f"{a.to_source}; {a.reason}")
            if not a.cursor_after:
                # Queued before v3.70: its ack advances no send record, so
                # the keeper re-reads its rows too (final review M2).
                line += ("; it carries no cursor_after, so its ack advances "
                         "no send record and both producers re-send their "
                         "rows of its batch")
            out.append(line)
        else:
            out.append(f"{head}: stays under {OLD_PSKREPORTER_PIPELINE}; "
                       f"{a.reason}, and no --keep-retry-for names a "
                       "pipeline to move it to")
    return out


def _apply_retries(conn: sqlite3.Connection, plan: list[_RetryAction]) -> None:
    for a in plan:
        if a.action == "delete":
            conn.execute("DELETE FROM deliverables WHERE id = ?", (a.id,))
        elif a.action == "move":
            # The key's source part follows the pipeline; dest and table
            # stay.  cursor_after and commit_token stay too: both hold a
            # pending_uploads id, which the producer source reads the same
            # way, and its commit deletes only its own producer's rows.
            conn.execute(
                "UPDATE deliverables SET pipeline = ?, source_id = ? "
                "WHERE id = ?", (a.to_pipeline, a.to_source, a.id))


def _validate(ctx: MigrateContext) -> None:
    if (ctx.keep_retry_for is not None
            and ctx.keep_retry_for not in NEW_PSKREPORTER_PIPELINES):
        raise ValueError(
            f"--keep-retry-for {ctx.keep_retry_for!r}: must name one of "
            f"{', '.join(NEW_PSKREPORTER_PIPELINES)}")

# The highest version above; append a Migration and this follows.
SCHEMA_VERSION = max(m.version for m in MIGRATIONS)


def _connect(db: Path, mode: str) -> sqlite3.Connection:
    """Open ``db`` with ``mode`` ``ro`` or ``rw``; neither creates a file.
    as_uri() percent-encodes a space, '?' or '#' in the path."""
    uri = f"{db.resolve().as_uri()}?mode={mode}"
    return sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_S,
                           isolation_level=None)


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _inspect(conn: sqlite3.Connection, db: Path) -> int:
    """The file's version.  Refuse a file that holds no ``watermarks``
    table, such as ``sink.db`` passed by mistake, before anything writes
    to it.  Through a read-write connection this first read also rolls
    back a hot journal."""
    found = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='watermarks'"
    ).fetchone()
    if found is None:
        raise MigrateError(
            f"{db} holds no watermarks table, so it is not an "
            "hs-uploader watermark store"
        )
    return _user_version(conn)


def _hot_journal(exc: sqlite3.OperationalError, db: Path) -> bool:
    """True when a read-only read failed because a hot journal waits to
    be rolled back."""
    name = getattr(exc, "sqlite_errorname", None)    # Python 3.11 and later
    if name is not None:
        return name == "SQLITE_READONLY_ROLLBACK"
    return "readonly" in str(exc) and Path(f"{db}-journal").exists()


def _check(db: Path, ctx: MigrateContext) -> MigrateReport:
    conn = _connect(db, "ro")
    try:
        try:
            current = _inspect(conn, db)
        except sqlite3.OperationalError as exc:
            if _hot_journal(exc, db):
                raise JournalRecoveryNeeded(
                    f"a crash left {db}-journal to recover") from exc
            raise
        pending = [m for m in MIGRATIONS if m.version > current]
        lines: list[str] = []
        for m in pending:
            if m.preview is not None:
                lines.extend(m.preview(conn, ctx))
        final = max([current] + [m.version for m in pending])
        # Only a pending migration 2 copies every missing key itself, so
        # only it makes the fill redundant: the fill would find nothing left
        # to copy after it.  A later migration does not copy keys, and
        # migrate() still runs the fill after it, so --check must preview it.
        fill = (_fill_missing_producer_keys(conn, write=False)
                if _fills(final) and not any(m.version == 2 for m in pending)
                else [])
        lines.extend(fill)
        plan = _plan_retries(conn, ctx, assume_filled=_fills(final))
        lines.extend(_retry_lines(plan, done=False))
    finally:
        conn.close()
    return MigrateReport(
        from_version=current, to_version=current,
        pending=[m.name for m in pending], lines=lines,
        would_write=(bool(pending) or bool(fill)
                     or any(a.action != "stay" for a in plan)))


def migrate(path: str, *, check: bool = False,
            ctx: MigrateContext = MigrateContext()) -> MigrateReport:
    """Bring the store at ``path`` to ``SCHEMA_VERSION``, fill any missing
    producer key, then run the retry pass.

    ``check=True`` opens the file read-only and reports what would run,
    the fill and the retry pass included.  A store already at, or above,
    this code's version, with no producer key to fill and no retry under
    ``psk-pskreporter`` to delete or move, needs nothing and gets no write lock: a station rolled back from a
    newer release keeps its newer number.  ``ctx`` names the sink and the
    pipeline that keeps a retry (``MigrateContext``); a ``keep_retry_for``
    outside ``NEW_PSKREPORTER_PIPELINES`` raises ValueError before any
    read.  Raises FileNotFoundError for a missing or empty file when run as
    root or with ``check=True`` (otherwise migrate creates the store), MigrateError for a file that holds no store, and
    sqlite3.Error when SQLite refuses, for example a write lock held past
    ``BUSY_TIMEOUT_S``.  With ``check=True`` it raises
    JournalRecoveryNeeded on a hot journal and changes nothing.
    """
    _validate(ctx)
    db = Path(path)
    empty = not db.exists() or (db.is_file() and db.stat().st_size == 0)
    if empty and not check and os.geteuid() != 0:
        # A store that holds no table has no data for a migration to move.
        # The store stamps SCHEMA_VERSION on what it creates.  Root never
        # creates it: the file would belong to root, not to hsupload.
        from .sqlite import SqliteWatermarkStore
        SqliteWatermarkStore(db).close()
        return MigrateReport(from_version=0, to_version=SCHEMA_VERSION)
    if not db.is_file():
        raise FileNotFoundError(f"no store file at {db}")
    if check:
        return _check(db, ctx)

    # Read-write from the first read: a hot journal left by a crash rolls
    # back here, as it would when the daemon opens the store.
    conn = _connect(db, "rw")
    try:
        current = _inspect(conn, db)
        plan = _plan_retries(conn, ctx, assume_filled=_fills(current))
        if (not any(m.version > current for m in MIGRATIONS)
                and not (_fills(current)
                         and _fill_missing_producer_keys(conn, write=False))
                and all(a.action == "stay" for a in plan)):
            # Nothing to do: no write lock taken, no byte written.  A
            # retry that stays still gets its line.
            return MigrateReport(from_version=current, to_version=current,
                                 lines=_retry_lines(plan, done=True))
        # IMMEDIATE takes the write lock now, waiting out a concurrent
        # writer, so no other process slips a write between our read of
        # the version and our update of it.
        conn.execute("BEGIN IMMEDIATE")
        try:
            start = _user_version(conn)   # again, now under the lock
            version = start
            applied: list[str] = []
            lines: list[str] = []
            for m in MIGRATIONS:
                if m.version <= version:
                    continue
                lines.extend(m.apply(conn, ctx) or [])
                conn.execute("PRAGMA user_version = %d" % m.version)
                version = m.version
                applied.append(m.name)
            # Every migrate: fill a missing producer key before the retry
            # pass, so a deleted retry's rows get re-read (spec §4.4 item 4).
            if _fills(version):
                lines.extend(_fill_missing_producer_keys(conn, write=True))
            # The retry pass, decided again under the lock.
            plan = _plan_retries(conn, ctx)
            _apply_retries(conn, plan)
            lines.extend(_retry_lines(plan, done=True))
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
    return MigrateReport(from_version=start, to_version=version,
                         applied=applied, lines=lines)
