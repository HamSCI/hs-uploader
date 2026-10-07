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
``SqliteWatermarkStore``'s constructor nor the daemon at start reads or
writes the version (D10 in sigmond/tasks/plan-sink-control.md): sigmond
runs the migration once every component's code sits in place and before
it restarts the daemon, and a store still on an older version keeps
working as it stands.

Version 1 (v3.70) changes no row and no table.  It only records that the
store now carries a version, so v3.71's key migration has a number to
start from.  Never edit a migration that has shipped; append a new one
and raise ``SCHEMA_VERSION`` to match.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

__all__ = [
    "BUSY_TIMEOUT_S",
    "MIGRATIONS",
    "SCHEMA_VERSION",
    "JournalRecoveryNeeded",
    "MigrateError",
    "MigrateReport",
    "Migration",
    "migrate",
]

SCHEMA_VERSION = 1

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
class Migration:
    version: int
    summary: str
    apply: Callable[[sqlite3.Connection], None]

    @property
    def name(self) -> str:
        return f"{self.version}: {self.summary}"


@dataclass
class MigrateReport:
    from_version: int          # the file's version when migrate looked
    to_version: int            # the file's version when migrate returned
    applied: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)


def _record_version_only(conn: sqlite3.Connection) -> None:
    """Version 1 changes nothing; the runner records the number."""


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "record the schema version; changes no row and no table",
              _record_version_only),
)


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


def _check(db: Path) -> MigrateReport:
    conn = _connect(db, "ro")
    try:
        current = _inspect(conn, db)
    except sqlite3.OperationalError as exc:
        if _hot_journal(exc, db):
            raise JournalRecoveryNeeded(
                f"a crash left {db}-journal to recover") from exc
        raise
    finally:
        conn.close()
    return MigrateReport(from_version=current, to_version=current,
                         pending=[m.name for m in MIGRATIONS
                                  if m.version > current])


def migrate(path: str, *, check: bool = False) -> MigrateReport:
    """Bring the store at ``path`` to ``SCHEMA_VERSION``.

    ``check=True`` opens the file read-only and reports what would run.  A
    store already at, or above, this code's version needs nothing and gets
    no write: a station rolled back from a newer release keeps its newer
    number.  Raises FileNotFoundError for a missing file (migrate never
    creates the store), MigrateError for a file that holds no store, and
    sqlite3.Error when SQLite refuses, for example a write lock held past
    ``BUSY_TIMEOUT_S``.  With ``check=True`` it raises
    JournalRecoveryNeeded on a hot journal and changes nothing.
    """
    db = Path(path)
    if not db.is_file():
        raise FileNotFoundError(f"no store file at {db}")
    if check:
        return _check(db)

    # Read-write from the first read: a hot journal left by a crash rolls
    # back here, as it would when the daemon opens the store.
    conn = _connect(db, "rw")
    try:
        current = _inspect(conn, db)
        if not any(m.version > current for m in MIGRATIONS):
            # Nothing to do: no write lock taken, no byte written.
            return MigrateReport(from_version=current, to_version=current)
        # IMMEDIATE takes the write lock now, waiting out a concurrent
        # writer, so no other process slips a write between our read of
        # the version and our update of it.
        conn.execute("BEGIN IMMEDIATE")
        try:
            start = _user_version(conn)   # again, now under the lock
            version = start
            applied: list[str] = []
            for m in MIGRATIONS:
                if m.version <= version:
                    continue
                m.apply(conn)
                conn.execute("PRAGMA user_version = %d" % m.version)
                version = m.version
                applied.append(m.name)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
    return MigrateReport(from_version=start, to_version=version, applied=applied)
