"""SQLite source — reads from `sigmond.hamsci_ch.SqliteWriter`'s
`pending_uploads` queue.

Mirrors `ClickHouseSource`'s role and shape: yields `RecordBatch`es
starting strictly after the supplied opaque cursor, with strict
schema-version checking.  Drop-in replacement on hosts that have
flipped to SQLite via `smd storage migrate-to-sqlite`.

Pipeline shape::

    Producer.hamsci_ch.Writer.from_env()  → SqliteWriter.flush()
        → pending_uploads (target_db, target_table, schema_version,
                           payload_json, queued_at)
    SqliteSource.iter_batches(cursor, limit)
        → SELECT rows WHERE id > cursor
                       AND target_db   = <database>
                       AND target_table = <table>
                       AND schema_version IN (accepted)
        → ORDER BY id ASC LIMIT N
    Transport ACKs → orchestrator calls `source.commit(commit_token)`
        → DELETE FROM pending_uploads WHERE id <= commit_token

Cursor format
-------------

Opaque bytes; internally an ASCII decimal integer encoding the last
consumed ``id``.  Empty cursor (``b""``) means "from the beginning of
the queue" — translated to ``0``.

Schema-version handling differs from CH
---------------------------------------

The CH source can compute a column hash over the live table and look
that up against `hs_uploader.schema._BUILTINS`.  SQLite payloads are
JSON, so there is no "table schema" to hash — but every row carries
the producer's `schema_version` int.  Strict mode here is:

1. SELECT filters on `schema_version IN (accepted_schema_versions)`.
2. If any row exists with a non-accepted schema_version, health is
   surfaced as `"stale-schema"` (operator must upgrade producer or
   widen accepted_schema_versions).

`extra_where`
-------------

`[("radiod_id", "=", "my-rx888"), ("mode", "IN", ["ft8","ft4"])]`
is rendered against `json_extract(payload_json, '$.<col>')` so
multi-instance scoping works the same way it does on CH.  The same
restricted operator set applies as in the CH source — values pass
through SQLite parameterization, column names are validated alnum
before being inlined.

`start_at`
----------

Empty cursor + `start_at="now"` (or a `datetime`) → cursor is set to
the current max ``id`` in `pending_uploads`, so a freshly-deployed
uploader does not re-ship historical rows that a previous uploader
already handled.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence, Union

from ..core import Record, RecordBatch

logger = logging.getLogger(__name__)


HEALTH_OK = "ok"
HEALTH_UNREACHABLE = "unreachable"
HEALTH_STALE_SCHEMA = "stale-schema"
HEALTH_NOOP = "noop"


_ALLOWED_EXTRA_OPS = {"=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN"}


# ---- cursor ----------------------------------------------------------------


@dataclass
class _Cursor:
    """Internal cursor representation — the last consumed `id`.

    Serializes as an ASCII decimal integer in opaque bytes, matching the
    "opaque blob" contract the watermark store treats it as.
    """

    last_id: int = 0

    @classmethod
    def from_bytes(cls, blob: bytes) -> "_Cursor":
        if not blob:
            return cls(last_id=0)
        try:
            return cls(last_id=int(blob.decode("ascii")))
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError(f"SqliteSource cursor must be ASCII int, got {blob!r}") from exc

    def to_bytes(self) -> bytes:
        return str(self.last_id).encode("ascii")


# ---- connection config -----------------------------------------------------


@dataclass
class _ConnectionConfig:
    """SQLite connection config — just the path."""

    path: str

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> Optional["_ConnectionConfig"]:
        """Resolve from coordination.env.

        Returns ``None`` (→ no-op source) when neither `SIGMOND_SQLITE_PATH`
        is set nor the default sink db exists.  Matches the no-op semantics
        of the CH source when no `SIGMOND_CLICKHOUSE_URL` is set.
        """
        e = env if env is not None else os.environ
        path = (e.get("SIGMOND_SQLITE_PATH") or "").strip()
        if path:
            return cls(path=path)
        # Default sink path — only adopt it if the file is actually present
        # (the writer creates it on first flush).  Without this guard, a
        # standalone client with no sigmond install would silently become
        # an active source on a nonexistent file.
        default = "/var/lib/sigmond/sink.db"
        if Path(default).exists():
            return cls(path=default)
        return None


def _default_connect_factory(cfg: _ConnectionConfig) -> sqlite3.Connection:
    """Open the SQLite database the writer is filling.

    Read-write is required (commit deletes rows).  WAL mode (set by the
    writer when it initialises the schema) lets us read concurrently
    while the producer is mid-insert.
    """
    return sqlite3.connect(cfg.path, timeout=30.0)


# ---- source ----------------------------------------------------------------


class SqliteSource:
    """Read-side of sigmond.hamsci_ch.SqliteWriter's `pending_uploads`
    queue.

    The (database, table) pair filters which rows belong to this
    source — matching the `target_db`/`target_table` tags the writer
    inserts.  `source_id()` returns `"sqlite:<database>.<table>"` so a
    single watermark store can host cursors for many sources without
    collision.

    Construction args mirror `ClickHouseSource` for shim-side
    symmetry.  CH-specific args (`cursor_column`, `primary_key_columns`)
    are accepted but ignored — `id` is the natural monotone cursor and
    SQLite needs no tiebreak hash.
    """

    def __init__(
        self,
        database: str,
        table: str,
        *,
        accepted_schema_versions: Sequence[int],
        primary_key_columns: Sequence[str] = (),  # accepted for API parity; ignored
        select_columns: Optional[Sequence[str]] = None,
        cursor_column: str = "time",  # accepted for API parity; ignored (id is the cursor)
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        start_at: Optional[Union[str, datetime]] = None,
        delete_on_commit: bool = True,
        config: Optional[_ConnectionConfig] = None,
        connect_factory: Optional[Callable[[_ConnectionConfig], sqlite3.Connection]] = None,
    ):
        self.database = database
        self.table = table
        self.accepted_schema_versions = list(accepted_schema_versions)
        self.select_columns = list(select_columns) if select_columns else None
        # CH-isms accepted for API parity, retained for diagnostics only.
        self._cursor_column_hint = cursor_column
        self._primary_key_columns = list(primary_key_columns)
        self.start_at = start_at
        # When False, commit() advances the watermark cursor but does NOT
        # DELETE rows from pending_uploads.  Required when multiple
        # pipelines consume the same logical (database, table) queue —
        # e.g. wspr.spots feeding BOTH wsprnet.org and wsprdaemon.org
        # transports.  In that case rely on a separate retention
        # janitor (`smd storage trim`) to bound the queue size.
        self.delete_on_commit = bool(delete_on_commit)
        self.extra_where = list(extra_where) if extra_where else []
        for col, op, _val in self.extra_where:
            if op not in _ALLOWED_EXTRA_OPS:
                raise ValueError(
                    f"extra_where operator {op!r} not allowed; "
                    f"supported: {sorted(_ALLOWED_EXTRA_OPS)}"
                )
            if not col.replace("_", "").isalnum():
                raise ValueError(
                    f"extra_where column name {col!r} must be alphanumeric/underscore"
                )
        self._config = config
        self._connect_factory = connect_factory or _default_connect_factory
        self._conn: Optional[sqlite3.Connection] = None
        self._schema_checked = False
        # Synthetic-cursor cache for start_at.  Unlike CH's time-based
        # start_at (stable across calls), SQLite's max(id) anchor would
        # drift on every empty poll, silently skipping rows that
        # arrived between polls.  Cache once at first evaluation.
        self._start_at_cursor: Optional[_Cursor] = None
        self._health = HEALTH_NOOP if config is None else HEALTH_OK

    # ---- factory ----

    @classmethod
    def from_env(
        cls,
        database: str,
        table: str,
        *,
        accepted_schema_versions: Sequence[int],
        primary_key_columns: Sequence[str] = (),
        select_columns: Optional[Sequence[str]] = None,
        cursor_column: str = "time",
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        start_at: Optional[Union[str, datetime]] = None,
        delete_on_commit: bool = True,
        env: Optional[dict] = None,
        connect_factory: Optional[Callable[[_ConnectionConfig], sqlite3.Connection]] = None,
    ) -> "SqliteSource":
        cfg = _ConnectionConfig.from_env(env)
        return cls(
            database=database,
            table=table,
            accepted_schema_versions=accepted_schema_versions,
            primary_key_columns=primary_key_columns,
            select_columns=select_columns,
            cursor_column=cursor_column,
            extra_where=extra_where,
            start_at=start_at,
            delete_on_commit=delete_on_commit,
            config=cfg,
            connect_factory=connect_factory,
        )

    # ---- Source protocol ----

    def source_id(self) -> str:
        return f"sqlite:{self.database}.{self.table}"

    def health(self) -> str:
        return self._health

    def iter_batches(self, cursor: bytes, limit: int) -> Iterator[RecordBatch]:
        if self._config is None:
            # No SQLite configured — silent no-op, matches Writer's
            # behaviour.  The shim's CompositeSource (or fallback) will
            # try the next source.
            return iter([])
        try:
            ready = self._ensure_ready()
        except Exception as exc:
            self._health = HEALTH_UNREACHABLE
            logger.warning(
                "SQLite source %s.%s unreachable: %s",
                self.database, self.table, exc,
            )
            return iter([])
        if not ready:
            # Table doesn't exist yet — no producer has flushed.  Stay
            # at HEALTH_UNREACHABLE so operators see "producer not yet
            # writing", but don't escalate to stale-schema which is
            # reserved for an *unexpected* schema version.
            self._health = HEALTH_UNREACHABLE
            return iter([])
        self._health = HEALTH_OK
        cur = _Cursor.from_bytes(cursor)
        if not cursor and self.start_at is not None:
            cur = self._cursor_from_start_at()
            logger.info(
                "SQLite source %s.%s: empty watermark → starting at id=%d "
                "(start_at=%r)",
                self.database, self.table, cur.last_id, self.start_at,
            )
        return self._iter_one_batch(cur, limit)

    def commit(self, commit_token: bytes) -> None:
        """Delete acked rows from `pending_uploads` (when configured to).

        When ``delete_on_commit=True`` (default): rows with id ≤
        commit_token are deleted in one transaction.  Idempotent.

        When ``delete_on_commit=False``: this is a no-op — the
        Uploader's watermark store still advances the cursor so rows
        already shipped don't re-ship, but the rows themselves stay
        in pending_uploads for other consumers of the same queue
        (e.g. the wsprdaemon transport sharing the wspr.spots queue
        with the wsprnet transport).  A separate retention janitor
        (`smd storage trim`) must be configured to bound queue size.
        """
        if self._config is None or not commit_token:
            return
        if not self.delete_on_commit:
            return
        try:
            last_id = int(commit_token.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            logger.warning(
                "SqliteSource.commit: malformed commit_token %r — skipping cleanup",
                commit_token,
            )
            return
        try:
            conn = self._connect()
            with conn:
                conn.execute(
                    "DELETE FROM pending_uploads "
                    "WHERE id <= ? AND target_db = ? AND target_table = ?",
                    (last_id, self.database, self.table),
                )
        except Exception as exc:
            # Don't promote to a hard error — the watermark store has
            # already recorded the cursor advance, so rows will simply
            # be re-skipped on next iter_batches (by id > cursor),
            # remaining queued until a later commit succeeds.
            logger.warning(
                "SqliteSource.commit: DELETE failed for %s.%s up to id=%d: %s",
                self.database, self.table, last_id, exc,
            )

    # ---- internals ----

    def _ensure_ready(self) -> bool:
        """Check that the queue table exists.

        Returns ``True`` if the table is present (ready to read);
        ``False`` if the producer hasn't flushed yet (table absent).
        Raises on actual SQLite connection failures (file unreadable,
        corruption, etc.).
        """
        if self._schema_checked:
            return True
        conn = self._connect()
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='pending_uploads'"
        )
        if cur.fetchone() is None:
            return False
        self._schema_checked = True
        return True

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            assert self._config is not None
            self._conn = self._connect_factory(self._config)
        return self._conn

    def _iter_one_batch(self, cur: _Cursor, limit: int) -> Iterator[RecordBatch]:
        conn = self._connect()
        sql, params = self._build_query(cur, limit)
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            # Probe for stale-schema rows — only when no in-band data
            # came back, so we don't pay the cost on the hot path.
            self._check_stale_schema(conn)
            return

        records: list[Record] = []
        last_id = cur.last_id
        for row_id, schema_version, payload_json, queued_at in rows:
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "SQLite source %s.%s: skipping corrupt payload at id=%d: %s",
                    self.database, self.table, row_id, exc,
                )
                last_id = row_id
                continue
            time_value = self._extract_record_time(payload, queued_at)
            columns = self._project_columns(payload)
            records.append(
                Record(
                    table=f"{self.database}.{self.table}",
                    time=time_value,
                    columns=columns,
                )
            )
            last_id = row_id

        new_cursor = _Cursor(last_id=last_id).to_bytes()
        yield RecordBatch(
            records=tuple(records),
            cursor_after=new_cursor,
            commit_token=new_cursor,
        )

    def _build_query(self, cur: _Cursor, limit: int) -> tuple[str, list]:
        clauses = [
            "id > ?",
            "target_db = ?",
            "target_table = ?",
        ]
        params: list[Any] = [cur.last_id, self.database, self.table]

        if self.accepted_schema_versions:
            placeholders = ",".join("?" for _ in self.accepted_schema_versions)
            clauses.append(f"schema_version IN ({placeholders})")
            params.extend(self.accepted_schema_versions)

        for col, op, value in self.extra_where:
            if op in ("IN", "NOT IN"):
                if not isinstance(value, (list, tuple)) or not value:
                    raise ValueError(
                        f"extra_where {col!r} {op} needs a non-empty list/tuple"
                    )
                placeholders = ",".join("?" for _ in value)
                clauses.append(
                    f"json_extract(payload_json, '$.{col}') {op} ({placeholders})"
                )
                params.extend(value)
            else:
                clauses.append(
                    f"json_extract(payload_json, '$.{col}') {op} ?"
                )
                params.append(value)

        sql = (
            "SELECT id, schema_version, payload_json, queued_at "
            "FROM pending_uploads "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY id ASC "
            "LIMIT ?"
        )
        params.append(int(limit))
        return sql, params

    def _check_stale_schema(self, conn: sqlite3.Connection) -> None:
        """Probe for rows with schema_versions outside `accepted`.

        Called only on empty-result iter_batches so it doesn't add
        per-batch cost on the hot path.  If even one row is found at an
        unexpected version, health flips to stale-schema and subsequent
        polls will return early at the top of iter_batches.
        """
        if not self.accepted_schema_versions:
            return
        placeholders = ",".join("?" for _ in self.accepted_schema_versions)
        sql = (
            "SELECT schema_version FROM pending_uploads "
            "WHERE target_db = ? AND target_table = ? "
            f"AND schema_version NOT IN ({placeholders}) LIMIT 1"
        )
        params = [self.database, self.table, *self.accepted_schema_versions]
        row = conn.execute(sql, params).fetchone()
        if row is not None:
            self._health = HEALTH_STALE_SCHEMA
            logger.warning(
                "SQLite source %s.%s: stale-schema row at version=%s "
                "(accepted=%s); refusing to yield",
                self.database, self.table, row[0],
                self.accepted_schema_versions,
            )

    def _extract_record_time(self, payload: dict, queued_at: str) -> datetime:
        """Pick the canonical observation time for the Record.

        Convention matches CH: prefer a `time` field in the payload
        (the producer's decode timestamp), fall back to `queued_at`
        (the writer's wallclock at flush time) when absent.  Both are
        ISO 8601 strings produced by sqlite_writer's `_json_default`.
        """
        for key in ("time", "decode_time", "utc"):
            v = payload.get(key)
            if isinstance(v, str):
                try:
                    return _parse_iso(v)
                except ValueError:
                    pass
            elif isinstance(v, datetime):
                return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        try:
            return _parse_iso(queued_at)
        except ValueError:
            return datetime.now(timezone.utc)

    def _project_columns(self, payload: dict) -> dict:
        """If select_columns was provided, restrict to that subset.

        Mirrors CH's `SELECT cols, ...` behaviour from the consumer's
        point of view — transports get the same column projection
        regardless of backend.
        """
        if not self.select_columns:
            return dict(payload)
        return {k: payload.get(k) for k in self.select_columns}

    def _cursor_from_start_at(self) -> _Cursor:
        """Build the synthetic first-pump cursor when `start_at` is set,
        cached across iter_batches calls.

        SQLite's start_at maps to ``max(id)`` at first evaluation, which
        is monotonic — re-evaluating later would silently skip rows
        that arrived in between, so the first answer is cached on
        ``self._start_at_cursor`` and returned on every subsequent
        empty-watermark call until the watermark store hands us a
        real persisted cursor.
        """
        if self._start_at_cursor is not None:
            return self._start_at_cursor
        conn = self._connect()
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM pending_uploads "
            "WHERE target_db = ? AND target_table = ?",
            (self.database, self.table),
        ).fetchone()
        self._start_at_cursor = _Cursor(last_id=int(row[0]))
        return self._start_at_cursor


# ---- helpers ---------------------------------------------------------------


def _parse_iso(s: str) -> datetime:
    """Tolerant ISO 8601 parser — accepts `Z` suffix and microseconds.

    sqlite_writer._json_default emits `datetime.isoformat()` which
    includes the offset; the producer's own decode timestamps are
    likewise ISO.  Python 3.11+ `fromisoformat` accepts both shapes.
    """
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
