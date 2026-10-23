"""SQLite source — reads the `pending_uploads` queue that
`hs_uploader.sink.Writer` fills.

This is hs-uploader's database source: it yields `RecordBatch`es
starting strictly after the supplied opaque cursor, with strict
schema-version checking.  `hs_uploader.sink.Writer.from_env()` stages
rows into this queue by default (`/var/lib/sigmond/sink.db`).  The
writer moved into this package in v3.70.  sigmond keeps
`sigmond.hamsci_sink` as a compatibility import, the name clients still
use.

Pipeline shape::

    Producer: hs_uploader.sink.Writer.from_env()  → Writer.flush()
        → pending_uploads (target_db, target_table, schema_version,
                           payload_json, queued_at, producer, local)
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

Schema-version handling
-----------------------

SQLite payloads are JSON, so there is no "table schema" to hash — but
every row carries the producer's `schema_version` int.  Strict mode is:

1. SELECT filters on `schema_version IN (accepted_schema_versions)`.
2. If any row exists with a non-accepted schema_version, health is
   surfaced as `"stale-schema"` (operator must upgrade producer or
   widen accepted_schema_versions).

`extra_where`
-------------

`[("radiod_id", "=", "my-rx888"), ("mode", "IN", ["ft8","ft4"])]`
is rendered against `json_extract(payload_json, '$.<col>')` so a
multi-instance host can scope a pipeline to one producer.  Values
pass through SQLite parameterization; column names are validated
alnum before being inlined.

`producer` and `radiod_id`
--------------------------

`producer="psk-recorder"` scopes the source to one client's rows and
names it in the key: `source_id()` becomes
`sqlite:<db>.<table>@<producer>`.  `radiod_id="rx-a"` (allowed only
with a producer) narrows it to one instance, by the payload's
`radiod_id`, and gives `sqlite:<db>.<table>@<producer>/<radiod_id>`.
Every statement that touches rows applies the scope: the batch
selection, the de-duplication query, the `start_at` anchor, the
stale-schema probe and `commit()`'s DELETE.

Rows stored with `producer = ''` (before v3.70) belong where the
writer's `infer_producer` would put them (D12): MSK144, compared
trimmed and case-folded, to meteor-scatter; every other mode, a
missing one included, to psk-recorder.  A `sink.db` without the
`producer` column gets that rule alone.  Any other producer matches
the column only.  With neither set, the source behaves as before,
key included.

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
from ._local_rows import LocalRowFilter

logger = logging.getLogger(__name__)


HEALTH_OK = "ok"
HEALTH_UNREACHABLE = "unreachable"
HEALTH_STALE_SCHEMA = "stale-schema"
HEALTH_NOOP = "noop"


_ALLOWED_EXTRA_OPS = {"=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN"}


# D12 in SQL, for rows that name no producer.  It must agree with
# `hs_uploader.sink.writer.infer_producer` on every row: mode trimmed
# and case-folded, a missing or non-string mode counted as not MSK144.
# (A JSON number 144 reads as '144', which is not 'msk144'.)
_D12_MODE = "lower(trim(coalesce(json_extract(payload_json, '$.mode'), '')))"
_D12_BY_PRODUCER = {
    "meteor-scatter": f"{_D12_MODE} = 'msk144'",
    "psk-recorder": f"{_D12_MODE} != 'msk144'",
}


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
        is set nor the default sink db exists — the standalone-safe no-op
        that mirrors `hs_uploader.sink.Writer.from_env()`.
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

    ``temp_store=MEMORY`` keeps SQLite's transient sort/materialise
    B-trees in RAM instead of spilling them to the temp directory.
    Consumer units run under systemd ``ProtectSystem=strict``, which
    mounts ``/tmp`` and ``/var/tmp`` read-only; a query that spills
    (e.g. the cycle-ceiling scan over a large ``pending_uploads``
    backlog) would otherwise fail with ``sqlite3.OperationalError:
    disk I/O error``.  Our batches are ``LIMIT``-bounded, so the
    in-memory temp store cannot grow unboundedly.
    """
    conn = sqlite3.connect(cfg.path, timeout=30.0)
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


# ---- source ----------------------------------------------------------------


class SqliteSource:
    """Read side of the `pending_uploads` queue that
    `hs_uploader.sink.Writer` fills (clients import the writer as
    `sigmond.hamsci_sink`, the compatibility import sigmond keeps).

    The (database, table) pair filters which rows belong to this
    source — matching the `target_db`/`target_table` tags the writer
    inserts.  `source_id()` returns `"sqlite:<database>.<table>"` so a
    single watermark store can host cursors for many sources without
    collision.

    The queue's autoincrement `id` is the natural monotone cursor, so
    there is no cursor-column or tiebreak-hash configuration to supply.
    """

    def __init__(
        self,
        database: str,
        table: str,
        *,
        accepted_schema_versions: Sequence[int],
        select_columns: Optional[Sequence[str]] = None,
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        start_at: Optional[Union[str, datetime]] = None,
        delete_on_commit: bool = True,
        dedup_partition_by: Optional[Sequence[str]] = None,
        dedup_order_by_desc: Optional[str] = None,
        ceiling_column: Optional[str] = None,
        ceiling_provider: Optional[Callable[[sqlite3.Connection], Optional[str]]] = None,
        config: Optional[_ConnectionConfig] = None,
        connect_factory: Optional[Callable[[_ConnectionConfig], sqlite3.Connection]] = None,
        producer: Optional[str] = None,
        radiod_id: Optional[str] = None,
    ):
        self.database = database
        self.table = table
        # `producer` and `radiod_id` scope every statement that touches
        # rows and name the scope in source_id() (spec-key-split-v373
        # §4.1, §3.3).  An empty string counts as unset.
        self.producer = producer or None
        self.radiod_id = radiod_id or None
        if self.radiod_id is not None and self.producer is None:
            raise ValueError("radiod_id needs a producer")
        # source_id() joins them as `@<producer>/<radiod_id>`; a '/' or '@'
        # inside either would make that key ambiguous.
        for name, value in (("producer", self.producer),
                            ("radiod_id", self.radiod_id)):
            if value is not None and ("/" in value or "@" in value):
                raise ValueError(
                    f"{name} {value!r} must not contain '/' or '@'")
        # Whether pending_uploads has the `producer` column; probed once
        # per connection (an old sink.db lacks it).
        self._has_producer_column: Optional[bool] = None
        # Whether pending_uploads has the `local` column (spec §5.2);
        # probed once per connection by the shared helper.
        self._local_rows = LocalRowFilter()
        self.accepted_schema_versions = list(accepted_schema_versions)
        self.select_columns = list(select_columns) if select_columns else None
        self.start_at = start_at
        # Optional ship ceiling.  When BOTH ``ceiling_column`` and
        # ``ceiling_provider`` are set, every query (simple AND dedup
        # CTE) gains a ``json_extract(payload_json,'$.<ceiling_column>')
        # <= ceiling_provider(conn)`` clause.  Used by the wsprnet merge
        # pipeline to hold back rows from WSPR cycles that aren't yet
        # complete across all receivers (the provider returns the newest
        # contiguous-complete cycle from wspr_completion.shippable_
        # ceiling).  Holding incomplete cycles out of the dedup CTE is
        # what makes the cross-RX best-SNR pick correct — otherwise the
        # first receiver's copy ships and a later better-SNR copy is
        # dropped as an already-visited partition.  Provider returning
        # None means "nothing shippable yet" → empty batch.
        self.ceiling_column = ceiling_column
        self.ceiling_provider = ceiling_provider
        if (self.ceiling_column is not None) != (self.ceiling_provider is not None):
            raise ValueError(
                "ceiling_column and ceiling_provider must both be set or both None"
            )
        if self.ceiling_column and not self.ceiling_column.replace("_", "").isalnum():
            raise ValueError(
                f"ceiling_column {self.ceiling_column!r} must be alphanumeric/underscore"
            )
        # Optional max-key-wins dedup at the SQL layer.  When BOTH
        # ``dedup_partition_by`` and ``dedup_order_by_desc`` are set,
        # the query is wrapped in a window-function CTE that selects
        # the row with the largest ``dedup_order_by_desc`` JSON field
        # per ``dedup_partition_by`` tuple.  Other rows in the
        # partition are NEVER yielded (regardless of cursor position),
        # so a follower transport on the same queue that DOESN'T set
        # these params still sees every row — used for the wsprnet
        # vs wsprdaemon-tar split where wsprnet wants max-SNR-only
        # and wsprdaemon-tar wants every receiver's row for diversity.
        #
        # Both fields must be present in the payload_json or the
        # row is skipped (the ORDER BY treats NULL as smallest, so
        # rows missing the order field never win their partition).
        #
        # See ``wsprnet`` pipeline in wspr-recorder/hs_uploader_shim.py
        # for the canonical multi-RX888 dedup use:
        #   dedup_partition_by=('time', 'callsign', 'frequency_hz'),
        #   dedup_order_by_desc='snr_db',
        self.dedup_partition_by = (
            tuple(dedup_partition_by) if dedup_partition_by else None
        )
        self.dedup_order_by_desc = dedup_order_by_desc or None
        if (self.dedup_partition_by is not None) != bool(self.dedup_order_by_desc):
            raise ValueError(
                "dedup_partition_by and dedup_order_by_desc must be set "
                "together (one without the other is meaningless)"
            )
        # Sanity-check the JSON field names — they go into the SQL
        # directly so reject anything that isn't alphanumeric/underscore.
        for f in (self.dedup_partition_by or ()):
            if not f.replace("_", "").isalnum():
                raise ValueError(
                    f"dedup_partition_by field {f!r} must be alphanumeric/underscore"
                )
        if self.dedup_order_by_desc:
            if not self.dedup_order_by_desc.replace("_", "").isalnum():
                raise ValueError(
                    f"dedup_order_by_desc {self.dedup_order_by_desc!r} "
                    "must be alphanumeric/underscore"
                )
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
        # Synthetic-cursor cache for start_at.  The max(id) anchor would
        # drift on every empty poll, silently skipping rows that arrived
        # between polls — so cache it once at first evaluation.
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
        select_columns: Optional[Sequence[str]] = None,
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        start_at: Optional[Union[str, datetime]] = None,
        delete_on_commit: bool = True,
        dedup_partition_by: Optional[Sequence[str]] = None,
        dedup_order_by_desc: Optional[str] = None,
        ceiling_column: Optional[str] = None,
        ceiling_provider: Optional[Callable[[sqlite3.Connection], Optional[str]]] = None,
        env: Optional[dict] = None,
        connect_factory: Optional[Callable[[_ConnectionConfig], sqlite3.Connection]] = None,
        producer: Optional[str] = None,
        radiod_id: Optional[str] = None,
    ) -> "SqliteSource":
        cfg = _ConnectionConfig.from_env(env)
        return cls(
            database=database,
            table=table,
            accepted_schema_versions=accepted_schema_versions,
            select_columns=select_columns,
            extra_where=extra_where,
            start_at=start_at,
            delete_on_commit=delete_on_commit,
            dedup_partition_by=dedup_partition_by,
            dedup_order_by_desc=dedup_order_by_desc,
            ceiling_column=ceiling_column,
            ceiling_provider=ceiling_provider,
            config=cfg,
            connect_factory=connect_factory,
            producer=producer,
            radiod_id=radiod_id,
        )

    # ---- Source protocol ----

    def source_id(self) -> str:
        if self.producer is not None:
            return self._scoped_source_id()
        return f"sqlite:{self.database}.{self.table}"

    def _scoped_source_id(self) -> str:
        """`sqlite:<db>.<table>@<producer>[/<radiod_id>]` (spec §3.1)."""
        sid = f"sqlite:{self.database}.{self.table}@{self.producer}"
        if self.radiod_id is not None:
            sid += f"/{self.radiod_id}"
        return sid

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
            scope_sql, scope_params = self._scope_clauses(conn)
            scope_sql = scope_sql + self._local_rows.clause(conn)
            with conn:
                conn.execute(
                    "DELETE FROM pending_uploads "
                    "WHERE id <= ? AND target_db = ? AND target_table = ?"
                    + "".join(f" AND {c}" for c in scope_sql),
                    (last_id, self.database, self.table, *scope_params),
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

    def cursor_is_after(self, new: bytes, stored: bytes) -> bool:
        """Compare two send records as integer ids, the order of the
        ``id > ?`` filter.  A byte compare would rank ``b"999"`` above
        ``b"1000"``.  An empty ``stored`` answers True; so does a value
        that is not an ASCII integer (logged at DEBUG)."""
        if not stored:
            return True
        try:
            return _Cursor.from_bytes(new).last_id > _Cursor.from_bytes(stored).last_id
        except ValueError as exc:
            logger.debug(
                "SqliteSource %s.%s: cannot order send records %r and %r (%s); "
                "treating the new one as after",
                self.database, self.table, new, stored, exc,
            )
            return True

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

    def close(self) -> None:
        """Close the connection, on the thread that opened it.

        The daemon calls this when a rebuild removes the pipeline.  A later
        read opens a new connection.
        """
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                logger.debug("SqliteSource: closing %s failed",
                             self.database, exc_info=True)
            self._conn = None
            self._has_producer_column = None
            self._schema_checked = False

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            assert self._config is not None
            self._conn = self._connect_factory(self._config)
            self._has_producer_column = None
        return self._conn

    def _scope_clauses(
        self, conn: Optional[sqlite3.Connection],
    ) -> tuple[list[str], list[Any]]:
        """The producer and radiod_id clauses, each to be ANDed into a
        statement on pending_uploads.  Empty when no producer is set.

        With the `producer` column: rows naming this producer, plus rows
        naming none that D12 assigns to it.  Without the column (a
        sink.db older than v3.70): D12 alone.  A producer D12 does not
        know matches the column only, and nothing on an old file.
        """
        if self.producer is None:
            return [], []
        if self._has_producer_column is None:
            conn = conn or self._connect()
            cols = {r[1] for r in conn.execute("PRAGMA table_info(pending_uploads)")}
            self._has_producer_column = "producer" in cols
        d12 = _D12_BY_PRODUCER.get(self.producer)
        clauses: list[str] = []
        params: list[Any] = []
        if self._has_producer_column:
            if d12 is not None:
                clauses.append(f"(producer = ? OR (producer = '' AND {d12}))")
            else:
                clauses.append("producer = ?")
            params.append(self.producer)
        else:
            clauses.append(d12 if d12 is not None else "0")
        if self.radiod_id is not None:
            clauses.append("json_extract(payload_json, '$.radiod_id') = ?")
            params.append(self.radiod_id)
        return clauses, params

    def _iter_one_batch(self, cur: _Cursor, limit: int) -> Iterator[RecordBatch]:
        conn = self._connect()
        ceiling = None
        if self.ceiling_provider is not None:
            ceiling = self.ceiling_provider(conn)
            if ceiling is None:
                # Nothing is complete enough to ship yet (every recent
                # cycle is still waiting on a receiver, none past the
                # backstop).  Empty batch — try again next pump.
                return
        sql, params = self._build_query(cur, limit, ceiling=ceiling, conn=conn)
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

    def _build_query(
        self, cur: _Cursor, limit: int, *, ceiling: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> tuple[str, list]:
        clauses = [
            "id > ?",
            "target_db = ?",
            "target_table = ?",
        ]
        params: list[Any] = [cur.last_id, self.database, self.table]

        # Producer/instance scope.  It lands after the leading cursor
        # clause, so the dedup CTE (clauses[1:]) carries it too.
        scope_sql, scope_params = self._scope_clauses(conn)
        clauses.extend(scope_sql)
        params.extend(scope_params)
        # A local row never ships (spec §5.2).  Applies with or without
        # a producer, and lands in the dedup CTE too, so a local row
        # never wins a partition.
        clauses.extend(self._local_rows.clause(conn or self._connect()))

        if self.accepted_schema_versions:
            placeholders = ",".join("?" for _ in self.accepted_schema_versions)
            clauses.append(f"schema_version IN ({placeholders})")
            params.extend(self.accepted_schema_versions)

        # Ship ceiling: exclude rows whose ceiling_column value is newer
        # than the provided ceiling.  Appended AFTER the cursor/target/
        # schema clauses but BEFORE extra_where, so it lands in both the
        # simple query and the dedup CTE filter (cte_clauses = clauses
        # minus the leading "id > ?").  That keeps incomplete-cycle rows
        # out of the dedup partition ranking — see __init__.
        if self.ceiling_column is not None and ceiling is not None:
            clauses.append(
                f"json_extract(payload_json, '$.{self.ceiling_column}') <= ?"
            )
            params.append(ceiling)

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

        if self.dedup_partition_by and self.dedup_order_by_desc:
            sql, params = self._build_dedup_query(clauses, params, cur, limit)
        else:
            sql = (
                "SELECT id, schema_version, payload_json, queued_at "
                "FROM pending_uploads "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY id ASC "
                "LIMIT ?"
            )
            params.append(int(limit))
        return sql, params

    def _build_dedup_query(
        self, clauses: list, params: list, cur: _Cursor, limit: int,
    ) -> tuple[str, list]:
        """Build a max-key-wins dedup query using a window function.

        The inner CTE ranks every row in pending_uploads (filtered by
        the same target_db/target_table/schema_versions/extra_where
        clauses as the simple query, but NOT by ``id > cursor``) into
        partitions defined by ``dedup_partition_by`` JSON fields,
        ordered by ``dedup_order_by_desc`` JSON field descending with
        ``id ASC`` as the tiebreak.  The outer SELECT pulls only the
        rank-1 winners with ``id > cursor``, so the cursor still
        advances normally and the loser rows stay in the queue for
        any sibling pipeline (e.g. wsprdaemon-tar's all-receiver
        diversity feed).

        Important consequence: loser rows whose partition winner was
        already shipped are silently SKIPPED on subsequent polls
        (rn != 1).  This is the desired behaviour — wsprnet only
        wants the best version of each spot, exactly once.

        Late-arriving rows in a partition that was ALREADY visited by
        an earlier pump (i.e. ``MIN(id) OVER partition <= cursor``)
        are excluded too — otherwise the second rx's copy of a spot
        that arrived in pending_uploads after the first rx's spot was
        already shipped would be uploaded again, wsprnet would reject
        it as a duplicate, and our wsprnet_audit table would skip the
        row via INSERT OR IGNORE PK collision (leaving an audit_batch
        row with no spot — the bug that hid ~93% of rejection causes
        from ``smd watch wspr --rejected``).  Trade-off: we lose a
        chance to upgrade to a higher-SNR version, but the spot is
        already on wsprnet's books and a few dB of SNR isn't worth
        the rejection rate.  Cross-server diversity still works via
        the wsprdaemon-tar pipeline which doesn't enable dedup.
        """
        # The CTE filter clauses re-use everything from the simple
        # query EXCEPT the cursor — the window function needs to see
        # ALL rows in the partition to rank them correctly.  We split
        # the params list accordingly.
        cursor_param = params[0]  # id > ?  (the cur.last_id we set first)
        cte_clauses = clauses[1:]  # everything except "id > ?"
        cte_params = params[1:]    # everything except cursor

        partition_exprs = ", ".join(
            f"json_extract(payload_json, '$.{f}')"
            for f in self.dedup_partition_by
        )
        # CAST so SQLite treats stored numbers as numeric for ORDER BY.
        # json_extract returns the JSON type as-is (text vs real); the
        # CAST normalises and NULLs sort last.
        order_expr = (
            f"CAST(json_extract(payload_json, "
            f"'$.{self.dedup_order_by_desc}') AS REAL) DESC, id ASC"
        )
        sql = (
            "WITH ranked AS ("
            "SELECT id, schema_version, payload_json, queued_at, "
            f"ROW_NUMBER() OVER ("
            f"PARTITION BY {partition_exprs} "
            f"ORDER BY {order_expr}"
            f") AS rn, "
            f"MIN(id) OVER ("
            f"PARTITION BY {partition_exprs}"
            f") AS partition_min_id "
            "FROM pending_uploads "
            f"WHERE {' AND '.join(cte_clauses)}"
            ") "
            "SELECT id, schema_version, payload_json, queued_at "
            "FROM ranked "
            "WHERE rn = 1 AND id > ? AND partition_min_id > ? "
            "ORDER BY id ASC "
            "LIMIT ?"
        )
        new_params = (
            list(cte_params) + [cursor_param, cursor_param, int(limit)]
        )
        return sql, new_params

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
        scope_sql, scope_params = self._scope_clauses(conn)
        sql = (
            "SELECT schema_version FROM pending_uploads "
            "WHERE target_db = ? AND target_table = ? "
            + "".join(f"AND {c} " for c in scope_sql)
            + f"AND schema_version NOT IN ({placeholders}) LIMIT 1"
        )
        params = [
            self.database, self.table, *scope_params,
            *self.accepted_schema_versions,
        ]
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

        Prefer a `time` field in the payload (the producer's decode
        timestamp), fall back to `queued_at` (the writer's wallclock at
        flush time) when absent.  Both are ISO 8601 strings produced by
        sqlite_writer's `_json_default`.
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

        Gives transports a stable column projection regardless of what
        extra fields the producer happened to stage in the payload.
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
        self._start_at_cursor = self._present_position(conn, self._local_rows)
        return self._start_at_cursor

    def _present_position(
        self, conn: sqlite3.Connection, local_rows: LocalRowFilter,
    ) -> _Cursor:
        """The highest ``id`` within this source's scope: the producer and
        instance clauses and ``local = 0``.  0 on an empty queue."""
        scope_sql, scope_params = self._scope_clauses(conn)
        scope_sql = scope_sql + local_rows.clause(conn)
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM pending_uploads "
            "WHERE target_db = ? AND target_table = ?"
            + "".join(f" AND {c}" for c in scope_sql),
            (self.database, self.table, *scope_params),
        ).fetchone()
        return _Cursor(last_id=int(row[0]))

    def initial_cursor(self) -> Optional[bytes]:
        """The send record the first pump would anchor at, in this source's
        cursor encoding (spec sink-2b-part2 §3.5).  ``None`` when the source
        anchors nowhere (``start_at`` unset: it ships from the start of the
        queue) or has no sink configured.

        An anchor a pump already fixed (``_start_at_cursor``) wins, so the
        record is exactly what that pump would write.  Otherwise: the highest
        ``id`` in scope now, or ``b"0"`` when the sink file or its queue
        table does not exist yet, since every row then arrives later.

        Safe to call on any thread between pumps: it reads through a
        connection of its own and closes it, so it never touches the
        connection the pump thread owns, and it never creates the sink file.
        Raises on a database error.
        """
        if self._config is None or self.start_at is None:
            return None
        if self._start_at_cursor is not None:
            return self._start_at_cursor.to_bytes()
        if (self._connect_factory is _default_connect_factory
                and not Path(self._config.path).exists()):
            return _Cursor(last_id=0).to_bytes()
        conn = self._connect_factory(self._config)
        try:
            has_table = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='pending_uploads'"
            ).fetchone() is not None
            if not has_table:
                return _Cursor(last_id=0).to_bytes()
            return self._present_position(conn, LocalRowFilter()).to_bytes()
        finally:
            conn.close()


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
