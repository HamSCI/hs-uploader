"""ClickHouse source — reads rows from a sigmond local ClickHouse table.

Mirrors the connection conventions of ``sigmond.hamsci_ch.Writer``:

* ``SIGMOND_CLICKHOUSE_URL`` / ``_USER`` / ``_PASSWORD_FILE`` env vars.
* Per-mode db alias via ``SIGMOND_CLICKHOUSE_DB_<MODE>`` (rarely used).
* Lazy import of ``clickhouse_connect`` so consumers without the
  optional dep installed get a clean import error only when they
  actually try to construct the source.

Cursor format is a JSON blob: ``{"time": "<iso>", "tiebreak": "<uint64>"}``.
Stored in the watermark store as bytes.  Empty cursor (``b""``) means
"from the beginning" — translated to ``time = '1970-01-01'`` and
``tiebreak = 0`` server-side.

Schema-version check: the source is constructed with an explicit list of
accepted schema versions (numbers matching the producer's
``clickhouse/schema/<mode>/NNN_*.sql`` migration numbers).  At first
poll, the source queries the live table's column hash and compares
against a registry of known per-version hashes.  Mismatch → health
becomes ``stale-schema`` and ``iter_batches`` yields nothing.  Per the
plan: strict policy.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

from ..core import Record, RecordBatch
from .. import schema as schema_registry

logger = logging.getLogger(__name__)


HEALTH_OK = "ok"
HEALTH_UNREACHABLE = "unreachable"
HEALTH_STALE_SCHEMA = "stale-schema"
HEALTH_NOOP = "noop"


@dataclass
class _Cursor:
    """Internal cursor representation.

    Serializes to/from the opaque bytes the watermark store persists.
    """

    time_iso: str  # "1970-01-01 00:00:00.000" for "from the beginning"
    tiebreak: int

    @classmethod
    def from_bytes(cls, blob: bytes) -> "_Cursor":
        if not blob:
            return cls(time_iso="1970-01-01 00:00:00.000", tiebreak=0)
        d = json.loads(blob.decode("utf-8"))
        return cls(time_iso=d["time"], tiebreak=int(d["tiebreak"]))

    def to_bytes(self) -> bytes:
        return json.dumps(
            {"time": self.time_iso, "tiebreak": str(self.tiebreak)},
            separators=(",", ":"),
        ).encode("utf-8")


@dataclass
class _ConnectionConfig:
    url: str
    user: str = "default"
    password_file: Optional[str] = None

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> Optional["_ConnectionConfig"]:
        e = env if env is not None else os.environ
        url = (e.get("SIGMOND_CLICKHOUSE_URL") or "").strip()
        if not url:
            return None
        return cls(
            url=url,
            user=e.get("SIGMOND_CLICKHOUSE_USER", "default"),
            password_file=e.get("SIGMOND_CLICKHOUSE_PASSWORD_FILE") or None,
        )

    def password(self) -> str:
        if not self.password_file:
            return ""
        try:
            return Path(self.password_file).read_text().strip()
        except OSError:
            return ""


def _default_client_factory(cfg: _ConnectionConfig) -> Any:
    # Lazy import — keeps the core importable without the optional dep.
    import clickhouse_connect  # type: ignore[import-not-found]
    return clickhouse_connect.get_client(
        interface=None,  # honour scheme in URL
        dsn=cfg.url,
        username=cfg.user,
        password=cfg.password(),
    )


# Operators allowed in ``extra_where`` filters.  Restricted to a known
# safe set so the column name + operator can be inlined into the SQL
# without injection risk; values are still passed through parameterized
# substitution.  Add operators here as concrete needs surface; broader
# than this should be a separate higher-level filter API.
_ALLOWED_EXTRA_OPS = {"=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN"}


class ClickHouseSource:
    """Yields rows from one ``<database>.<table>`` ordered by
    ``(cursor_column, tiebreak)``.

    Strict schema check: the source is built with a list of accepted
    schema versions; on first poll it computes the live table's column
    hash and compares it against the version registry in
    ``hs_uploader.schema``.  Mismatch → health becomes
    ``"stale-schema"`` and the source yields nothing until restarted
    (or until the producer is upgraded).

    The cursor column defaults to ``time``.  Tables whose decode time
    is non-monotonic across writers (e.g. psk.spots, where ChTailer
    flushes per-mode batches asynchronously) should pass
    ``cursor_column="ingested_at"`` so the watermark advances with
    INSERT order, not with decode-time order.

    ``extra_where`` lets a caller add static filters to the query —
    e.g. ``[("radiod_id", "=", "my-rx888"), ("mode", "IN", ["ft8",
    "ft4"])]`` to scope a multi-instance daemon's poll to its own
    radiod's rows.  Each filter is rendered as ``AND <col> <op>
    <param>`` with the value passed through CH parameterization.
    """

    def __init__(
        self,
        database: str,
        table: str,
        *,
        accepted_schema_versions: Sequence[int],
        primary_key_columns: Sequence[str],
        select_columns: Optional[Sequence[str]] = None,
        cursor_column: str = "time",
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        config: Optional[_ConnectionConfig] = None,
        client_factory: Optional[Callable[[_ConnectionConfig], Any]] = None,
    ):
        self.database = database
        self.table = table
        self.accepted_schema_versions = list(accepted_schema_versions)
        self.primary_key_columns = list(primary_key_columns)
        self.select_columns = list(select_columns) if select_columns else None
        self.cursor_column = cursor_column
        self.extra_where = list(extra_where) if extra_where else []
        for col, op, _val in self.extra_where:
            if op not in _ALLOWED_EXTRA_OPS:
                raise ValueError(
                    f"extra_where operator {op!r} not allowed; "
                    f"supported: {sorted(_ALLOWED_EXTRA_OPS)}"
                )
            if not col.replace("_", "").isalnum():
                # Conservative — column names are inlined into SQL.
                raise ValueError(
                    f"extra_where column name {col!r} must be alphanumeric/underscore"
                )
        self._config = config
        self._client_factory = client_factory or _default_client_factory
        self._client: Any = None
        self._schema_checked = False
        self._health = HEALTH_NOOP if config is None else HEALTH_OK

    # ---- factory helpers ----

    @classmethod
    def from_env(
        cls,
        database: str,
        table: str,
        *,
        accepted_schema_versions: Sequence[int],
        primary_key_columns: Sequence[str],
        select_columns: Optional[Sequence[str]] = None,
        cursor_column: str = "time",
        extra_where: Optional[Sequence[tuple[str, str, Any]]] = None,
        env: Optional[dict] = None,
        client_factory: Optional[Callable[[_ConnectionConfig], Any]] = None,
    ) -> "ClickHouseSource":
        cfg = _ConnectionConfig.from_env(env)
        return cls(
            database=database,
            table=table,
            accepted_schema_versions=accepted_schema_versions,
            primary_key_columns=primary_key_columns,
            select_columns=select_columns,
            cursor_column=cursor_column,
            extra_where=extra_where,
            config=cfg,
            client_factory=client_factory,
        )

    # ---- Source protocol ----

    def source_id(self) -> str:
        return f"ch:{self.database}.{self.table}"

    def health(self) -> str:
        return self._health

    def commit(self, commit_token: bytes) -> None:
        # No external cleanup — the watermark store's cursor advance
        # alone is sufficient for the CH source.
        return None

    def iter_batches(self, cursor: bytes, limit: int) -> Iterator[RecordBatch]:
        if self._config is None:
            # Standalone / no-CH mode — silent no-op, matches Writer's
            # behaviour.  The CompositeSource will swing to the file
            # fallback.
            return iter([])
        try:
            self._ensure_schema()
        except _SchemaMismatch:
            self._health = HEALTH_STALE_SCHEMA
            return iter([])
        except Exception as exc:  # connection failure
            self._health = HEALTH_UNREACHABLE
            logger.warning("CH source unreachable: %s", exc)
            return iter([])
        return self._iter_one_batch(_Cursor.from_bytes(cursor), limit)

    # ---- internals ----

    def _ensure_schema(self) -> None:
        if self._schema_checked:
            return
        self._connect_if_needed()
        live_hash = self._fetch_column_hash()
        version = schema_registry.version_for_hash(
            f"{self.database}.{self.table}", live_hash
        )
        if version is None or version not in self.accepted_schema_versions:
            raise _SchemaMismatch(
                f"{self.database}.{self.table} live column-hash {live_hash} "
                f"resolves to version {version!r}; accepted={self.accepted_schema_versions}"
            )
        self._schema_checked = True

    def _connect_if_needed(self) -> None:
        if self._client is None:
            assert self._config is not None
            self._client = self._client_factory(self._config)

    def _fetch_column_hash(self) -> str:
        column_hash = schema_registry.compute_column_hash(
            self._client, self.database, self.table,
        )
        if not column_hash:
            raise _SchemaMismatch(
                f"table {self.database}.{self.table} not found"
            )
        return column_hash

    def _iter_one_batch(self, cur: _Cursor, limit: int) -> Iterator[RecordBatch]:
        sql, params = self._build_query(cur, limit)
        result = self._client.query(sql, parameters=params)
        rows = result.result_rows
        col_names = result.column_names

        if not rows:
            return

        records: list[Record] = []
        last_time_iso = cur.time_iso
        last_tiebreak = cur.tiebreak
        cursor_idx = col_names.index("__cursor__")
        tiebreak_idx = col_names.index("__tiebreak__")
        # The Record's `time` field comes from the row's `time` column
        # if present, else the cursor column — Records always carry a
        # canonical decode timestamp for transports to use, but the
        # watermark may advance on a different column.
        time_idx = col_names.index("time") if "time" in col_names else cursor_idx
        for row in rows:
            cols = {
                name: val
                for name, val in zip(col_names, row)
                if name not in ("__cursor__", "__tiebreak__")
            }
            records.append(
                Record(
                    table=f"{self.database}.{self.table}",
                    time=row[time_idx],
                    columns=cols,
                )
            )
            last_time_iso = _format_time(row[cursor_idx])
            last_tiebreak = int(row[tiebreak_idx])

        new_cursor = _Cursor(time_iso=last_time_iso, tiebreak=last_tiebreak).to_bytes()
        yield RecordBatch(records=tuple(records), cursor_after=new_cursor)

    def _build_query(self, cur: _Cursor, limit: int) -> tuple[str, dict]:
        pk = ", ".join(self.primary_key_columns)
        select_cols = (
            ", ".join(self.select_columns) if self.select_columns else "*"
        )
        cursor_col = self.cursor_column
        params: dict[str, Any] = {
            "cursor_value": cur.time_iso,
            "tiebreak": cur.tiebreak,
            "limit": int(limit),
        }
        extra_clauses = []
        for i, (col, op, value) in enumerate(self.extra_where):
            param_name = f"extra_{i}"
            extra_clauses.append(f" AND {col} {op} %({param_name})s")
            params[param_name] = value
        sql = (
            f"SELECT {select_cols}, {cursor_col} AS __cursor__, "
            f"cityHash64({pk}) AS __tiebreak__ "
            f"FROM {self.database}.{self.table} "
            f"WHERE ({cursor_col}, cityHash64({pk})) > "
            f"(parseDateTime64BestEffort(%(cursor_value)s), %(tiebreak)s)"
            f"{''.join(extra_clauses)} "
            f"ORDER BY {cursor_col}, cityHash64({pk}) "
            f"LIMIT %(limit)s"
        )
        return sql, params


class _SchemaMismatch(Exception):
    pass


def _format_time(t: Any) -> str:
    """Format a CH-returned time value back into the ISO string form
    that the cursor stores.

    CH's Python driver returns a ``datetime.datetime`` for both
    ``DateTime`` and ``DateTime64`` columns; we always format with
    millisecond precision to keep the string round-trippable through
    ``parseDateTime64BestEffort``.
    """
    from datetime import datetime
    if isinstance(t, datetime):
        return t.strftime("%Y-%m-%d %H:%M:%S.") + f"{t.microsecond // 1000:03d}"
    return str(t)
