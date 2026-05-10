"""Schema-version registry: maps live column-hash -> known version.

Each producer client (psk-recorder, hfdl-recorder, ...) ships its own
schema migrations under ``clickhouse/schema/<mode>/NNN_*.sql``.  At
build time, the canonical column hash for each version is computed by
taking the ordered (name, type) tuples from ``system.columns``, joining
them with NULs, and SHA-256'ing the result (truncated to 16 hex chars).

This registry is the strict-mode lookup: ``ClickHouseSource`` queries
the live column hash and asks this module which version it is.  An
unknown hash means either a pre-release schema or schema drift; either
way the source refuses to yield.

Known production schemas live in ``_BUILTINS`` and are auto-registered
at import.  Tests use ``clear()`` to reset to the built-in baseline
between cases (so test-specific ``register()`` calls don't bleed across
tests, but production hashes never need to be re-registered).

Adding a new entry: deploy the migration, then run
``hs-uploader schema-hash <db> <table>`` against the live CH and add
the resulting ``(version, hash)`` tuple here.
"""

from __future__ import annotations

import hashlib
from typing import Any, Optional


# Map "<database>.<table>" -> { hash: version_int }.
_REGISTRY: dict[str, dict[str, int]] = {}


# Known production schemas.  Add an entry per locked migration — each
# version corresponds to a column-set + per-column type tuple that the
# producer has shipped to a deployed sigmond CH.  The 16-hex-char hash
# is what ``compute_column_hash`` returns when run against that
# deployed schema.  Use ``hs-uploader schema-hash <db> <table>`` to
# compute the value for a new entry.
#
# Engine changes (e.g. ReplacingMergeTree → MergeTree) do not affect
# the column-hash and so do NOT bump the version here.  Type qualifier
# changes (e.g. ``DateTime`` → ``DateTime('UTC')``) do — register a
# new version when those land.
_BUILTINS: dict[str, list[tuple[int, str]]] = {
    # psk-recorder v2: jt9 columns added (snr_db, spectral_width_hz,
    # decoder_kind) per migration 002_add_jt9_columns.sql.  ``time``
    # and ``ingested_at`` are plain ``DateTime`` (migration 003 not
    # yet applied on bee1 as of 2026-05-10).  When migration 003
    # (pin UTC) lands, add a v3 entry with the new hash and update
    # PskReporterTcp.ACCEPTS to include [2, 3] during the transition.
    "psk.spots": [(2, "8ee544049db79fd0")],
}


def _register_builtins() -> None:
    for table, entries in _BUILTINS.items():
        table_map = _REGISTRY.setdefault(table, {})
        for version, column_hash in entries:
            table_map[column_hash] = version


_register_builtins()


def register(table: str, *, version: int, column_hash: str) -> None:
    """Register a known (table, version, hash) triple.

    Idempotent for the same (table, hash, version) tuple; overlapping
    inserts that disagree raise a ``ValueError``.
    """
    table_map = _REGISTRY.setdefault(table, {})
    existing = table_map.get(column_hash)
    if existing is not None and existing != version:
        raise ValueError(
            f"schema registry conflict for {table} hash {column_hash}: "
            f"already registered as v{existing}, attempted v{version}"
        )
    table_map[column_hash] = version


def version_for_hash(table: str, column_hash: str) -> Optional[int]:
    """Return the version for ``(table, hash)`` or None if unknown."""
    return _REGISTRY.get(table, {}).get(column_hash)


def clear() -> None:
    """Reset the registry to the built-in known schemas.

    Used by tests between cases.  Built-in production hashes survive;
    test-specific ``register()`` calls do not.
    """
    _REGISTRY.clear()
    _register_builtins()


def compute_column_hash(client: Any, database: str, table: str) -> str:
    """Compute the canonical column-hash for a CH table.

    Same shape as ``sigmond.hamsci_ch.writer``'s schema check: hash
    the ordered ``(name, type)`` tuples from ``system.columns`` joined
    by NULs, take the first 16 hex chars of SHA-256.  Stable across
    CH versions and across the way each producer declares its table.

    Exposed as a module-level helper so the admin CLI
    (``hs-uploader schema-hash``) and the offline tooling that
    populates ``_BUILTINS`` can reuse it without instantiating a full
    ``ClickHouseSource``.

    Returns an empty string if the table is unknown to the server.
    """
    rows = client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database=%(db)s AND table=%(t)s "
        "ORDER BY position",
        parameters={"db": database, "t": table},
    ).result_rows
    if not rows:
        return ""
    h = hashlib.sha256()
    for name, typ in rows:
        h.update(f"{name}\x00{typ}\x00".encode("utf-8"))
    return h.hexdigest()[:16]
