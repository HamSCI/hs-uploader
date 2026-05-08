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

The hashes here are placeholders for v1 — the Phase 1 deliverable
focuses on the abstraction; the actual hashes will be filled in as
each producer's schema is locked.  Tests can pass arbitrary hashes via
``register()`` to exercise the strict-mismatch path.
"""

from __future__ import annotations

from typing import Optional


# Map "<database>.<table>" -> { hash: version_int }.
# Empty by default; populated by ``register()`` calls or test setup.
_REGISTRY: dict[str, dict[str, int]] = {}


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
    """Reset the registry — used by tests."""
    _REGISTRY.clear()
