"""The `local` mark on a `pending_uploads` row (spec-sink-2b-v377 §5.2).

A writer may mark a row `local = 1`: it stays in the sink and no source
ships or deletes it.  Every source statement that selects rows to send,
or deletes rows it sent, ANDs in `local = 0`.

A sink older than v3.70 has no `local` column, and a statement naming
the column would fail there.  `LocalRowFilter.clause` therefore returns
the clause only when `PRAGMA table_info(pending_uploads)` lists the
column.  It probes once per connection and answers from memory after.
One module serves both sources, so the rule lives in one place.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

LOCAL_CLAUSE = "local = 0"


class LocalRowFilter:
    """Probes whether `pending_uploads` has a `local` column, once per
    connection."""

    def __init__(self) -> None:
        self._conn: Optional[sqlite3.Connection] = None
        self._has_local = False

    def has_local(self, conn: sqlite3.Connection) -> bool:
        if self._conn is not conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(pending_uploads)")}
            self._has_local = "local" in cols
            self._conn = conn
        return self._has_local

    def clause(self, conn: sqlite3.Connection) -> list[str]:
        """`["local = 0"]` when the column exists, else `[]`."""
        return [LOCAL_CLAUSE] if self.has_local(conn) else []
