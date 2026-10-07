"""HamSCI sink writer (CONTRACT §17), the write side of hs-uploader.

Producer clients call `Writer.from_env(...)` to get a local-sink
writer.  The backend is SQLite — a store-and-forward queue under
sigmond's state dir that hs-uploader's sources drain upstream:

- `SIGMOND_SQLITE_PATH` set → writer at that path (explicit override).
- unset                    → `/var/lib/sigmond/sink.db` if its
  directory is writable, else no-op (preserves standalone-safety for
  clients running outside a sigmond install).

SQLite suits a sigmond client host: the local sink is just a buffer
for `hs-uploader`, and a daemon-backed columnar store would burn
1-2 GB of RAM and several merge-CPU cores for no benefit there.

`BufferFull` is the exception the writer raises on prolonged sink
failure rather than silently losing rows.

`PENDING_UPLOADS_DDL`, `ensure_columns` and `infer_producer` give
readers, tools and tests the writer's own schema and producer rule.

This package moved here from `sigmond.hamsci_sink` in v3.70; clients
still import it through that name (tasks/plan-sink-control.md §10.4).
"""

from .writer import (
    PENDING_UPLOADS_DDL,
    BufferFull,
    SqliteConfig,
    Writer,
    ensure_columns,
    infer_producer,
)

__all__ = [
    "Writer",
    "BufferFull",
    "SqliteConfig",
    "PENDING_UPLOADS_DDL",
    "ensure_columns",
    "infer_producer",
]
