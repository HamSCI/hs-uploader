"""HamSCI sink writer (CONTRACT §17), the write side of hs-uploader.

Producer clients call `Writer.from_env(...)` to get a local-sink
writer.  The backend is SQLite, a store-and-forward queue that
hs-uploader's sources drain upstream.

`sink_path()` finds the sink, first match wins: `HAMSCI_SINK_PATH` or
`SIGMOND_SQLITE_PATH` in the env; `path` in `/etc/hs-uploader/sink.toml`;
an existing `/var/lib/sigmond/sink.db`; `<state dir>/sink.db` when its
directory is writable.  With none of these the writer is a no-op.

SQLite suits a sigmond client host: the local sink is just a buffer
for `hs-uploader`, and a daemon-backed columnar store would burn
1-2 GB of RAM and several merge-CPU cores for no benefit there.

`BufferFull` is the exception the writer raises on prolonged sink
failure rather than silently losing rows.

`PENDING_UPLOADS_DDL`, `ensure_columns` and `infer_producer` give
readers, tools and tests the writer's own schema and producer rule.

This package moved here from `sigmond.hamsci_sink` in v3.70.  Clients
import it as `hs_uploader.sink`, and fall back to `sigmond.hamsci_sink`
for one release.
"""

from .writer import (
    PENDING_UPLOADS_DDL,
    BufferFull,
    SqliteConfig,
    Writer,
    ensure_columns,
    infer_producer,
    sink_path,
)

__all__ = [
    "Writer",
    "BufferFull",
    "SqliteConfig",
    "PENDING_UPLOADS_DDL",
    "ensure_columns",
    "infer_producer",
    "sink_path",
]
