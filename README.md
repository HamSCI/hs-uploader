# hs-uploader

Library and host daemon for shipping HamSCI sigmond observations to HF
reporting destinations.

`hs-uploader` holds both halves of a station's sink.  Clients store records
in sigmond's local SQLite sink through its sink writer,
`hs_uploader.sink.Writer`.  Its sources and transports then forward those
records, or files from spool directories, to a HamSCI or community
destination: wsprdaemon.org, wsprnet.org, PSKReporter, PSWS.  The host
daemon, `hs-uploader serve`, runs every pipeline a station declares in one
process.

## Status

Active. Sources, transports, and watermark store are all working:

- **Sources:** `SqliteSource` (preferred — reads the
  `pending_uploads` queue that `hs_uploader.sink.Writer` fills, with
  `extra_where` and `start_at` knobs and a strict `schema_version`
  check), `WsprCycleSource` (cycle-aligned variant over the same
  queue — yields one `RecordBatch` per 2-minute WSPR cycle, bundling
  `wspr.spots` and `wspr.noise` for a single per-cycle tar),
  `FileTreeSource` (delete-on-ack or keep retention; per-file
  parsers may return one or many records per file).
- **Transports:** `PskReporterTcp` (owns the socket; no external
  `pskreporter` dependency), `WsprdaemonTarSftp` / `WsprdaemonTarFtp`,
  `WsprNet` (HTTP multipart POST to `wsprnet.org/meptspots.php`),
  `PswsMagnetometerSftp`.
- **Watermark store:** `SqliteWatermarkStore` with deliverable retry +
  per-attempt audit table.
- **Schema safety:** every queue row carries the producer's
  `schema_version`; rows outside the pipeline's accepted set are
  filtered out and flip source health to `stale-schema` — a clean halt
  rather than shipping records a transport may misread.

Current consumer: `psk-recorder` ships `psk.spots` rows via
`PskReporterTcp`, behind the `PSK_USE_HS_UPLOADER=1` feature flag.

## Sink writer

`hs_uploader.sink` stores a client's records in `/var/lib/sigmond/sink.db`,
the queue that `SqliteSource` and `WsprCycleSource` read.  It moved here from
sigmond in v3.70, and clients still import it as `sigmond.hamsci_sink`.

```python
from hs_uploader.sink import Writer

with Writer.from_env(table="spots", mode="psk", schema_version=2) as w:
    w.insert([{"time": "2026-10-07T12:00:00+00:00", "mode": "ft8",
               "frequency": 14074000}])
```

- `SIGMOND_SQLITE_PATH` names the file.  Without it the writer uses
  `/var/lib/sigmond/sink.db` when that directory accepts writes, and
  otherwise stores nothing, so a client outside a sigmond install stays safe.
- Each row records its `producer`, the client that stored it.  A caller may
  pass `producer=`; otherwise `infer_producer` names it from the row's
  `target_db`, and within `psk` from its `mode` (MSK144 rows belong to
  meteor-scatter, the rest to psk-recorder).
- `local` marks a row kept only as a local archive.  In v3.70 the writer
  stores 0 on every row.
- On a `sink.db` from before v3.70, the writer adds both columns with
  `ALTER TABLE ADD COLUMN` (`ensure_columns`).  SQLite rewrites no row, and
  older rows keep `producer = ''`.
- `PENDING_UPLOADS_DDL` gives tools and tests the writer's own schema.

## Architecture

```
   ┌───────────────┐   Record    ┌──────────────────┐    Outcome
   │   Source      │ ─────────► │    Transport     │ ─────────────► destination
   │(SQLite|Files) │             │  (per-protocol)  │                (network)
   └───────────────┘             └──────────────────┘
            ▲                              │
            │            advance/retry     ▼
            └─────────  Watermark  ◄───  Pipeline (orchestrator)
                       (SQLite)
```

Three orthogonal abstractions:

- **Source** — yields `Record`s starting from an opaque cursor.
  `SqliteSource` is preferred (`WsprCycleSource` is its cycle-aligned
  variant for wsprdaemon.org tars); `FileTreeSource` is the fallback.
- **Transport** — accepts a batch and reports an `Outcome` (acked, partial-ack,
  retry-later, dead). One per upstream destination.
- **WatermarkStore** — owns per-`(source, destination, table)` cursor and
  retry-deliverable state. SQLite-backed.

A **Pipeline** binds one source + one transport + one watermark slot. An
**Uploader** orchestrates N pipelines.

The library is synchronous and idempotent. No threads of its own beyond a
per-pipeline pump worker; restarts re-derive the batch from the cursor, so
there is no in-flight state to lose.

## Install

```bash
pip install -e ".[dev,wsprdaemon,wsprnet,pskreporter,psws]"
```

Optional extras let consuming clients pull only the transports they use.

## License

MIT — see LICENSE.
