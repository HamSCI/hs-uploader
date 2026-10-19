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

## Send records: one key per producer

A send record is the cursor that says how far one pipeline has sent.  The
store keeps one per `(source, destination, table)`.  The source part of
the key names what the pipeline reads:

- `sqlite:<db>.<table>` reads every row of the table.
- `sqlite:<db>.<table>@<producer>` reads one client's rows.
- `sqlite:<db>.<table>@<producer>/<radiod_id>` reads one client's rows on
  one radiod instance.

A pipeline sets `producer` (and, for a multi-instance host, `radiod_id`)
on its SQLite source to take the second or third form.  Two clients that
send to the same destination then keep two records, and one never moves
the other's cursor.  The pskreporter pipelines show this.  Sigmond renders
`psk-recorder-pskreporter` (modes ft8 and ft4, key
`sqlite:psk.spots@psk-recorder`) and `meteor-scatter-pskreporter` (mode
msk144, key `sqlite:psk.spots@meteor-scatter`).  A row with an empty
`producer` belongs to meteor-scatter when its mode is msk144, and to
psk-recorder otherwise.

A send record moves forward only.  A new cursor that lies before the
stored one changes nothing.  The pipeline logs "acked, record held",
once per interval per key.  If the cursor comparison itself raises, the
store writes the cursor anyway and logs the error, so a broken comparison
never freezes a pipeline.

A pipeline with a `producer` needs store version 2.  On an older store the
daemon refuses that pipeline, runs the others, and records the refusal in
the store's `pipeline_state` table.  `hs-uploader status` prints it.
`hs-uploader capabilities` prints the store version this release writes.

`hs-uploader status` marks a superseded key `[SUPERSEDED by ...]`, and
counts dead letters whose pipeline the manifest no longer builds as
orphaned.  `reset-cursor` refuses a superseded key and names the keys to
reset instead.

## The store's schema version: `hs-uploader migrate`

The watermark store, `/var/lib/hs-uploader/watermarks.db`, records its
schema version in `PRAGMA user_version`.  `hs-uploader migrate` brings
the store up to the version this release knows:

```bash
hs-uploader migrate --check     # print the version and what would run; write nothing; exit 3 if a migrate would write
hs-uploader migrate             # run each pending migration once, in order
hs-uploader migrate --db PATH   # act on another file (default: --state, else the path above)
hs-uploader migrate --sink PATH --keep-retry-for NAME   # settle a queued psk-pskreporter retry
```

Run it once the new code sits in place and before the daemon restarts.
Neither the daemon nor the store migrates on its own.  Sigmond's manifest
step (`smd admin uploader manifest`) stops the daemon, runs the migrate and
starts the daemon again.

- Version 1 changes no row and no table.  It records the number.
- Version 2 copies each record keyed `sqlite:psk.spots` with a pskreporter
  destination to both producer keys, unless a record already sits there.
  The old row stays, frozen, and the store records which keys superseded it
  in `superseded_keys`.
- The retry pass runs on every migrate.  It handles a retry still queued
  under `psk-pskreporter`.  With `--sink PATH`, a retry whose rows the sink
  still holds is deleted, and each new pipeline re-reads those rows.
  Otherwise the retry moves to the pipeline named by `--keep-retry-for`
  (`psk-recorder-pskreporter` or `meteor-scatter-pskreporter`).  Without
  that flag the retry stays.  Dead letters stay.
- `--check` exits 3 when a migrate would write: a pending migration, a retry
  the pass would delete or move, or a hot journal.  It exits 0 when a
  migrate would write nothing.

On a missing file, `migrate` says so, creates nothing and leaves the file
for the daemon to create.  A crash in the middle of a write can leave
`watermarks.db-journal` beside the store.  `migrate` rolls that journal
back first, as the daemon does when it opens the store.  `--check` cannot
roll it back read-only, so it says so, changes nothing and exits 3.  The
command exits 0 when it finished or found nothing to do, 1 when it
failed, and 2 on a usage error.

`scripts/prove_key_split.py --sink SINK.db --store WM.db` checks the split
against a copy of a station's files.  It runs the old shared source and the
two new sources over the copy, and exits 0 when the two new row sets are
disjoint and their union equals the old set.

## License

MIT — see LICENSE.
