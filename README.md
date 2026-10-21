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
- `local` marks a row kept only as a local archive.  `Writer(local=True)`
  and `Writer.from_env(local=True)` store 1 on every row they write; the
  default stores 0.  `SqliteSource` and `WsprCycleSource`, the two sources
  that read the sink, skip a local row.  Each selects none, and
  `SqliteSource`'s delete-on-ack never removes one.  A sink older than v3.70 has no
  `local` column, and the sources then add no `local` condition.
- On a `sink.db` from before v3.70, the writer adds both columns with
  `ALTER TABLE ADD COLUMN` (`ensure_columns`).  SQLite rewrites no row, and
  older rows keep `producer = ''`.
- The writer creates the index `idx_pending_uploads_producer`
  (`target_db`, `target_table`, `producer`, `id`) after it adds the
  columns.  A source that reads one client's rows walks it.
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

## Pipeline files: `pipelines.d/`

The daemon reads `/etc/hs-uploader/pipelines.toml`, the legacy manifest.
It also reads `pipelines.d/` beside it, one file per client:
`/etc/hs-uploader/pipelines.d/<client>.toml`.  The file's name gives the
client, which must match `^[a-z0-9][a-z0-9-]*$`; the name `site` stays
reserved.  Only names ending in `.toml` and not starting with a dot load,
so an editor's backup never does.

A file holds `[[pipeline]]` tables in the legacy form.  The daemon sets each
entry's `client` from the file name.  An entry that names another client
refuses the file.  An optional `governed = true | false` (default true)
marks whether a client's switch applies to the entry (see Sink switches).  The daemon stores
it and uses it only to choose which clients its switch report names.

Precedence and refusal follow these rules:

- Once `pipelines.d/<client>.toml` exists, every legacy entry whose `client`
  reads that name drops out.  A file that fails to parse silences those
  entries too.  A legacy entry with no `client` stays.
- The daemon accepts or refuses a file whole.  A file refused at start sends
  nothing.  At a rebuild, a file that now fails keeps the pipelines it last
  built.  A refused file never stops the daemon.
- It refuses a file that fails to parse, names a client other than its own,
  or fails to build.
- It refuses a file that collides with a pipeline already accepted.  A
  collision is the same pipeline name, or the same send-record key.  The
  send-record key is the triple (source, destination, table) under which
  the store keeps a pipeline's cursor.
- It refuses a file whose name gives an illegal client name, `site`
  included, and a name in `pipelines.d/` that is not a regular file.
- It refuses a file that holds a `[daemon]` table.  It refuses one whose
  `[identity]` names `ssh_key_file`.  Both settings are host-wide and
  belong in the legacy manifest.
- It refuses `start_at = "now"`.  A `wspr_cycle` source in a file may set
  `start_at` to `"beginning"` only, and defaults to it.  The legacy
  manifest keeps `"now"` as its default.
- The daemon ignores any other `[identity]` content, with one INFO line;
  nothing reads per-instance identity yet.
- Two legacy entries that collide log one ERROR and both run, as before.

The daemon records each pipeline, and each refused file, in the store's
`pipeline_state` table.  `hs-uploader status` prints it.

**Rebuild.**  The daemon starts with the legacy manifest as it reads it and
never re-reads it; an edit there waits for a restart.  Before each pump it
stats `pipelines.d/`.  When the directory changed, or when the process
receives SIGHUP, the daemon rebuilds between two pumps.  SIGHUP rebuilds
from `pipelines.d/` without a restart, and `smd admin log hs-uploader
--level` sends SIGHUP.  It keeps every
pipeline whose entry did not change, running on its own thread with its
own open source.  It closes the sources and transports of a pipeline that
leaves, starts new ones, and runs the store-version check again.  A file
that now fails keeps the pipelines it last built.  A rebuild that raises
leaves the running set alone and tries again at the next pump.  If every
file is refused and no pipeline can run, the daemon stays up and idle until
a file changes.  At start it exits 1 when nothing built and no file was
refused, and when the store check refused every pipeline that built and no
file was refused.

## Sink switches

A client's switch is a file, `sinks/<client>.toml`, beside the legacy
manifest.  `sinks/site.toml` is the site switch.  Each file holds a `state`
of `off`, `fill` or `upload`.  A client's effective state is the stricter of
its own file and the site file, `off` being the strictest and `upload` the
loosest.  A missing client file reads `off`.  A
missing site file sets no limit.  A file that fails to read or parse reads
`off`, and the error logs once.  `hs_uploader.sinkswitch` reads and writes
these files; stdlib only, so a client's venv can import it.

No switch stops or gates anything yet.  The daemon reads the files and
reports what each governed client's switch would say, as one INFO line per
change.  It sends exactly what the manifests say.

```bash
hs-uploader sink status [--json] [--manifest PATH] [--sinks DIR]
```

`sink status` lists the site file and each client that owns a pipeline or a
switch file: the file's state, the effective state and any read error.  It
counts pipelines that carry no client, since no switch applies to them, and
it says "not enforced".  `--sinks` defaults to `sinks/` beside `--manifest`.
It exits 2 when it cannot read the manifest.

## Features: `hs-uploader capabilities --features`

`hs-uploader capabilities` prints the store schema version, one integer.
With `--features` it prints one feature word per line instead:

- `pipelines.d` -- the daemon reads pipeline files.
- `send-log-v3` -- the store has migration 3 and the send log names its
  pipeline and client.
- `switch-files` -- the code reads switch files and reports them.

An hs-uploader without the flag exits 2.  Sigmond's manifest step prints the
answer.

## The send log: `attempts` and `last_send`

Each send attempt writes a row to `attempts`.  From store version 3, the row
also names its `pipeline` and `client`.  The store also keeps `last_send`,
one row per `(client, pipeline)`: the outcome, time and record count of the
latest acked or partly acked send.  The daemon writes the attempt row and
the `last_send` row in one transaction.  A pipeline with `discard = true`
ships nothing, so it writes attempts and no `last_send` row.

The store writes these columns and rows only when it has reached version 3.
It reads its version when it opens, at the daemon's start and at each
rebuild.  On an older store it writes the old attempt row alone, so a daemon that
runs before `hs-uploader migrate` adds no column and sets no version.
`hs_uploader.backlog.summarize` returns the rows under a top-level
`last_send` key, empty on a store below version 3.

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
- Version 3 adds `attempts.pipeline` and `attempts.client` (`TEXT NOT NULL
  DEFAULT ''`) and creates `last_send`.  It changes no old row.  The default
  lets a v3.76 daemon, which names neither column, keep inserting.
- Every migrate also copies the old record to a producer key that holds no
  record yet, and records it in `superseded_keys`.  It never touches a key
  that exists.  A new store starts at version 3.  A store the v3.76 daemon
  created sits at version 2 with no producer keys, and without this copy each producer pipeline would start
  at "now" and skip the rows the shared pipeline had not sent.
- The retry pass runs on every migrate, after that copy.  It handles a
  retry still queued under `psk-pskreporter`.  With `--sink PATH`, a retry
  whose rows the sink still holds is deleted, and each new pipeline
  re-reads those rows.  It deletes one only when both producer keys hold a
  record.
  Otherwise the retry moves to the pipeline named by `--keep-retry-for`
  (`psk-recorder-pskreporter` or `meteor-scatter-pskreporter`).  It replays
  once there and its ack advances that pipeline's key, so only the other
  producer re-sends its own rows of that batch.  A retry queued before
  v3.70 may carry no `cursor_after`.  Its ack then advances no key, and both
  producers re-send their rows of its batch; the report line says so.  Without
  that flag the retry stays.  Dead letters stay.
- `--check` exits 3 when a migrate would write: a pending migration, a
  producer key to fill, a retry the pass would delete or move, or a hot
  journal.  It exits 0 when a
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
