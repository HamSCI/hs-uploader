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
- The writer stores nothing for a repository while a producer's switch
  reads `off` (see Sink switches).  `insert()` drops a row when all four
  hold: the writer is not local, the carry-over marker stands,
  `sinks/<producer>.toml` exists, and the producer's effective state reads
  `off`.  A broken or unreadable switch file reads `off`, so its rows drop
  too.  `fill` and `upload` store.  The writer takes each row's producer
  as `flush()` does, so a mixed batch splits row by row: meteor-scatter at
  `off` drops its MSK144 rows while psk-recorder's FT8 rows store.  It
  reads the switch at each `insert()`, so a change counts at the next
  `insert()` with no restart.  `Writer.dropped` counts the dropped rows
  per producer; `health` ignores the count.  The writer logs one INFO line
  each time a producer's verdict changes ("sink switch reads off for X:
  storing nothing for repositories"), never one per row.  When the check
  itself fails, the writer stores the row and logs one ERROR line per
  exception type, then a reminder every 6 hours: the daemon's gate still
  holds what must not ship.  A venv without `hs_uploader.sinkswitch`
  (sigmond's bundled copy of this writer) gets no check at all.

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
marks whether a client's switch applies to the entry (see The gate).  The
gate honours `governed = false` only from `pipelines.d/sigmond.toml`, the
file that carries sigmond's heartbeat.  Any other entry that says it,
in another file or in the legacy manifest, gets one WARNING line, and the
switches judge it all the same.

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

| Effective state | The writer | The daemon |
|---|---|---|
| `upload` | stores | sends |
| `fill` | stores | holds the client's pipelines |
| `off` | stores nothing for a repository | holds the client's pipelines |

**The carry-over marker.**  The switches take effect only while the marker
`/var/lib/hs-uploader/carry-over-v1.done` stands.  `HS_UPLOADER_STATE_DIR`
moves it, as it moves the store.  sigmond writes the marker once it has
carried each station's `[uploads]` setting into switch files.  While no
marker exists, the gate passes every pipeline, the writer stores
every row and the WSPR tar keeps every psk row: the station sends as part
1 did.  The marker counts when anything stands at its path, a dangling
link included.  An error other than "no such file", such as a directory
the reader may not search, reads as standing, so the switches keep
governing.

### The gate

The daemon asks one `gate.SinkGate` before every send.  Its `may_send`
answers by five rules; the first that applies decides:

1. `governed = false` in `pipelines.d/sigmond.toml`: send.  That file
   carries the heartbeat.
2. The legacy heartbeat, an entry of the legacy manifest whose `client`
   reads `sigmond` or whose transport is `heartbeat_sftp`: send.  An older
   sigmond renders the heartbeat there.
3. No carry-over marker: send.
4. The pipeline names no client: send.  sigmond v3.69 to v3.72 renders no
   `client`, and that manifest's own policy governs.  The daemon logs one
   INFO line per such pipeline.
5. Otherwise send when the client's effective state reads `upload`, and
   hold in every other case.  `fill` and `off` hold alike, and so do a
   missing client file, a broken file, an unknown state and a missing
   `sinks/` directory.

The pump asks at the top of each pipeline's pump, before each queued
retry, and before each source batch, so a switch that changes in the
middle of a pump counts at the next send.  A held pipeline opens no
batch, ships nothing and writes no attempt row.  Its send record and its
queued retries stay as they stand; a retry keeps its attempt count and
replays first once the pipeline sends again.  Raising a client from `fill`
to `upload` therefore ships what it stored, oldest first, exactly once.  A
gate that raises holds the pipeline.

`Uploader(gate=None)`, the default, sends always.  The in-process senders
that psk-recorder, meteor-scatter and wspr-recorder build pass no gate, so
the switches do not govern them; sigmond turns those senders off instead.

The gate logs each pipeline's change between sending and held once, at
INFO: "pipeline X held: sink filling (site)", "pipeline X sending: sink
uploading".  A pipeline held at its first decision logs its held line
then, so the daemon's start names each held pipeline.  A pipeline held
for a week logs nothing more.  A fault (a broken switch file, `sinks/`
missing while the marker stands, an exception inside the gate) logs at
ERROR once, then a reminder every 6 hours while it lasts.

**The starting record.**  A legacy entry with `start_at = "now"` fixes its
starting point at its first pump.  A pipeline held from its first pump
would never fix one, and the first real pump would skip everything stored
while it was held.  So while the marker stands, the daemon gives each
governed pipeline whose source starts at `now` and whose key holds no send
record a starting record.  It does so at each build, and again when the
marker first appears.  `SqliteSource`
anchors at the highest row id in its scope.  `WsprCycleSource` anchors at
the cycle before the current cycle floor, as its first pump would, so the
cycle in progress still ships.  The store writes the record through
`start_cursor`, an insert that does nothing when the key already holds a
row, so an existing record never moves.

**The psk rider.**  wspr-recorder's `wspr-wsprdaemon` pipeline sets
`include_psk = true`, and its `WsprCycleSource` adds the cycle's
`psk.spots` rows to each WSPR tar.  While the marker stands, a psk row
rides only when its producer's effective state reads `upload` as the
source builds that cycle's batch.  The producer comes from the row's
`producer` column, or from `infer_producer` when the column reads `''`.  A
missing client file keeps the row out.  The cycle's WSPR spots and noise
stay as they are, and the per-cycle line counts the rows left out
(`%d psk left out`).  A left-out row never rides a later tar, since the
cycle's send record moves when the tar ships; it still reaches
pskreporter.info through its producer's own pipeline once that reads
`upload`.  If the check itself raises, every psk row of that cycle stays
out and the source logs one ERROR line per exception type, then a
reminder every 6 hours.  The source reads the library's default paths
when given none, so wspr-recorder's in-process shim filters too.

### Commands

```bash
hs-uploader sink status [--json] [--manifest PATH] [--sinks DIR] [--marker PATH]
hs-uploader sink set <site|client> <off|fill|upload> --reason TEXT --by TEXT \
    [--identity-json FILE|-] [--sinks DIR | --manifest PATH]
hs-uploader sink mark-carried [--note TEXT] [--marker PATH]
```

`sink status` lists the site file and each client that owns a governed
pipeline or a switch file: the file's state, the effective state, which
switch holds it, and any read error.  It says "enforced" while the marker
stands, and lists each pipeline the gate holds, with the switch that holds
it.  The held list comes from the daemon's own gate, asked about the
pipelines the daemon would build from the same files.  The effective
states do not depend on the marker; only "enforced" and the held list do.
It also lists the pipeline files the daemon refused, the pipelines that
name no client, and the `builder` entries it cannot see inside.
`--sinks` defaults to `sinks/` beside `--manifest`, and `--marker` to the
marker path above.  The command exits 2 when it cannot read the manifest,
and 0 otherwise.

`sink status --json` prints one object, `"schema": 1`.  Its keys:

- `schema` (1), `enforced` (true while the marker stands), `marker`,
  `sinks_dir`;
- `site`: `state` (`off`, `fill`, `upload`, `missing` or `error`),
  `error`, `set_at`, `set_by`, `reason`;
- `clients[]`: `client`, `file_state` (a state, `missing` or `error`),
  `effective`, `held_by` (`site`, `client`, `fault`, or null while the
  client reads `upload`), `error`, `pipelines`;
- `held_pipelines[]`: `pipeline`, `client`, `held_by` (`site`, `client` or
  `fault`), `reason`;
- `refused_files[]`: `file`, `pipeline`, `client`, `reason`.  A refused
  pipeline file names its `file`, with `pipeline` null.  A pipeline the
  daemon refused for the store's schema has `file` null and names its
  `pipeline`;
- `no_client_pipelines`, `builder_pipelines`: pipeline names;
- `errors`: what the command could not read, such as the manifest or the
  store.

`sink set` writes one switch file through `write_switch`: under
`sinks/.lock`, atomically, with one new `[[history]]` entry.  It refuses an
unknown state or a bad name, and exits 1 with the reason.  On the real
`sinks/` it refuses a caller who is not root.  `--identity-json -` reads
the history entry's identity tables from stdin, at most 1 MiB, and refuses
a value TOML cannot hold.  `sink mark-carried` writes the marker, mode
0644, through a temporary file and a rename: one line with the UTC time,
then the `--note` line if given.  sigmond's carry-over passes one note
line that names every switch file it wrote.

sigmond runs each of these through the daemon's own interpreter as
`<python> -I -m hs_uploader.cli <args>`, from `/`.  The package has no
`__main__`, so `python -m hs_uploader` does not run the CLI.

At each change the daemon also logs one line per governed client, for
example "sink switch for psk-recorder reads fill (enforced)", and again
when the marker appears or goes.

## Features: `hs-uploader capabilities --features`

`hs-uploader capabilities` prints the store schema version, one integer.
With `--features` it prints one feature word per line instead, sorted:

- `gate` -- the switches take effect: the gate in the pump, the starting
  record, the writer's check, the psk rider check, `sink set` and
  `sink mark-carried`.
- `pipelines.d` -- the daemon reads pipeline files.
- `send-log-v3` -- the store has migration 3 and the send log names its
  pipeline and client.
- `switch-files` -- the code reads switch files and reports them.

An hs-uploader without the flag exits 2.  sigmond's manifest step prints the
answer, and carries the station over to the switches only when it sees
`gate`.

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
