# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

**hs-uploader** holds both halves of a station's sink.  Its sink
writer, `hs_uploader.sink.Writer`, stores the observation records of
sigmond clients (the recorders) in a local SQLite sink
(`/var/lib/sigmond/sink.db`).  Its sources and transports then forward
those records to HamSCI and community destinations: wsprdaemon.org,
wsprnet.org, PSKReporter, PSWS.  The writer moved here from sigmond in
v3.70.  Clients import `hs_uploader.sink` first and fall back to
`sigmond.hamsci_sink` for one release; all six that write a sink declare
hs-uploader.  sigmond keeps a bundled copy of the writer for older client
checkouts.

Part of the HamSCI sigmond suite — see `/opt/git/sigmond/sigmond/CLAUDE.md`
(orchestrator) and `/opt/git/sigmond/CLAUDE.md` (umbrella) for
cross-repo context.  hs-uploader runs both as a library and as a
daemon.  The host daemon, `hs-uploader serve` (`daemon.py`,
`systemd/hs-uploader.service`), runs every pipeline in
`/etc/hs-uploader/pipelines.toml` in one process.  Some recorders still
run in-process senders built from the same library.

## Authors

- Michael Hauan (AC0G, GitHub: mijahauan)
- Repo: https://github.com/HamSCI/hs-uploader

## Commands

```bash
# Development — uv canonical
uv sync --extra dev
uv run pytest tests/
uv run pytest tests/test_<area>.py -v             # one file
uv run pytest -k watermark -v                     # by keyword

# Build distribution
uv build

# Do two trees send the same thing?  Runs OLD and NEW on copies of a
# station's state; never writes the given files (v3.70 neutrality proof).
# --sinks/--marker give NEW the switches.  Each pipeline ends AGREE, HELD,
# DISAGREE or INCONCLUSIVE; the tool predicts HELD with its own switch
# reader, never hs_uploader's.  The last line reads
# RESULT: PASS (a AGREE, h HELD, d DISAGREE), or RESULT: DISAGREE (...),
# or RESULT: INCONCLUSIVE (...; reason); exit 0 only on PASS (spec 2b-part2 §11.2)
.venv/bin/python tools/neutrality_check.py --manifest pipelines.toml \
    --sink sink.db --watermarks watermarks.db --old OLD/src --new NEW/src \
    [--sinks sinks/ --marker carry-over-v1.done]

# CLI (operator inspector — not the consumer integration path)
hs-uploader --help
hs-uploader migrate --check    # watermarks.db schema version; writes nothing
hs-uploader migrate            # run pending migrations (sigmond does this on each manifest write)
hs-uploader capabilities            # store schema version this code writes (one integer, now 3)
hs-uploader capabilities --features # feature words: gate, pipelines.d, send-log-v3, switch-files
hs-uploader sink status [--json] [--marker P]  # switches, effective states, and what the gate holds ("enforced" once the marker stands)
hs-uploader sink set <site|client> <off|fill|upload> --reason R --by B   # through write_switch (root on the real sinks/)
hs-uploader sink mark-carried [--note N]   # writes the carry-over marker atomically
```

Clients store rows through `hs_uploader.sink.Writer` and build
in-process senders from `hs_uploader`'s sources and transports.  The CLI
inspects state and runs the host daemon (`hs-uploader serve`).

To run the CLI without the console script, use
`<venv>/bin/python -I -m hs_uploader.cli <args>`, as sigmond does (from
`cwd=/`).  The package has no `__main__.py`, so `python -m hs_uploader`
fails.

## Architecture

```
   ┌───────────────┐   Record    ┌──────────────────┐    Outcome
   │   Source      │ ─────────►  │    Transport     │ ─────────►  destination
   │(SQLite|Files) │             │  (per-protocol)  │             (network)
   └───────────────┘             └──────────────────┘
            ▲                              │
            │            advance/retry     ▼
            └─────────  Watermark  ◄───  Pipeline (orchestrator)
                       (SQLite)
```

### Three orthogonal abstractions

- **Source** (`sources/`) — yields `Record`s from an opaque cursor.
  - `SqliteSource` (preferred) reads the `pending_uploads` queue that
    `hs_uploader.sink.Writer` fills. Supports `extra_where` and `start_at`
    knobs.  `producer` (and, with it, `radiod_id`) scopes every row
    statement to one client (instance) and names it in the key,
    `sqlite:<db>.<table>@<producer>[/<radiod_id>]`; rows stored with
    `producer = ''` follow the writer's D12 rule.  It enforces a strict
    `schema_version` check (rows outside the pipeline's accepted set
    flip the source to `stale-schema` — a clean halt rather than
    shipping mis-typed records).
  - `WsprCycleSource` — cycle-aligned variant over the same queue;
    yields one `RecordBatch` per 2-minute WSPR cycle, bundling
    `wspr.spots` + `wspr.noise` so a single tar can ship both.
  - `FileTreeSource` — fallback for hosts without a sigmond sink.
    Per-file parsers may return one or many records per file;
    `delete_on_ack` or keep-retention selectable.
- **Transport** (`transports/`) — accepts a `RecordBatch` and returns
  an `Outcome` (acked / partial-ack / retry-later / dead). One per
  upstream destination:
  - `PskReporterTcp` — owns the TCP socket end-to-end; no external
    `pskreporter` binary dependency.
  - `WsprdaemonTarSftp` / `WsprdaemonTarFtp` — cycle-aligned tar
    bundles to wsprdaemon.org.
  - `WsprNet` — HTTP multipart POST to `wsprnet.org/meptspots.php`.
    ⚠ Mutually exclusive with the wsprdaemon transports'
    `forward_to_wsprnet=True` (below): running both double-posts.
  - `PswsMagnetometerSftp` — daily zip to PSWS for mag-recorder.
- **WatermarkStore** (`watermark/`) — owns per-`(source, destination,
  table)` cursor and retry-deliverable state. `SqliteWatermarkStore`
  with a per-attempt audit table; restarts re-derive batches from
  the cursor.

A **Pipeline** binds one source + one transport + one watermark slot.
An **Uploader** orchestrates N pipelines. The library is synchronous
and idempotent; no threads of its own beyond a per-pipeline pump
worker. There is no in-flight state to lose across restarts.

## Project structure

```
src/hs_uploader/
  core.py                 # Pipeline + Uploader orchestration
  cli.py                  # diagnostic CLI
  config.py               # config loading helpers
  daemon.py               # host daemon (`hs-uploader serve`); Rebuilder rebuilds between pumps
  pipelines_d.py          # loads pipelines.d/<client>.toml; precedence and refusal rules
  sinkswitch.py           # reads and writes sinks/<client>.toml and site.toml (stdlib only)
  sink/
    writer.py             # sink Writer: stores client rows in sink.db
                          # (moved from sigmond in v3.70)
  sources/
    base.py               # Source ABC + Record / RecordBatch types
    sqlite.py             # SqliteSource (preferred path)
    wspr_cycle.py         # WsprCycleSource (cycle-aligned variant)
    _local_rows.py        # the `local = 0` condition both queue sources share
    files.py              # FileTreeSource (fallback)
  transports/
    base.py               # Transport ABC + Outcome variants
    pskreporter.py        # PskReporterTcp — owns the TCP socket
    wsprdaemon.py         # WsprdaemonTarSftp / WsprdaemonTarFtp
    wsprnet.py            # WsprNet — HTTP MEPT
    psws_magnetometer.py  # PswsMagnetometerSftp
  watermark/
    base.py               # WatermarkStore ABC
    sqlite.py             # SqliteWatermarkStore
    schema.py             # PRAGMA user_version + migrate() (`hs-uploader migrate`);
                          # migration 3 names pipeline and client, adds last_send
  payload/
    psk_pskr.py           # PSK Reporter binary frame builder
tests/
install.sh                # convenience installer (rarely used; consumers normally
                          # vendor hs-uploader via [tool.uv.sources] editable)
tmpfiles.d/               # systemd-tmpfiles snippet for /var/lib/hs-uploader
```

## Optional extras (pyproject)

Consumers pull only the transports they need:

| Extra | Adds |
|---|---|
| `psws` | (no extra deps; included for symmetry) |
| `wsprnet` | (no extra deps yet) |
| `wsprdaemon` | (no extra deps yet) |
| `pskreporter` | (no extra deps yet) |
| `http` | `httpx>=0.27` (for WsprNet HTTP transport) |
| `dev` | `pytest>=7.0`, `pytest-httpserver>=1.0` |

The empty extras are placeholders — they keep the import surface
explicit even when no third-party deps are required.

## Consumers (current)

- **psk-recorder** — `psk.spots` via `PskReporterTcp` (gated on
  `PSK_USE_HS_UPLOADER=1`; this is now the sole upload path).
- **wspr-recorder** — `wspr.spots` via `WsprNet` (HTTP MEPT) and
  `wspr.spots + wspr.noise` cycle-aligned tars via
  `WsprdaemonTarSftp` (gated on `WSPR_USE_HS_UPLOADER=1`).
- **hfdl-recorder** — does not ship spots through hs-uploader;
  dumphfdl owns the airframes.io TCP feed directly.
- **mag-recorder** — daily PSWS magnetometer zip via
  `PswsMagnetometerSftp`.
- **codar-sounder** — additive `codar.spots` rows are written by
  the recorder to the sink (CONTRACT §17); no current
  hs-uploader transport for them yet.

## Production paths

- Sink path: `sink_path()` resolves it, in this order: `HAMSCI_SINK_PATH`,
  then `SIGMOND_SQLITE_PATH`, then `path` in `/etc/hs-uploader/sink.toml`,
  then `/var/lib/sigmond/sink.db` if that file exists, then
  `<HS_UPLOADER_STATE_DIR or /var/lib/hs-uploader>/sink.db` if writable,
  else no sink.  `install.sh` (`write_sink_config`) writes `sink.toml` when
  absent: the legacy file on a station that holds one, the state-dir file
  on a fresh install.  It never rewrites an existing config.
- Sigmond sink (legacy location): `/var/lib/sigmond/sink.db`.  Clients write it through
  `hs_uploader.sink.Writer` (imported as `sigmond.hamsci_sink`), and the
  sources here read it.  Since v3.70 each row carries `producer` and
  `local`; the writer adds both columns when it opens an older file and
  never rewrites an existing row.  `Writer(local=True)` stores `local = 1`.
  `SqliteSource` and `WsprCycleSource` both add the condition `local = 0` to every
  statement that selects rows to send, and `SqliteSource`'s delete-on-ack
  does too, so a local row stays in the sink.  `sources/_local_rows.py`
  adds the condition only when `PRAGMA table_info` lists the column.  The
  writer also creates `idx_pending_uploads_producer`, after it adds the
  columns.
- Watermark store: `/var/lib/hs-uploader/watermarks.db` (per-consumer
  state; survives restarts).
- File spool (fallback): consumer-defined per pipeline.

## pipelines.d/, rebuild and switch files

- `pipelines_d.py` derives `pipelines.d/` and `sinks/` from the legacy
  manifest's own path.  `load_layout` reads the legacy manifest and each
  `pipelines.d/<client>.toml`.  The file name gives the client
  (`^[a-z0-9][a-z0-9-]*$`, `site` reserved).  A present file drops every
  legacy entry of that client, even when the file fails to parse.
  Legacy entries with no client stay.
- A file is accepted or refused whole (`Refusal` in the layout).  Refusals:
  parse failure, an entry naming another client, `[daemon]`,
  `[identity] ssh_key_file`, `start_at = "now"`, a `wspr_cycle` `start_at`
  other than `"beginning"`, a build failure, a collision on pipeline name
  or send-record key (source, destination, table) with an accepted pipeline.  A refusal never stops the
  daemon; it lands in `pipeline_state` under `pipelines.d/<file>`.  A
  file-sourced `wspr_cycle` entry defaults to `start_at = "beginning"`
  (`pipeline_factory.default_start_at`); legacy keeps `"now"`.
- `daemon.Rebuilder` holds the running set.  Before each pump the loop
  compares `pipelines_signature` (mtime, size, inode of the directory and
  each entry) with the last build's; SIGHUP also triggers it.  SIGHUP rebuilds from
  `pipelines.d/` without a restart, and `smd admin log hs-uploader --level`
  sends SIGHUP.  The rebuild
  runs between pumps, reuses each entry that did not change, passes the
  start's copy of the legacy manifest (the daemon never re-reads it), runs
  the store check against a freshly read version, and rewrites
  `pipeline_state`.  `Uploader.replace_pipelines` closes removed
  pipelines on their own threads.  A refused file at rebuild keeps its
  last good pipelines.  A raise leaves the running set and signature as
  they were; the next pump retries.
- `sinkswitch.py` reads `sinks/<client>.toml` and `sinks/site.toml`
  (`state` is `off`, `fill` or `upload`; the effective state is the stricter of the client's file and the site file, where `off` is strictest; a
  missing client file or a read failure reads `off`; a missing site file
  sets no limit).  `write_switch` writes under `sinks/.lock`, atomically.
- The daemon builds one `gate.SinkGate` at start, on the same `sinks/` and
  marker path that the Rebuilder, `sink status` and the writer read, and
  passes its `may_send` to the one `Uploader`.  The Uploader, and so the
  gate, lasts across rebuilds.  The gate enforces the switches only while
  the carry-over marker stands (rule 3).  It logs each pipeline's change
  between sending and held once, at INFO ("pipeline X held: sink filling
  (site)", "pipeline X sending: sink uploading"); a pipeline held at its
  first decision logs its held line then.  The rule 4 line ("names no
  client") appears once per pipeline per daemon.  A fault (a broken switch
  file, `sinks/` missing under the marker, an exception in the gate) logs
  at ERROR once, then a reminder every 6 h (`gate.REMIND_S`) while it
  lasts; every table of logged keys is capped at `gate.SAID_CAP`.
  `Rebuilder.report_switches` logs each governed client's switch, "reads
  fill (enforced)" or "(not enforced: no carry-over marker)", once per
  change, and again when the marker appears or goes.
- `hs-uploader sink status` says "enforced" while the marker stands.  Its
  held pipelines come from `SinkGate.decide` itself, asked about
  `daemon.pipe_views(layout)`: each pipeline the daemon builds from the
  same files, with its transport but no source, store or connection.
  `effective` and `held_by` (`gate.held_by`: site, client, fault or none)
  read the switches alone and do not depend on the marker; a bad client
  name never becomes a path.  Status cannot see inside a `builder` entry:
  `builder_pipelines` names those.  Refused files come from this read of
  `pipelines.d/` and from `pipeline_state`, opened read-only; a pipeline
  refused for the store's schema shows with `file: null`.  An unreadable
  manifest (exit 2) or store lands in `errors`.  The command puts a
  NullHandler on the `hs_uploader` logger while it runs, so library log
  lines stay off stderr.  `--json` carries `"schema": 1`.  `sink set`
  writes through `write_switch` (lock, atomic) and refuses a non-root
  caller on the real `sinks/`; `--identity-json -` reads the identity
  tables from stdin (1 MiB at most), and refuses a value TOML cannot hold
  (null, an array of values, an empty key), naming instance and key.
  `sink mark-carried` writes the marker through `write_marker`.
- `Uploader(gate=...)` asks the gate at the top of each pipeline's pump,
  before each retry pop and before each source batch; a held pipeline
  touches nothing and resumes where it stopped.  `gate=None` (the default,
  and every in-process shim) sends always.  A gate that raises holds the
  pipeline and logs at ERROR once per exception type (capped).
- The starting record (spec sink-2b-part2 §3.5): while the carry-over marker
  stands, `Rebuilder` runs `daemon.write_starting_records` at each build,
  and `Rebuilder.marker_check()` runs it before the next pump when the marker
  appears.  Each governed pipeline (`gate.exempt_rule` returns 0) whose key
  holds no row and whose source's `initial_cursor()` returns bytes gets
  that cursor through `SqliteWatermarkStore.start_cursor`, an insert that
  does nothing when the key holds a row, so an existing record and its
  `last_ack` stay untouched.  `initial_cursor()` is the anchor the first
  pump would fix (or already fixed): `SqliteSource`, the highest in-scope
  id, `b"0"` when no sink file exists at the configured path or the file
  holds no queue table yet (sigmond's installer creates an empty file),
  `None` without `start_at` or with no sink path configured;
  `WsprCycleSource`, the cycle before the current floor, `None` for
  `"beginning"`.  So a pipeline
  held from its first pump ships its `fill` rows when raised.
- The writer's switch check (spec sink-2b-part2 §4; `sink/writer.py`,
  `Writer._judge`): `insert()` drops a row when the writer is not local,
  the marker stands, `sinks/<producer>.toml` exists and
  `effective_state(producer)` reads `off`.  A broken or unreadable switch
  file reads `off` there too, so its rows drop.  The producer comes from
  `self.producer or infer_producer(...)`, row by row.  The switch is read
  at each `insert()`; nothing is cached.  `Writer.dropped` counts per
  producer; `health` ignores it.  One INFO line per change of a
  producer's verdict.  A check that raises stores the row and logs ERROR
  once per exception type, then every 6 h.  `hs_uploader.sinkswitch` is
  imported by absolute name inside a guard: an `ImportError` or
  `AttributeError` means no enforcement, which is what sigmond's bundled
  copy needs.  Any commit here that touches `sink/writer.py` needs a
  refresh of sigmond's `lib/sigmond/hamsci_sink/_bundled.py` (recipe in
  sigmond's CLAUDE.md); its compat test fails until then.
- The psk rider check (spec §5; `sources/wspr_cycle.py`,
  `WsprCycleSource._judge_riders`): while the marker stands, a `psk.spots`
  row rides in the WSPR tar only when its producer's effective state reads
  `upload` as the cycle's batch is built.  Producer: the `producer` column,
  else `infer_producer`.  A missing client file keeps the row out.
  `sinks_dir=None, marker=None` mean the library defaults, read each cycle,
  so wspr-recorder's shim filters too; the daemon passes its own paths
  through `pipeline_factory`.  A check that raises keeps every rider of
  that cycle out and ships the WSPR rows.  The per-cycle INFO line counts
  `psk left out`.
- `capabilities --features` prints `gate` (`cli.FEATURES`).  The word
  promises all of: the pump gate (rules 1-5), the starting record, the
  writer check, the rider check, `sink set` and `sink mark-carried`.
  sigmond carries a station over to the switches only when it sees it.
- `sink status --json`, schema 1, top-level keys: `schema`, `enforced`,
  `marker`, `sinks_dir`, `site` (`state`, `error`, `set_at`, `set_by`,
  `reason`), `clients[]` (`client`, `file_state`, `effective`, `held_by`,
  `error`, `pipelines`), `held_pipelines[]` (`pipeline`, `client`,
  `held_by`, `reason`), `refused_files[]` (`file`, `pipeline`, `client`,
  `reason`; `file` is null for a pipeline refused for the store's schema),
  `no_client_pipelines`, `builder_pipelines`, `errors`.  sigmond refuses
  any other `schema` number (`sigmond/lib/sigmond/sink_state.py`).  A
  change to this layout needs a new number and a sigmond that reads it.
- The marker (`sinkswitch.marker_path()`, `$HS_UPLOADER_STATE_DIR` or
  `/var/lib/hs-uploader`, then `carry-over-v1.done`): `marker_stands` uses
  `lstat`, so anything at the path counts; only ENOENT/ENOTDIR read
  absent.  `write_marker` writes `carried over at <UTC>` and one line per
  note, mode 0644, atomically.

## Proxying wsprnet.org through the WD servers (`forward_to_wsprnet`)

`WsprdaemonTarSftp` / `WsprdaemonTarFtp` take three optional station-level
arguments:

| Argument | Meaning |
|---|---|
| `forward_to_wsprnet` | `True`/`False` asks the WD servers to post this station's WSPR spots to wsprnet.org on its behalf. `None` (default) says nothing. |
| `wsprnet_reporter` | `(call, grid)` this station posts to wsprnet **under**, when that differs from its `rx_call`. |
| `psk_reporter` | `(call, grid)` this station is reported to **PSKReporter** under. Declared ONLY to merge with another receiver. |

A station that proxies stops sending its WSPR spots twice — once in the tar
and again over its own `WsprNet` pipeline.  The servers derive the MEPT
submission from the extended rows the tar already carries.

- The switch is written **both** into `routing.json` (`default.forward_to_wsprnet`)
  **and** into field 33 of every extended spot line, from this one argument,
  so the two can never disagree.  Different server vintages read different
  ones.
- **⛔ `None` writes nothing at all**, not `false`.  On the server, absent
  means "leave field 33 alone"; an explicit `false` from a station that
  merely has no opinion would switch off a legacy per-spot setting nobody
  asked us to touch.  So a station that has not opted in builds a
  **byte-identical** tar to before this existed — the switch is not a flag
  day for the fleet.
- `routing.json` is emitted for a **WSPR-only** tar when either argument is
  set.  The station most likely to want this has no `ft8`/`ft4` at all;
  gating the file on psk rows would make the switch unsendable for exactly
  those sites.
- **⚠ Turning this on does NOT stop the `WsprNet` transport.**  That is a
  separate pipeline.  Switch them together or wsprnet gets both copies.

### `psk_reporter` — PSKReporter identity, and the one way to merge

`psk_reporter` is **not** a switch and has no `forward_to_` twin: the
decision to proxy PSK rows is `forward_to_pskreporter`, per receiver,
already in `routing.json`.  This argument answers a different question —
**who the spots are reported as.**

- **Omitting it is the safe default and the normal case.**  The server
  reports every row under its own `rx_sign` and grid, which is exactly the
  identity this station uses posting to PSKReporter directly.  A station
  that says nothing cannot have its credit or its map pin moved by moving
  onto the proxy.
- **It exists to MERGE.**  Two receivers are reported as one station when
  they declare the SAME `(call, grid)` — that is "merge, and merge with
  what?", expressed as data.  Merging is never inferred from a shared
  callsign prefix: `AC0G/B4` is in Missouri and `AC0G/ND` in North Dakota.
- A declaration with no `call` is dropped rather than half-applied: a grid
  alone cannot name a reporter.
- Like `wsprnet_reporter`, it is written only under `default` and triggers
  a `routing.json` even on a WSPR-only tar — the station most likely to
  declare one may have no `ft8`/`ft4` tree to trigger the file.
- Manifest keys (cross-repo contract with sigmond): `psk_reporter_call`,
  `psk_reporter_grid`, read by `pipeline_factory._wsprnet_proxy_kw`.

Server side (the parse, the `proxy_upload` column, the reporter-identity
tables): `rrobinett/wsprdaemon-server`, CLAUDE.md § "wsprnet.org forwarder"
and § "Reporter identity (`psk.reporter`)".
`tests/test_transport_wsprdaemon.py` holds a cross-repo contract test that
imports the server's parser and asserts it reads back exactly what this
builder wrote — the key names live in two repos with nothing joining them,
and a rename on one side is silent.

## watermarks.db schema version (`hs-uploader migrate`)

`PRAGMA user_version` in `watermarks.db` holds the store's schema
version.  `src/hs_uploader/watermark/schema.py` owns it: `SCHEMA_VERSION`,
the ordered `MIGRATIONS`, and `migrate(path, *, check=False, ctx=MigrateContext())
-> MigrateReport`.

- Only `hs-uploader migrate` migrates.  The store constructor never
  writes `user_version` and the daemon never migrates while it starts
  (D10 in sigmond/tasks/plan-sink-control.md).  The daemon does read the
  version: it refuses a pipeline with a `producer` on a store below
  version 2 and records that in `pipeline_state`.
  From v3.70, sigmond runs `hs-uploader migrate` on each manifest write
  (`smd admin uploader manifest`, which `smd apply`, `smd align`,
  bring-up, `smd sink …` and `smd admin manifest restore` run), after
  the write and before it starts or restarts the daemon.  `smd update`
  alone does not run it.
- `migrate` runs every pending migration, in order, inside one
  `BEGIN IMMEDIATE` transaction.  It waits up to `BUSY_TIMEOUT_S` (30 s)
  for another process's write lock.  A failure rolls back every step.
- `migrate` opens the file read-write from its first read.  A crash in
  the middle of a write leaves a hot journal (`watermarks.db-journal`),
  and that first read rolls it back, exactly as the daemon's own open
  would.  A read-only open cannot roll it back and refuses to read.
- `--check` opens the file read-only and writes nothing.  On a hot
  journal it prints `watermarks.db: a crash left a journal to recover;
  run hs-uploader migrate without --check (or start the daemon) to
  recover it` and exits 3 (`JournalRecoveryNeeded` in the library).
- `--check` exits 3 when a real migrate would write anything: a pending
  migration, a producer key to fill, a `psk-pskreporter` retry the pass would delete or move
  (given the same `--sink` and `--keep-retry-for`), or a hot journal.  It
  exits 0 when a migrate would write nothing, a missing file included.
  sigmond's manifest step (spec-key-split-v373 §5.2 step 3) stops the
  daemon and migrates only on 3; `MigrateReport.would_write` carries the
  answer in the library.
- A missing file: exit 0, and migrate creates nothing.  The daemon
  creates the store on its first start, as `hsupload`; a migrate run as
  root must not create it first.
- A file without a `watermarks` table, such as `sink.db` passed by
  mistake: exit 1, file untouched.
- A store newer than the code, after a rollback: exit 0, and migrate
  leaves the number as it stands.  Older store code opens a newer file
  unchanged.
- Version 1 (v3.70) changes no row and no table.  It records the number
  and nothing else.
- Version 2 (v3.73) copies each send record keyed `sqlite:psk.spots` with
  a pskreporter destination to `sqlite:psk.spots@psk-recorder` and
  `sqlite:psk.spots@meteor-scatter`, unless a record already sits there.
  The old row stays, frozen, and `superseded_keys` pairs each old key
  with each new one (spec-key-split-v373 §4.4).
- Version 3 adds `attempts.pipeline` and `attempts.client` (`TEXT
  NOT NULL DEFAULT ''`) and creates `last_send(client, pipeline, outcome,
  ts, records)`, keyed on `(client, pipeline)`.  It changes no old row.
  `SCHEMA_VERSION` is 3.  `--check` previews the producer-key fill
  unless a pending migration 2 will do the copy itself.
- `SqliteWatermarkStore.record_attempt` writes the version-3 columns and
  upserts `last_send` (acked and partial_ack only, one transaction with
  the attempt row) only when the cached version is 3 or above.  The store
  reads the version on open (`refresh_schema_version`), and the daemon
  refreshes it at start and at each rebuild.  A transport with
  `ships = False` (`DiscardTransport`) leaves `last_send` alone.
  `backlog.summarize` returns the table as top-level `last_send`, `[]`
  below version 3.
- Every migrate at version 2 or above (up to this code's) then copies the
  old record to any producer key that holds no record yet, and records it
  in `superseded_keys`; it never touches an existing key.  A store the
  daemon created has version 2 with no producer keys (final review
  I1).  `--check` counts that copy as a write.
- The retry pass runs on every migrate, after the migrations and the
  copy, in the same transaction.  For each retry still queued under
  `psk-pskreporter`: when `--sink PATH` shows the sink still holds a
  `psk.spots` row at or before the old record, and both producer keys
  hold a record, it deletes the retry; otherwise it moves the
  retry whole to `--keep-retry-for NAME` (one of the two new pipelines),
  under that pipeline's source part; with no `--keep-retry-for` the retry
  stays.  When nothing needs a write, migrate takes no write lock.
  Dead letters stay.  `--check` reports all of it and writes nothing.
- To add a migration, append a `Migration` to `MIGRATIONS`; `SCHEMA_VERSION`
  follows.  Never edit a migration that has shipped.
- Exit codes: 0 done or nothing to do, 1 failure, 2 usage error, and
  with `--check` 3 for "a migrate would write".  An
  hs-uploader that predates `migrate` also exits 2, and prints
  `invalid choice: 'migrate'` on stderr.

## Library lockfile policy

`uv.lock` for libraries doesn't bind downstream consumers. Each
consumer pins hs-uploader via its own `uv.lock` (and via
`[tool.uv.sources]` editable path during dev).
