# hs-uploader — Requirements Specification

**Status:** v0.1 baseline (retroactive). **Owner:** Michael Hauan (AC0G).
**Last reconciled against code:** hs-uploader `0.1.0` (2026-06-25).
**Prefix:** `HSU`.

> Application of [sigmond/docs/REQUIREMENTS-TEMPLATE.md](https://github.com/HamSCI/sigmond/blob/main/docs/REQUIREMENTS-TEMPLATE.md)
> to a **library** (with a thin ops CLI), not a daemon — so the usual
> sigmond↔client contract surface does **not** apply. hs-uploader is the
> read-side counterpart to the sigmond sink `Writer`: it reads the
> `pending_uploads` queue and ships staged records upstream. The two
> seams that matter here are (a) its Python **Source / Transport /
> WatermarkStore** API and (b) the **station↔PSWS upload boundary**,
> governed once by [PSWS-INTERFACE-BOUNDARY.md](https://github.com/HamSCI/sigmond/blob/main/docs/PSWS-INTERFACE-BOUNDARY.md)
> and referenced — not restated — here (§8.3). Provenance tags:
> `[DOC]` documented · `[CODE]` implicit-in-code · `[NEW]` surfaced by
> this review. Status: ✅ implemented · 🟡 partial/unverified · ⬜ planned.

## 1. Context & problem statement

Every DASI2 recorder (wspr-recorder, psk-recorder, mag-recorder,
codar-sounder, …) stages its uploadable observations into one local
SQLite queue — `sigmond.hamsci_sink.Writer`'s `pending_uploads` table in
`/var/lib/sigmond/sink.db`. That queue is the station's local source of
truth; nothing in acquisition depends on any upstream server being
reachable. **hs-uploader is the piece that closes the loop**: it reads
that queue (or, as a fallback, a file spool) and ships the records to the
HF/HamSCI ingest destinations — wsprnet.org, wsprdaemon.org, PSKReporter,
and the PSWS network — over each destination's native wire protocol.

It is deliberately a **library, not a daemon**. Each consuming client
imports it and runs a per-pipeline pump worker in its own process (the
`PSK_USE_HS_UPLOADER` / `WSPR_USE_HS_UPLOADER` shims), so the recorder
that owns the data owns its upload cadence and identity. A thin
`hs-uploader` CLI exists only for operator inspection/recovery of the
watermark store.

Its defining design principle is **idempotent, synchronous,
state-re-derived uploading**: there is no in-flight queue in memory.
"What is owed to a destination" is a *query* — rows in `pending_uploads`
past a persisted cursor (the watermark) — not an enqueued list. A
restart re-derives the next batch from the cursor, so a lost wake, a
duplicate ack, or a crash mid-ship cannot desync delivery. This mirrors
the sink's own "durable shared state is truth; notifications are hints"
invariant and is what makes the station↔upstream boundary safe across
restarts on either side.

## 2. Goals & objectives

- Ship records staged in `pending_uploads` (preferred) or a file spool
  (fallback) to each upstream destination in that destination's exact
  wire format, byte-compatible with the wsprdaemon-client tooling it
  replaces.
- Be **idempotent across restarts**: re-derive owed work from a durable
  cursor; never hold ship state only in memory.
- Make the **three abstractions orthogonal** — a Source, a Transport, and
  a WatermarkStore compose into a Pipeline with no cross-coupling, so a
  new destination is one new Transport and a new producer is one new
  Source.
- **Refuse to ship records a transport would misread** — a strict
  per-row `schema_version` gate that halts cleanly rather than corrupting
  upstream.
- Bound retry/back-off, persist a deliverable queue + per-attempt audit
  across restarts, and dead-letter terminal failures.
- Run usefully **standalone** (no sigmond install): a no-op source when
  no sink is present, an auto-generated SSH identity, group-writable
  shared state.
- Let consuming clients pull only the transports they use (optional
  extras), with a stdlib-only core.

## 3. Non-goals / out of scope

- **Producing records.** Writing `pending_uploads` is the recorders' job
  via `sigmond.hamsci_sink.Writer`; hs-uploader only reads. (Owner: each
  recorder + sigmond's hamsci_sink.)
- **Being a service.** It owns no systemd unit, no daemon loop, no
  scheduler — the consuming client drives `pump()` / `pump_until_idle()`.
- **Server-side processing.** Dedup across stations, ingest QA,
  long-term storage, Madrigal/HAPI APIs, and visualization are PSWS /
  upstream-community scope (PSWS-INTERFACE-BOUNDARY §2).
- **Cross-process coordination.** The merge "all N receivers done"
  decision is the producer's (`wspr_completion`); hs-uploader consumes
  the ceiling, it does not count wakes.
- **Owning every client's upload.** hfdl-recorder ships airframes.io via
  dumphfdl directly; codar-sounder's `codar.spots` rows have no transport
  here yet (§12).

## 4. Stakeholders & actors

Consuming clients (psk-recorder, wspr-recorder, mag-recorder — current;
codar-sounder, wsprdaemon-client — prospective) · the sigmond sink
`Writer` / `pending_uploads` queue (input) · the shared SQLite sink at
`/var/lib/sigmond/sink.db` · the watermark store at
`/var/lib/hs-uploader/watermarks.db` (shared across client users in the
`sigmond` group) · upstream destinations (wsprnet.org, wsprdaemon.org
gateways, report.pskreporter.info, `pswsnetwork.eng.ua.edu`) · the PSWS
registration/identity handshake (station id + portal SSH key) · the
station operator (CLI inspection/recovery) · sigmond's
`smd admin storage trim` retention janitor (bounds shared queues).

## 5. Assumptions & constraints

- `HSU-C-001` `[DOC]` ✅ Core SHALL be **stdlib-only**; third-party deps
  are confined to opt-in extras (`http` → `httpx`; `wsprdaemon` zstd is
  an optional import with a bz2 fallback). Python ≥3.10.
- `HSU-C-002` `[DOC]` ✅ The library SHALL be **synchronous and
  idempotent**: no in-flight state survives only in memory; a restart
  re-derives the batch from the persisted cursor.
- `HSU-C-003` `[CODE]` ✅ The `pending_uploads` queue's autoincrement `id`
  SHALL be the monotone cursor for SqliteSource; cursors are **opaque
  bytes** to everything but the source that emits them.
- `HSU-C-004` `[CODE]` ✅ A `SqliteSource` connection is **pinned to its
  creator thread**; the Uploader SHALL give each pipeline a dedicated
  single-worker executor to honor that.
- `HSU-C-005` `[CODE]` ✅ One process SHALL own one `watermarks.db`;
  multiple pumps writing the same watermark file cross-process is an
  operator config error (single-writer; per-call connections from a
  thread-locked store).
- `HSU-C-006` `[DOC]` ✅ A single SSH keypair **per station** SHALL be
  shared across all hs-uploader-using clients (default
  `/etc/hs-uploader/keys/id_ed25519`), auto-generated on first use.
- `HSU-C-007` `[CODE]` ✅ Shared state under `/var/lib/hs-uploader` SHALL
  be group-writable (`sigmond` group) so any client user can ack;
  enforced by `tmpfiles.d` + best-effort `chmod` on the db + WAL/SHM
  sidecars.
- `HSU-C-008` `[DOC]` ✅ Per-destination config (servers, ports,
  fallbacks) SHALL live in the **consuming client's** config, not in
  hs-uploader; hs-uploader's own config is only the station identity
  block.

## 6. Functional requirements

### 6.1 Sources (read side)
- `HSU-F-001` `[DOC]` ✅ `SqliteSource` SHALL read `pending_uploads` for a
  `(target_db, target_table)` pair, yielding rows with `id > cursor`,
  ordered `id ASC`, `LIMIT`-bounded, as a `RecordBatch` whose
  `cursor_after`/`commit_token` is the last consumed `id`.
- `HSU-F-002` `[DOC]` ✅ `SqliteSource` SHALL enforce a strict
  `schema_version IN (accepted)` filter and, on finding any row outside
  the accepted set, SHALL flip health to `stale-schema` and refuse to
  yield (a clean halt).
- `HSU-F-003` `[CODE]` ✅ `SqliteSource` SHALL support `extra_where`
  predicates rendered against `json_extract(payload_json,'$.<col>')`
  (column names validated alnum, values parameterized) so a
  multi-instance host can scope a pipeline to one producer.
- `HSU-F-004` `[CODE]` ✅ `SqliteSource` SHALL support `start_at` (`"now"`
  or a datetime): an empty watermark anchors at the current `max(id)` so
  a fresh deploy does not replay historical rows; the anchor SHALL be
  cached so it cannot drift across empty polls.
- `HSU-F-005` `[CODE]` ✅ `SqliteSource` SHALL support a SQL-layer
  **max-key-wins dedup** (`dedup_partition_by` + `dedup_order_by_desc`)
  via a window-function CTE that yields only the per-partition winner and
  excludes already-visited partitions, so a follower transport on the
  same queue still sees every row.
- `HSU-F-006` `[CODE]` ✅ `SqliteSource` SHALL support an optional ship
  **ceiling** (`ceiling_column` + `ceiling_provider`) holding back rows
  newer than a provider-supplied value (used to gate incomplete WSPR
  cycles out of the dedup ranking).
- `HSU-F-007` `[CODE]` ✅ `SqliteSource.commit` SHALL delete acked rows
  (`id ≤ commit_token`) when `delete_on_commit=True`, and SHALL be a
  no-op (cursor-advance only) when `False` so multiple pipelines can
  share one queue with `smd storage trim` as the retention janitor.
- `HSU-F-008` `[DOC]` ✅ `WsprCycleSource` SHALL read `wspr.spots` +
  `wspr.noise` (optionally `psk.spots`) as one logical stream grouped by
  the 2-minute WSPR cycle, yielding **one `RecordBatch` per cycle**,
  oldest first, gated to cycles older than the in-progress boundary.
- `HSU-F-009` `[CODE]` ✅ `WsprCycleSource` SHALL, when
  `expected_reporters` is set, ship a cycle only once every listed
  receiver's noise rows are present **or** the cycle is past
  `backstop_sec`, and SHALL ship spots-only / noise-only with a WARNING
  rather than block.
- `HSU-F-010` `[DOC]` ✅ `FileTreeSource` SHALL walk glob patterns
  oldest-first by mtime with two retention modes: `delete_on_ack`
  (commit_token carries paths to delete + prunes empty dirs) and `keep`
  (mtime cursor, no delete).
- `HSU-F-011` `[CODE]` ✅ `FileTreeSource` per-extension parsers MAY
  return one **or many** records per file; a row's `time` key becomes
  `Record.time`, else the file mtime is used.
- `HSU-F-012` `[CODE]` ✅ Every Source SHALL be deterministic for a given
  cursor (same on-disk state ⇒ same first batch) and expose
  `source_id()` + `health()`.

### 6.2 Transports (write side)
- `HSU-F-020` `[DOC]` ✅ `PskReporterTcp` SHALL own a keep-alive TCP
  socket to `report.pskreporter.info:4739`, build IPFIX-style packets,
  hold the connection across `ship()` calls, regenerate the session id on
  reconnect, and retry up to N sends per packet before `retry_later`.
- `HSU-F-021` `[DOC]` ✅ `WsprdaemonTarSftp` SHALL bundle a per-cycle
  compressed tar (bz2-9 default, zstd selectable) in the Phase-2 `wspr/`
  layout and ship via SFTP `.part`-then-rename to a list of gateways,
  with cycle-stable tar names.
- `HSU-F-022` `[CODE]` ✅ `WsprdaemonTarSftp` SHALL build the tar from
  **either** FileTreeSource records (`payload_path`) or SqliteSource
  records (`columns`, reconstructing the wsprdaemon wire-format lines +
  geodesy in memory), filing each row under its **own** receiver's
  RX_SITE/RECEIVER for multi-RX diversity.
- `HSU-F-023` `[CODE]` ✅ `WsprdaemonTarSftp` SHALL recover from a changed
  gateway host key (detect, `ssh-keygen -R` on a writable known_hosts,
  retry) and MAY fall back to `WsprdaemonTarFtp` when all SFTP servers
  fail.
- `HSU-F-024` `[DOC]` ✅ `WsprdaemonTarFtp` SHALL be a byte-identical-tar
  FTP fallback that additionally includes `client_upload_info.txt` so the
  gateway can auto-provision SFTP for the reporter on the next cycle.
- `HSU-F-025` `[DOC]` ✅ `WsprNet` SHALL POST `wspr.spots` as MEPT
  `multipart/form-data` to `wsprnet.org/meptspots.php` (stdlib `urllib`),
  byte-compatible with `wd-upload-wsprnet`, capped at 999 spots/POST,
  sorted by (date,time,freq), max-SNR-deduped across receivers.
- `HSU-F-026` `[CODE]` ✅ `WsprNet` SHALL treat **any** server response
  (incl. HTTP error) and **any** network/timeout as `acked` (no retry)
  per operator policy, stashing the server's "N out of M added" count in
  `Outcome.reason`; it MAY use the async submit→nonce→poll API when
  `api_base_url` is set.
- `HSU-F-027` `[DOC]` ✅ `PswsMagnetometerSftp` SHALL upload one daily
  `OBS<date>T<HH:MM>.zip` per record to `pswsnetwork.eng.ua.edu` via
  `.part`-then-rename and `mkdir` a Grape-style trigger dir
  (`c<dataset>_#<instrument>_#<ts>`), authorizing as the PSWS station id +
  portal SSH key (see §8.3).
- `HSU-F-028` `[CODE]` ✅ Every Transport SHALL declare `ACCEPTS`
  (table → accepted schema versions), a `primary_table()`, a
  `batch_policy()`, and SHALL implement `ship` / `serialize_for_retry` /
  `replay` so a retry replays a byte-stable payload.

### 6.3 Watermark store & retry
- `HSU-F-030` `[DOC]` ✅ `SqliteWatermarkStore` SHALL persist one cursor
  per `(source_id, dest_id, table)` and advance it only on ack
  (first-attempt or replay).
- `HSU-F-031` `[DOC]` ✅ It SHALL persist a **deliverables** retry queue
  (with `cursor_after` + `commit_token`) across restarts, and a
  **dead_letter** table for terminal failures (cursor NOT advanced).
- `HSU-F-032` `[CODE]` ✅ It SHALL keep a ring-buffered per-attempt
  `attempts` audit table (last 10k rows: ts, source, dest, table,
  outcome, records, bytes, error).
- `HSU-F-033` `[CODE]` ✅ It SHALL serialize all access through a process
  lock and open with `check_same_thread=False`, and SHALL make the db +
  WAL/SHM sidecars group-writable (best-effort, silent on
  PermissionError).

### 6.4 Orchestration (Pipeline / Uploader)
- `HSU-F-040` `[DOC]` ✅ A `Pipeline` SHALL bind one source + one
  transport + one watermark slot + a `StationIdentity`; the same source
  MAY feed two transports independently.
- `HSU-F-041` `[DOC]` ✅ `Uploader.pump()` SHALL do one pass per pipeline:
  drain due deliverables first, then — only if no deliverable is queued —
  drain the source up to the batch/per-pump budget; `pump_until_idle()`
  SHALL drain to empty for cron use.
- `HSU-F-042` `[CODE]` ✅ With >1 pipeline, `pump()` SHALL run pipelines
  concurrently on per-pipeline executors and **materialize every
  future's result before folding** (no `any()` short-circuit) so a slow
  pipeline's tally isn't mis-logged.
- `HSU-F-043` `[CODE]` ✅ On `retry_later` the orchestrator SHALL enqueue
  a deliverable at `delay_for(0)` and re-attempt with capped exponential
  back-off up to `max_attempts`, then dead-letter; on `permanent` it
  SHALL dead-letter immediately.
- `HSU-F-044` `[CODE]` ✅ An optional `on_batch_outcome(pipeline, batch,
  outcome)` callback SHALL fire for every first-attempt outcome kind, and
  exceptions in it SHALL be swallowed (visibility, not control).

### 6.5 Identity & CLI
- `HSU-F-050` `[CODE]` ✅ `StationIdentity.load` SHALL read the
  `[hs_uploader.station]` TOML block then apply `HS_UPLOADER_*` env
  overrides (call, grid, station_id, ssh_key_file, radiod_id).
- `HSU-F-051` `[CODE]` ✅ `ensure_ssh_key` SHALL auto-generate an ed25519
  keypair at `ssh_key_file` if absent (0600/0644), and `public_key()`
  SHALL return the `.pub` contents for pubkey-publishing flows.
- `HSU-F-052` `[DOC]` ✅ The `hs-uploader` CLI SHALL expose
  `status` / `peek` / `reset-cursor` / `kick` against `watermarks.db` for
  operator inspection/recovery; it SHALL NOT run a pump.

## 7. Quality / non-functional requirements

- `HSU-Q-001` `[DOC]` ✅ Uploads SHALL be idempotent: a restart, a lost
  wake, a duplicate or reordered notification SHALL NOT desync delivery
  (truth is the cursor + queue, not in-memory state).
- `HSU-Q-002` `[CODE]` ✅ SqliteSource SHALL set `PRAGMA
  temp_store=MEMORY` so a spill-prone scan cannot fail under systemd
  `ProtectSystem=strict` read-only `/tmp`; batches are `LIMIT`-bounded so
  the in-memory temp store cannot grow unbounded.
- `HSU-Q-003` `[CODE]` ✅ A failed `commit` DELETE SHALL NOT be promoted
  to a hard error — the cursor already advanced, so rows are simply
  re-skipped until a later commit succeeds.
- `HSU-Q-004` `[CODE]` ✅ Retries SHALL be byte-stable: `serialize_for_retry`
  captures the on-the-wire payload (MEPT body / tar bytes / encoded
  spots) so `replay` re-sends bit-identically.
- `HSU-Q-005` `[CODE]` ✅ Source health SHALL degrade legibly
  (`ok` / `unreachable` / `stale-schema` / `noop`) for the consuming
  client to surface; a missing sink SHALL be a silent no-op
  (standalone-safe), mirroring `Writer.from_env`.
- `HSU-Q-006` `[CODE]` ✅ A missing optional dep SHALL degrade, never
  hard-fail (zstd → bz2 with a one-shot warning; `sftp`/`ssh-keygen`
  absence → `retry_later`).
- `HSU-Q-007` `[CODE]` ✅ Concurrent multi-pipeline pumps SHALL not share
  mutable state; a shared `on_batch_outcome` callback is the caller's
  thread-safety responsibility (documented).
- `HSU-Q-008` `[CODE]` ✅ Wire formats SHALL stay byte-compatible with the
  wsprdaemon-client tooling they replace (MEPT lines, multipart boundary,
  tar arcname layout, noise/extended-spot field widths) for staged
  migration.
- `HSU-Q-009` `[NEW]` 🟡 `ACCEPTS` is currently **advisory** — no
  orchestrator gate enforces transport↔row schema agreement (the
  stale-schema halt lives only in SqliteSource's accepted-versions
  filter). The transport-side gate SHALL be enforced or `ACCEPTS`
  documented as advisory. *(gap — `HSU-F-091`.)*

## 8. External interfaces

### 8.1 Inputs
- **`pending_uploads`** in `/var/lib/sigmond/sink.db` (or
  `SIGMOND_SQLITE_PATH`): `id`, `target_db`, `target_table`,
  `schema_version`, `payload_json`, `queued_at` (schema owned by
  `sigmond.hamsci_sink.Writer`, §8.3). No sink present ⇒ no-op source.
- **File spools** (consumer-defined roots) for `FileTreeSource` — daily
  PSWS zips, legacy wsprdaemon spool files.
- **Station identity**: `[hs_uploader.station]` in `coordination.toml`
  (call, grid, station_id, ssh_key_file, radiod_id) + `HS_UPLOADER_*`
  env overrides; `/etc/sigmond/coordination.env` for sigmond hosts.
- **Per-destination config** supplied by the consuming client at Pipeline
  construction (servers, ports, timeouts, fallbacks, compression).
- **State dir** `HS_UPLOADER_STATE_DIR` (default `/var/lib/hs-uploader`)
  for `watermarks.db` and a writable `known_hosts`.

### 8.2 Outputs
- **wsprnet.org** — HTTP MEPT multipart POST to `meptspots.php` (or async
  `api/upload/v1`).
- **wsprdaemon.org** — compressed tar over SFTP (FTP fallback) to gateway
  list; `wspr/{spots,noise}/<RX_SITE>/<RECEIVER>/<BAND>/…` +
  `ft8|ft4/…/<cycle>.jsonl` + `uploads_config.txt` / `routing.json`.
- **PSKReporter** — IPFIX-style reception-report packets over TCP.
- **PSWS** (`pswsnetwork.eng.ua.edu`) — daily mag zip over SFTP +
  Grape-style trigger dir (the PSWS seam, §8.3).
- **Local state**: `watermarks.db` (cursors, deliverables, dead_letter,
  attempts audit). **Logs**: Python logging to the consumer's journal.
- **CLI**: `hs-uploader status|peek|reset-cursor|kick` (human/text).

### 8.3 Contracts / APIs (reference, not restated)
- `HSU-I-001` `[CODE]` ✅ **Python API contract.** The three abstractions
  are the stable surface: `Source` (`source_id`, `health`, `iter_batches`,
  `commit`), `Transport` (`name`, `ACCEPTS`, `primary_table`,
  `batch_policy`, `ship`, `serialize_for_retry`, `replay`), `WatermarkStore`
  (cursor/deliverable/dead-letter/attempt ops). Cursors and commit_tokens
  are opaque bytes; `Record`/`RecordBatch`/`Outcome`/`BatchPolicy`/
  `RetryPolicy`/`Pipeline`/`Uploader` are the composition types.
- `HSU-I-002` `[DOC]` ✅ **Sink-queue contract (producer seam).** Reads the
  `pending_uploads` schema exactly as `sigmond.hamsci_sink.Writer`
  produces it; `(target_db, target_table)` names the stream and
  per-row `schema_version` is the safety gate (§3.1/§3.4 of
  [PSWS-INTERFACE-BOUNDARY.md](https://github.com/HamSCI/sigmond/blob/main/docs/PSWS-INTERFACE-BOUNDARY.md)).
  This is the *only* thing a client must produce to participate in upload.
- `HSU-I-003` `[DOC]` ✅ **Station↔PSWS upload seam.** `PswsMagnetometerSftp`
  is one of the two transports that hit the PSWS server itself; it
  authorizes with `sftp_user` = PSWS **station id** (default
  `identity.station_id`) and `ssh_key_file` = the **portal-registered**
  key (shared with the hf-timestd Grape path). Governed by
  PSWS-INTERFACE-BOUNDARY §2/§3.3 (board #6 items #25 registration, #5
  WW0WWV→PSWS) — not restated here.
- `HSU-I-004` `[NEW]` ⬜ **Heartbeat transport (roadmap).** PSWS board
  items #20/#39 (network health) and #19 (Level-0 pull) assume a
  station→server **heartbeat** carrying health/availability, specified in
  [PSWS-HEARTBEAT-SPEC.md](https://github.com/HamSCI/sigmond/blob/main/docs/PSWS-HEARTBEAT-SPEC.md).
  When built, it is the natural next hs-uploader transport (a
  request/response widening of this interface). *(roadmap — `HSU-F-092`.)*

## 9. Data requirements

- **`Record`** (frozen): `table` (routing key), `time` (UTC), `columns`
  (row payload) **xor** `payload_path` (dataset bytes on disk),
  `dedup_key`. **`RecordBatch`**: records + opaque `cursor_after` +
  source-specific `commit_token`.
- **`watermarks.db`** (default `/var/lib/hs-uploader/watermarks.db`, WAL,
  group-writable): `watermarks(source_id, dest_id, table_name, cursor,
  last_ack)`; `attempts(...)` ring-buffered to 10k; `deliverables(...)`
  retry queue with `cursor_after`/`commit_token`; `dead_letter(...)`.
- **Provenance/time labels.** `Record.time` is taken from the payload's
  `time`/`decode_time`/`utc`, else the row's `queued_at`, else now — so
  every shipped record carries the producer's observation time.
- **Retention.** hs-uploader does not own queue retention when
  `delete_on_commit=False`; `smd admin storage trim` (per-target TTL,
  30-min floor) bounds shared queues. Audit is self-trimming.

## 10. Dependencies & development sequence

**Runtime deps:** Python ≥3.10, stdlib only for the core (`sqlite3`,
`urllib`, `ftplib`, `tarfile`, `bz2`, `subprocess` for `sftp`/`ssh-keygen`).
`tomli` only on <3.11. Optional extras: `http` (`httpx`), `wsprdaemon`
(optional `zstandard`), `pskreporter`/`wsprnet`/`psws` (placeholders).
**Producer dep:** `sigmond.hamsci_sink.Writer` populates `pending_uploads`
(lazy-imported / standalone-safe). External services: wsprnet,
wsprdaemon gateways, pskreporter, pswsnetwork.

**Development sequence (intended, recovered as requirement):**
- **Phase 1:** core types + `SqliteSource` + `SqliteWatermarkStore` +
  CLI (status/peek/reset/kick) — read-mostly, no transports.
- **Phase 2:** transports land — `PskReporterTcp` (first consumer:
  psk-recorder behind `PSK_USE_HS_UPLOADER`), then `WsprdaemonTarSftp/Ftp`
  + `WsprCycleSource`, then `WsprNet`, then `PswsMagnetometerSftp`.
- **Hardening (PRs through 2026-05):** multi-RX dedup CTE + ship ceiling +
  cross-receiver completion gating; group-writable shared state;
  per-pipeline executors + non-short-circuit fold; wsprnet no-retry
  policy + async API; bz2-vs-zstd + dual tar-root migration.
- **Roadmap:** codar transport; PSWS heartbeat transport
  (PSWS-HEARTBEAT-SPEC); transport-side `ACCEPTS` enforcement;
  WsprdaemonTar SqliteSource noise emission (currently spots-only on that
  path) and a richer CLI.

## 11. Acceptance criteria & verification

- **Idempotency/orchestration** → `tests/test_core_orchestration.py`
  (deliverable retry, dead-letter, cursor advance on replay-ack).
- **Source correctness** → `test_sqlite_source.py` (cursor, schema gate,
  extra_where, dedup CTE, ceiling), `test_wspr_completion.py`,
  `test_file_source.py`, `test_watermark_sqlite.py`.
- **Wire-format compatibility** → `test_transport_wsprnet.py` (MEPT bytes,
  999-cap, no-retry policy), `test_transport_wsprdaemon.py` (tar layout),
  `test_transport_pskreporter_tcp.py`, `test_payload_psk_pskr.py`,
  `test_transport_psws_magnetometer.py` (trigger dir, dry-run).
- **CLI** → `test_cli.py` (status/peek/reset/kick).
- **Operational acceptance** → live delivery audited producer-side by
  `smd admin verifier report` (wspr/psk cohorts) and the wsprnet_audit
  table; queue bounded by `smd admin storage trim`.
- **Standalone** → `SqliteSource.from_env` with no sink ⇒ no-op source,
  no crash.

## 12. Risks & open questions

- `HSU-F-090` `[NEW]` 🟡 **WsprdaemonTar SqliteSource path emits
  spots-only.** Noise tar files come only from the file-source path;
  wspr-recorder Pipeline-v2 noise-in-sink → tar-noise is not yet wired
  (server tolerates spots-only tars). SHALL be wired or documented as the
  intended split. *(candidate #18 Clients issue.)*
- `HSU-F-091` `[NEW]` 🟡 **`ACCEPTS` is advisory** (`HSU-Q-009`): the
  only hard schema gate is SqliteSource's accepted-versions filter; the
  transport declaration is unenforced and has already drifted historically
  (declared `[3]` no producer wrote). SHALL be enforced or explicitly
  marked advisory in the API docs.
- `HSU-F-092` `[NEW]` ⬜ **No PSWS heartbeat transport** (`HSU-I-004`):
  the board's health/availability/Level-0-pull items need a station→server
  heartbeat (PSWS-HEARTBEAT-SPEC); not yet built.
- `HSU-F-093` `[NEW]` ⬜ **No codar transport.** `codar.spots` rows are
  written to the sink (CONTRACT §17) but have no hs-uploader transport;
  the upload destination is undecided.
- `HSU-F-094` `[NEW]` ⬜ **CLI cannot pump.** Recovery is inspection-only;
  there is no `hs-uploader pump --once` for an operator to drain a queue
  without the consuming client running (by design today; revisit for ops
  ergonomics).
- **Cross-process watermark assumption** (`HSU-C-005`): single-writer is
  asserted but not enforced; two pumps on one `watermarks.db` would be an
  undetected operator error.

## 13. Traceability

| Requirement | #18 issue | Verification | PSWS #6 |
|---|---|---|---|
| HSU-I-002 (sink-queue seam) | Clients: upload boundary | test_sqlite_source | #6:31 (sensor integ.) |
| HSU-I-003 (PSWS mag SFTP) | mag-recorder upload | test_transport_psws_magnetometer | #6:25 registration / #6:5 WW0WWV→PSWS |
| HSU-I-004 / F-092 (heartbeat) | *(new — file)* | — | #6:20/#39 health, #6:19 Level-0 |
| HSU-F-002 (schema-version halt) | Clients: upload boundary | test_sqlite_source | #6:31 |
| HSU-F-025/026 (wsprnet MEPT + no-retry) | wspr upload parity | test_transport_wsprnet | — |
| HSU-F-021/022 (wsprdaemon tar) | wspr upload parity | test_transport_wsprdaemon | — |
| HSU-F-090 (tar noise spots-only) | *(new — file)* | tar noise test | — |
| HSU-F-091 (ACCEPTS advisory) | *(new — file)* | transport gate test | — |
| HSU-F-093 (codar transport) | Clients: codar-sounder | *(none yet)* | #6:31 |

*New rows (HSU-F-090/091/092/093/094, HSU-Q-009) are this review's
surfaced gaps; promote to #18 Clients / the PSWS heartbeat epic.*
