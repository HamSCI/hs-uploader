# hs-uploader

Library for shipping HamSCI sigmond observations to HF reporting destinations.

`hs-uploader` is the read-side counterpart to `sigmond.hamsci_ch`'s
`SqliteWriter`: clients import it to forward records they have staged in
sigmond's local SQLite sink (preferred) or in spool files (fallback) up to a
HamSCI / community ingest destination — wsprdaemon.org, wsprnet.org,
PSKReporter, PSWS, etc.

## Status

Active. Sources, transports, and watermark store are all working:

- **Sources:** `SqliteSource` (preferred — reads
  `sigmond.hamsci_ch.SqliteWriter`'s `pending_uploads` queue, with
  `extra_where` and `start_at` knobs and a strict `schema_version`
  check), `FileTreeSource` (delete-on-ack or keep retention; per-file
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
  `SqliteSource` is preferred; `FileTreeSource` is the fallback.
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
