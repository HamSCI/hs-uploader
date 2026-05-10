# hs-uploader

Library for shipping HamSCI sigmond observations to HF reporting destinations.

`hs-uploader` is the read-side counterpart to `sigmond.hamsci_ch.Writer`: clients
import it to forward records they have staged in sigmond's local ClickHouse
(preferred) or in spool files (fallback) up to a HamSCI / community ingest
destination — wsprdaemon.org, wsprnet.org, PSKReporter, PSWS, etc.

## Status

Active. Sources, transports, and watermark store are all working:

- **Sources:** `ClickHouseSource` (preferred, with `cursor_column`,
  `extra_where`, and `start_at` knobs), `FileTreeSource` (delete-on-ack
  or keep retention; per-file parsers may return one or many records
  per file).
- **Transports:** `PskReporterTcp` (owns the socket; no external
  `pskreporter` dependency), `WsprdaemonTarSftp` / `WsprdaemonTarFtp`,
  `WsprNet` (HTTP multipart POST to `wsprnet.org/meptspots.php`).
- **Watermark store:** `SqliteWatermarkStore` with deliverable retry +
  per-attempt audit table.
- **Schema registry:** strict per-version column-hash check; producer
  upgrades trigger a clean stale-schema halt rather than data corruption.

Current consumer: `psk-recorder` ships `psk.spots` rows via
`PskReporterTcp`, behind the `PSK_USE_HS_UPLOADER=1` feature flag.

## Architecture

```
   ┌───────────────┐   Record    ┌──────────────────┐    Outcome
   │   Source      │ ─────────► │    Transport     │ ─────────────► destination
   │ (CH | Files)  │             │  (per-protocol)  │                (network)
   └───────────────┘             └──────────────────┘
            ▲                              │
            │            advance/retry     ▼
            └─────────  Watermark  ◄───  Pipeline (orchestrator)
                       (SQLite)
```

Three orthogonal abstractions:

- **Source** — yields `Record`s starting from an opaque cursor.
  `ClickHouseSource` is preferred; `FileTreeSource` is the fallback.
- **Transport** — accepts a batch and reports an `Outcome` (acked, partial-ack,
  retry-later, dead). One per upstream destination.
- **WatermarkStore** — owns per-`(source, destination, table)` cursor and
  retry-deliverable state. SQLite-backed.

A **Pipeline** binds one source + one transport + one watermark slot. An
**Uploader** orchestrates N pipelines.

The library is synchronous and idempotent. No threads, no asyncio. Restarts
re-derive the batch from the cursor; there is no in-flight state to lose.

## Install

```bash
pip install -e ".[dev,clickhouse,wsprdaemon,wsprnet,pskreporter,psws]"
```

Optional extras let consuming clients pull only the transports they use.

## License

MIT — see LICENSE.
