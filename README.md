# hs-uploader

Library for shipping HamSCI sigmond observations to HF reporting destinations.

`hs-uploader` is the read-side counterpart to `sigmond.hamsci_ch.Writer`: clients
import it to forward records they have staged in sigmond's local ClickHouse
(preferred) or in spool files (fallback) up to a HamSCI / community ingest
destination — wsprdaemon.org, wsprnet.org, PSKReporter, PSWS, etc.

## Status

**Phase 1 — scaffolding and core abstractions.** No transports yet; one
working `SqliteWatermarkStore` and a `ClickHouseSource`. See the design plan
for the full phase plan.

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
