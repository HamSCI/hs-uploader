# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

**hs-uploader** is the read-side counterpart to `sigmond.hamsci_sink`'s
`Writer`. Sigmond clients (the recorders) stage observation records
into a local SQLite sink (`/var/lib/sigmond/sink.db`); this library
forwards them up to HamSCI / community ingest destinations —
wsprdaemon.org, wsprnet.org, PSKReporter, PSWS.

Part of the HamSCI sigmond suite — see `/opt/git/sigmond/sigmond/CLAUDE.md`
(orchestrator) and `/opt/git/sigmond/CLAUDE.md` (umbrella) for
cross-repo context. This is a library; it has no daemon of its own.
Its consumers run a per-pipeline pump worker in-process.

## Authors

- Michael Hauan (AC0G, GitHub: mijahauan)
- Repo: https://github.com/mijahauan/hs-uploader

## Commands

```bash
# Development — uv canonical
uv sync --extra dev
uv run pytest tests/
uv run pytest tests/test_<area>.py -v             # one file
uv run pytest -k watermark -v                     # by keyword

# Build distribution
uv build

# CLI (operator inspector — not the consumer integration path)
hs-uploader --help
```

Consumers integrate via `from hs_uploader import …`, not the CLI.
The CLI is a thin diagnostic entry point.

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
  - `SqliteSource` (preferred) reads `sigmond.hamsci_sink.Writer`'s
    `pending_uploads` queue. Supports `extra_where` and `start_at`
    knobs and enforces a strict `schema_version` check (rows outside
    the pipeline's accepted set flip the source to `stale-schema` —
    a clean halt rather than shipping mis-typed records).
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
  sources/
    base.py               # Source ABC + Record / RecordBatch types
    sqlite.py             # SqliteSource (preferred path)
    wspr_cycle.py         # WsprCycleSource (cycle-aligned variant)
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
  payload/
    psk_pskr.py           # PSK Reporter binary frame builder
tests/                    # 11 files
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

- Sigmond sink: `/var/lib/sigmond/sink.db` (read-side; sigmond owns
  writes via `hamsci_sink.Writer`).
- Watermark store: `/var/lib/hs-uploader/watermarks.db` (per-consumer
  state; survives restarts).
- File spool (fallback): consumer-defined per pipeline.

## Library lockfile policy

`uv.lock` for libraries doesn't bind downstream consumers. Each
consumer pins hs-uploader via its own `uv.lock` (and via
`[tool.uv.sources]` editable path during dev).
