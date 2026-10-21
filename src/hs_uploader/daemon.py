"""Host uploader daemon — runs every outbound pipeline on a host in ONE process.

This is the single-host uploader: one process, one OS user, one SSH key, draining
every producer (sink.db + dataset spools) to every destination (wsprnet,
wsprdaemon, pskreporter, PSWS).  It replaces the per-recorder in-process
uploaders.

It reads a manifest (``/etc/hs-uploader/pipelines.toml``) describing the host's
pipelines and composes them into one ``hs_uploader.core.Uploader``:

* simple pipelines (``[pipeline.source]`` + ``[pipeline.transport]``) are built
  by :mod:`hs_uploader.pipeline_factory` from generic Source/Transport classes;
* pipelines whose construction needs runtime objects/callables (e.g. the
  cycle-aligned wsprdaemon tar's ceiling-provider + FTP-fallback) declare a
  ``builder = "module:function"`` entrypoint that returns ``list[Pipeline]`` —
  the client owns that code, the daemon just composes + runs it.

The pump loop is wake-driven (the shared ``upload-wake.sock``) with a polling
backstop, and speaks systemd ``Type=notify`` + ``WatchdogSec`` (stdlib only).

Run via ``hs-uploader serve --manifest /etc/hs-uploader/pipelines.toml``.
"""

from __future__ import annotations

import importlib
import logging
import os
import signal
import socket
import threading
import tomllib
from pathlib import Path
from typing import Any, Mapping

from .config import StationIdentity
from .core import Pipeline, Uploader
from .pipeline_factory import build_entry
from .pipeline_factory import build_pipelines as build_generic_pipelines
from .pipelines_d import LEGACY, Layout, Refusal, load_layout
from .transports.discard import wrap_if_discard
from .wake import WakeListener
from .watermark.sqlite import SqliteWatermarkStore, default_path

logger = logging.getLogger(__name__)

_DEFAULT_MANIFEST = "/etc/hs-uploader/pipelines.toml"


# ---- systemd Type=notify (stdlib only) --------------------------------------


def _sd_notify(message: bytes) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.sendall(message)
    except OSError:
        logger.debug("sd_notify %r failed (not under systemd?)", message)


def _watchdog_loop(stop: threading.Event) -> None:
    usec = os.environ.get("WATCHDOG_USEC")
    if not usec:
        return
    try:
        interval = max(int(usec) / 1_000_000 / 2, 1.0)
    except ValueError:
        return
    while not stop.wait(interval):
        _sd_notify(b"WATCHDOG=1")


# ---- manifest → pipelines ---------------------------------------------------


def load_manifest(path: str | Path) -> Mapping[str, Any]:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def resolve_builder(spec: str):
    """Resolve a ``"module.path:function"`` entrypoint to the callable."""
    if ":" not in spec:
        raise ValueError(f"builder must be 'module:function', got {spec!r}")
    mod_name, _, func_name = spec.partition(":")
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)


def _base_identity(manifest: Mapping[str, Any]) -> StationIdentity:
    block = manifest.get("identity", {}) or {}
    ident = StationIdentity()
    for field in ("call", "grid", "station_id", "ssh_key_file", "radiod_id"):
        if block.get(field):
            setattr(ident, field, str(block[field]))
    return ident


def _build_with_builder(
    entry: Mapping[str, Any],
    *,
    base_identity: StationIdentity,
    watermark: SqliteWatermarkStore,
    origin: str = LEGACY,
) -> list[Pipeline]:
    """Run one ``builder = "module:function"`` entry; raise on failure."""
    spec = str(entry["builder"])
    name = str(entry.get("name") or spec)
    fn = resolve_builder(spec)
    built = fn(
        identity=base_identity,
        watermark=watermark,
        config=entry.get("config", {}) or {},
        name=name,
    )
    if built is None:
        return []
    if isinstance(built, Pipeline):
        built = [built]
    for p in built:
        p.client = str(entry.get("client") or "")
        p.governed = bool(entry.get("governed", True))
        p.origin = origin
        p.transport = wrap_if_discard(entry, p.transport)
    logger.info("daemon: builder %s contributed %d pipeline(s)", name, len(built))
    return list(built)


def build_all_pipelines(
    manifest: Mapping[str, Any],
    *,
    watermark: SqliteWatermarkStore,
) -> list[Pipeline]:
    """Compose every pipeline in the manifest — generic + builder entrypoints.

    The legacy path, unchanged: a generic entry that fails to build raises; a
    builder that fails is logged and skipped.
    """
    pipelines: list[Pipeline] = list(
        build_generic_pipelines(manifest, watermark=watermark)
    )
    base_identity = _base_identity(manifest)
    for entry in manifest.get("pipeline", []) or []:
        spec = entry.get("builder")
        if not spec:
            continue
        name = str(entry.get("name") or spec)
        try:
            pipelines.extend(_build_with_builder(
                entry, base_identity=base_identity, watermark=watermark))
        except Exception:
            logger.exception("daemon: builder %s failed for %s", spec, name)
    return pipelines


# ---- pipelines.d: build the layout, refuse whole files (spec sink-2b §3.2-§3.3)


def pipeline_key(p: Pipeline) -> tuple[str, str, str]:
    """The send-record key: (source, destination, table)."""
    return (p.source_id(), p.dest_id(), p.transport.primary_table())


def _collision(p: Pipeline, names: Mapping[str, Pipeline],
               keys: Mapping[tuple, Pipeline]) -> str:
    """Describe p's first collision with an accepted pipeline, or ""."""
    other = names.get(p.name)
    if other is not None:
        what = "name"
    else:
        other = keys.get(pipeline_key(p))
        what = "key " + "|".join(pipeline_key(p))
        if other is None:
            return ""
    return (f"pipeline {p.name!r} ({p.origin}) collides on {what} with "
            f"pipeline {other.name!r} ({other.origin})")


def _close_quietly(pipes: list[Pipeline]) -> None:
    for p in pipes:
        close = getattr(p.source, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                logger.debug("daemon: closing %s's source failed", p.name)


def _build_file(entries: list[Mapping[str, Any]], origin: str, *,
                manifest: Mapping[str, Any],
                watermark: SqliteWatermarkStore) -> tuple[list[Pipeline], str]:
    """Build every entry of one pipeline file: (pipelines, "") or ([], reason)."""
    base = manifest.get("identity", {}) or {}
    built: list[Pipeline] = []
    for i, entry in enumerate(entries):
        label = entry.get("name") or entry.get("builder") or f"entry {i + 1}"
        try:
            if entry.get("builder"):
                built.extend(_build_with_builder(
                    entry, base_identity=_base_identity(manifest),
                    watermark=watermark, origin=origin))
            else:
                built.append(build_entry(entry, base_identity=base,
                                         watermark=watermark, origin=origin,
                                         index=i))
        except Exception as exc:
            _close_quietly(built)
            return [], f"pipeline {label!r} failed to build: {exc!r}"
    return built, ""


def build_layout(
    layout: Layout, *, watermark: SqliteWatermarkStore
) -> tuple[list[Pipeline], list[Refusal]]:
    """Build every entry of ``layout``; return (pipelines, every refused file).

    Legacy entries build as v3.76 built them; a collision among them alone
    logs one ERROR naming both and both run (§3.3).  Each pipeline file then
    builds whole or not at all: a failed entry, or a name or key that an
    accepted pipeline already holds, refuses the file and names both
    pipelines.  The refusals returned include ``layout.refused``.
    """
    manifest = layout.manifest
    legacy = [e for e, origin in layout.entries if origin == LEGACY]
    pipelines = build_all_pipelines({**manifest, "pipeline": legacy},
                                    watermark=watermark)
    names: dict[str, Pipeline] = {}
    keys: dict[tuple, Pipeline] = {}
    for p in pipelines:
        clash = _collision(p, names, keys)
        if clash:
            logger.error("daemon: legacy manifest: %s; running both as "
                         "before", clash)
        names.setdefault(p.name, p)
        keys.setdefault(pipeline_key(p), p)

    by_file: dict[str, list[Mapping[str, Any]]] = {}
    for entry, origin in layout.entries:
        if origin != LEGACY:
            by_file.setdefault(origin, []).append(entry)

    refused = list(layout.refused)
    for origin, entries in by_file.items():
        filename = origin.split("/", 1)[1]
        client = filename[: -len(".toml")]
        built, reason = _build_file(entries, origin, manifest=manifest,
                                    watermark=watermark)
        if not reason:
            f_names, f_keys = dict(names), dict(keys)
            for p in built:
                reason = _collision(p, f_names, f_keys)
                if reason:
                    break
                f_names[p.name] = p
                f_keys[pipeline_key(p)] = p
        if reason:
            _close_quietly(built)
            refused.append(Refusal(filename, client, reason))
            continue
        for p in built:
            names[p.name] = p
            keys[pipeline_key(p)] = p
        pipelines.extend(built)

    for r in refused:
        logger.error("daemon: refusing %s (client %s): %s",
                     r.row_name, r.client, r.reason)
    for p in pipelines:
        logger.info("daemon: pipeline %s (client %s, origin %s)",
                    p.name, p.client or "(none)", p.origin)
    return pipelines, refused


# A pipeline whose source has a ``producer`` reads one client's rows and
# keeps one send record per producer; it needs store version 2 (spec §4.7).
_PRODUCER_STORE_VERSION = 2


def split_refused(
    pipelines: list[Pipeline], store_version: int
) -> tuple[list[Pipeline], dict[str, str]]:
    """Return (pipelines that may run, {name: reason} for the refused)."""
    runnable: list[Pipeline] = []
    refused: dict[str, str] = {}
    for p in pipelines:
        if (getattr(p.source, "producer", None)
                and store_version < _PRODUCER_STORE_VERSION):
            reason = (f"store is at schema version {store_version}; "
                      f"producer pipelines need version "
                      f"{_PRODUCER_STORE_VERSION} (run `hs-uploader migrate`)")
            logger.error("daemon: refusing pipeline %s: %s", p.name, reason)
            refused[p.name] = reason
        else:
            runnable.append(p)
    return runnable, refused


# ---- daemon -----------------------------------------------------------------


def run(manifest_path: str | Path = _DEFAULT_MANIFEST,
        *, dry_run: bool = False, once: bool = False) -> int:
    """Build the host uploader and run it; return a process exit code."""
    try:
        manifest = load_manifest(manifest_path)
    except FileNotFoundError:
        logger.error("daemon: manifest not found: %s", manifest_path)
        return 1
    except Exception:
        logger.exception("daemon: cannot parse manifest %s", manifest_path)
        return 1

    # pipelines.d/ never stops the daemon: a bad file becomes a refusal.
    layout = load_layout(manifest_path, manifest=manifest)
    watermark = SqliteWatermarkStore(default_path())
    pipelines, file_refused = build_layout(layout, watermark=watermark)
    if not pipelines:
        logger.error("daemon: no pipelines built from %s — nothing to do", manifest_path)
        return 1

    built = pipelines
    pipelines, refused = split_refused(built, watermark.refresh_schema_version())
    watermark.record_pipeline_states(
        [(p.name, p.client, refused.get(p.name, "")) for p in built]
        + [(r.row_name, r.client, r.reason) for r in file_refused],
        replace=True)
    if not pipelines:
        logger.error("daemon: every pipeline refused — nothing to do")
        return 1

    logger.info("daemon: %d pipeline(s): %s",
                len(pipelines), ", ".join(p.name for p in pipelines))

    uploader = Uploader(pipelines)

    if dry_run:
        logger.info("daemon: --dry-run — pipelines built, not pumping")
        return 0

    if once:
        passes = uploader.pump_until_idle()
        logger.info("daemon: --once drained in %d pass(es)", passes)
        return 0

    dcfg = manifest.get("daemon", {}) or {}
    interval = float(dcfg.get("pump_interval_sec", 30.0))
    wake_sock = dcfg.get("wake_socket")  # None → default path

    # Only bind the shared upload-wake socket when at least one pipeline reads
    # from the sink (SqliteSource/WsprCycleSource) — those are the producers
    # that fire wakes.  A file-spool-only daemon (e.g. GRAPE during migration)
    # must NOT bind it, or it steals the socket from a recorder's still-running
    # in-process uploader.  Overridable via [daemon] wake = true/false.
    from .sources.sqlite import SqliteSource
    try:
        from .sources.wspr_cycle import WsprCycleSource
        _sink_types: tuple = (SqliteSource, WsprCycleSource)
    except Exception:
        _sink_types = (SqliteSource,)
    needs_wake = any(isinstance(p.source, _sink_types) for p in pipelines)
    wake_cfg = dcfg.get("wake")
    use_wake = needs_wake if wake_cfg is None else bool(wake_cfg)

    stop = threading.Event()
    wake = threading.Event()

    listener = None
    if use_wake:
        listener = WakeListener(on_wake=wake.set, path=wake_sock)
        listener.start()  # best-effort; polling backstop covers a bind failure
    else:
        logger.info("daemon: wake socket not bound (no sink pipeline); "
                    "polling every %.0fs", interval)

    def _handle(sig, _frame):
        logger.info("daemon: signal %s — shutting down", sig)
        stop.set()
        wake.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    wd = threading.Thread(target=_watchdog_loop, args=(stop,),
                          name="hs-uploader-watchdog", daemon=True)
    wd.start()

    _sd_notify(b"READY=1")
    logger.info("daemon: active (pump interval %.0fs, wake %s)",
                interval, listener.path if listener else "disabled")

    while not stop.is_set():
        wake.wait(interval)
        wake.clear()
        if stop.is_set():
            break
        try:
            uploader.pump()
        except Exception:
            logger.exception("daemon: error in pump loop")

    _sd_notify(b"STOPPING=1")
    if listener is not None:
        listener.stop()
    logger.info("daemon: stopped")
    return 0


def main(argv=None) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="hs-uploader serve",
                                description="Run the host uploader daemon.")
    p.add_argument("--manifest", default=_DEFAULT_MANIFEST,
                   help=f"pipeline manifest TOML (default {_DEFAULT_MANIFEST})")
    p.add_argument("--dry-run", action="store_true",
                   help="build pipelines and exit (no pumping)")
    p.add_argument("--once", action="store_true",
                   help="drain once (pump_until_idle) and exit")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    return run(args.manifest, dry_run=args.dry_run, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
