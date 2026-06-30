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
from .pipeline_factory import build_pipelines as build_generic_pipelines
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


def build_all_pipelines(
    manifest: Mapping[str, Any],
    *,
    watermark: SqliteWatermarkStore,
) -> list[Pipeline]:
    """Compose every pipeline in the manifest — generic + builder entrypoints."""
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
            fn = resolve_builder(str(spec))
        except Exception:
            logger.exception("daemon: cannot resolve builder %s for %s", spec, name)
            continue
        try:
            built = fn(
                identity=base_identity,
                watermark=watermark,
                config=entry.get("config", {}) or {},
                name=name,
            )
        except Exception:
            logger.exception("daemon: builder %s failed for %s", spec, name)
            continue
        if built is None:
            continue
        if isinstance(built, Pipeline):
            built = [built]
        pipelines.extend(built)
        logger.info("daemon: builder %s contributed %d pipeline(s)", name, len(built))
    return pipelines


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

    watermark = SqliteWatermarkStore(default_path())
    pipelines = build_all_pipelines(manifest, watermark=watermark)
    if not pipelines:
        logger.error("daemon: no pipelines built from %s — nothing to do", manifest_path)
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

    stop = threading.Event()
    wake = threading.Event()

    listener = WakeListener(on_wake=wake.set, path=wake_sock)
    listener.start()  # best-effort; polling backstop covers a bind failure

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
                interval, listener.path)

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
