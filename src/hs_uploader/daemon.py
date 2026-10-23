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

Beside the legacy manifest, ``pipelines.d/<client>.toml`` adds pipelines
(:mod:`hs_uploader.pipelines_d`).  Before each pump the daemon stats that
directory; when it moved, or on SIGHUP, :class:`Rebuilder` rebuilds between
pumps, keeps every unchanged pipeline running, and runs the store check
again.  The legacy manifest stays as read at start: an edit to it waits for
a restart.

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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .config import StationIdentity
from .core import Pipeline, Uploader
from .pipeline_factory import build_entry
from .pipeline_factory import build_pipelines as build_generic_pipelines
from .pipelines_d import (LEGACY, Layout, Refusal, load_layout,
                          pipelines_dir_for, pipelines_signature,
                          sinks_dir_for)
from .sinkswitch import effective_state, marker_path, marker_stands
from .gate import SinkGate  # also re-exported: spec §11.2 finds hs_uploader.daemon.SinkGate
from .gate import exempt_rule
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


# ---- pipelines.d: build the layout, refuse whole files (spec sink-2b §3.2-§3.4)


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
    """Close the sources of pipelines that never pumped (main thread)."""
    for p in pipes:
        close = getattr(p.source, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                logger.debug("daemon: closing %s's source failed", p.name)


def _always(_key: tuple) -> bool:
    return True


class _Once:
    """Say whether a keyed message should log: once while its key recurs.

    Each build calls ``begin()`` first and ``end()`` last.  A key the previous
    build also met stays quiet.  A key that stops recurring is forgotten, so
    it logs again if it comes back.  A refused file's key holds its own stat
    and its reason, so touching a side file never repeats its ERROR (§3.4
    step 2, ruling C15 note).
    """

    def __init__(self) -> None:
        self._seen: set = set()
        self._now: set = set()

    def begin(self) -> None:
        self._now = set()

    def end(self) -> None:
        self._seen, self._now = self._now, set()

    def __call__(self, key: tuple) -> bool:
        self._now.add(key)
        return key not in self._seen


def _file_stat(signature: tuple, filename: str) -> Optional[tuple]:
    """One file's (mtime_ns, size, inode) from a ``pipelines_signature``."""
    if len(signature) == 2:
        for name, st in signature[1]:
            if name == filename:
                return st
    return None


@dataclass
class _Slot:
    """One entry and the pipelines it built: what a rebuild may reuse."""

    origin: str
    index: Optional[int]          # naming index of an unnamed generic entry
    entry: Mapping[str, Any]
    pipes: list[Pipeline]


@dataclass
class Build:
    """One build's result, kept so the next rebuild can reuse it (§3.4).

    ``pipelines`` holds every pipeline built, before the store check.
    ``good`` holds each pipeline file's last accepted entries.
    """

    pipelines: list[Pipeline] = field(default_factory=list)
    refused: list[Refusal] = field(default_factory=list)
    slots: list[_Slot] = field(default_factory=list)
    good: dict[str, list[Mapping[str, Any]]] = field(default_factory=dict)
    # Pipelines this build made new (not reused): the ones to close if the
    # rebuild fails after building them.
    fresh: list[Pipeline] = field(default_factory=list)


class _Pool:
    """The previous build's slots; each one may be reused once."""

    def __init__(self, slots: list[_Slot]):
        self._free = list(slots)

    def take(self, origin: str, index: Optional[int],
             entry: Mapping[str, Any]) -> Optional[_Slot]:
        for i, s in enumerate(self._free):
            if s.origin == origin and s.index == index and s.entry == entry:
                return self._free.pop(i)
        return None

    def give_back(self, slots: list[_Slot]) -> None:
        self._free.extend(slots)


def _naming_index(entry: Mapping[str, Any], i: int) -> Optional[int]:
    """The index an entry's default name depends on, or None when it has none.

    An unnamed generic entry is named ``pipeline-<i>``, so its index is part
    of what makes it the same entry; a named one, or a builder, does not
    move when an entry before it drops out.
    """
    return None if entry.get("name") or entry.get("builder") else i


def _build_one(entry: Mapping[str, Any], origin: str, i: int, *,
               manifest: Mapping[str, Any],
               watermark: SqliteWatermarkStore) -> list[Pipeline]:
    """Build one entry, generic or builder; raise on failure."""
    if entry.get("builder"):
        return _build_with_builder(entry, base_identity=_base_identity(manifest),
                                   watermark=watermark, origin=origin)
    return [build_entry(entry, base_identity=manifest.get("identity", {}) or {},
                        watermark=watermark, origin=origin, index=i)]


def _build_legacy(entries: list[Mapping[str, Any]], *,
                  manifest: Mapping[str, Any],
                  watermark: SqliteWatermarkStore, pool: _Pool,
                  strict: bool) -> tuple[list[_Slot], list[Pipeline]]:
    """Build the legacy entries as v3.76 did: generic ones, then builders.

    A generic entry that fails raises when ``strict`` (the start, as v3.76),
    else logs and builds nothing; a builder that fails logs and is skipped.
    Returns (slots, the pipelines built fresh).
    """
    slots: list[_Slot] = []
    fresh: list[Pipeline] = []
    generic = [e for e in entries if not e.get("builder")]
    for i, entry in enumerate(generic):
        index = _naming_index(entry, i)
        slot = pool.take(LEGACY, index, entry)
        if slot is None:
            try:
                pipes = _build_one(entry, LEGACY, i, manifest=manifest,
                                   watermark=watermark)
            except Exception:
                if strict:
                    _close_quietly(fresh)
                    raise
                logger.exception("daemon: legacy pipeline %s failed to build;"
                                 " skipped", entry.get("name") or f"pipeline-{i}")
                pipes = []
            fresh.extend(pipes)
            slot = _Slot(LEGACY, index, entry, pipes)
        slots.append(slot)
    for entry in entries:
        if not entry.get("builder"):
            continue
        slot = pool.take(LEGACY, None, entry)
        if slot is None:
            spec = str(entry["builder"])
            try:
                pipes = _build_one(entry, LEGACY, 0, manifest=manifest,
                                   watermark=watermark)
            except Exception:
                logger.exception("daemon: builder %s failed for %s", spec,
                                 str(entry.get("name") or spec))
                pipes = []
            fresh.extend(pipes)
            slot = _Slot(LEGACY, None, entry, pipes)
        slots.append(slot)
    return slots, fresh


def _build_file(entries: list[Mapping[str, Any]], origin: str, *,
                manifest: Mapping[str, Any], watermark: SqliteWatermarkStore,
                pool: _Pool) -> tuple[list[_Slot], list[_Slot], list[Pipeline], str]:
    """Build one pipeline file whole, reusing unchanged entries.

    Returns (slots, the slots reused, the pipelines built fresh, "") or, when
    an entry fails, ([], [], [], reason) with the fresh ones closed and the
    reused ones back in the pool.
    """
    slots: list[_Slot] = []
    taken: list[_Slot] = []
    fresh: list[Pipeline] = []
    for i, entry in enumerate(entries):
        index = _naming_index(entry, i)
        slot = pool.take(origin, index, entry)
        if slot is not None:
            taken.append(slot)
            slots.append(slot)
            continue
        label = entry.get("name") or entry.get("builder") or f"entry {i + 1}"
        try:
            pipes = _build_one(entry, origin, i, manifest=manifest,
                               watermark=watermark)
        except Exception as exc:
            _close_quietly(fresh)
            pool.give_back(taken)
            return [], [], [], f"pipeline {label!r} failed to build: {exc!r}"
        fresh.extend(pipes)
        slots.append(_Slot(origin, index, entry, pipes))
    return slots, taken, fresh, ""


def _admit(slots: list[_Slot], names: dict[str, Pipeline],
           keys: dict[tuple, Pipeline]) -> str:
    """Accept one file's pipelines unless one collides; return the reason."""
    f_names, f_keys = dict(names), dict(keys)
    for s in slots:
        for p in s.pipes:
            reason = _collision(p, f_names, f_keys)
            if reason:
                return reason
            f_names[p.name] = p
            f_keys[pipeline_key(p)] = p
    names.update(f_names)
    keys.update(f_keys)
    return ""


def _assemble(layout: Layout, *, watermark: SqliteWatermarkStore,
              previous: Optional[Build] = None, strict: bool = False,
              note: Callable[[tuple], bool] = _always) -> Build:
    """Build ``layout``, reusing every unchanged entry of ``previous``.

    Legacy entries build as v3.76 built them; a collision among them alone
    logs one ERROR naming both and both run (§3.3).  Each pipeline file then
    builds whole or not at all: a failed entry, or a name or key an accepted
    pipeline already holds, refuses the file and names both pipelines.  A
    file refused at a rebuild keeps the pipelines it last built (§3.4 step
    2).  Files whose entries match their last good build go first, so an
    edited file cannot take a name or key from one that did not change.

    Pipelines built here and then refused are closed here.  A previous
    pipeline not reused is the caller's to close: it may still run.
    """
    manifest = layout.manifest
    pool = _Pool(previous.slots if previous else [])
    prev_good = previous.good if previous else {}

    legacy = [e for e, origin in layout.entries if origin == LEGACY]
    legacy_slots, fresh = _build_legacy(legacy, manifest=manifest,
                                        watermark=watermark, pool=pool,
                                        strict=strict)
    names: dict[str, Pipeline] = {}
    keys: dict[tuple, Pipeline] = {}
    for p in (p for s in legacy_slots for p in s.pipes):
        clash = _collision(p, names, keys)
        if clash and note(("legacy-collision", clash)):
            logger.error("daemon: legacy manifest: %s; running both as "
                         "before", clash)
        names.setdefault(p.name, p)
        keys.setdefault(pipeline_key(p), p)

    by_file: dict[str, list[Mapping[str, Any]]] = {}
    for entry, origin in layout.entries:
        if origin != LEGACY:
            by_file.setdefault(origin, []).append(entry)
    load_refused = {r.row_name: r for r in layout.refused}
    origins = sorted(set(by_file) | set(load_refused))

    def unchanged(o: str) -> bool:
        return o in by_file and prev_good.get(o) == by_file[o]

    order = ([o for o in origins if unchanged(o)]
             + [o for o in origins if not unchanged(o)])

    accepted: dict[str, list[_Slot]] = {}
    good: dict[str, list[Mapping[str, Any]]] = {}
    refused: list[Refusal] = []
    for origin in order:
        filename = origin.split("/", 1)[1]
        if origin in load_refused:
            refused.append(load_refused[origin])
        else:
            entries = by_file[origin]
            slots, taken, new, reason = _build_file(
                entries, origin, manifest=manifest, watermark=watermark,
                pool=pool)
            if not reason:
                reason = _admit(slots, names, keys)
                if reason:
                    _close_quietly(new)
                    pool.give_back(taken)
            if not reason:
                accepted[origin] = slots
                good[origin] = entries
                fresh.extend(new)
                continue
            refused.append(Refusal(filename, filename[: -len(".toml")], reason))
        last = prev_good.get(origin)
        if last is None:
            continue
        # §3.4 step 2: a file that now fails keeps what it last built.
        slots, taken, new, reason = _build_file(
            last, origin, manifest=manifest, watermark=watermark, pool=pool)
        if not reason:
            reason = _admit(slots, names, keys)
            if reason:
                _close_quietly(new)
                pool.give_back(taken)
        if reason:
            if note(("last-good", origin, reason)):
                logger.error("daemon: %s: cannot keep its last good build "
                             "either: %s", origin, reason)
            continue
        accepted[origin] = slots
        good[origin] = last
        fresh.extend(new)

    slots = legacy_slots + [s for o in sorted(accepted) for s in accepted[o]]
    pipelines = [p for s in slots for p in s.pipes]

    for r in refused:
        if note(("refused", r.file, _file_stat(layout.signature, r.file),
                 r.reason)):
            logger.error("daemon: refusing %s (client %s): %s",
                         r.row_name, r.client, r.reason)
    fresh_ids = {id(p) for p in fresh}
    for p in pipelines:
        if id(p) in fresh_ids:
            logger.info("daemon: pipeline %s (client %s, origin %s)",
                        p.name, p.client or "(none)", p.origin)
    return Build(pipelines=pipelines, refused=refused, slots=slots, good=good,
                 fresh=[p for p in pipelines if id(p) in fresh_ids])


def build_layout(
    layout: Layout, *, watermark: SqliteWatermarkStore
) -> tuple[list[Pipeline], list[Refusal]]:
    """Build every entry of ``layout``; return (pipelines, every refused file).

    The start's build: legacy entries build as v3.76 built them, and a
    generic legacy entry that fails raises, as it did.  Each pipeline file
    builds whole or not at all (§3.2-§3.3).  The refusals returned include
    ``layout.refused``.
    """
    build = _assemble(layout, watermark=watermark, strict=True)
    return build.pipelines, build.refused


# A pipeline whose source has a ``producer`` reads one client's rows and
# keeps one send record per producer; it needs store version 2 (spec §4.7).
_PRODUCER_STORE_VERSION = 2


def split_refused(
    pipelines: list[Pipeline], store_version: int, *,
    note: Callable[[tuple], bool] = _always,
) -> tuple[list[Pipeline], dict[str, str]]:
    """Return (pipelines that may run, {name: reason} for the refused).

    ``note`` decides whether a refusal logs (the daemon logs each once
    while it recurs across rebuilds).
    """
    runnable: list[Pipeline] = []
    refused: dict[str, str] = {}
    for p in pipelines:
        if (getattr(p.source, "producer", None)
                and store_version < _PRODUCER_STORE_VERSION):
            reason = (f"store is at schema version {store_version}; "
                      f"producer pipelines need version "
                      f"{_PRODUCER_STORE_VERSION} (run `hs-uploader migrate`)")
            if note(("store", p.name, reason)):
                logger.error("daemon: refusing pipeline %s: %s", p.name, reason)
            refused[p.name] = reason
        else:
            runnable.append(p)
    return runnable, refused


# ---- the starting record (spec sink-2b-part2 §3.5) ---------------------------


def write_starting_records(pipelines, store) -> list[str]:
    """Give each governed pipeline whose source starts at now, and whose key
    holds no send record, a starting record at the source's present position
    (spec sink-2b-part2 §3.5).  Return the keys written, as
    ``"source|destination|table"``, in the order of ``pipelines``.

    "Governed" means the gate's rules 1 and 2 do not exempt it
    (``gate.exempt_rule``).  "Starts at now" means the source offers
    ``initial_cursor()`` and it returns bytes.  The record is what the
    source's first pump would anchor at, written insert-only
    (``store.start_cursor``): a key that holds any row, even one another
    process wrote a moment ago, keeps its cursor and ``last_ack`` untouched.

    The caller decides when: the daemon runs this while the marker stands.
    Anything that fails for one pipeline (its key, the gate's rules, its
    source or the store) skips that pipeline with a WARNING naming the
    exception type and text, and the rest are still written; this never
    raises.  The caller runs it at a build or when the marker appears,
    never per pump.
    """
    written: list[str] = []
    for pipe in pipelines:
        name = getattr(pipe, "name", "?")
        try:
            initial = getattr(pipe.source, "initial_cursor", None)
            if not callable(initial) or exempt_rule(pipe):
                continue
            key = pipeline_key(pipe)
            if store.get_cursor(*key):
                continue
            cursor = initial()
            if not cursor:
                continue
            wrote = store.start_cursor(
                *key, cursor=cursor,
                last_ack=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        except Exception as exc:  # noqa: BLE001 — one pipeline never stops the rest
            logger.warning("daemon: no starting record for %s: %s: %s",
                           name, type(exc).__name__, exc)
            continue
        if wrote:
            label = "|".join(key)
            written.append(label)
            logger.info("daemon: starting record for %s (%s) at %r",
                        name, label, cursor)
    return written


# ---- the running build, and the rebuild between pumps (spec sink-2b §3.4-§3.5)


class Rebuilder:
    """The daemon's running set, and the rebuild between pumps.

    The legacy manifest stays as the daemon read it at start (Michael,
    10-21): a rebuild re-reads every file in ``pipelines.d/`` and passes the
    start's copy of the legacy manifest, so its entries, ``[identity]`` and
    host-wide settings never change while the daemon runs.

    Each build runs the store check (§3.5) on a freshly read store version,
    which also sets the version the send log writes for (§4.3), and writes
    ``pipeline_state`` with ``replace=True``: one row per pipeline built,
    and one ``pipelines.d/<file>`` row per refused file.
    """

    def __init__(self, manifest_path: str | Path,
                 manifest: Mapping[str, Any],
                 watermark: SqliteWatermarkStore,
                 *, marker: Optional[Path] = None):
        self.manifest_path = Path(manifest_path)
        self.manifest = manifest
        self.watermark = watermark
        self.pdir = pipelines_dir_for(manifest_path)
        self.sdir = sinks_dir_for(manifest_path)
        self._said: dict[str, tuple] = {}    # client -> (state, enforced) logged
        self._said_no_files = False
        self.signature: Optional[tuple] = None
        self.build = Build()
        self.running: list[Pipeline] = []
        self._rows: list[tuple[str, str, str]] = []
        self._once = _Once()
        # The carry-over marker; None runs no starting-record pass.  The
        # last reading of it, so marker_check() sees it appear (ruling X3;
        # the gate itself keeps no cache, ruling 4).
        self.marker = None if marker is None else Path(marker)
        self._marker_seen = False

    def _starting_pass(self, pipes: list[Pipeline]) -> list[str]:
        """At each build: while the marker stands, write starting records."""
        if self.marker is None:
            return []
        self._marker_seen = marker_stands(self.marker)
        if not self._marker_seen:
            return []
        return write_starting_records(pipes, self.watermark)

    def marker_check(self) -> list[str]:
        """Before each pump: when the marker has appeared since the last
        reading, run the starting-record pass over the running set, so it
        lands before any held pump (plan ruling 4).  Returns the keys written.
        When the marker appears or goes, the switch report runs again, so it
        says once whether the gate now enforces the switches."""
        if self.marker is None:
            return []
        stands = marker_stands(self.marker)
        appeared = stands and not self._marker_seen
        changed = stands != self._marker_seen
        self._marker_seen = stands
        if not appeared:
            if changed:   # it went: the report says "not enforced" once
                self.report_switches()
            return []
        logger.info("daemon: carry-over marker %s appeared", self.marker)
        written = write_starting_records(self.running, self.watermark)
        self.report_switches()   # now "enforced", once per client
        return written

    def due(self) -> bool:
        """Whether ``pipelines.d/`` moved since the last build (ruling X5)."""
        return pipelines_signature(self.pdir) != self.signature

    def _make(self, previous: Optional[Build], *, strict: bool):
        """Build; return (signature, build, running, rows).

        Commits nothing: the caller records the signature only once the
        whole rebuild has succeeded, so a failure leaves ``due()`` true and
        the next pump tries again (§3.4: "differs from the one it last
        built").  On a failure after the build, the pipelines it made new
        are closed here (none of them ever pumped).
        """
        self._once.begin()
        build: Optional[Build] = None
        try:
            layout = load_layout(self.manifest_path, manifest=self.manifest,
                                 note=self._once)
            build = _assemble(layout, watermark=self.watermark,
                              previous=previous, strict=strict,
                              note=self._once)
            running, store_refused = split_refused(
                build.pipelines, self.watermark.refresh_schema_version(),
                note=self._once)
        except BaseException:
            if build is not None:
                _close_quietly(build.fresh)
            raise
        finally:
            self._once.end()
        rows = ([(p.name, p.client, store_refused.get(p.name, ""))
                 for p in build.pipelines]
                + [(r.row_name, r.client, r.reason) for r in build.refused])
        return layout.signature, build, running, rows

    def report_switches(self) -> None:
        """Log what each governed client's switch reads, and whether the
        gate enforces it (spec §9.2).

        The gate enforces the switches while the carry-over marker stands
        (rule 3).  One INFO line per client when its answer or the
        enforcement changes; one line when ``sinks/`` is missing and the
        marker does not stand.  With the marker standing, a missing
        ``sinks/`` is a fault, and the gate reports it (ERROR, then a
        reminder every 6 h).  ``effective_state`` logs each file error once
        per signature.  The gate itself logs each pipeline's transitions.
        """
        enforced = self.marker is not None and marker_stands(self.marker)
        if not self.sdir.is_dir():
            if not self._said_no_files and not enforced:
                logger.info("no switch files; the manifest's policy governs")
                self._said_no_files = True
            self._said.clear()
            return
        self._said_no_files = False
        clients = sorted({p.client for p in self.build.pipelines
                          if p.client and p.governed})
        for client in clients:
            state = effective_state(client, sinks_dir=self.sdir).state
            answer = (state, enforced)
            if self._said.get(client) != answer:
                self._said[client] = answer
                if enforced:
                    logger.info("sink switch for %s reads %s (enforced)",
                                client, state)
                else:
                    logger.info("sink switch for %s reads %s (not enforced: "
                                "no carry-over marker)", client, state)

    def start(self) -> tuple[list[Pipeline], list[Pipeline], list[Refusal]]:
        """The start's build: (running, every pipeline built, refused files)."""
        signature, build, running, rows = self._make(None, strict=True)
        self.watermark.record_pipeline_states(rows, replace=True)
        self.build, self.running, self._rows = build, running, rows
        self.signature = signature
        self._starting_pass(running)
        self.report_switches()
        return running, build.pipelines, build.refused

    def rebuild(self, uploader: Uploader) -> bool:
        """Rebuild between pumps; return whether the running set changed.

        Never call it during a pump: ``Uploader.pump()`` joins every
        pipeline's future before it returns.  When it raises, the running
        set, the build and the signature all stay as they were.
        """
        previous = self.build
        signature, build, running, rows = self._make(previous, strict=False)
        before = [id(p) for p in self.running]
        changed = not (rows == self._rows
                       and [id(p) for p in running] == before)
        if changed:
            try:
                self.watermark.record_pipeline_states(rows, replace=True)
            except BaseException:
                _close_quietly(build.fresh)
                raise
        self.build = build
        self.signature = signature
        self._starting_pass(running)
        self.report_switches()
        pumped = {id(p) for p in uploader.pipelines}
        kept = {id(p) for p in build.pipelines}
        # A previous pipeline the store check held back never pumped: close
        # it here.  One that pumped closes on its own thread, below.
        _close_quietly([p for p in previous.pipelines
                        if id(p) not in kept and id(p) not in pumped])
        if not changed:
            logger.debug("daemon: rebuild of %s changed nothing "
                         "(%d pipeline(s) running)", self.pdir, len(running))
            return False
        self._rows = rows
        uploader.replace_pipelines(running)
        now = {id(p) for p in running}
        added = [p.name for p in running if id(p) not in set(before)]
        removed = [p.name for p in self.running if id(p) not in now]
        self.running = running
        logger.info("daemon: rebuilt from %s: %d pipeline(s) running; "
                    "started %s; stopped %s", self.pdir, len(running),
                    ", ".join(added) or "none", ", ".join(removed) or "none")
        return True


def _needs_wake(pipes: list[Pipeline]) -> bool:
    """Whether any pipeline reads the sink, whose producers fire wakes."""
    from .sources.sqlite import SqliteSource
    try:
        from .sources.wspr_cycle import WsprCycleSource
        sink_types: tuple = (SqliteSource, WsprCycleSource)
    except Exception:
        sink_types = (SqliteSource,)
    return any(isinstance(p.source, sink_types) for p in pipes)


def _new_uploader(pipelines: list[Pipeline], **kw) -> Uploader:
    """The daemon's Uploader: every pipeline on its own thread, even a lone
    one, since a rebuild may add a second (a pipeline never moves thread)."""
    return Uploader(pipelines, inline_single=False, **kw)


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

    # SIGHUP asks for a rebuild.  The handler only sets flags (logging
    # inside a signal handler can raise a reentrant-call error and lose the
    # signal), and it goes in before the start's build, so a SIGHUP during
    # the build no longer kills the daemon; the loop rebuilds once it runs.
    stop = threading.Event()
    wake = threading.Event()
    hangup = threading.Event()
    if not (dry_run or once):
        def _hangup(_sig, _frame):
            hangup.set()
            wake.set()

        signal.signal(signal.SIGHUP, _hangup)

    # pipelines.d/ never stops the daemon: a bad file becomes a refusal.
    # start() writes pipeline_state, refusal rows included, before any early
    # return, so a refusal always leaves its reason (ruling C15).
    watermark = SqliteWatermarkStore(default_path())
    rebuilder = Rebuilder(manifest_path, manifest, watermark,
                          marker=marker_path())
    # ONE gate for the daemon's life (spec §3.2): every rebuild keeps the
    # Uploader, and with it this gate, so each pipeline's last decision and
    # the once-only lines survive a rebuild.  It reads the same sinks/ and
    # marker as the Rebuilder, `sink status` and the writer.
    gate = SinkGate(rebuilder.sdir, rebuilder.marker)
    pipelines, built, file_refused = rebuilder.start()
    if not pipelines:
        if file_refused:
            # A refused file must not stop the daemon (spec §3.2, §13): exit
            # here and systemd restarts it in a loop.  Run idle instead, so a
            # rebuild can pick up the corrected file.
            logger.error("daemon: no pipeline can run (%d pipelines.d file(s) "
                         "refused: %s); running idle until a file changes",
                         len(file_refused),
                         ", ".join(r.row_name for r in file_refused))
        elif not built:
            logger.error("daemon: no pipelines built from %s — nothing to do",
                         manifest_path)
            return 1
        else:
            logger.error("daemon: every pipeline refused — nothing to do")
            return 1

    logger.info("daemon: %d pipeline(s): %s",
                len(pipelines), ", ".join(p.name for p in pipelines))

    uploader = _new_uploader(pipelines, gate=gate.may_send)

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
    wake_cfg = dcfg.get("wake")

    listener: Optional[WakeListener] = None

    def bind_wake_if_needed(pipes: list[Pipeline]) -> None:
        # Only bind the shared upload-wake socket when at least one pipeline
        # reads from the sink (SqliteSource/WsprCycleSource) — those are the
        # producers that fire wakes.  A file-spool-only daemon (e.g. GRAPE
        # during migration) must NOT bind it, or it steals the socket from a
        # recorder's still-running in-process uploader.  Overridable via
        # [daemon] wake = true/false.  A rebuild binds it when the new set
        # first needs it, and nothing unbinds it while running (§3.4 step 6).
        nonlocal listener
        if listener is not None:
            return
        if not (_needs_wake(pipes) if wake_cfg is None else bool(wake_cfg)):
            return
        listener = WakeListener(on_wake=wake.set, path=wake_sock)
        listener.start()  # best-effort; polling backstop covers a bind failure

    bind_wake_if_needed(pipelines)
    if listener is None:
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

    hup_pending = False
    last_failure: Optional[tuple[str, str]] = None
    while not stop.is_set():
        wake.wait(interval)
        wake.clear()
        if stop.is_set():
            break
        # Between pumps only (§3.4): pump() has joined every future.  A
        # failed rebuild leaves due() true, so it retries every pump; its
        # ERROR logs once while the same failure recurs.
        try:
            if hangup.is_set():
                hangup.clear()
                logger.info("daemon: SIGHUP — rebuilding from %s",
                            rebuilder.pdir)
                hup_pending = True
            if hup_pending or rebuilder.due():
                changed = rebuilder.rebuild(uploader)
                hup_pending = False
                last_failure = None
                if changed:
                    bind_wake_if_needed(rebuilder.running)
        except Exception as exc:
            failure = (type(exc).__name__, str(exc))
            if failure != last_failure:
                logger.exception("daemon: rebuild failed; the running set "
                                 "stays, and the next pump tries again")
            last_failure = failure
        # The marker may have appeared since the last pump: the starting
        # records land before any held pump (spec §3.5, ruling X3).
        try:
            rebuilder.marker_check()
        except Exception:
            logger.exception("daemon: starting-record pass failed")
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
