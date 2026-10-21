"""Load ``pipelines.d/<client>.toml`` beside the legacy manifest (spec sink-2b §3.1-§3.3).

The daemon derives two directories from the legacy manifest's own path:
``pipelines.d/`` (pipeline files, one per client) and ``sinks/`` (switch
files, :mod:`hs_uploader.sinkswitch`).  ``--manifest /etc/hs-uploader/pipelines.toml``
gives ``/etc/hs-uploader/pipelines.d/`` and ``/etc/hs-uploader/sinks/``.

This module reads and checks the files; it builds nothing.
:func:`hs_uploader.daemon.build_layout` builds the entries and refuses a file
that fails to build or brings a collision.

The rules:

* Only names ending in ``.toml`` and not starting with a dot load, so an
  editor's backup (``x.toml~``) or a side file (``x.toml.new``) never does.
* The file's name gives the client, which must pass
  :func:`hs_uploader.sinkswitch.valid_client_name` (``site`` stays reserved).
* Once ``pipelines.d/<client>.toml`` exists, parsed or not, every legacy entry
  whose ``client`` reads that name drops out.  A broken file therefore
  silences its client rather than reviving stale legacy entries.
* A file is accepted or refused whole.  A file refused at start sends
  nothing.  At a rebuild, a file that now fails keeps the pipelines it last
  built.  A refused file never stops the daemon.
"""

from __future__ import annotations

import logging
import os
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .sinkswitch import valid_client_name

logger = logging.getLogger(__name__)

PIPELINES_DIR = "pipelines.d"
SINKS_DIR = "sinks"
LEGACY = "legacy"


def pipelines_dir_for(manifest_path: str | Path) -> Path:
    """The ``pipelines.d/`` directory beside the legacy manifest."""
    return Path(manifest_path).parent / PIPELINES_DIR


def sinks_dir_for(manifest_path: str | Path) -> Path:
    """The ``sinks/`` (switch-file) directory beside the legacy manifest."""
    return Path(manifest_path).parent / SINKS_DIR


def origin_for(filename: str) -> str:
    """The origin a file's pipelines carry, and its ``pipeline_state`` row name."""
    return f"{PIPELINES_DIR}/{filename}"


def is_pipeline_file(filename: str) -> bool:
    return filename.endswith(".toml") and not filename.startswith(".")


def file_client(filename: str) -> Optional[str]:
    """The client a pipeline file names, or ``None`` when the name is illegal."""
    if not is_pipeline_file(filename):
        return None
    stem = filename[: -len(".toml")]
    return stem if valid_client_name(stem) else None


@dataclass
class Refusal:
    """A pipeline file the daemon refused whole, and why."""

    file: str      # the bare file name inside pipelines.d/
    client: str    # the client the file's name gives (even an illegal one)
    reason: str

    @property
    def row_name(self) -> str:
        """The ``pipeline_state`` row that records this refusal."""
        return origin_for(self.file)


@dataclass
class Layout:
    """What the daemon will try to build.

    ``entries`` pairs each entry with its origin, ``"legacy"`` or
    ``"pipelines.d/<file>"``: the legacy entries first, in manifest order,
    less those a present file silences; then each accepted file's entries,
    files in name order.  ``signature`` was taken before anything was read.
    """

    manifest: Mapping[str, Any]
    entries: list[tuple[Mapping[str, Any], str]] = field(default_factory=list)
    refused: list[Refusal] = field(default_factory=list)
    signature: tuple = ()


# ---- the signature (ruling X5: one function, used at start and before each pump)


def _stat_key(st: os.stat_result) -> tuple:
    return (st.st_mtime_ns, st.st_size, st.st_ino)


def pipelines_signature(directory: str | Path) -> tuple:
    """Stat the directory and every entry in it; ``()`` when it is missing.

    Two calls return equal tuples exactly when nothing in the directory moved
    (modification time in ns, size, inode), so the daemon can compare the
    signature it built from with a fresh one before each pump.
    """
    d = Path(directory)
    try:
        top = os.stat(d)
        names = sorted(os.listdir(d))
    except FileNotFoundError:
        return ()
    except OSError as exc:
        return (("error", type(exc).__name__),)
    files = []
    for name in names:
        try:
            files.append((name, _stat_key(os.stat(d / name))))
        except OSError:
            files.append((name, None))
    return (_stat_key(top), tuple(files))


# ---- reading ------------------------------------------------------------------


def _read_toml(path: Path) -> Mapping[str, Any]:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _start_at_problem(source: Mapping[str, Any]) -> str:
    """Why a file entry's ``start_at`` cannot stand, or "" (spec §3.2).

    ``start_at = "now"`` never may.  A ``wspr_cycle`` source reads every value
    except ``"beginning"`` as now (``WsprCycleSource._effective_cursor``; it
    parses no datetime), so in a file it may name only ``"beginning"``, or
    nothing, which defaults to it.
    """
    if "start_at" not in source:
        return ""
    value = source["start_at"]
    if value == "now":
        return 'start_at = "now" is not allowed in a pipeline file'
    if (str(source.get("type", "")).strip().lower() == "wspr_cycle"
            and value != "beginning"):
        return (f"start_at = {value!r}: a wspr_cycle source reads it as now; "
                f'a pipeline file may name only "beginning"')
    return ""


def _always(_key: tuple) -> bool:
    return True


def _check_file(filename: str, data: Mapping[str, Any],
                note: Callable[[tuple], bool] = _always,
                ) -> tuple[list[Mapping[str, Any]], str]:
    """Return (entries with ``client`` set, "") or ([], reason)."""
    client = filename[: -len(".toml")]
    if "daemon" in data:
        return [], "a [daemon] table is host-wide; it belongs in the legacy manifest"
    ident = data.get("identity")
    if ident is not None:
        if not isinstance(ident, Mapping):
            return [], "identity must be a table"
        if "ssh_key_file" in ident:
            return [], ("[identity] names ssh_key_file, a host-wide setting; "
                        "it belongs in the legacy manifest")
        if ident and note(("identity", filename, tuple(sorted(ident)))):
            logger.info("pipelines.d: %s: ignoring [identity] tables (%s); "
                        "nothing reads per-instance identity yet",
                        filename, ", ".join(sorted(ident)))
    raw = data.get("pipeline", [])
    if not isinstance(raw, list) or not all(isinstance(e, Mapping) for e in raw):
        return [], "pipeline must be an array of tables ([[pipeline]])"
    entries: list[Mapping[str, Any]] = []
    for i, entry in enumerate(raw):
        label = entry.get("name") or f"entry {i + 1}"
        named = entry.get("client")
        if named is not None and named != client:
            return [], (f"pipeline {label!r} names client {named!r}; "
                        f"the file's name gives {client!r}")
        governed = entry.get("governed", True)
        if not isinstance(governed, bool):
            return [], f"pipeline {label!r}: governed must be true or false"
        source = entry.get("source")
        if isinstance(source, Mapping):
            reason = _start_at_problem(source)
            if reason:
                return [], f"pipeline {label!r}: {reason}"
        entries.append({**entry, "client": client})
    return entries, ""


def load_layout(manifest_path: str | Path, *,
                manifest: Optional[Mapping[str, Any]] = None,
                note: Callable[[tuple], bool] = _always) -> Layout:
    """Read the legacy manifest and ``pipelines.d/``; apply precedence and the file rules.

    The legacy manifest stays required: a missing or broken one raises, as
    today.  ``manifest`` passes one already read (the daemon reads it first so
    its exit codes stay as they were; a rebuild passes the copy read at start,
    since the daemon never re-reads the legacy manifest).  Nothing in
    ``pipelines.d/`` ever raises: each bad file becomes a :class:`Refusal`.
    ``note`` decides whether a keyed INFO line logs; the daemon passes one
    that logs each once while it recurs across rebuilds.
    """
    pdir = pipelines_dir_for(manifest_path)
    signature = pipelines_signature(pdir)          # before anything is read (X5)
    if manifest is None:
        manifest = _read_toml(Path(manifest_path))

    try:
        names = sorted(n for n in os.listdir(pdir) if is_pipeline_file(n))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        logger.error("pipelines.d: cannot list %s: %s", pdir, exc)
        names = []

    present = {n[: -len(".toml")] for n in names}   # "exists": parsed or not
    legacy = [(e, LEGACY) for e in (manifest.get("pipeline", []) or [])
              if str(e.get("client") or "") not in present]

    files: list[tuple[Mapping[str, Any], str]] = []
    refused: list[Refusal] = []
    for name in names:
        client = name[: -len(".toml")]
        if file_client(name) is None:
            refused.append(Refusal(name, client,
                                   f"{client!r} is not a legal client name "
                                   f"([a-z0-9][a-z0-9-]*, not 'site')"))
            continue
        try:
            if not stat.S_ISREG(os.stat(pdir / name).st_mode):
                refused.append(Refusal(name, client, "not a regular file"))
                continue
            data = _read_toml(pdir / name)
        except Exception as exc:  # parse error, unreadable, a directory ...
            refused.append(Refusal(name, client, f"cannot read: {exc}"))
            continue
        entries, reason = _check_file(name, data, note)
        if reason:
            refused.append(Refusal(name, client, reason))
            continue
        files.extend((e, origin_for(name)) for e in entries)

    return Layout(manifest=manifest, entries=legacy + files,
                  refused=refused, signature=signature)
