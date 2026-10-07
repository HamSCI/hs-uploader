#!/usr/bin/env python3
"""neutrality_check: show that two hs-uploader trees would send the same thing.

v3.70 promises to change nothing a station sends (sigmond
tasks/plan-sink-control.md §10.4).  This tool tests that promise on copies
of a station's state.  It builds every pipeline in a pipelines.toml twice,
once with an OLD hs-uploader source tree and once with a NEW one, each in
its own Python process.  For each pipeline it reports:

  * the send-record key the tree's core reads: (source_id, dest_id, table);
  * the send record (cursor) stored under that key;
  * how many queued retries wait under the pipeline's name;
  * the next batch the source hands its transport: the number of records,
    the first and last pending_uploads row id when the rows carry one, the
    cursor the batch would store, and a digest of the records.

It then says whether OLD and NEW agree.

Usage, from an hs-uploader checkout:

    .venv/bin/python tools/neutrality_check.py \\
        --manifest pipelines.toml --sink sink.db --watermarks watermarks.db \\
        --old OLD/src --new NEW/src [--now 2026-10-07T15:51:00Z]

--old and --new each name a directory that holds the hs_uploader package,
for example the src/ of `git archive <sha> | tar -x -C DIR`.

Nothing leaves the host, and nothing writes to the given files:

  * each tree reads its own copy of sink.db and watermarks.db, made in a
    temporary directory (set TMPDIR to choose where);
  * a recorder replaces every transport.  It borrows the real transport's
    name and table, so the tree forms its real key.  It captures the first
    batch and stops the pump before any send, cursor advance, retry or
    commit;
  * the check counts queued retries and never replays them;
  * the tool hashes the given files before and after the run, and fails
    if anything changed them.

Filetree pipelines (GRAPE, the magnetometer, the heartbeat) read their
spool directories on the machine that runs the check.  A dev box has none
of those directories, so there the check compares only their keys.

--now freezes the clock that the wspr_cycle and sqlite sources consult, so
both trees judge at the same instant which WSPR cycles have closed.  It
defaults to the current UTC time, read once and handed to both trees.
Pass the snapshot's time to judge cycles as the station saw them.

Exit status: 0 when every pipeline agrees; 1 when any pipeline disagrees,
a tree fails to build, or a given file changed; 2 on a usage error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# The fields of one pipeline's result that OLD and NEW must share.
_FIELDS = ("key", "stored", "queued", "batch", "error")

# Modules whose clock --now freezes.  Transports also read the clock, but
# the recorder stops every pump before a transport runs.
_CLOCK_MODULES = (
    "hs_uploader.sources.wspr_cycle",
    "hs_uploader.sources.wspr_completion",
    "hs_uploader.sources.sqlite",
)


# ---- worker: runs inside one tree's own process ------------------------------


class _Captured(BaseException):
    """The recorder raises this at the first send.  It stops the pump before
    the tree advances a cursor, queues a retry or commits a batch.  It
    derives from BaseException so that no `except Exception` in the tree
    swallows it."""


class _Recorder:
    """Stands in for a pipeline's transport.  It borrows the real transport's
    name, table, ACCEPTS and batch policy, so the tree forms the key it
    forms in production.  It keeps the first batch and stops the pump."""

    def __init__(self, inner):
        self.inner = inner
        self.name = inner.name
        self.ACCEPTS = getattr(inner, "ACCEPTS", {})
        self.batch = None

    def primary_table(self):
        return self.inner.primary_table()

    def batch_policy(self):
        return self.inner.batch_policy()

    def ship(self, batch, identity):
        self.batch = batch
        raise _Captured()

    def serialize_for_retry(self, batch, identity):
        raise _Captured()

    def replay(self, payload_blob, identity):
        raise _Captured()


def _text(cursor) -> str | None:
    if cursor is None:
        return None
    if isinstance(cursor, (bytes, bytearray, memoryview)):
        return bytes(cursor).decode("ascii", "backslashreplace")
    return str(cursor)


def _describe(batch, ids: list) -> dict:
    records = [
        [r.table, r.time.isoformat(), dict(r.columns),
         str(r.payload_path) if r.payload_path else None]
        for r in batch.records
    ]
    blob = json.dumps(records, sort_keys=True, default=str).encode("utf-8")
    return {
        "records": len(records),
        "first_id": ids[0] if ids else None,
        "last_id": ids[-1] if ids else None,
        "cursor_after": _text(batch.cursor_after),
        "digest": hashlib.sha256(blob).hexdigest(),
    }


def _tap_sqlite(sqlite3, sink: Path, ids: list) -> None:
    """Record the pending_uploads ids that each read of the sink copy returns.

    SqliteSource puts no row id on its Records, so the check reads the ids
    where the source reads them: from every query on the sink copy whose
    first result column is `id`.  Connections to any other file stay
    untouched."""
    real_connect = sqlite3.connect
    target = os.path.realpath(sink)

    class _Rows:
        def __init__(self, rows, description):
            self._rows = list(rows)
            self.description = description

        def fetchall(self):
            rows, self._rows = self._rows, []
            return rows

        def fetchone(self):
            return self._rows.pop(0) if self._rows else None

        def fetchmany(self, size=1):
            rows, self._rows = self._rows[:size], self._rows[size:]
            return rows

        def __iter__(self):
            return iter(self.fetchall())

    class _TapConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            cur = super().execute(sql, parameters)
            if cur.description and cur.description[0][0] == "id":
                rows = cur.fetchall()
                ids.extend(row[0] for row in rows)
                return _Rows(rows, cur.description)
            return cur

    def connect(database, *args, **kwargs):
        if "factory" not in kwargs and os.path.realpath(str(database)) == target:
            kwargs["factory"] = _TapConnection
        return real_connect(database, *args, **kwargs)

    sqlite3.connect = connect


def _freeze_clock(now: datetime) -> None:
    import datetime as dt
    import importlib

    class _FrozenDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return now.astimezone().replace(tzinfo=None)
            return now.astimezone(tz)

        @classmethod
        def utcnow(cls):
            return now.astimezone(dt.timezone.utc).replace(tzinfo=None)

    for name in _CLOCK_MODULES:
        try:
            mod = importlib.import_module(name)
        except ImportError:
            continue
        if getattr(mod, "datetime", None) is dt.datetime:
            mod.datetime = _FrozenDatetime


def _worker(tree: Path, manifest_path: Path, sink: Path, watermarks: Path,
            now_iso: str, out: Path) -> int:
    import logging
    import sqlite3

    logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                        format="%(levelname)s:%(name)s:%(message)s")
    sys.path.insert(0, str(tree))
    import hs_uploader

    pkg = Path(hs_uploader.__file__).resolve().parent
    if pkg.parent != tree.resolve():
        print(f"imported hs_uploader from {pkg}, not from {tree}", file=sys.stderr)
        return 3

    ids: list = []
    _tap_sqlite(sqlite3, sink, ids)
    now = _parse_now(now_iso)
    _freeze_clock(now)

    from hs_uploader.core import Uploader
    from hs_uploader.daemon import build_all_pipelines, load_manifest
    from hs_uploader.watermark.sqlite import SqliteWatermarkStore

    class _TapStore(SqliteWatermarkStore):
        """The tree's own store, opened on the copy.  It records each key the
        tree's core reads.  It hides queued retries from the pump, so the
        pump reaches the source; the check counts them instead."""

        def __init__(self, path):
            super().__init__(path)
            self.reads: list = []

        def get_cursor(self, source_id, dest_id, table):
            cursor = super().get_cursor(source_id, dest_id, table)
            self.reads.append(((source_id, dest_id, table), cursor))
            return cursor

        def pop_due_deliverable(self, pipeline, *, now):
            return None

        def deliverable_count(self, pipeline=None):
            return 0

        def queued(self, pipeline):
            return super().deliverable_count(pipeline)

    manifest = load_manifest(manifest_path)
    entries = manifest.get("pipeline", []) or []
    for entry in entries:
        source = entry.get("source") or {}
        if str(source.get("type", "")).strip().lower() == "wspr_cycle":
            source["db_path"] = str(sink)    # SqliteSource reads SIGMOND_SQLITE_PATH
    named = [str(e["name"]) for e in entries if e.get("name")]

    store = _TapStore(watermarks)
    result: dict = {"tree": str(pkg), "pipelines": {}, "unbuilt": []}
    try:
        pipelines = build_all_pipelines(manifest, watermark=store)
    except Exception as exc:  # noqa: BLE001 - reported, never hidden
        result["build_error"] = f"{type(exc).__name__}: {exc}"
        out.write_text(json.dumps(result))
        return 0

    built = [p.name for p in pipelines]
    result["unbuilt"] = [n for n in named if n not in built]
    for pipe in pipelines:
        label, n = pipe.name, 2
        while label in result["pipelines"]:
            label, n = f"{pipe.name}#{n}", n + 1
        recorder = _Recorder(pipe.transport)
        pipe.transport = recorder
        store.reads.clear()
        ids.clear()
        entry = {"key": None, "stored": None, "queued": None,
                 "batch": None, "error": None}
        try:
            entry["queued"] = store.queued(pipe.name)
            uploader = Uploader([pipe], now_fn=now.timestamp)
            try:
                uploader.pump()
            except _Captured:
                pass
            finally:
                uploader.close()
        except Exception as exc:  # noqa: BLE001 - reported per pipeline
            entry["error"] = f"{type(exc).__name__}: {exc}"
        if store.reads:
            key, cursor = store.reads[0]
            entry["key"] = list(key)
            entry["stored"] = _text(cursor)
        if recorder.batch is not None:
            entry["batch"] = _describe(recorder.batch, list(ids))
        result["pipelines"][label] = entry
    out.write_text(json.dumps(result))
    return 0


# ---- parent: copy, run both trees, compare ------------------------------------


def _parse_now(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError(f"--now needs a UTC offset or a trailing Z: {text!r}")
    return value.astimezone(timezone.utc)


def _with_wal(path: Path) -> list:
    wal = Path(f"{path}-wal")
    return [path, wal] if wal.exists() else [path]


def _fingerprint(paths) -> dict:
    prints = {}
    for p in paths:
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        prints[str(p)] = h.hexdigest()
    return prints


def _copy_db(src: Path, dst: Path) -> None:
    """Copy a database and any -wal beside it.  The copy carries pages not
    yet checkpointed; SQLite recovers them when the worker opens it."""
    shutil.copyfile(src, dst)
    wal = Path(f"{src}-wal")
    if wal.exists():
        shutil.copyfile(wal, Path(f"{dst}-wal"))


def _run_tree(label: str, tree: Path, args, now_iso: str, scratch: Path) -> dict:
    work = scratch / label.lower()
    work.mkdir()
    sink, watermarks = work / "sink.db", work / "watermarks.db"
    _copy_db(args.sink, sink)
    _copy_db(args.watermarks, watermarks)
    out = work / "result.json"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["HS_UPLOADER_STATE_DIR"] = str(work)
    env["SIGMOND_SQLITE_PATH"] = str(sink)
    cmd = [sys.executable, "-B", str(Path(__file__).resolve()),
           "--worker", str(tree), "--manifest", str(args.manifest),
           "--sink", str(sink), "--watermarks", str(watermarks),
           "--now", now_iso, "--out", str(out)]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True,
                          timeout=1800)
    for line in (proc.stdout + proc.stderr).splitlines():
        print(f"[{label}] {line}", file=sys.stderr)
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(f"the {label} tree's worker exited {proc.returncode}")
    return json.loads(out.read_text())


def _key_text(entry) -> str:
    if entry is None:
        return "(not built)"
    if entry["key"] is None:
        return "(no key read)"
    return "(" + ", ".join(entry["key"]) + ")"


def _next_text(entry) -> str:
    if entry is None:
        return "(not built)"
    if entry["error"]:
        return f"error {entry['error']}"
    stored = entry["stored"] or "(empty)"
    queued = f"{entry['queued']} queued"
    batch = entry["batch"]
    if batch is None:
        return f"no batch; stored cursor {stored}; {queued}"
    n = batch["records"]
    rows = ""
    if batch["first_id"] is not None:
        rows = f"rows {batch['first_id']}..{batch['last_id']}, "
    return (f"{n} record{'' if n == 1 else 's'}, {rows}"
            f"cursor {stored} -> {batch['cursor_after']}, "
            f"digest {batch['digest'][:12]}; {queued}")


def _agree(old, new) -> bool:
    if old is None or new is None:
        return False
    return all(old.get(f) == new.get(f) for f in _FIELDS)


def _report(old: dict, new: dict) -> int:
    names = list(old["pipelines"])
    names += [n for n in new["pipelines"] if n not in names]
    agree = 0
    for name in names:
        o, n = old["pipelines"].get(name), new["pipelines"].get(name)
        same = _agree(o, n)
        agree += same
        print(f"{name}: {'AGREE' if same else 'DISAGREE'}")
        print(f"  OLD key  {_key_text(o)}")
        print(f"  NEW key  {_key_text(n)}")
        print(f"  OLD next {_next_text(o)}")
        print(f"  NEW next {_next_text(n)}")
    for label, res in (("OLD", old), ("NEW", new)):
        if res["unbuilt"]:
            print(f"{label} built no pipeline for: {', '.join(res['unbuilt'])}")
    raised = [n for n in names
              if any((res["pipelines"].get(n) or {}).get("error") for res in (old, new))]
    if raised:
        print(f"An error stopped these pipelines, so the check shows nothing "
              f"of what they send: {', '.join(raised)}")
    print(f"{len(names)} pipelines: {agree} agree, {len(names) - agree} disagree")
    ok = agree == len(names) and old["unbuilt"] == new["unbuilt"]
    note = f" ({len(raised)} raised an error; see above)" if raised else ""
    print(f"RESULT: {'AGREE' if ok else 'DISAGREE'}{note}")
    return 0 if ok else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="neutrality_check",
        description="Show whether two hs-uploader trees form the same "
                    "send-record keys and select the same next batch, on "
                    "copies of a station's pipelines.toml, sink.db and "
                    "watermarks.db.")
    p.add_argument("--manifest", "--pipelines", dest="manifest", type=Path,
                   required=True,
                   help="the station's /etc/hs-uploader/pipelines.toml (a copy)")
    p.add_argument("--sink", type=Path, required=True,
                   help="a snapshot of /var/lib/sigmond/sink.db")
    p.add_argument("--watermarks", type=Path, required=True,
                   help="a snapshot of /var/lib/hs-uploader/watermarks.db")
    p.add_argument("--old", type=Path, help="directory that holds the OLD hs_uploader")
    p.add_argument("--new", type=Path, help="directory that holds the NEW hs_uploader")
    p.add_argument("--now", help="UTC time both trees read as now "
                                 "(default: the current time, read once)")
    p.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--out", type=Path, help=argparse.SUPPRESS)
    args = p.parse_args(argv)

    if args.worker is not None:
        return _worker(args.worker, args.manifest, args.sink, args.watermarks,
                       args.now, args.out)

    for flag in ("old", "new"):
        tree = getattr(args, flag)
        if tree is None:
            p.error(f"--{flag} is required")
        if not (tree / "hs_uploader" / "__init__.py").is_file():
            p.error(f"--{flag} {tree} holds no hs_uploader package")
    for flag in ("manifest", "sink", "watermarks"):
        if not getattr(args, flag).is_file():
            p.error(f"--{flag} {getattr(args, flag)} is not a file")
    try:
        now = _parse_now(args.now) if args.now else datetime.now(timezone.utc)
    except ValueError as exc:
        p.error(str(exc))
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    given = [args.manifest, *_with_wal(args.sink), *_with_wal(args.watermarks)]
    before = _fingerprint(given)
    print(f"neutrality check: OLD {args.old}  NEW {args.new}  now {now_iso}")
    try:
        with tempfile.TemporaryDirectory(prefix="neutrality-") as scratch:
            old = _run_tree("OLD", args.old, args, now_iso, Path(scratch))
            new = _run_tree("NEW", args.new, args, now_iso, Path(scratch))
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
        print(f"ERROR: {exc}")
        return 1
    if _fingerprint(given) != before:
        print("ERROR: a given file changed while the check ran.  Run the "
              "check on a snapshot, never on a live database.")
        return 1
    failed = False
    for label, res in (("OLD", old), ("NEW", new)):
        if "build_error" in res:
            print(f"ERROR: the {label} tree could not build the pipelines: "
                  f"{res['build_error']}")
            failed = True
    if failed:
        return 1
    return _report(old, new)


if __name__ == "__main__":
    sys.exit(main())
