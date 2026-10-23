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

It then says whether OLD and NEW agree.  The comparison covers the record
digest (table, time, columns, payload path and dedup key of every record, in
order, plus the batch's commit token), the cursor, the send record stored
under the key, the queued retries and the batch limits the core applies.

Usage, from an hs-uploader checkout:

    .venv/bin/python tools/neutrality_check.py \\
        --manifest pipelines.toml --sink sink.db --watermarks watermarks.db \\
        --old OLD/src --new NEW/src [--now 2026-10-07T15:51:00Z] \\
        [--sinks sinks/ --marker carry-over-v1.done]

--old and --new each name a directory that holds the hs_uploader package,
for example the src/ of `git archive <sha> | tar -x -C DIR`.

--sinks and --marker give NEW the station's switch files and carry-over
marker (sink-2b part 2, spec §11.2); OLD never sees them.  The worker
builds each Uploader with the tree's own SinkGate when the tree has one,
and passes the same paths to the tree's build (the psk rider gate).  A
tree without a gate (part 1 or older) runs as it always ran.  While the
marker stands, NEW's starting-record pass runs before any pump, as the
daemon runs it.  The tool diffs the store copy's send records around the
pass itself: it names each key the pass added, and a record that existed
and changed ends that pipeline, and the run, DISAGREE.

Once the marker stands, a pipeline the tool cannot find in its own read of
the manifest (a builder entry), or one of several entries sharing a name
whose build order the tool cannot confirm, ends INCONCLUSIVE: the tool
never borrows NEW's own idea of what governs it.

The tool predicts which pipelines the switches hold with its own small
reader (tomllib; the stricter of the client's file and site.toml; a
missing client file, a bad name, no sinks/, or any read, parse or state
fault reads off; a missing site.toml sets no limit), never through
hs_uploader's (Ruling 15).  For a WSPR tar that carries psk rows it works
out NEW's expected batch: OLD's batch less each psk row whose producer
(the row's column, else the tool's own copy of D12's rule) does not read
upload.  Each pipeline then ends in one of three verdicts:

  AGREE      both trees produced the same key, record, retries, limits and
             batch (for a WSPR tar with psk rows, NEW's batch is the
             expected one);
  HELD       the tool's reader says the switches hold it, and NEW read no
             send record, produced no batch, sent nothing, asked for no
             retry and changed neither its store copy nor its sink copy;
  DISAGREE   anything else: an unexpected hold, an expected hold that did
             anything, a tar other than the expected one.  HELD never
             stands in for a DISAGREE.

Nothing leaves the host, and nothing writes to the given files:

  * each tree reads its own copy of sink.db and watermarks.db, made in a
    temporary directory (set TMPDIR to choose where);
  * a recorder replaces every transport.  It borrows the real transport's
    name and table, so the tree forms its real key.  It captures the first
    batch and stops the pump before any send, cursor advance, retry or
    commit;
  * the check counts queued retries and never replays them;
  * each worker reports every database file it opened, and the tool fails
    if one is a given file or if both trees opened the same file;
  * the tool hashes the given files (and any -wal beside them) before and
    after the run, and fails if anything changed them.

Filetree pipelines (GRAPE, the magnetometer, the heartbeat) read their
spool directories on the machine that runs the check.  A dev box has none
of those directories, so there the check compares only their keys.

--now freezes the clock that the wspr_cycle and sqlite sources consult, so
both trees judge at the same instant which WSPR cycles have closed.  It
defaults to the current UTC time, read once and handed to both trees.
Pass the snapshot's time to judge cycles as the station saw them.

The last line counts each verdict, as (<a> AGREE, <h> HELD, <d> DISAGREE),
and reads one of:

  RESULT: PASS (<counts>)             no DISAGREE, every HELD expected, and
                                      the check saw something: at least one
                                      pipeline, all of them built by both
                                      trees, none raising an error;
  RESULT: DISAGREE (<counts>)         some pipeline ends DISAGREE, or the
                                      trees built different pipelines;
  RESULT: INCONCLUSIVE (<counts>; <reason>)
                                      no DISAGREE, but the check compared
                                      nothing, or a pipeline did not build,
                                      or a pipeline raised an error, or a
                                      file was refused.  An error stops the
                                      check short of the source, so it
                                      proves nothing.

Exit status: 0 only on RESULT: PASS; 1 on DISAGREE, INCONCLUSIVE, a tree
that fails to build, a worker that crashes, a copy that is not a usable
database, or a given file that changed; 2 on a usage error.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

# The fields of one pipeline's result that OLD and NEW must share.
_FIELDS = ("key", "stored", "queued", "limits", "batch", "error")

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
        self.sent = False

    def primary_table(self):
        return self.inner.primary_table()

    def batch_policy(self):
        return self.inner.batch_policy()

    def ship(self, batch, identity):
        self.batch = batch
        self.sent = True
        raise _Captured()

    def serialize_for_retry(self, batch, identity):
        self.sent = True
        raise _Captured()

    def replay(self, payload_blob, identity):
        self.sent = True
        raise _Captured()


def _text(cursor) -> str | None:
    if cursor is None:
        return None
    if isinstance(cursor, (bytes, bytearray, memoryview)):
        return bytes(cursor).decode("ascii", "backslashreplace")
    return str(cursor)


def _records(batch) -> list:
    """Each record of a batch as the digest covers it: table, time, columns,
    payload path, dedup key."""
    return [
        [r.table, r.time.isoformat(), dict(r.columns),
         str(r.payload_path) if r.payload_path else None,
         _text(getattr(r, "dedup_key", None))]
        for r in batch.records
    ]


def _digest(records: list, token) -> str:
    blob = json.dumps({"records": records, "commit_token": token},
                      sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _describe(batch, ids: list) -> dict:
    """The next batch, as the check compares it.  The digest covers every
    record in order (table, time, columns, payload path, dedup key) and the
    batch's commit token: the token tells the source what to delete once the
    transport acks."""
    records = _records(batch)
    token = _text(batch.commit_token)
    return {
        "records": len(records),
        "first_id": ids[0] if ids else None,
        "last_id": ids[-1] if ids else None,
        "cursor_after": _text(batch.cursor_after),
        "commit_token": token,
        "digest": _digest(records, token),
    }


def _limits(transport, pipe) -> list:
    """The caps the core applies when it asks the source for a batch:
    pipeline batch_limit, the transport's max_records, and the pipeline's
    max_records_per_pump.  A small backlog hides a changed cap in the
    batch itself, so the check compares the caps too."""
    try:
        cap = transport.batch_policy().max_records
    except Exception as exc:  # noqa: BLE001 - compared, never hidden
        cap = f"{type(exc).__name__}: {exc}"
    return [getattr(pipe, "batch_limit", None), cap,
            getattr(pipe, "max_records_per_pump", None)]


def _opened_path(database, uri: bool) -> str | None:
    """The real path a sqlite3.connect() call names; None for an in-memory
    database."""
    text = os.fsdecode(database)
    if uri and text.startswith("file:"):
        text = unquote(urlparse(text).path)
    if not text or text.startswith(":memory:"):
        return None
    return os.path.realpath(text)


def _tap_sqlite(sqlite3, sink: Path, ids: list, opened: set) -> None:
    """Record the pending_uploads ids that each read of the sink copy returns,
    and every database file the tree opens.

    SqliteSource puts no row id on its Records, so the check reads the ids
    where the source reads them: from every query on the sink copy whose
    first result column is `id`.  Connections to any other file stay
    untouched, but each one lands in `opened`, so the parent can tell that
    the tree worked on its own copies and on nothing it was given."""
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
        path = _opened_path(database, bool(kwargs.get("uri")))
        if path is not None:
            opened.add(path)
            if "factory" not in kwargs and path == target:
                kwargs["factory"] = _TapConnection
        return real_connect(database, *args, **kwargs)

    sqlite3.connect = connect


def _freeze_clock(now: datetime) -> None:
    """Freeze the clock of every module in _CLOCK_MODULES that the tree has.

    A module the tree lacks has no clock to freeze.  A module the tree has
    but cannot be frozen (its import fails, or its `datetime` is no longer
    datetime.datetime) raises: the tree would judge on the real clock, and
    a silent skip would let it."""
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
        except ModuleNotFoundError as exc:
            if exc.name == name:
                continue
            raise
        if getattr(mod, "datetime", None) is not dt.datetime:
            raise RuntimeError(
                f"cannot freeze the clock of {name}: its datetime is "
                f"{getattr(mod, 'datetime', None)!r}, not datetime.datetime")
        mod.datetime = _FrozenDatetime


def _store_print(path: Path) -> str:
    """A digest of the store copy's bytes (the file, and any -wal or
    -journal beside it), so the check sees any write a pump makes."""
    h = hashlib.sha256()
    for suffix in ("", "-wal", "-journal"):
        f = Path(f"{path}{suffix}")
        h.update(suffix.encode("ascii"))
        if f.exists():
            h.update(f.read_bytes())
    return h.hexdigest()


def _sink_print(path: Path) -> tuple:
    """(size, mtime_ns, inode) of the sink copy and any -wal or -journal
    beside it.  Any write changes one of them; a read changes none, since
    the worker holds a guard connection open, so no reader's close can
    checkpoint the -wal into the file."""
    marks = []
    for suffix in ("", "-wal", "-journal"):
        try:
            st = os.stat(f"{path}{suffix}")
        except FileNotFoundError:
            marks.append(None)
        else:
            marks.append((st.st_size, st.st_mtime_ns, st.st_ino))
    return tuple(marks)


def _store_rows(sqlite3, watermarks: Path) -> dict:
    """Every send record in the store copy, read by the tool itself:
    "source|dest|table" -> (cursor, last_ack).  The tool learns what NEW's
    starting pass wrote from this, never from NEW's own account (Ruling 15)."""
    conn = sqlite3.connect(watermarks)
    try:
        return {f"{s}|{d}|{tb}": (bytes(c).hex() if isinstance(c, (bytes, memoryview))
                                  else repr(c), a)
                for s, d, tb, c, a in conn.execute(
                    "SELECT source_id, dest_id, table_name, cursor, last_ack "
                    "FROM watermarks")}
    finally:
        conn.close()


def _rider_producers(sqlite3, sink: Path, records: list) -> list:
    """For each psk.spots record of a cycle batch, the `producer` column of
    the sink row it came from ('' when the column reads '' or the sink has
    none); None for every other record.  Rows match by payload, in id
    order, as the cycle source reads them.  The parent judges the producer;
    this only reads the column."""
    times = sorted({str(r[2].get("time")) for r in records
                    if r[0] == "psk.spots" and isinstance(r[2], dict)})
    pool: dict = {}
    if times:
        conn = sqlite3.connect(sink)
        try:
            cols = {row[1] for row in
                    conn.execute("PRAGMA table_info(pending_uploads)")}
            producer = "producer" if "producer" in cols else "''"
            local = " AND local = 0" if "local" in cols else ""
            marks = ",".join("?" * len(times))
            for payload_json, made_by in conn.execute(
                    f"SELECT payload_json, {producer} FROM pending_uploads "
                    f"WHERE target_db = 'psk' AND target_table = 'spots'{local} "
                    f"AND json_extract(payload_json, '$.time') IN ({marks}) "
                    f"ORDER BY id ASC", times):
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError:
                    continue
                pool.setdefault(json.dumps(payload, sort_keys=True),
                                []).append(made_by or "")
        finally:
            conn.close()
    out = []
    for r in records:
        if r[0] != "psk.spots":
            out.append(None)
            continue
        queue = pool.get(json.dumps(r[2], sort_keys=True, default=str))
        out.append(queue.pop(0) if queue else "")
    return out


def _attrs(pipe) -> dict:
    """What the tree built: the fields the gate's rules read, as plain data.
    The parent prefers its own read of the manifest (_own_layout) and uses
    these only for a pipeline it cannot find there (a builder entry)."""
    transport = pipe.transport
    while type(transport).__name__ == "DiscardTransport":
        transport = transport.inner
    source = pipe.source
    return {
        "client": str(getattr(pipe, "client", "") or ""),
        "origin": str(getattr(pipe, "origin", "legacy") or "legacy"),
        "governed": bool(getattr(pipe, "governed", True)),
        "heartbeat": type(transport).__name__ == "HeartbeatSftp",
        "rider": (type(source).__name__ == "WsprCycleSource"
                  and bool(getattr(source, "include_psk", False))),
    }


def _worker(tree: Path, manifest_path: Path, sink: Path, watermarks: Path,
            now_iso: str, out: Path, sinks_dir: Path | None = None,
            marker: Path | None = None) -> int:
    import inspect
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
    opened: set = set()
    _tap_sqlite(sqlite3, sink, ids, opened)
    # Held open for the whole run, so no other connection's close is the
    # last one and checkpoints the sink's -wal (see _sink_print).
    guard = sqlite3.connect(sink)
    guard.execute("SELECT count(*) FROM sqlite_master").fetchall()
    now = _parse_now(now_iso)
    try:
        _freeze_clock(now)
        freeze_error = None
    except Exception as exc:  # noqa: BLE001 - reported on every pipeline
        freeze_error = f"{type(exc).__name__}: {exc}"

    import hs_uploader.daemon as daemon_mod
    from hs_uploader.core import Uploader
    from hs_uploader.daemon import build_all_pipelines, load_manifest
    from hs_uploader.watermark.sqlite import SqliteWatermarkStore

    # The daemon's own gate, when the tree has one (spec §11.2).  A tree
    # from before it (part 1 or older) runs as it always ran.
    try:
        from hs_uploader.gate import SinkGate
    except ModuleNotFoundError as exc:
        if exc.name != "hs_uploader.gate":
            raise
        SinkGate = None
    sink_paths = getattr(daemon_mod, "SinkPaths", None)
    gate = None
    if (SinkGate is not None and sinks_dir is not None and marker is not None
            and "gate" in inspect.signature(Uploader).parameters):
        gate = SinkGate(sinks_dir, marker)

    # A tree from before pipelines.d has no loader; it reads the legacy
    # manifest alone, as it always did.
    try:
        from hs_uploader.pipelines_d import load_layout
    except ModuleNotFoundError as exc:
        if exc.name != "hs_uploader.pipelines_d":
            raise
        load_layout = None
    if load_layout is not None:
        from hs_uploader.daemon import build_layout

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
            # Counted, never popped: a held pipeline must not ask at all.
            self.pops = getattr(self, "pops", 0) + 1
            return None

        def deliverable_count(self, pipeline=None):
            return 0

        def queued(self, pipeline):
            return super().deliverable_count(pipeline)

    layout = None
    if load_layout is not None:
        # pipelines.d sits beside the manifest copy, where the loader looks.
        layout = load_layout(manifest_path)
        manifest = layout.manifest
        entries = [e for e, _origin in layout.entries]
    else:
        manifest = load_manifest(manifest_path)
        entries = manifest.get("pipeline", []) or []
    for entry in entries:
        source = entry.get("source") or {}
        if str(source.get("type", "")).strip().lower() == "wspr_cycle":
            source["db_path"] = str(sink)    # SqliteSource reads SIGMOND_SQLITE_PATH
    named = [str(e["name"]) for e in entries if e.get("name")]

    store = _TapStore(watermarks)
    result: dict = {"tree": str(pkg), "pipelines": {}, "unbuilt": [],
                    "refused": [], "gate": gate is not None,
                    "starting_records": None, "start_moved": []}

    def finish() -> int:
        guard.close()
        result["opened"] = sorted(opened)
        out.write_text(json.dumps(result))
        return 0

    if layout is None and (manifest_path.parent / "pipelines.d").exists():
        # The tree cannot read the directory it was given.  Say so: its
        # result would otherwise stand for a layout it never saw.
        result["refused"].append({
            "file": "(directory)", "client": "",
            "reason": "this tree has no pipelines.d loader; the directory "
                      "was not read"})
    try:
        if layout is not None:
            build_kw = {}
            if (sink_paths is not None and sinks_dir is not None
                    and "paths" in inspect.signature(build_layout).parameters):
                build_kw["paths"] = sink_paths(sinks_dir, marker)
            pipelines, refused = build_layout(layout, watermark=store,
                                              **build_kw)
            result["refused"] = [
                {"file": r.file, "client": r.client, "reason": r.reason}
                for r in refused]
        else:
            pipelines = build_all_pipelines(manifest, watermark=store)
    except Exception as exc:  # noqa: BLE001 - reported, never hidden
        result["build_error"] = f"{type(exc).__name__}: {exc}"
        return finish()

    built = [p.name for p in pipelines]
    result["unbuilt"] = [n for n in named if n not in built]
    # The daemon's starting-record pass (spec §3.5, ruling X9): while the
    # marker stands, before any pump, as the daemon runs it at its build.
    write_start = getattr(daemon_mod, "write_starting_records", None)
    if gate is not None and write_start is not None and not freeze_error:
        if os.path.lexists(marker):
            # What the pass wrote comes from the tool's own diff of the
            # store, never from the pass's return value (review I1).  A
            # record that existed and changed, or vanished, is a fault.
            before = _store_rows(sqlite3, watermarks)
            write_start(pipelines, store)
            after = _store_rows(sqlite3, watermarks)
            store.reads.clear()
            result["starting_records"] = sorted(k for k in after
                                                if k not in before)
            result["start_moved"] = sorted(
                k for k in before if after.get(k) != before[k])
    seen: dict = {}
    for pipe in pipelines:
        label, n = pipe.name, 2
        while label in result["pipelines"]:
            label, n = f"{pipe.name}#{n}", n + 1
        # The tool matches a pipeline to its manifest entry by its real name
        # and its place among the pipelines of that name, never by parsing
        # the label (review I2).
        seen[pipe.name] = seen.get(pipe.name, 0) + 1
        limits = _limits(pipe.transport, pipe)
        recorder = _Recorder(pipe.transport)
        pipe.transport = recorder
        store.reads.clear()
        ids.clear()
        entry = {"key": None, "stored": None, "queued": None,
                 "limits": limits, "batch": None, "error": None,
                 "sent": False, "pops": 0, "wrote": False,
                 "sink_wrote": False, "attrs": _attrs(pipe),
                 "name": pipe.name, "occurrence": seen[pipe.name]}
        if freeze_error:
            # Pumping now would run on the real clock.  Say so, and stop.
            entry["error"] = freeze_error
            result["pipelines"][label] = entry
            continue
        store.pops = 0
        try:
            entry["queued"] = store.queued(pipe.name)
            kw = {} if gate is None else {"gate": gate.may_send}
            before = _store_print(watermarks)
            sink_before = _sink_print(sink)
            uploader = Uploader([pipe], now_fn=now.timestamp, **kw)
            try:
                uploader.pump()
            except _Captured:
                pass
            finally:
                uploader.close()
                entry["wrote"] = _store_print(watermarks) != before
                entry["sink_wrote"] = _sink_print(sink) != sink_before
        except Exception as exc:  # noqa: BLE001 - reported per pipeline
            entry["error"] = f"{type(exc).__name__}: {exc}"
        entry["sent"] = recorder.sent
        entry["pops"] = store.pops
        if store.reads:
            key, cursor = store.reads[0]
            entry["key"] = list(key)
            entry["stored"] = _text(cursor)
        if recorder.batch is not None:
            entry["batch"] = _describe(recorder.batch, list(ids))
            records = _records(recorder.batch)
            if entry["attrs"]["rider"] and any(
                    r[0] == "psk.spots" for r in records):
                # What the parent needs to work out the expected NEW batch
                # (spec §11.2): every record, and each rider's producer.
                entry["rider_records"] = records
                entry["rider_producers"] = _rider_producers(
                    sqlite3, sink, records)
        result["pipelines"][label] = entry
    return finish()


# ---- the tool's own reading of the switches (Ruling 15) ----------------------
#
# The tool predicts which pipelines NEW must hold, and which psk rows NEW
# must leave out of a WSPR tar, without asking the library it tests.  Were
# it to ask hs_uploader's switch reader or its D12 inference, a defect there
# would shape NEW and the prediction alike, and hide itself.  These lines
# restate spec §3.1 and §5 (design §3.4) from their text, with tomllib.

_STATES = ("off", "fill", "upload")       # strictest first
_CLIENT_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")
_SIGMOND_ORIGIN = "pipelines.d/sigmond.toml"
_MARKER_NAME = "carry-over-v1.done"


def _own_switch(path: Path) -> tuple:
    """("missing", None), ("bad", None) or ("ok", state) for one file."""
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return "missing", None
    except OSError:            # unreadable, a directory, sinks/ not a directory
        return "bad", None
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        return "bad", None
    state = data.get("state")
    if state not in _STATES:
        return "bad", None
    return "ok", state


def _own_effective(client, sinks_dir) -> str:
    """The client's effective state: the stricter of its own file and
    site.toml, in the order off < fill < upload.  It fails closed: a bad
    client name, no sinks/ directory, a missing client file, and any read,
    parse or state fault in either file all read off.  A missing site.toml
    sets no limit."""
    if (not isinstance(client, str) or client == "site"
            or not _CLIENT_NAME.fullmatch(client)):
        return "off"
    if sinks_dir is None:
        return "off"
    c_how, c_state = _own_switch(Path(sinks_dir) / f"{client}.toml")
    s_how, s_state = _own_switch(Path(sinks_dir) / "site.toml")
    if c_how != "ok" or s_how == "bad":
        return "off"
    if s_how == "missing":
        return c_state
    return _STATES[min(_STATES.index(c_state), _STATES.index(s_state))]


def _own_marker_stands(marker) -> bool:
    """Anything at the path stands, a dangling link included.  An error
    other than "no such file" reads as standing: the switches then govern."""
    if marker is None:
        return False
    try:
        os.lstat(marker)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        return True
    return True


def _own_psk_producer(payload) -> str:
    """D12 for a psk row stored with producer '': MSK144 rows belong to
    meteor-scatter, every other psk row to psk-recorder."""
    mode = payload.get("mode") if isinstance(payload, dict) else None
    if isinstance(mode, str) and mode.strip().lower() == "msk144":
        return "meteor-scatter"
    return "psk-recorder"


def _own_layout(manifest: Path, pdir) -> dict:
    """pipeline name -> a list of the fields the gate's rules read, one per
    entry of that name, in the daemon's build order (legacy entries in
    manifest order, then each file in name order), from the tool's own read
    of the manifest and pipelines.d/.  A file present in pipelines.d/
    silences its client's legacy entries; a file's client is its name.  A
    file that does not parse adds nothing (NEW refuses it)."""
    def fields(entry, client, origin):
        source = entry.get("source") or {}
        transport = entry.get("transport") or {}
        return {
            "client": client,
            "origin": origin,
            "governed": entry.get("governed", True) is not False,
            "heartbeat": str(transport.get("type", "")).strip().lower()
            == "heartbeat_sftp",
            # bool(), as pipeline_factory reads it (review M2).
            "rider": (str(source.get("type", "")).strip().lower() == "wspr_cycle"
                      and bool(source.get("include_psk", False))),
        }

    files = []
    if pdir is not None and Path(pdir).is_dir():
        files = sorted(n for n in os.listdir(pdir)
                       if n.endswith(".toml") and not n.startswith("."))
    present = {n[:-len(".toml")] for n in files}
    found: dict = {}
    try:
        legacy = tomllib.loads(Path(manifest).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        legacy = {}
    for entry in legacy.get("pipeline", []) or []:
        client = str(entry.get("client") or "")
        if entry.get("name") and client not in present:
            found.setdefault(str(entry["name"]), []).append(
                fields(entry, client, "legacy"))
    for name in files:
        try:
            data = tomllib.loads((Path(pdir) / name).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
            continue
        for entry in data.get("pipeline", []) or []:
            if isinstance(entry, dict) and entry.get("name"):
                found.setdefault(str(entry["name"]), []).append(fields(
                    entry, name[:-len(".toml")], f"pipelines.d/{name}"))
    return found


def _own_hold(fields: dict, marker: bool, sinks_dir) -> tuple:
    """(hold?, why) by spec §3.1's five rules, first that applies."""
    if not fields["governed"] and fields["origin"] == _SIGMOND_ORIGIN:
        return False, "rule 1: governed = false in sigmond.toml"
    if fields["origin"] == "legacy" and (fields["client"] == "sigmond"
                                         or fields["heartbeat"]):
        return False, "rule 2: the legacy heartbeat"
    if not marker:
        return False, "rule 3: no carry-over marker"
    if not fields["client"]:
        return False, "rule 4: names no client"
    state = _own_effective(fields["client"], sinks_dir)
    return state != "upload", f"{fields['client']} reads {state}"


def _own_rider(old_entry: dict, sinks_dir) -> dict | None:
    """NEW's expected batch for a WSPR tar that carries psk rows (spec §5,
    §11.2): OLD's batch less each psk row whose producer, from its column
    or by the tool's own D12 rule, does not read upload."""
    records = old_entry.get("rider_records")
    producers = old_entry.get("rider_producers")
    batch = old_entry.get("batch")
    if not records or batch is None or producers is None:
        return None
    keep, left_out, rides = [], 0, {}
    for record, made_by in zip(records, producers):
        if record[0] != "psk.spots":
            keep.append(record)
            continue
        producer = made_by or _own_psk_producer(record[2])
        if producer not in rides:
            rides[producer] = _own_effective(producer, sinks_dir) == "upload"
        if rides[producer]:
            keep.append(record)
        else:
            left_out += 1
    if not left_out:
        return {"batch": batch, "left_out": 0}
    expected = None
    if keep:
        expected = dict(batch, records=len(keep),
                        digest=_digest(keep, batch.get("commit_token")))
    return {"batch": expected, "left_out": left_out}


def _expectations(args, old: dict, new: dict) -> dict:
    """For each pipeline NEW built: does the tool expect it held, why, and
    for a WSPR tar with psk rows, the batch it expects."""
    marker = _own_marker_stands(getattr(args, "marker", None))
    sinks = getattr(args, "sinks", None)
    layout = _own_layout(getattr(args, "new_manifest", None) or args.manifest,
                         getattr(args, "new_pipelines_d", None))
    built: dict = {}       # name -> how many pipelines NEW built of it
    for label, entry in new["pipelines"].items():
        name = (entry or {}).get("name", label)
        built[name] = built.get(name, 0) + 1
    out = {}
    for label, entry in new["pipelines"].items():
        entry = entry or {}
        name, occurrence = entry.get("name", label), entry.get("occurrence", 1)
        own = layout.get(name) or []
        if not marker:
            # Rule 3: nothing is held, and no rider leaves out a row.
            out[label] = {"hold": False, "why": "", "rider": None}
            continue
        unjudged = None
        if occurrence > len(own):
            # A builder entry, or anything else the tool cannot read for
            # itself: its expectation never comes from NEW (review M1).
            unjudged = ("not in the manifest the tool read (a builder "
                        "entry?); the tool cannot judge its switches")
        elif len(own) > 1:
            # Several entries share the name.  NEW's n-th pipeline of the
            # name is the n-th entry in build order; NEW's own attributes
            # serve only to confirm that order (review I2).
            mine = own[occurrence - 1]
            theirs = entry.get("attrs") or {}
            if (built.get(name) != len(own)
                    or theirs.get("client") != mine["client"]
                    or theirs.get("origin") != mine["origin"]):
                unjudged = (f"{len(own)} entries share the name {name!r} and "
                            f"the tool cannot match this one to its entry")
        if unjudged:
            out[label] = {"hold": None, "why": unjudged, "rider": None,
                          "unjudged": True}
            continue
        fields = own[occurrence - 1]
        hold, why = _own_hold(fields, marker, sinks)
        rider = None
        o = old["pipelines"].get(label)
        if not hold and marker and fields.get("rider") and o:
            rider = _own_rider(o, sinks)
        out[label] = {"hold": hold, "why": why, "rider": rider}
    return out


# ---- parent: copy, run both trees, compare ------------------------------------


def _parse_now(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError(f"--now needs a UTC offset or a trailing Z: {text!r}")
    return value.astimezone(timezone.utc)


def _with_wal(path: Path) -> list:
    wal = Path(f"{path}-wal")
    return [path, wal] if wal.exists() else [path]


def _given_files(args) -> list:
    """The files the check must leave alone: the manifest, sink.db and
    watermarks.db, each with the -wal beside it when there is one."""
    files = [*_manifests(args)]
    for pdir in [*_pipelines_dirs(args), getattr(args, "sinks", None)]:
        if pdir is None:
            continue
        for root, _dirs, names in os.walk(pdir):
            files.extend(Path(root) / n for n in sorted(names)
                         if (Path(root) / n).is_file())
    marker = getattr(args, "marker", None)
    if marker is not None and marker.is_file():
        files.append(marker)
    return [*dict.fromkeys(files),
            *_with_wal(args.sink), *_with_wal(args.watermarks)]


def _manifests(args) -> list:
    """The manifest each side reads (X6): --old-manifest and --new-manifest,
    each defaulting to --manifest."""
    return [args.old_manifest or args.manifest, args.new_manifest or args.manifest]


def _pipelines_dirs(args) -> list:
    return [d for d in (args.old_pipelines_d, args.new_pipelines_d) if d]


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


def _check_copy(path: Path, what: str, table: str) -> None:
    """Refuse a copy that cannot give the check a real read.  A torn or
    wrong file would make every pipeline read as 'nothing queued', in both
    trees alike, and the trees would agree about nothing."""
    import sqlite3

    try:
        conn = sqlite3.connect(path)
        try:
            verdict = conn.execute("PRAGMA quick_check").fetchall()
            if verdict != [("ok",)]:
                raise RuntimeError(
                    f"the copy of {what} fails PRAGMA quick_check: {verdict[0][0]}")
            found = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone()
            if found is None:
                raise RuntimeError(
                    f"{what} holds no {table} table; is it the right file?")
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(
            f"the copy of {what} cannot be read as a database: {exc}") from exc


def _real_names(*paths) -> set:
    """Each path, with the -wal and -shm that SQLite keeps beside it."""
    return {os.path.realpath(f"{p}{suffix}")
            for p in paths for suffix in ("", "-wal", "-shm")}


def _run_tree(label: str, tree: Path, args, now_iso: str, scratch: Path) -> dict:
    work = scratch / label.lower()
    work.mkdir()
    sink, watermarks = work / "sink.db", work / "watermarks.db"
    _copy_db(args.sink, sink)
    _copy_db(args.watermarks, watermarks)
    _check_copy(sink, "sink.db", "pending_uploads")
    _check_copy(watermarks, "watermarks.db", "watermarks")
    # The manifest and its pipelines.d go side by side, so the tree's loader
    # finds the directory where it finds it on a station.
    etc = work / "etc"
    etc.mkdir()
    side_manifest = (getattr(args, f"{label.lower()}_manifest", None)
                     or args.manifest)
    side_pdir = getattr(args, f"{label.lower()}_pipelines_d", None)
    manifest = etc / "pipelines.toml"
    shutil.copyfile(side_manifest, manifest)
    if side_pdir is not None:
        shutil.copytree(side_pdir, etc / "pipelines.d")
    # The switches and the marker reach NEW alone (spec §11.2), as copies
    # where a station keeps them: sinks/ beside the manifest, the marker
    # in the state directory.  Presence alone makes a marker.
    sinks_copy, marker_copy = etc / "sinks", work / _MARKER_NAME
    if label == "NEW" and getattr(args, "sinks", None) is not None:
        shutil.copytree(args.sinks, sinks_copy, symlinks=True)
    if label == "NEW" and _own_marker_stands(getattr(args, "marker", None)):
        marker_copy.write_text("")
    out = work / "result.json"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["HS_UPLOADER_STATE_DIR"] = str(work)
    env["SIGMOND_SQLITE_PATH"] = str(sink)
    cmd = [sys.executable, "-B", str(Path(__file__).resolve()),
           "--worker", str(tree), "--manifest", str(manifest),
           "--sink", str(sink), "--watermarks", str(watermarks),
           "--now", now_iso, "--out", str(out),
           "--worker-sinks", str(sinks_copy), "--worker-marker", str(marker_copy)]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True,
                          timeout=1800)
    for line in (proc.stdout + proc.stderr).splitlines():
        print(f"[{label}] {line}", file=sys.stderr)
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(f"the {label} tree's worker exited {proc.returncode}")
    result = json.loads(out.read_text())
    opened = set(result.get("opened", []))
    touched = sorted(opened & _real_names(args.sink, args.watermarks))
    if touched:
        raise RuntimeError(
            f"the {label} tree opened {touched[0]}, a file you gave; "
            f"each tree must work on its own copy")
    if os.path.realpath(watermarks) not in opened:
        raise RuntimeError(
            f"the {label} tree never opened its copy of watermarks.db, so "
            f"the check cannot show that it worked on a copy")
    return result


def _key_text(entry, verdict: str = "") -> str:
    if entry is None:
        return "(not built)"
    if verdict == "HELD":
        return "(not read: held)"
    if entry["key"] is None:
        return "(no key read)"
    return "(" + ", ".join(entry["key"]) + ")"


def _next_text(entry, verdict: str = "") -> str:
    if entry is None:
        return "(not built)"
    if verdict == "HELD":
        return "held: no batch, no retry popped, no record written"
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


def _differing(old, new) -> list:
    """The compared fields in which OLD and NEW differ.  Some of them (the
    limits, and a commit token inside the digest) show on no stdout line, so
    the report names them on stderr."""
    if old is None or new is None:
        return ["pipeline built by one tree only"]
    return [f for f in _FIELDS if old.get(f) != new.get(f)]


def _did(entry: dict) -> list:
    """What a pipeline's pump did, worded; empty when it held: no send
    record read, no batch, nothing sent, no retry popped, no write."""
    did = []
    if entry.get("error"):
        did.append(f"raised {entry['error']}")
    if entry.get("key") is not None:
        did.append("read its send record")
    if entry.get("batch") is not None:
        did.append("produced a batch")
    elif entry.get("sent"):
        did.append("sent")
    if entry.get("pops"):
        did.append("asked for a retry")
    if entry.get("wrote"):
        did.append("wrote the store")
    if entry.get("sink_wrote"):
        did.append("wrote the sink")
    return did


def _rows(n: int) -> str:
    return f"{n} psk row{'' if n == 1 else 's'}"


def _verdict(old, new, expect=None, moved=frozenset()) -> tuple:
    """(AGREE | HELD | DISAGREE, why, differing fields).

    HELD only where the tool's own reader expects the hold and NEW did
    nothing at all.  An expected hold that did anything, a hold the reader
    did not expect, and a WSPR tar other than the expected one are each a
    DISAGREE: HELD never stands in for one."""
    if old is None or new is None:
        return "DISAGREE", "", _differing(old, new)
    if old.get("key") is not None and "|".join(old["key"]) in moved:
        return ("DISAGREE", "the starting pass moved its send record",
                ["the send record, before any pump"])
    expect = expect or {"hold": False, "why": "", "rider": None}
    if expect.get("unjudged"):
        return "INCONCLUSIVE", expect["why"], []
    did = _did(new)
    if expect["hold"]:
        if not did:
            return "HELD", expect["why"], []
        return ("DISAGREE",
                f"the switches hold it: {expect['why']}; NEW {', '.join(did)}",
                ["a hold the switches order"])
    if not did and old.get("key") is not None:
        return ("DISAGREE", "NEW held it; the switches let it send",
                ["a hold the switches do not order"])
    rider = expect.get("rider")
    if rider and rider["left_out"]:
        differ = [f for f in _FIELDS if f != "batch" and old.get(f) != new.get(f)]
        if new.get("batch") != rider["batch"]:
            differ.append("batch")
        if differ:
            return ("DISAGREE", f"expected {_rows(rider['left_out'])} left out",
                    differ)
        return ("AGREE", f"{_rows(rider['left_out'])} left out, as expected", [])
    if _agree(old, new):
        return "AGREE", "", []
    return "DISAGREE", "", _differing(old, new)


def _report(old: dict, new: dict, expect: dict | None = None) -> int:
    expect = expect or {}
    names = list(old["pipelines"])
    names += [n for n in new["pipelines"] if n not in names]
    counts = {"AGREE": 0, "HELD": 0, "DISAGREE": 0, "INCONCLUSIVE": 0}
    moved = set(new.get("start_moved") or [])
    for name in names:
        o, n = old["pipelines"].get(name), new["pipelines"].get(name)
        verdict, why, differ = _verdict(o, n, expect.get(name), moved)
        counts[verdict] += 1
        print(f"{name}: {verdict}" + (f" ({why})" if why else ""))
        print(f"  OLD key  {_key_text(o)}")
        print(f"  NEW key  {_key_text(n, verdict)}")
        print(f"  OLD next {_next_text(o)}")
        print(f"  NEW next {_next_text(n, verdict)}")
        if verdict == "DISAGREE":
            print(f"[compare] {name}: OLD and NEW differ in: "
                  f"{', '.join(differ)}", file=sys.stderr)
    for label, res in (("OLD", old), ("NEW", new)):
        written = res.get("starting_records")
        if written is None:
            continue
        if written:
            print(f"{label} wrote starting records: {', '.join(written)}")
        else:
            print(f"{label} wrote no starting record")
    for key in sorted(moved):
        print(f"NEW's starting pass changed an existing send record: {key}")
    refused = 0
    for label, res in (("OLD", old), ("NEW", new)):
        for r in res.get("refused", []):
            refused += 1
            print(f"{label} refused pipelines.d/{r['file']}: {r['reason']}")
    for label, res in (("OLD", old), ("NEW", new)):
        if res["unbuilt"]:
            print(f"{label} built no pipeline for: {', '.join(res['unbuilt'])}")
    raised = [n for n in names
              if any((res["pipelines"].get(n) or {}).get("error") for res in (old, new))]
    if raised:
        print(f"An error stopped these pipelines, so the check shows nothing "
              f"of what they send: {', '.join(raised)}")
    print(f"{len(names)} pipelines: {counts['AGREE']} agree, "
          f"{counts['HELD']} held, {counts['DISAGREE']} disagree")
    tally = (f"{counts['AGREE']} AGREE, {counts['HELD']} HELD, "
             f"{counts['DISAGREE']} DISAGREE")
    if counts["DISAGREE"] or moved or old["unbuilt"] != new["unbuilt"]:
        note = f"; {len(raised)} raised an error; see above" if raised else ""
        print(f"RESULT: DISAGREE ({tally}{note})")
        return 1
    # No difference found.  That proves neutrality only if the check saw
    # something: a pipeline compared, built by both trees, and run to the
    # source without an error.
    reasons = []
    if not names:
        reasons.append("no pipeline was compared")
    unbuilt = set(old["unbuilt"]) | set(new["unbuilt"])
    if unbuilt:
        reasons.append(f"{len(unbuilt)} not built; see above")
    if raised:
        reasons.append(f"{len(raised)} raised an error; see above")
    if counts["INCONCLUSIVE"]:
        reasons.append(f"{counts['INCONCLUSIVE']} not judged; see above")
    if refused:
        # A refused file sends nothing, so the pipelines in it were never
        # compared (Ruling 2).
        reasons.append(f"{refused} refused file{'' if refused == 1 else 's'}; "
                       f"see above")
    if reasons:
        print(f"RESULT: INCONCLUSIVE ({tally}; {'; '.join(reasons)})")
        return 1
    # PASS: no DISAGREE, and every HELD was one the tool's reader expected
    # (_verdict gives HELD to nothing else).
    print(f"RESULT: PASS ({tally})")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="neutrality_check",
        description="Show whether two hs-uploader trees form the same "
                    "send-record keys and select the same next batch, on "
                    "copies of a station's pipelines.toml, sink.db and "
                    "watermarks.db.")
    p.add_argument("--manifest", "--pipelines", dest="manifest", type=Path,
                   help="the station's /etc/hs-uploader/pipelines.toml (a copy); "
                        "required unless --old-manifest and --new-manifest "
                        "both name one")
    p.add_argument("--old-manifest", type=Path,
                   help="the manifest OLD reads (default: --manifest)")
    p.add_argument("--new-manifest", type=Path,
                   help="the manifest NEW reads (default: --manifest)")
    p.add_argument("--old-pipelines-d", type=Path,
                   help="a copy of pipelines.d for OLD (default: none)")
    p.add_argument("--new-pipelines-d", type=Path,
                   help="a copy of pipelines.d for NEW (default: none)")
    p.add_argument("--sink", type=Path, required=True,
                   help="a snapshot of /var/lib/sigmond/sink.db")
    p.add_argument("--watermarks", type=Path, required=True,
                   help="a snapshot of /var/lib/hs-uploader/watermarks.db")
    p.add_argument("--old", type=Path, help="directory that holds the OLD hs_uploader")
    p.add_argument("--new", type=Path, help="directory that holds the NEW hs_uploader")
    p.add_argument("--now", help="UTC time both trees read as now "
                                 "(default: the current time, read once)")
    p.add_argument("--sinks", type=Path,
                   help="a copy of /etc/hs-uploader/sinks for NEW (default: "
                        "none, which fails closed once the marker stands)")
    p.add_argument("--marker", type=Path,
                   help="the carry-over marker for NEW; it stands when "
                        "anything exists at the path (default: none)")
    p.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--out", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--worker-sinks", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--worker-marker", type=Path, help=argparse.SUPPRESS)
    args = p.parse_args(argv)

    if args.worker is not None:
        return _worker(args.worker, args.manifest, args.sink, args.watermarks,
                       args.now, args.out, args.worker_sinks,
                       args.worker_marker)

    for flag in ("old", "new"):
        tree = getattr(args, flag)
        if tree is None:
            p.error(f"--{flag} is required")
        if not (tree / "hs_uploader" / "__init__.py").is_file():
            p.error(f"--{flag} {tree} holds no hs_uploader package")
    for side in ("old", "new"):
        if args.manifest is None and getattr(args, f"{side}_manifest") is None:
            p.error(f"--manifest or --{side}-manifest is required")
    for flag in ("manifest", "sink", "watermarks"):
        if getattr(args, flag) is None:
            continue
        if not getattr(args, flag).is_file():
            p.error(f"--{flag} {getattr(args, flag)} is not a file")
    for flag in ("old_manifest", "new_manifest"):
        given = getattr(args, flag)
        if given is not None and not given.is_file():
            p.error(f"--{flag.replace('_', '-')} {given} is not a file")
    for flag in ("old_pipelines_d", "new_pipelines_d", "sinks"):
        given = getattr(args, flag)
        if given is not None and not given.is_dir():
            p.error(f"--{flag.replace('_', '-')} {given} is not a directory")
    try:
        now = _parse_now(args.now) if args.now else datetime.now(timezone.utc)
    except ValueError as exc:
        p.error(str(exc))
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"neutrality check: OLD {args.old}  NEW {args.new}  now {now_iso}")
    print(f"NEW switches: {args.sinks or '(none)'}  marker: "
          f"{'stands' if _own_marker_stands(args.marker) else 'absent'}"
          f"{f' ({args.marker})' if args.marker else ''}")
    try:
        before = _fingerprint(_given_files(args))
        with tempfile.TemporaryDirectory(prefix="neutrality-") as scratch:
            old = _run_tree("OLD", args.old, args, now_iso, Path(scratch))
            new = _run_tree("NEW", args.new, args, now_iso, Path(scratch))
        shared = sorted(set(old.get("opened", [])) & set(new.get("opened", [])))
        if shared:
            raise RuntimeError(
                f"both trees opened {shared[0]}; each tree must work on its "
                f"own copy")
        # List the files again: a -wal that a live writer created during
        # the run joins the comparison.
        after = _fingerprint(_given_files(args))
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
        print(f"ERROR: {exc}")
        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
            print(f"Each tree works on copies of sink.db and watermarks.db "
                  f"under {tempfile.gettempdir()}.  Set TMPDIR to a "
                  f"directory with room for four copies.")
        return 1
    if after != before:
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
    return _report(old, new, _expectations(args, old, new))


if __name__ == "__main__":
    sys.exit(main())
