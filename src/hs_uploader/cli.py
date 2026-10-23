"""Admin CLI for hs-uploader.

Subcommands:

* ``hs-uploader status`` — show watermark cursors and recent attempts.
* ``hs-uploader peek``   — show the last N attempts (or for one pipeline).
* ``hs-uploader reset-cursor --source <id> --dest <id> --table <name>``
   — drop one watermark row so the next pump re-ships from the
   beginning.  Used carefully for ops recovery.
* ``hs-uploader kick``   — bump every deliverable's
  ``next_attempt_at`` to now, so the next ``pump`` retries
  immediately instead of waiting out the backoff.
* ``hs-uploader migrate [--db PATH] [--check] [--sink PATH]
  [--keep-retry-for NAME]`` — bring watermarks.db to this release's
  schema version (``PRAGMA user_version``), then handle each retry still
  queued under ``psk-pskreporter``.  ``--check`` reports the version and
  what would run, and writes nothing.  Exit 0 done or nothing to do,
  1 failure, 2 usage error; ``--check`` exits 3 when a migrate would
  write.

Phase 1 is read-mostly: ``pump`` is **not** wired up here because there
are no transports yet.  It will land in Phase 2 once
``WsprdaemonTarSftp`` exists.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import sinkswitch
from .watermark.sqlite import SqliteWatermarkStore, default_path


# A word appears only in the release that ships its code complete (spec 7.1).
FEATURES = ("gate", "pipelines.d", "send-log-v3", "switch-files")

# The production manifest; sinks/ beside it is the real switch directory.
_REAL_MANIFEST = "/etc/hs-uploader/pipelines.toml"

# `sink status --json` layout version; sigmond refuses another (V14).
SINK_STATUS_SCHEMA = 1


def _euid() -> int:
    """The caller's effective uid; tests replace it to drive the root check."""
    return os.geteuid()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hs-uploader",
        description="Admin CLI for the hs-uploader watermark store.",
    )
    p.add_argument(
        "--state",
        type=Path,
        default=None,
        help=f"Path to watermarks.db (default: {default_path()}).",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sp_status = sub.add_parser("status", help="Show cursors + queue summary.")
    sp_status.set_defaults(func=_cmd_status)

    sp_peek = sub.add_parser("peek", help="Show recent attempt log entries.")
    sp_peek.add_argument("--limit", type=int, default=20)
    sp_peek.set_defaults(func=_cmd_peek)

    sp_reset = sub.add_parser(
        "reset-cursor",
        help="Drop one watermark row so the next pump starts from "
             "the beginning.",
    )
    sp_reset.add_argument("--source", required=True)
    sp_reset.add_argument("--dest", required=True)
    sp_reset.add_argument("--table", required=True)
    sp_reset.set_defaults(func=_cmd_reset_cursor)

    sp_kick = sub.add_parser(
        "kick",
        help="Set every deliverable's next_attempt_at to now so the "
             "next pump retries immediately.",
    )
    sp_kick.set_defaults(func=_cmd_kick)

    sp_purge = sub.add_parser(
        "purge-dead-letters",
        help="Save dead letters as SQL that restores them, then delete them.",
    )
    sp_purge.add_argument(
        "--before", default=None,
        help="Only rows older than this ISO time (rows whose time cannot be "
             "read are kept).")
    sp_purge.add_argument("--pipeline", default=None,
                          help="Only this pipeline's rows.")
    sp_purge.add_argument(
        "--backup-dir", type=Path, default=None,
        help="Where to write dead_letter-<UTC>.sql (default: beside the store).")
    sp_purge.set_defaults(func=_cmd_purge_dead_letters)

    sp_migrate = sub.add_parser(
        "migrate",
        help="Bring watermarks.db to this release's schema version.  Run it "
             "once the new code sits in place, before the daemon restarts.",
    )
    sp_migrate.add_argument(
        "--db",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to watermarks.db (default: --state, else "
             f"{default_path()}).",
    )
    sp_migrate.add_argument(
        "--check",
        action="store_true",
        help="Report the version and the pending migrations; write nothing.  "
             "Exits 3 when a migrate would write, 0 when it would not.",
    )
    sp_migrate.add_argument(
        "--sink",
        type=Path,
        default=None,
        metavar="PATH",
        help="sink.db, read only: a psk-pskreporter retry whose rows it "
             "still holds gets deleted, and each new pipeline re-reads them.",
    )
    sp_migrate.add_argument(
        "--keep-retry-for",
        default=None,
        metavar="NAME",
        help="The pipeline that inherits a psk-pskreporter retry the sink "
             "cannot vouch for: psk-recorder-pskreporter or "
             "meteor-scatter-pskreporter.  Without it such a retry stays.",
    )
    sp_migrate.set_defaults(func=None)

    sp_caps = sub.add_parser(
        "capabilities",
        help="Print the store schema version this release supports.",
    )
    sp_caps.add_argument(
        "--features", action="store_true",
        help="Print one feature word per line instead of the schema version.")
    sp_caps.set_defaults(func=None)

    sp_sink = sub.add_parser(
        "sink", help="Sink switches: set one, mark the carry-over done, and "
                     "report what the gate enforces.")
    sink_sub = sp_sink.add_subparsers(dest="sink_cmd", required=True)
    sp_ss = sink_sub.add_parser(
        "status", help="Show the site switch, each client's switch file and "
                       "effective state, and the pipelines the gate holds.")
    sp_ss.add_argument("--json", action="store_true")
    sp_ss.add_argument("--manifest", default=_REAL_MANIFEST,
                       help="legacy manifest; sinks/ lies beside it")
    sp_ss.add_argument("--sinks", default=None,
                       help="switch directory (default: sinks/ beside --manifest)")
    sp_ss.add_argument("--marker", default=None,
                       help="carry-over marker (default: "
                            "$HS_UPLOADER_STATE_DIR/carry-over-v1.done)")
    sp_ss.set_defaults(func=None)

    sp_set = sink_sub.add_parser(
        "set", help="Set the site switch or one client's switch, under the "
                    "switch lock (root, on the real directory).")
    sp_set.add_argument("name", help="site, or a client name")
    sp_set.add_argument("state", help="off, fill or upload")
    sp_set.add_argument("--reason", required=True)
    sp_set.add_argument("--by", required=True,
                        help="who set it, e.g. 'smd sink (mjh)'")
    sp_set.add_argument(
        "--identity-json", default=None, metavar="FILE|-",
        help="JSON object of identity tables, one per instance, for this "
             "history entry; - reads stdin")
    where = sp_set.add_mutually_exclusive_group()
    where.add_argument("--sinks", default=None,
                       help=f"switch directory (default: "
                            f"{sinkswitch.DEFAULT_SINKS_DIR})")
    where.add_argument("--manifest", default=None,
                       help="legacy manifest; sinks/ lies beside it")
    sp_set.set_defaults(func=None)

    sp_mark = sink_sub.add_parser(
        "mark-carried", help="Write the carry-over marker: the switches "
                             "govern from now on.")
    sp_mark.add_argument("--note", default=None,
                         help="one line written below the time line")
    sp_mark.add_argument("--marker", default=None,
                         help="marker path (default: "
                              "$HS_UPLOADER_STATE_DIR/carry-over-v1.done)")
    sp_mark.set_defaults(func=None)

    sp_serve = sub.add_parser(
        "serve",
        help="Run the host uploader daemon — every outbound pipeline in the "
             "manifest, in one process (the single-host uploader).",
    )
    sp_serve.add_argument("--manifest", default="/etc/hs-uploader/pipelines.toml")
    sp_serve.add_argument("--dry-run", action="store_true",
                          help="build pipelines and exit (no pumping)")
    sp_serve.add_argument("--once", action="store_true",
                          help="drain once (pump_until_idle) and exit")
    sp_serve.add_argument("--log-level", default="INFO")
    sp_serve.set_defaults(func=None)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    # `serve` runs the daemon, which manages its own watermark store — it does
    # not go through the read-only store-command dispatch below.
    if args.cmd == "serve":
        import logging
        logging.basicConfig(
            level=getattr(logging, str(args.log_level).upper(), logging.INFO),
            format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        )
        from . import daemon
        return daemon.run(args.manifest, dry_run=args.dry_run, once=args.once)
    if args.cmd == "sink":
        if args.sink_cmd == "set":
            return _cmd_sink_set(args)
        if args.sink_cmd == "mark-carried":
            return _cmd_sink_mark_carried(args)
        return _cmd_sink_status(args)
    if args.cmd == "capabilities":
        if args.features:
            for word in sorted(FEATURES):
                print(word)
            return 0
        # Bare output stays one integer: sigmond v3.73-v3.76 reads it as
        # int(stdout.strip()).
        from .watermark import schema
        print(schema.SCHEMA_VERSION)
        return 0
    # `migrate` opens the file itself.  Constructing the store would create
    # a missing file and change its mode, and `--check` writes nothing.
    if args.cmd == "migrate":
        return _cmd_migrate(args)
    state_path = args.state or default_path()
    if not state_path.exists() and args.cmd in ("reset-cursor", "kick",
                                                  "purge-dead-letters"):
        print(f"hs-uploader: state file not found: {state_path}", file=sys.stderr)
        return 2
    state_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteWatermarkStore(state_path)
    try:
        return args.func(args, store)
    finally:
        store.close()


# ---- subcommands ----

def _on_real_sinks_dir(sinks: Path) -> bool:
    """True when ``sinks`` is the production switch directory: the
    library's default, or ``sinks/`` beside the production manifest."""
    from .pipelines_d import sinks_dir_for
    real = {Path(sinkswitch.DEFAULT_SINKS_DIR).resolve(),
            sinks_dir_for(_REAL_MANIFEST).resolve()}
    return sinks.resolve() in real


IDENTITY_JSON_CAP = 1 << 20  # bytes; --identity-json refuses anything larger


def _toml_problem(v, path: str) -> tuple[str, str]:
    """(key path, what is wrong) for a value a switch file cannot hold, or
    ("", "") when ``v`` fits.

    The switch writer emits strings, booleans, integers, floats, tables,
    empty arrays and arrays of tables.  Anything else (null, an array of
    values, an empty key) would fail inside ``write_switch`` and blame the
    switch file, so ``sink set`` refuses it first and names the key.
    """
    if isinstance(v, (str, bool, int, float)):
        return "", ""
    if v is None:
        return path, "null has no TOML form"
    if isinstance(v, dict):
        for k, x in v.items():
            if not k:
                return path, "an empty key has no TOML form"
            bad = _toml_problem(x, f"{path}.{k}" if path else k)
            if bad[0] or bad[1]:
                return bad
        return "", ""
    if isinstance(v, list):
        if v and not all(isinstance(x, dict) for x in v):
            return path, "an array of values (only arrays of tables fit)"
        for i, x in enumerate(v):
            bad = _toml_problem(x, f"{path}[{i}]")
            if bad[0] or bad[1]:
                return bad
        return "", ""
    return path, f"a {type(v).__name__} has no TOML form"


def _read_identity(spec: str) -> dict:
    """The identity tables of ``--identity-json``: a JSON object of objects.

    ``-`` reads stdin, so sigmond passes them without a file (ruling 11).
    At most IDENTITY_JSON_CAP bytes are read; more is refused.  Every value
    is checked before anything is written.
    """
    import json
    if spec == "-":
        stream = getattr(sys.stdin, "buffer", sys.stdin)
        raw = stream.read(IDENTITY_JSON_CAP + 1)
    else:
        with open(spec, "rb") as fh:
            raw = fh.read(IDENTITY_JSON_CAP + 1)
    if len(raw) > IDENTITY_JSON_CAP:
        raise ValueError(f"identity JSON is larger than {IDENTITY_JSON_CAP} "
                         "bytes")
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"identity JSON does not parse: {exc}") from None
    if not isinstance(data, dict) or not all(
            isinstance(v, dict) for v in data.values()):
        raise ValueError("identity JSON must be an object of tables, one "
                         "per instance")
    for instance, table in data.items():
        key, what = _toml_problem(table, "")
        if not instance:
            key, what = "", "an empty instance name has no TOML form"
        if what:
            raise ValueError(f"identity JSON: instance {instance!r}, key "
                             f"{key or '(the table)'}: {what}")
    return data


def _cmd_sink_set(args) -> int:
    """``sink set <site|client> <off|fill|upload>`` (spec §9.1)."""
    from .pipelines_d import sinks_dir_for

    def fail(msg: str) -> int:
        print(f"hs-uploader sink set: {msg}", file=sys.stderr)
        return 1

    if args.state not in sinkswitch.STATES:
        return fail(f"state {args.state!r} is not one of "
                    f"{'/'.join(sinkswitch.STATES)}; nothing written")
    if args.name != sinkswitch.SITE and not sinkswitch.valid_client_name(
            args.name):
        return fail(f"{args.name!r} is neither 'site' nor a client name "
                    "([a-z0-9][a-z0-9-]*); nothing written")
    if args.sinks:
        sinks = Path(args.sinks)
    elif args.manifest:
        sinks = sinks_dir_for(args.manifest)
    else:
        sinks = Path(sinkswitch.DEFAULT_SINKS_DIR)
    if _euid() != 0 and _on_real_sinks_dir(sinks):
        return fail(f"{sinks} belongs to root:sigmond; run as root "
                    "(nothing written)")
    change = {"state": args.state, "reason": args.reason, "set_by": args.by}
    if args.identity_json is not None:
        try:
            ident = _read_identity(args.identity_json)
        except (OSError, ValueError) as exc:
            return fail(f"--identity-json: {exc}; nothing written")
        if ident:
            change["identity"] = ident
    path = sinks / f"{args.name}.toml"
    try:
        before = sinkswitch.write_switch(path, change)
    except (OSError, ValueError, TimeoutError) as exc:
        return fail(str(exc))
    print(f"sink set: {args.name} {before} -> {args.state} ({path})")
    return 0


def _cmd_sink_mark_carried(args) -> int:
    """``sink mark-carried``: the carry-over marker, written atomically."""
    marker = Path(args.marker) if args.marker else sinkswitch.marker_path()
    try:
        sinkswitch.write_marker(marker, [args.note] if args.note else [])
    except OSError as exc:
        print(f"hs-uploader sink mark-carried: {marker}: {exc}",
              file=sys.stderr)
        return 1
    print(f"sink mark-carried: {marker}")
    return 0


def _store_rows(path: Path) -> tuple[list[tuple[str, str, str]], str]:
    """(refused ``pipeline_state`` rows, error) from the store.

    Opened read-only (``mode=ro``), so status never creates or changes the
    store.  A missing store is no error (the daemon has not run yet); one
    that exists and cannot be read is.
    """
    import sqlite3
    path = Path(path)
    if not path.exists():
        return [], ""
    conn = None
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro",
                               uri=True, timeout=2.0)
        rows = conn.execute(
            "SELECT name, client, refused_reason FROM pipeline_state "
            "WHERE refused_reason != '' ORDER BY name").fetchall()
    except sqlite3.Error as exc:
        return [], (f"cannot read the store {path}: {type(exc).__name__}: "
                    f"{exc}; refusals the daemon recorded are not shown")
    finally:
        if conn is not None:
            conn.close()
    return rows, ""


def _plain(v) -> str:
    return "" if v is None else str(v)


def _cmd_sink_status(args) -> int:
    """``sink status``: what the switches say and what the gate holds.

    ``held_pipelines`` comes from the daemon's own gate (``SinkGate.decide``)
    on the pipelines the daemon builds from the same files, so it says "held"
    only while the carry-over marker stands.  ``effective`` and ``held_by``
    read the switches alone and do not depend on the marker (ruling X5).
    Status cannot see inside a ``builder`` entry; ``builder_pipelines``
    names those.  The library's log lines stay off stderr: what they would
    say shows in the report itself.
    """
    import logging
    quiet = logging.NullHandler()
    pkg = logging.getLogger("hs_uploader")
    pkg.addHandler(quiet)
    try:
        return _sink_status(args)
    finally:
        pkg.removeHandler(quiet)


def _sink_status(args) -> int:
    import json
    from .daemon import pipe_views
    from .gate import SinkGate, exempt_rule, held_by
    from .pipelines_d import PIPELINES_DIR, load_layout, sinks_dir_for

    sinks = Path(args.sinks) if args.sinks else sinks_dir_for(args.manifest)
    marker = Path(args.marker) if args.marker else sinkswitch.marker_path()
    errors: list[str] = []
    views = []
    refused: dict[tuple, dict] = {}
    read_failed = False
    try:
        layout = load_layout(args.manifest)
        views = pipe_views(layout)
        for r in layout.refused:
            refused[(r.file, None)] = {"file": r.file, "pipeline": None,
                                       "client": r.client, "reason": r.reason}
    except Exception as exc:  # noqa: BLE001 -- report it, exit 2
        read_failed = True
        errors.append(f"cannot read {args.manifest}: {exc}")
        print(f"hs-uploader: cannot read {args.manifest}: {exc}",
              file=sys.stderr)
    # What the daemon refused at its last build wins over this read.  A
    # pipeline refused for the store's schema has no file.
    rows, store_error = _store_rows(args.state or default_path())
    if store_error:
        errors.append(store_error)
    prefix = f"{PIPELINES_DIR}/"
    for name, client, reason in rows:
        if name.startswith(prefix):
            f = name[len(prefix):]
            refused[(f, None)] = {"file": f, "pipeline": None,
                                  "client": client, "reason": reason}
        else:
            refused[("", name)] = {"file": None, "pipeline": name,
                                   "client": client, "reason": reason}

    def file_state(r) -> str:
        return r.state or ("missing" if r.missing else "error")

    names = {v.client for v in views if v.client and exempt_rule(v) == 0}
    has_dir = sinks.is_dir()
    if has_dir:
        for f in sinks.glob("*.toml"):
            if sinkswitch.valid_client_name(f.stem):
                names.add(f.stem)
    s = sinkswitch.read_switch(sinks / f"{sinkswitch.SITE}.toml")
    site = {"state": file_state(s), "error": s.error,
            "set_at": _plain(s.data.get("set_at")),
            "set_by": _plain(s.data.get("set_by")),
            "reason": _plain(s.data.get("reason"))}
    clients = []
    for name in sorted(names):
        e = sinkswitch.effective_state(name, sinks_dir=sinks)
        if sinkswitch.valid_client_name(name):
            fs = file_state(sinkswitch.read_switch(sinks / f"{name}.toml"))
        else:
            fs = "error"       # never form a path from a bad name
        clients.append({
            "client": name, "file_state": fs,
            "effective": e.state, "held_by": held_by(name, e),
            "error": "; ".join(e.errors),
            "pipelines": [v.name for v in views if v.client == name]})
    by_client = {c["client"]: c["held_by"] for c in clients}

    enforced = sinkswitch.marker_stands(marker)
    gate = SinkGate(sinks, marker)
    held = []
    for v in views:
        d = gate.decide(v)
        if not d.send:
            who = by_client.get(v.client) if d.rule == 5 else None
            held.append({"pipeline": v.name, "client": v.client,
                         "held_by": who or "fault", "reason": d.reason})

    no_client = [v.name for v in views if not v.client]
    builders = [v.name for v in views if v.builder]
    refused_files = [refused[k] for k in sorted(refused)]
    rc = 2 if read_failed else 0

    if args.json:
        print(json.dumps({
            "schema": SINK_STATUS_SCHEMA, "enforced": enforced,
            "marker": str(marker), "sinks_dir": str(sinks), "site": site,
            "clients": clients, "held_pipelines": held,
            "refused_files": refused_files,
            "no_client_pipelines": no_client,
            "builder_pipelines": builders, "errors": errors}, indent=2))
        return rc
    print(f"hs-uploader sink status - {sinks}")
    if enforced:
        print(f"  enforced: the carry-over marker {marker} stands")
    else:
        print(f"  not enforced: no carry-over marker at {marker}; "
              "every pipeline sends as its manifest says")
    if not has_dir:
        print(f"  no switch files in {sinks}")
    err = f"  error: {site['error']}" if site["error"] else ""
    print(f"  site: file {site['state']}{err}")
    for c in clients:
        by = f", held by {c['held_by']}" if c["held_by"] else ""
        err = f"  error: {c['error']}" if c["error"] else ""
        print(f"  {c['client']}: file {c['file_state']}, effective "
              f"{c['effective']}{by}{err}")
    for h in held:
        print(f"  held: {h['pipeline']} ({h['client'] or '-'}): {h['reason']}")
    for r in refused_files:
        what = (f"pipelines.d/{r['file']}" if r["file"] is not None
                else f"pipeline {r['pipeline']}")
        print(f"  refused: {what} ({r['client'] or '-'}): {r['reason']}")
    if no_client:
        print(f"  {len(no_client)} pipeline(s) carry no client; no switch "
              f"applies to them: {', '.join(no_client)}")
    if builders:
        print(f"  {len(builders)} builder entr(y/ies) status cannot see "
              f"inside: {', '.join(builders)}")
    for e in errors:
        print(f"  error: {e}")
    return rc


def _cmd_status(args, store: SqliteWatermarkStore) -> int:
    from . import backlog
    cursors = store.all_cursors()
    print(f"hs-uploader status — {store.path}")
    print(f"  {len(cursors)} cursor(s)")
    if cursors:
        for row in cursors:
            new = store.superseded_by(
                row["source_id"], row["dest_id"], row["table_name"])
            mark = f" [SUPERSEDED by {', '.join(new)}]" if new else ""
            print(
                f"    {row['source_id']} → {row['dest_id']} "
                f"({row['table_name']}): last_ack={row['last_ack']} "
                f"cursor_len={row['cursor_len']} bytes{mark}"
            )
    pending = store.deliverable_count()
    dl = store.dead_letter_count()
    print(f"  {pending} deliverable(s) pending retry, {dl} in dead-letter")
    summary = backlog.summarize(store.path)
    orphans = summary.get("orphaned_dead_letters", 0)
    if orphans:
        print(f"  {orphans} of those dead letter(s) belong to a pipeline the "
              f"manifest no longer builds (orphaned)")
    for p in summary.get("pipelines", []):
        if p.get("refused_reason"):
            print(f"  pipeline {p['name']} ({p.get('client') or '?'}) "
                  f"REFUSED: {p['refused_reason']}")
    return 0


def _cmd_peek(args, store: SqliteWatermarkStore) -> int:
    rows = store.recent_attempts(limit=args.limit)
    if not rows:
        print("(no attempts logged yet)")
        return 0
    for row in rows:
        records = row["records"] if row["records"] is not None else "-"
        bytes_ = row["bytes"] if row["bytes"] is not None else "-"
        err = row["error"] or ""
        print(
            f"{row['ts']}  {row['outcome']:12s}  "
            f"{row['source_id']} → {row['dest_id']} ({row['table_name']})  "
            f"records={records} bytes={bytes_}  {err}"
        )
    return 0


def _cmd_reset_cursor(args, store: SqliteWatermarkStore) -> int:
    new = store.superseded_by(args.source, args.dest, args.table)
    if new:
        print(
            f"refusing: {args.source} → {args.dest} ({args.table}) is "
            f"superseded by {', '.join(new)}; reset one of those instead",
            file=sys.stderr,
        )
        return 2
    removed = store.reset_cursor(args.source, args.dest, args.table)
    if removed:
        print(
            f"reset cursor: {args.source} → {args.dest} ({args.table})"
        )
        return 0
    print(
        f"no cursor found for {args.source} → {args.dest} ({args.table})",
        file=sys.stderr,
    )
    return 1


def _cmd_kick(args, store: SqliteWatermarkStore) -> int:
    # Direct SQL — there's no public method for "bump all next_attempt_at"
    # since this is an explicitly operator-driven recovery action.
    with store._lock, store._conn:  # noqa: SLF001
        cur = store._conn.execute(
            "UPDATE deliverables SET next_attempt_at='1970-01-01T00:00:00+00:00'"
        )
        n = cur.rowcount
    print(f"kicked {n} deliverable(s) — next pump will retry them")
    return 0


def _sql_text(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def _cmd_purge_dead_letters(args, store: SqliteWatermarkStore) -> int:
    """Clear dead letters, saving them first.

    A dead letter never leaves the store on its own, and the heartbeat
    reported every one ever written: one bring-up hiccup kept a station
    INVALID for good (2026-10-14: 390 rows on four stations, cleared by a
    hand-typed sqlite3 line on each).  The rows go to a file of INSERTs that
    restores them (``sqlite3 watermarks.db < file``); the DELETE runs only
    after that file is written and synced.
    """
    from datetime import datetime, timezone

    before = None
    if args.before:
        before = datetime.fromisoformat(args.before)
        if before.tzinfo is None:
            before = before.replace(tzinfo=timezone.utc)

    with store._lock:  # noqa: SLF001
        rows = store._conn.execute(  # noqa: SLF001
            "SELECT rowid, ts, pipeline, payload_blob, final_error "
            "FROM dead_letter ORDER BY rowid").fetchall()

    def _matches(row) -> bool:
        if args.pipeline is not None and row["pipeline"] != args.pipeline:
            return False
        if before is None:
            return True
        try:
            t = datetime.fromisoformat(row["ts"])
        except (TypeError, ValueError):
            return False            # an unreadable age is never "old enough"
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t < before

    chosen = [r for r in rows if _matches(r)]
    if not chosen:
        print("nothing to purge")
        return 0

    out_dir = args.backup_dir or Path(store.path).parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = Path(out_dir) / f"dead_letter-{stamp}.sql"
    lines = [
        "INSERT INTO dead_letter(ts, pipeline, payload_blob, final_error) "
        f"VALUES({_sql_text(r['ts'])}, {_sql_text(r['pipeline'])}, "
        f"X'{bytes(r['payload_blob']).hex()}', {_sql_text(r['final_error'])});"
        for r in chosen
    ]
    try:
        with open(backup, "x", encoding="utf-8") as fh:
            fh.write("BEGIN;\n" + "\n".join(lines) + "\nCOMMIT;\n")
            fh.flush()
            import os
            os.fsync(fh.fileno())
    except OSError as exc:
        print(f"hs-uploader: cannot write {backup}: {exc} — nothing deleted",
              file=sys.stderr)
        return 1

    with store._lock, store._conn:  # noqa: SLF001
        store._conn.executemany(  # noqa: SLF001
            "DELETE FROM dead_letter WHERE rowid = ?",
            [(r["rowid"],) for r in chosen])
    print(f"purged {len(chosen)} dead letter(s); saved to {backup}")
    return 0


# `migrate --check` exits this when a real migrate would write: a pending
# migration, a psk-pskreporter retry the pass would delete or move, or a
# crash's journal to recover.  sigmond's manifest step stops the daemon
# and migrates only then (spec-key-split-v373 §5.2 step 3).
MIGRATE_CHECK_PENDING = 3


def _cmd_migrate(args) -> int:
    from .watermark import schema

    path = args.db or args.state or default_path()
    try:
        if not path.exists() and (args.check or os.geteuid() == 0
                                  or not path.parent.is_dir()):
            # The daemon creates the store on its first start, as hsupload.
            # A migrate run as root must not create it first.  A non-root
            # run creates it, stamped current, when its directory exists.
            print(f"watermarks.db: not found at {path}; nothing to migrate")
            return 0
        ctx = schema.MigrateContext(sink_path=args.sink,
                                    keep_retry_for=args.keep_retry_for)
        report = schema.migrate(str(path), check=args.check, ctx=ctx)
    except schema.JournalRecoveryNeeded:
        # Only --check meets this.  Its read-only open cannot roll back
        # what a crash left half written, and it leaves the file alone.
        print("watermarks.db: a crash left a journal to recover; run "
              "hs-uploader migrate without --check (or start the daemon) "
              "to recover it")
        return MIGRATE_CHECK_PENDING
    except Exception as exc:  # noqa: BLE001 -- any failure means exit 1
        print(f"hs-uploader migrate: {path}: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1
    print(f"watermarks.db: version {report.to_version}")
    for name in report.applied:
        print(f"  applied {name}")
    for name in report.pending:
        print(f"  pending {name}")
    for line in report.lines:
        print(f"  {line}")
    if report.to_version > schema.SCHEMA_VERSION:
        print(f"  newer than this hs-uploader, which knows versions up to "
              f"{schema.SCHEMA_VERSION}; left as it stands")
    elif not report.applied and not report.pending and not report.lines:
        print("  nothing to do")
    if args.check and report.would_write:
        return MIGRATE_CHECK_PENDING
    return 0


if __name__ == "__main__":
    sys.exit(main())
