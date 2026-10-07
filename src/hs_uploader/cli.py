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
* ``hs-uploader migrate [--db PATH] [--check]`` — bring watermarks.db
  to this release's schema version (``PRAGMA user_version``).
  ``--check`` reports the version and what would run, and writes
  nothing.  Exit 0 done or nothing to do, 1 failure, 2 usage error.

Phase 1 is read-mostly: ``pump`` is **not** wired up here because there
are no transports yet.  It will land in Phase 2 once
``WsprdaemonTarSftp`` exists.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .watermark.sqlite import SqliteWatermarkStore, default_path


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
        help="Report the version and the pending migrations; write nothing.",
    )
    sp_migrate.set_defaults(func=None)

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
    # `migrate` opens the file itself.  Constructing the store would create
    # a missing file and change its mode, and `--check` writes nothing.
    if args.cmd == "migrate":
        return _cmd_migrate(args)
    state_path = args.state or default_path()
    if not state_path.exists() and args.cmd in ("reset-cursor", "kick"):
        print(f"hs-uploader: state file not found: {state_path}", file=sys.stderr)
        return 2
    state_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteWatermarkStore(state_path)
    try:
        return args.func(args, store)
    finally:
        store.close()


# ---- subcommands ----


def _cmd_status(args, store: SqliteWatermarkStore) -> int:
    cursors = store.all_cursors()
    print(f"hs-uploader status — {store.path}")
    print(f"  {len(cursors)} cursor(s)")
    if cursors:
        for row in cursors:
            print(
                f"    {row['source_id']} → {row['dest_id']} "
                f"({row['table_name']}): last_ack={row['last_ack']} "
                f"cursor_len={row['cursor_len']} bytes"
            )
    pending = store.deliverable_count()
    dl = store.dead_letter_count()
    print(f"  {pending} deliverable(s) pending retry, {dl} in dead-letter")
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


def _cmd_migrate(args) -> int:
    from .watermark import schema

    path = args.db or args.state or default_path()
    if not path.exists():
        # The daemon creates the store on its first start, as hsupload.  A
        # migrate run as root must not create it first.
        print(f"watermarks.db: not found at {path}; nothing to migrate")
        return 0
    try:
        report = schema.migrate(str(path), check=args.check)
    except schema.JournalRecoveryNeeded:
        # Only --check meets this.  Its read-only open cannot roll back
        # what a crash left half written, and it leaves the file alone.
        print("watermarks.db: a crash left a journal to recover; run "
              "hs-uploader migrate without --check (or start the daemon) "
              "to recover it")
        return 0
    except Exception as exc:  # noqa: BLE001 -- any failure means exit 1
        print(f"hs-uploader migrate: {path}: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1
    print(f"watermarks.db: version {report.to_version}")
    for name in report.applied:
        print(f"  applied {name}")
    for name in report.pending:
        print(f"  pending {name}")
    if report.to_version > schema.SCHEMA_VERSION:
        print(f"  newer than this hs-uploader, which knows versions up to "
              f"{schema.SCHEMA_VERSION}; left as it stands")
    elif not report.applied and not report.pending:
        print("  nothing to do")
    return 0


if __name__ == "__main__":
    sys.exit(main())
