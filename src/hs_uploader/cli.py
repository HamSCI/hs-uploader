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

    sp_hash = sub.add_parser(
        "schema-hash",
        help="Print the column-hash for a CH table — used to populate "
             "schema._BUILTINS when a producer locks a new migration.",
    )
    sp_hash.add_argument("database")
    sp_hash.add_argument("table")
    sp_hash.set_defaults(func=_cmd_schema_hash)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    # schema-hash is a CH-only utility — doesn't touch the watermark
    # store.  Skip the SQLite open so it works on hosts that have only
    # the ClickHouse env vars configured.
    if args.cmd == "schema-hash":
        return _cmd_schema_hash(args, None)
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


def _cmd_schema_hash(args, _store) -> int:
    """Connect to CH (env-driven) and print the canonical column-hash.

    Output format: ``<hash>  v<version> | unknown`` where ``<version>``
    is the registered version if any, ``unknown`` otherwise.  Use this
    to populate ``hs_uploader.schema._BUILTINS`` when a producer locks
    a new migration.
    """
    from . import schema as schema_registry
    from .sources.clickhouse import _ConnectionConfig, _default_client_factory

    cfg = _ConnectionConfig.from_env()
    if cfg is None:
        print(
            "hs-uploader schema-hash: SIGMOND_CLICKHOUSE_URL unset",
            file=sys.stderr,
        )
        return 2
    try:
        client = _default_client_factory(cfg)
    except Exception as exc:  # noqa: BLE001
        print(f"hs-uploader schema-hash: connect failed: {exc}", file=sys.stderr)
        return 1
    column_hash = schema_registry.compute_column_hash(
        client, args.database, args.table,
    )
    if not column_hash:
        print(
            f"hs-uploader schema-hash: {args.database}.{args.table} not found",
            file=sys.stderr,
        )
        return 1
    version = schema_registry.version_for_hash(
        f"{args.database}.{args.table}", column_hash,
    )
    label = f"v{version}" if version is not None else "unknown"
    print(f"{column_hash}  {label}")
    return 0


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


if __name__ == "__main__":
    sys.exit(main())
