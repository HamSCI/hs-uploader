"""Export the honest upload backlog from a watermark store.

``pending_uploads`` MIN(queued_at)/COUNT(*) on the sigmond sink are
retention measures, not backlog: the wsprdaemon transport never
DELETEs rows, so a 24h trim timer pins those numbers regardless of
whether anything is actually stuck.  The watermark store's
``deliverables`` (retry-queue) and ``dead_letter`` tables — plus its
``watermarks`` (cursor) table — are the only place true backlog
lives.  This module reads them directly, read-only, and exports a
JSON-able summary for external monitoring.

``summarize()`` opens the sqlite file itself via a ``mode=ro`` URI
connection rather than instantiating ``SqliteWatermarkStore`` (whose
constructor runs ``CREATE TABLE IF NOT EXISTS`` and a best-effort
``chmod`` — both writes).  It queries the store's own tables
directly, so it works standalone against a bare copy of the db file
with no schema-init side effects and no write locks taken.

Failure handling is deliberately paranoid: on ANY error — missing
file, permission error, a directory at the path, a sqlite error, a
schema that doesn't look like a watermark store — ``summarize()``
returns ``{"readable": False, "reason": "<...>"}`` and never raises
and never fabricates zeros.  A monitoring consumer must be able to
tell "the store could not be read" apart from "the store was read
and genuinely has no backlog" (``{"readable": True, "pipelines": [],
"cursors": []}``); collapsing the two into the same zero is the exact
anti-pattern this module exists to kill.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

__all__ = ["summarize", "DEAD_LETTER_RECENT"]

# A dead letter never leaves the store on its own, so a bare count kept a
# station INVALID for good after one bring-up hiccup (K3LR, 2026-10-14: 376
# rows from 09-30..10-08), and that verdict hid a live fault behind it.  Rows
# younger than this count as RECENT; the consumer decides what history means.
DEAD_LETTER_RECENT = timedelta(hours=24)


def _is_recent(ts: str, cutoff: datetime) -> bool:
    """True when ``ts`` falls inside the window -- or cannot be read.  A row
    whose age we cannot tell is a live fault, never history."""
    try:
        t = datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return True
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t >= cutoff


def _optional_rows(conn: sqlite3.Connection, sql: str) -> list:
    """Rows of ``sql``, or [] when its table does not exist yet."""
    try:
        return list(conn.execute(sql))
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []
        raise


def summarize(watermark_db_path: Union[str, Path],
              now: Optional[datetime] = None) -> dict:
    """Summarize deliverable/dead-letter backlog + cursors, read-only.

    Returns on success::

        {"readable": True,
         "pipelines": [{"name": <pipeline>, "deliverable_count": int,
                         "dead_letter_count": int,     # rows = attempts
                         "dead_letter_recent_count": int,
                         "dead_letter_datasets": int,  # distinct payloads
                         "dead_letter_recent_datasets": int,
                         "dead_letter_newest": str | None,
                         "client": str, "refused_reason": str}, ...],
         "cursors": [{"source_id": ..., "dest_id": ..., "table_name": ...,
                       "last_ack": ..., "cursor_len": int,
                       "superseded_by": [new source ids; [] when live]}, ...],
         "orphaned_dead_letters": int,
         "last_send": [{"client": str, "pipeline": str, "outcome": str,
                         "ts": str, "records": int | None}, ...]}

    ``client`` and ``refused_reason`` come from ``pipeline_state`` (what the
    daemon knew at its last start; "" when the table is absent or has no row
    for the name).  Every pipeline in ``pipeline_state`` appears, idle or not.
    ``orphaned_dead_letters`` counts dead letters whose pipeline the current
    manifest no longer builds (no ``pipeline_state`` row); it is 0 when
    ``pipeline_state`` holds no rows, since orphans cannot then be told.
    ``last_send`` is top-level only; each per-pipeline dict keeps its keys.
    It is [] on a version-1 or version-2 store (spec 4.4).
    ``superseded_by`` reads ``superseded_keys`` (spec 4.5).

    ``pipelines`` is derived from the distinct pipeline names present
    in the store's own ``deliverables`` and ``dead_letter`` tables (a
    union of both) — not from any external manifest — so a pipeline
    with only dead-letters (and no queued deliverables), or vice
    versa, still appears with the other count at 0. A pipeline with
    neither (e.g. one that has only ever shipped cleanly) does not
    appear at all; the ``cursors`` list is where its shipping state
    lives.

    ``cursors`` mirrors ``SqliteWatermarkStore.all_cursors()``
    exactly: same columns (``source_id``, ``dest_id``, ``table_name``,
    ``last_ack``, ``cursor_len``), same field names, same query. Do
    not treat this as a schema to hand-invent — it is read directly
    from ``sqlite.py``'s own query. ``last_ack`` is returned exactly
    as the store persists it: an ISO8601 string (see
    ``SqliteWatermarkStore.advance_cursor``'s ``last_ack`` param);
    this function does not parse or reformat it. ``cursor_len`` is
    the byte length of the opaque cursor blob, not the cursor itself.

    On any failure returns ``{"readable": False, "reason":
    "<ExceptionClassName>: <message>"}`` and never raises. This
    includes: missing file, a directory at the path, permission
    errors, sqlite corruption, and a file that opens but doesn't have
    the expected watermark-store schema (missing tables). An empty
    but well-formed store (freshly constructed
    ``SqliteWatermarkStore``, nothing shipped yet) is NOT a failure —
    it returns ``readable: True`` with empty lists. Callers must
    treat ``readable: False`` as "unknown", never as "zero backlog".
    """
    path = str(watermark_db_path)
    try:
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            conn.row_factory = sqlite3.Row

            cursors = [
                dict(row)
                for row in conn.execute(
                    "SELECT source_id, dest_id, table_name, last_ack, "
                    "length(cursor) AS cursor_len FROM watermarks "
                    "ORDER BY source_id, dest_id, table_name"
                )
            ]
            deliverable_counts = {
                row["pipeline"]: row["n"]
                for row in conn.execute(
                    "SELECT pipeline, COUNT(*) AS n FROM deliverables "
                    "GROUP BY pipeline"
                )
            }
            # ⛔ COUNT THE DAMAGE, NOT THE RETRY CADENCE.  A dataset that
            # is dead-lettered, re-queued and dead-lettered again adds a row
            # each time, so the raw count measures how often we tried, not
            # how much is lost.  Measured on AC0G/B4 2026-10-27: 82 dead
            # letters covering TWO datasets — one day's mag zip and one
            # day's GRAPE — reported as "78 failed for good".
            #
            # `dataset_id` is the first rowid that carried this payload in
            # this pipeline, so every re-dead-lettering of the same thing
            # collapses onto it.  Done as a correlated subquery so the
            # blobs stay in SQLite and are never transferred.
            #
            # The `d2.pipeline = d1.pipeline` predicate cannot change any
            # per-pipeline count — the ids are only ever grouped within one
            # pipeline, so it is an equivalent mutant and no test covers it.
            # Keep it anyway: it makes the id mean "this dataset, here", so
            # a later caller may union ids across pipelines without the
            # same payload in two pipelines silently counting once.
            dead_rows = list(conn.execute(
                "SELECT pipeline, ts, ("
                "  SELECT MIN(d2.rowid) FROM dead_letter d2"
                "   WHERE d2.pipeline = d1.pipeline"
                "     AND d2.payload_blob = d1.payload_blob"
                ") AS dataset_id FROM dead_letter d1"))
            # The two tables of spec 4.5 are absent on a version-1 store;
            # that is a normal state, not an unreadable one.
            superseded = _optional_rows(
                conn, "SELECT old_source_id, old_dest_id, old_table, "
                      "new_source_id FROM superseded_keys")
            states = _optional_rows(
                conn, "SELECT name, client, refused_reason "
                      "FROM pipeline_state ORDER BY name")
            # Spec 4.4: absent on a version-1 store, empty on version 2.
            last_send = [dict(r) for r in _optional_rows(
                conn, "SELECT client, pipeline, outcome, ts, records "
                      "FROM last_send ORDER BY client, pipeline")]
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — deliberately catch-all
        return {"readable": False, "reason": f"{type(exc).__name__}: {exc}"}

    cutoff = (now or datetime.now(timezone.utc)) - DEAD_LETTER_RECENT
    dead_letter_counts: dict = {}
    dead_letter_recent: dict = {}
    dead_letter_newest: dict = {}
    dead_sets: dict = {}
    dead_sets_recent: dict = {}
    for row in dead_rows:
        name, ts = row["pipeline"], row["ts"]
        did = row["dataset_id"]
        dead_letter_counts[name] = dead_letter_counts.get(name, 0) + 1
        dead_sets.setdefault(name, set()).add(did)
        if _is_recent(ts, cutoff):
            dead_letter_recent[name] = dead_letter_recent.get(name, 0) + 1
            dead_sets_recent.setdefault(name, set()).add(did)
        if name not in dead_letter_newest or ts > dead_letter_newest[name]:
            dead_letter_newest[name] = ts
    dead_letter_datasets = {k: len(v) for k, v in dead_sets.items()}
    dead_letter_recent_datasets = {k: len(v) for k, v in dead_sets_recent.items()}

    successors: dict = {}
    for row in superseded:
        key = (row["old_source_id"], row["old_dest_id"], row["old_table"])
        successors.setdefault(key, set()).add(row["new_source_id"])
    for cur in cursors:
        key = (cur["source_id"], cur["dest_id"], cur["table_name"])
        cur["superseded_by"] = sorted(successors.get(key, ()))

    known = {row["name"]: row for row in states}
    pipeline_names = sorted(
        set(deliverable_counts) | set(dead_letter_counts) | set(known))
    # With no pipeline_state rows (a version-1 store, or a daemon that has
    # not started since the upgrade) "orphaned" cannot be told; count none.
    orphaned = sum(n for name, n in dead_letter_counts.items()
                   if name not in known) if known else 0
    pipelines = [
        {
            "name": name,
            "client": known[name]["client"] if name in known else "",
            "refused_reason": (known[name]["refused_reason"]
                               if name in known else ""),
            "deliverable_count": deliverable_counts.get(name, 0),
            "dead_letter_count": dead_letter_counts.get(name, 0),
            "dead_letter_recent_count": dead_letter_recent.get(name, 0),
            # distinct datasets, not attempts — see the query above
            "dead_letter_datasets": dead_letter_datasets.get(name, 0),
            "dead_letter_recent_datasets":
                dead_letter_recent_datasets.get(name, 0),
            "dead_letter_newest": dead_letter_newest.get(name),
        }
        for name in pipeline_names
    ]
    return {"readable": True, "pipelines": pipelines, "cursors": cursors,
            "orphaned_dead_letters": orphaned, "last_send": last_send}


def _main(argv: "list[str] | None" = None) -> int:
    """``python -m hs_uploader.backlog <db-path>`` — print JSON, exit 0.

    Prints exactly ``json.dumps(summarize(path))`` to stdout (nothing
    else on stdout) and always exits 0 — an unreadable store is a
    valid, complete answer for the caller (sigmond's heartbeat
    assembler subprocesses this), not a process failure. This module
    is stdlib-only by design so it can be invoked with nothing but
    ``PYTHONPATH=<checkout>/src`` and no venv.
    """
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print(json.dumps({
            "readable": False,
            "reason": "ValueError: usage: python -m hs_uploader.backlog <db-path>",
        }))
        return 0
    print(json.dumps(summarize(argv[0])))
    return 0


if __name__ == "__main__":
    sys.exit(_main())
