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


def summarize(watermark_db_path: Union[str, Path],
              now: Optional[datetime] = None) -> dict:
    """Summarize deliverable/dead-letter backlog + cursors, read-only.

    Returns on success::

        {"readable": True,
         "pipelines": [{"name": <pipeline>, "deliverable_count": int,
                         "dead_letter_count": int,
                         "dead_letter_recent_count": int,
                         "dead_letter_newest": str | None}, ...],
         "cursors": [{"source_id": ..., "dest_id": ..., "table_name": ...,
                       "last_ack": ..., "cursor_len": int}, ...]}

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
            dead_rows = list(conn.execute(
                "SELECT pipeline, ts FROM dead_letter"))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — deliberately catch-all
        return {"readable": False, "reason": f"{type(exc).__name__}: {exc}"}

    cutoff = (now or datetime.now(timezone.utc)) - DEAD_LETTER_RECENT
    dead_letter_counts: dict = {}
    dead_letter_recent: dict = {}
    dead_letter_newest: dict = {}
    for row in dead_rows:
        name, ts = row["pipeline"], row["ts"]
        dead_letter_counts[name] = dead_letter_counts.get(name, 0) + 1
        if _is_recent(ts, cutoff):
            dead_letter_recent[name] = dead_letter_recent.get(name, 0) + 1
        if name not in dead_letter_newest or ts > dead_letter_newest[name]:
            dead_letter_newest[name] = ts

    pipeline_names = sorted(set(deliverable_counts) | set(dead_letter_counts))
    pipelines = [
        {
            "name": name,
            "deliverable_count": deliverable_counts.get(name, 0),
            "dead_letter_count": dead_letter_counts.get(name, 0),
            "dead_letter_recent_count": dead_letter_recent.get(name, 0),
            "dead_letter_newest": dead_letter_newest.get(name),
        }
        for name in pipeline_names
    ]
    return {"readable": True, "pipelines": pipelines, "cursors": cursors}


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
