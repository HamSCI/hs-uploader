"""backlog.summarize() — the honest upload-backlog export.

``pending_uploads`` MIN(queued_at)/COUNT(*) on the sigmond sink are
retention measures, not backlog (wsprdaemon's transport never
DELETEs; a 24h trim timer pins them).  The watermark store's
``deliverable_count`` / ``dead_letter_count`` / ``all_cursors`` are
the only true backlog measures.  ``backlog.summarize()`` exports them
as a stable, read-only JSON-able dict for a monitoring assembler that
must never mistake "I could not read the store" for "there is no
backlog."

Each test opens its own ``SqliteWatermarkStore`` fixture, closes it,
then calls ``summarize()`` against the resulting file — mirroring the
pattern in ``test_cli.py`` and ``test_watermark_sqlite.py``.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

from hs_uploader.backlog import summarize
from hs_uploader.watermark import SqliteWatermarkStore

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = str(REPO_ROOT / "src")


def _seed_populated(path: Path) -> None:
    store = SqliteWatermarkStore(path)
    try:
        store._write_cursor(
            "ch:wspr.spots", "wsprnet", "wspr.spots",
            cursor=b'{"time":"2026-05-08","tiebreak":"42"}',
            last_ack="2026-05-08T12:00:00+00:00",
        )
        store._write_cursor(
            "ch:psk.spots", "pskreporter", "psk.spots",
            cursor=b"cursor-bytes",
            last_ack="2026-05-09T00:00:00+00:00",
        )
        # Two queued deliverables for the wsprnet pipeline.
        for i in range(2):
            store.enqueue_deliverable(
                pipeline="wsprnet",
                payload_blob=f"payload-{i}".encode(),
                enqueued_at="2026-05-08T12:00:00+00:00",
                next_attempt_at="2026-05-08T12:05:00+00:00",
            )
        # One dead-letter for a different pipeline (pskreporter) that
        # has no queued deliverables of its own.
        store.send_to_dead_letter(
            ts="2026-05-08T13:00:00+00:00",
            pipeline="pskreporter",
            payload_blob=b"dead payload",
            final_error="server returned 500",
        )
    finally:
        store.close()


def _seed_empty(path: Path) -> None:
    SqliteWatermarkStore(path).close()


# ---- populated store ----------------------------------------------------


def test_populated_store_counts_per_pipeline(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)

    result = summarize(db)

    assert result["readable"] is True
    by_name = {p["name"]: p for p in result["pipelines"]}
    assert by_name["wsprnet"]["deliverable_count"] == 2
    assert by_name["wsprnet"]["dead_letter_count"] == 0
    assert by_name["pskreporter"]["deliverable_count"] == 0
    assert by_name["pskreporter"]["dead_letter_count"] == 1


def test_populated_store_cursors_mirror_all_cursors(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)
    store = SqliteWatermarkStore(db)
    try:
        expected = [dict(row) for row in store.all_cursors()]
    finally:
        store.close()

    result = summarize(db)

    assert result["readable"] is True
    assert result["cursors"] == expected
    # Field names are exactly what all_cursors() returns — no
    # invented "table" key, etc.
    for row in result["cursors"]:
        assert set(row) == {
            "source_id", "dest_id", "table_name", "last_ack", "cursor_len",
        }


# ---- empty store ----------------------------------------------------------


def test_empty_store_is_readable_with_empty_lists(tmp_path):
    db = tmp_path / "wm.db"
    _seed_empty(db)

    result = summarize(db)

    assert result == {"readable": True, "pipelines": [], "cursors": []}


# ---- failure modes ---------------------------------------------------------


def test_missing_file_is_unreadable_with_reason(tmp_path):
    db = tmp_path / "does-not-exist.db"

    result = summarize(db)

    assert result["readable"] is False
    assert "reason" in result
    assert isinstance(result["reason"], str) and result["reason"]
    assert "pipelines" not in result
    assert "cursors" not in result


def test_chmod_000_file_is_unreadable(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)
    os.chmod(db, 0o000)
    try:
        if os.access(db, os.R_OK):
            # Running as root (or some sandboxing) makes chmod 000
            # toothless — nothing meaningful to assert here.
            return
        result = summarize(db)
        assert result["readable"] is False
        assert "reason" in result
    finally:
        os.chmod(db, 0o644)


def test_directory_at_path_is_unreadable(tmp_path):
    db = tmp_path / "wm.db"
    db.mkdir()

    result = summarize(db)

    assert result["readable"] is False
    assert "reason" in result


def test_schema_mismatch_is_unreadable(tmp_path):
    import sqlite3

    db = tmp_path / "wm.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE unrelated (x INTEGER)")
    conn.commit()
    conn.close()

    result = summarize(db)

    assert result["readable"] is False
    assert "reason" in result


# ---- never mutates ----------------------------------------------------------


def test_summarize_never_mutates_file(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)
    before = db.read_bytes()

    summarize(db)

    after = db.read_bytes()
    assert before == after


def test_summarize_on_empty_store_never_mutates_file(tmp_path):
    db = tmp_path / "wm.db"
    _seed_empty(db)
    before = db.read_bytes()

    summarize(db)

    after = db.read_bytes()
    assert before == after


# ---- __main__ CLI (addendum) -----------------------------------------------


def _run_cli(args, env_extra=None):
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "hs_uploader.backlog", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_cli_populated_db_prints_json_readable_true(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)

    proc = _run_cli([str(db)])

    assert proc.returncode == 0
    payload = json.loads(proc.stdout)
    assert payload["readable"] is True
    assert proc.stdout.strip().count("\n") == 0  # exactly one JSON line


def test_cli_missing_db_exits_zero_readable_false(tmp_path):
    db = tmp_path / "does-not-exist.db"

    proc = _run_cli([str(db)])

    assert proc.returncode == 0
    payload = json.loads(proc.stdout)
    assert payload["readable"] is False
    assert "reason" in payload


# ── dead-letter age (sigmond v3.72 #1) ───────────────────────────────────
# A dead letter never leaves the store on its own, so a bare count kept a
# station INVALID for good after one bring-up hiccup (K3LR: 376 rows from
# 09-30..10-08 still condemning it on 10-14), and that verdict hid a real
# fault behind it (B4's RX888 off the bus for 10 h on 10-14).  summarize()
# therefore says how many are RECENT and when the newest arrived, so the
# consumer can tell a live fault from history.

from datetime import datetime, timedelta, timezone

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def _seed_dead(path: Path, *stamps: str, pipeline: str = "heartbeat") -> None:
    store = SqliteWatermarkStore(path)
    try:
        for ts in stamps:
            store.send_to_dead_letter(ts=ts, pipeline=pipeline,
                                      payload_blob=b"x", final_error="denied")
    finally:
        store.close()


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _pipe(result: dict, name: str = "heartbeat") -> dict:
    return {p["name"]: p for p in result["pipelines"]}[name]


def test_old_dead_letters_are_not_recent(tmp_path):
    db = tmp_path / "wm.db"
    old = _iso(NOW - timedelta(days=3))
    _seed_dead(db, old)

    p = _pipe(summarize(db, now=NOW))

    assert p["dead_letter_count"] == 1
    assert p["dead_letter_recent_count"] == 0
    assert p["dead_letter_newest"] == old


def test_dead_letter_inside_the_window_is_recent(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, _iso(NOW - timedelta(hours=1)))

    assert _pipe(summarize(db, now=NOW))["dead_letter_recent_count"] == 1


def test_window_edge_is_twenty_four_hours(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, _iso(NOW - timedelta(hours=23, minutes=59)),
               _iso(NOW - timedelta(hours=24, minutes=1)))

    p = _pipe(summarize(db, now=NOW))

    assert p["dead_letter_count"] == 2
    assert p["dead_letter_recent_count"] == 1


def test_mixed_ages_report_the_newest(tmp_path):
    db = tmp_path / "wm.db"
    newest = _iso(NOW - timedelta(minutes=5))
    _seed_dead(db, _iso(NOW - timedelta(days=9)), newest,
               _iso(NOW - timedelta(days=2)))

    p = _pipe(summarize(db, now=NOW))

    assert p["dead_letter_count"] == 3
    assert p["dead_letter_recent_count"] == 1
    assert p["dead_letter_newest"] == newest


def test_unparseable_stamp_counts_as_recent(tmp_path):
    # Never greener than the truth: a row whose age we cannot read is
    # treated as a live fault, not as history.
    db = tmp_path / "wm.db"
    _seed_dead(db, "not-a-time")

    assert _pipe(summarize(db, now=NOW))["dead_letter_recent_count"] == 1


def test_pipeline_without_dead_letters_reports_zero_recent(tmp_path):
    db = tmp_path / "wm.db"
    _seed_populated(db)

    p = _pipe(summarize(db, now=NOW), "wsprnet")

    assert p["dead_letter_recent_count"] == 0
    assert p["dead_letter_newest"] is None
