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
    # all_cursors() columns, plus the spec 4.5 marker (empty: nothing superseded)
    assert result["cursors"] == [dict(c, superseded_by=[]) for c in expected]
    # Field names are exactly what all_cursors() returns — no
    # invented "table" key, etc.
    for row in result["cursors"]:
        assert set(row) == {
            "source_id", "dest_id", "table_name", "last_ack", "cursor_len",
            "superseded_by",
        }


# ---- empty store ----------------------------------------------------------


def test_empty_store_is_readable_with_empty_lists(tmp_path):
    db = tmp_path / "wm.db"
    _seed_empty(db)

    result = summarize(db)

    assert result == {"readable": True, "pipelines": [], "cursors": [],
                      "orphaned_dead_letters": 0, "last_send": []}


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


# ---- Task 6: superseded keys, pipeline clients and refusals (spec 4.5) ----

import sqlite3

from hs_uploader.cli import main as cli_main


def _supersede(store, old, new_ids):
    for n in new_ids:
        store._conn.execute(
            "INSERT INTO superseded_keys VALUES (?,?,?,?,?,?,?)",
            (old, "pskreporter", "psk.spots", n, "pskreporter", "psk.spots",
             "2026-10-19T00:00:00+00:00"))
    store._conn.commit()


def test_summarize_marks_superseded_cursor(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._write_cursor("sqlite:psk.spots", "pskreporter", "psk.spots",
                    cursor=b"c", last_ack="2026-10-01T00:00:00+00:00")
    s._write_cursor("sqlite:psk.spots@psk-recorder", "pskreporter", "psk.spots",
                    cursor=b"c", last_ack="2026-10-01T00:00:00+00:00")
    _supersede(s, "sqlite:psk.spots",
               ["sqlite:psk.spots@psk-recorder", "sqlite:psk.spots@meteor-scatter"])
    s.close()
    cur = {c["source_id"]: c for c in summarize(db)["cursors"]}
    assert cur["sqlite:psk.spots"]["superseded_by"] == [
        "sqlite:psk.spots@meteor-scatter", "sqlite:psk.spots@psk-recorder"]
    assert cur["sqlite:psk.spots@psk-recorder"]["superseded_by"] == []


def test_summarize_reports_pipeline_client_and_refusal(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s.record_pipeline_states([("grape", "grape", ""),
                              ("psk-recorder-pskreporter", "psk-recorder",
                               "store is version 1; needs 2")])
    s.close()
    out = summarize(db)
    by = {p["name"]: p for p in out["pipelines"]}
    assert by["grape"]["client"] == "grape" and by["grape"]["refused_reason"] == ""
    assert by["psk-recorder-pskreporter"]["client"] == "psk-recorder"
    assert "version 1" in by["psk-recorder-pskreporter"]["refused_reason"]
    assert by["grape"]["dead_letter_count"] == 0


def test_summarize_counts_orphaned_dead_letters(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s.record_pipeline_states([("live", "grape", "")])
    for name in ("live", "psk-pskreporter", "psk-pskreporter"):
        s.send_to_dead_letter(ts="2026-05-08T13:00:00+00:00", pipeline=name,
                              payload_blob=b"x", final_error="e")
    s.close()
    assert summarize(db)["orphaned_dead_letters"] == 2


def test_summarize_version1_store_without_new_tables(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._write_cursor("a", "b", "t", cursor=b"c", last_ack="x")
    s.send_to_dead_letter(ts="2026-05-08T13:00:00+00:00", pipeline="p",
                          payload_blob=b"x", final_error="e")
    s._conn.execute("DROP TABLE superseded_keys")
    s._conn.execute("DROP TABLE pipeline_state")
    s._conn.commit()
    s.close()
    os.chmod(db, 0o444)
    out = summarize(db)
    assert out["readable"] is True
    assert out["cursors"][0]["superseded_by"] == []
    assert out["orphaned_dead_letters"] == 0
    assert out["pipelines"][0]["client"] == ""
    conn = sqlite3.connect(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    conn.close()
    assert "pipeline_state" not in tables and "superseded_keys" not in tables


def test_status_marks_superseded_and_orphans(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._write_cursor("sqlite:psk.spots", "pskreporter", "psk.spots",
                    cursor=b"c", last_ack="2026-10-01T00:00:00+00:00")
    _supersede(s, "sqlite:psk.spots", ["sqlite:psk.spots@psk-recorder"])
    s.record_pipeline_states([("live", "grape", ""),
                              ("psk-recorder-pskreporter", "psk-recorder",
                               "needs store version 2")])
    s.send_to_dead_letter(ts="2026-05-08T13:00:00+00:00",
                          pipeline="psk-pskreporter", payload_blob=b"x",
                          final_error="e")
    s.close()
    assert cli_main(["--state", str(db), "status"]) == 0
    out = capsys.readouterr().out
    assert "SUPERSEDED by sqlite:psk.spots@psk-recorder" in out
    assert "1 of those dead letter(s)" in out and "orphaned" in out
    assert "psk-recorder-pskreporter" in out and "REFUSED" in out


def test_reset_cursor_refuses_superseded_key(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._write_cursor("sqlite:psk.spots", "pskreporter", "psk.spots",
                    cursor=b"c", last_ack="2026-10-01T00:00:00+00:00")
    _supersede(s, "sqlite:psk.spots",
               ["sqlite:psk.spots@psk-recorder", "sqlite:psk.spots@meteor-scatter"])
    s.close()
    rc = cli_main(["--state", str(db), "reset-cursor", "--source", "sqlite:psk.spots", "--dest",
                   "pskreporter", "--table", "psk.spots"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "sqlite:psk.spots@psk-recorder" in err
    assert "sqlite:psk.spots@meteor-scatter" in err
    s = SqliteWatermarkStore(db)
    assert len(s.all_cursors()) == 1   # nothing was removed
    s.close()


# --- spec 4.4, test 14: last_send at the top level ---------------------------

V376_PIPELINE_KEYS = {
    "name", "client", "refused_reason", "deliverable_count",
    "dead_letter_count", "dead_letter_recent_count", "dead_letter_newest",
}


def _seed_last_send(db):
    s = SqliteWatermarkStore(db)
    s.record_pipeline_states([("grape", "grape", "")])
    s.record_attempt(ts="2026-10-20T01:02:03+00:00", source_id="a",
                     dest_id="b", table="t", outcome="acked", records=7,
                     bytes_=1, error=None, pipeline="grape", client="grape")
    s.close()


def test_last_send_is_top_level_and_pipeline_keys_are_unchanged(tmp_path):
    db = tmp_path / "wm.db"
    _seed_last_send(db)
    out = summarize(db)
    assert out["last_send"] == [{
        "client": "grape", "pipeline": "grape", "outcome": "acked",
        "ts": "2026-10-20T01:02:03+00:00", "records": 7}]
    assert out["pipelines"]
    for p in out["pipelines"]:
        assert set(p) == V376_PIPELINE_KEYS


def test_last_send_reads_empty_on_a_version_2_store(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._conn.execute("PRAGMA user_version = 2")
    s.close()
    s = SqliteWatermarkStore(db)
    assert s._v3 is False
    s.record_attempt(ts="2026-10-20T01:02:03+00:00", source_id="a",
                     dest_id="b", table="t", outcome="acked", records=7,
                     bytes_=1, error=None, pipeline="grape", client="grape")
    s.close()
    out = summarize(db)
    assert out["readable"] is True and out["last_send"] == []


def test_last_send_reads_empty_when_the_table_is_absent(tmp_path):
    db = tmp_path / "wm.db"
    s = SqliteWatermarkStore(db)
    s._conn.execute("DROP TABLE last_send")
    s._conn.commit()
    s.close()
    assert summarize(db)["last_send"] == []
