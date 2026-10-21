"""hs-uploader CLI: status / peek / reset-cursor / kick.

Each test invokes ``cli.main(argv)`` against a tmp watermark store.
``capsys`` captures the human-readable output.
"""

from __future__ import annotations

from pathlib import Path

from hs_uploader.cli import main
from hs_uploader.watermark import SqliteWatermarkStore


def _seed_store(path: Path) -> SqliteWatermarkStore:
    store = SqliteWatermarkStore(path)
    store._write_cursor(
        "ch:wspr.spots", "wsprnet", "wspr.spots",
        cursor=b'{"time":"2026-05-08","tiebreak":"42"}',
        last_ack="2026-05-08T12:00:00+00:00",
    )
    store.record_attempt(
        ts="2026-05-08T12:00:00+00:00",
        source_id="ch:wspr.spots", dest_id="wsprnet", table="wspr.spots",
        outcome="acked", records=999, bytes_=12345, error=None,
    )
    return store


def test_status_shows_cursor(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = _seed_store(db); s.close()
    rc = main(["--state", str(db), "status"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "ch:wspr.spots" in out
    assert "wsprnet" in out
    assert "0 deliverable(s) pending retry" in out


def test_peek_shows_attempts(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = _seed_store(db); s.close()
    rc = main(["--state", str(db), "peek", "--limit", "10"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "acked" in out
    assert "ch:wspr.spots" in out
    assert "records=999" in out


def test_reset_cursor_drops_row(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = _seed_store(db); s.close()
    rc = main([
        "--state", str(db),
        "reset-cursor",
        "--source", "ch:wspr.spots",
        "--dest", "wsprnet",
        "--table", "wspr.spots",
    ])
    assert rc == 0
    # Verify the row is gone.
    s2 = SqliteWatermarkStore(db)
    assert s2.get_cursor("ch:wspr.spots", "wsprnet", "wspr.spots") == b""


def test_reset_cursor_unknown_returns_1(tmp_path, capsys):
    db = tmp_path / "wm.db"
    s = _seed_store(db); s.close()
    rc = main([
        "--state", str(db),
        "reset-cursor",
        "--source", "no-such-source",
        "--dest", "no-such-dest",
        "--table", "no-such-table",
    ])
    assert rc == 1


def test_kick_bumps_deliverables(tmp_path, capsys):
    db = tmp_path / "wm.db"
    store = _seed_store(db)
    store.enqueue_deliverable(
        pipeline="p", payload_blob=b"x",
        enqueued_at="2026-05-08T11:00:00+00:00",
        next_attempt_at="2099-01-01T00:00:00+00:00",  # far future
    )
    store.close()

    rc = main(["--state", str(db), "kick"])
    assert rc == 0
    # The deliverable should now be due immediately.
    s2 = SqliteWatermarkStore(db)
    d = s2.pop_due_deliverable("p", now="2026-05-08T12:00:00+00:00")
    assert d is not None
    assert d.payload_blob == b"x"


def test_status_on_missing_state_errors_for_mutating_cmds(tmp_path, capsys):
    """reset-cursor on a non-existent state file is an error."""
    rc = main([
        "--state", str(tmp_path / "ghost.db"),
        "reset-cursor",
        "--source", "x", "--dest", "y", "--table", "z",
    ])
    assert rc == 2


# ── purge-dead-letters (sigmond v3.72 #1) ────────────────────────────────
# A dead letter never leaves the store on its own.  Clearing them used to be
# a hand-typed sqlite3 one-liner per station (2026-10-14: 390 rows on four
# stations).  The command saves the rows as SQL that restores them, and
# deletes only after that file is safely written.

import os
import sqlite3


def _seed_dead(path: Path, rows) -> None:
    store = SqliteWatermarkStore(path)
    try:
        for ts, pipeline in rows:
            store.send_to_dead_letter(ts=ts, pipeline=pipeline,
                                      payload_blob=b"it's\x00binary",
                                      final_error="can't log in: Permission denied (publickey)")
    finally:
        store.close()


def _dead(path: Path):
    with sqlite3.connect(path) as c:
        return sorted(c.execute("SELECT ts, pipeline FROM dead_letter"))


def _backups(d: Path):
    return sorted(d.glob("dead_letter-*.sql"))


OLD = ("2026-10-01T00:00:00+00:00", "heartbeat")
NEW = ("2026-10-14T11:00:00+00:00", "heartbeat")
OTHER = ("2026-10-02T00:00:00+00:00", "wsprnet")


def test_purge_removes_all_and_saves_them(tmp_path, capsys):
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD, NEW, OTHER])

    rc = main(["--state", str(db), "purge-dead-letters"])

    assert rc == 0
    assert _dead(db) == []
    [backup] = _backups(tmp_path)
    out = capsys.readouterr().out
    assert "3 dead letter(s)" in out and backup.name in out


def test_purge_backup_restores_the_rows_byte_for_byte(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD, OTHER])
    main(["--state", str(db), "purge-dead-letters"])
    [backup] = _backups(tmp_path)

    fresh = tmp_path / "fresh.db"
    SqliteWatermarkStore(fresh).close()
    with sqlite3.connect(fresh) as c:
        c.executescript(backup.read_text())
        rows = sorted(c.execute(
            "SELECT ts, pipeline, payload_blob, final_error FROM dead_letter"))

    assert [(r[0], r[1]) for r in rows] == sorted([OLD, OTHER])
    assert all(r[2] == b"it's\x00binary" for r in rows)
    assert all(r[3] == "can't log in: Permission denied (publickey)" for r in rows)


def test_purge_before_keeps_newer_rows(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD, NEW])

    main(["--state", str(db), "purge-dead-letters",
          "--before", "2026-10-10T00:00:00+00:00"])

    assert _dead(db) == [NEW]


def test_purge_pipeline_keeps_other_pipelines(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD, OTHER])

    main(["--state", str(db), "purge-dead-letters", "--pipeline", "heartbeat"])

    assert _dead(db) == [OTHER]


def test_purge_nothing_matching_writes_no_file(tmp_path, capsys):
    db = tmp_path / "wm.db"
    _seed_dead(db, [NEW])

    rc = main(["--state", str(db), "purge-dead-letters", "--pipeline", "nope"])

    assert rc == 0
    assert _backups(tmp_path) == []
    assert _dead(db) == [NEW]
    assert "nothing to purge" in capsys.readouterr().out


def test_purge_deletes_nothing_when_the_backup_cannot_be_written(tmp_path):
    if os.geteuid() == 0:
        import pytest
        pytest.skip("root ignores directory permissions")
    # The store stays writable; only the backup's directory refuses.  A
    # read-only store directory would also block sqlite's journal, so the
    # delete would fail by itself and the test would prove nothing.
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD])
    locked = tmp_path / "locked"
    locked.mkdir()
    os.chmod(locked, 0o555)
    try:
        rc = main(["--state", str(db), "purge-dead-letters",
                   "--backup-dir", str(locked)])
    finally:
        os.chmod(locked, 0o755)

    assert rc != 0
    assert _dead(db) == [OLD]


def test_purge_on_missing_state_errors(tmp_path, capsys):
    rc = main(["--state", str(tmp_path / "nope.db"), "purge-dead-letters"])
    assert rc == 2
    assert not (tmp_path / "nope.db").exists()


def test_purge_before_keeps_rows_whose_time_cannot_be_read(tmp_path):
    db = tmp_path / "wm.db"
    _seed_dead(db, [OLD, ("not-a-time", "heartbeat")])

    main(["--state", str(db), "purge-dead-letters",
          "--before", "2026-10-10T00:00:00+00:00"])

    assert _dead(db) == [("not-a-time", "heartbeat")]


def test_capabilities_prints_store_version(capsys):
    from hs_uploader.watermark import schema
    assert main(["capabilities"]) == 0
    assert capsys.readouterr().out == f"{schema.SCHEMA_VERSION}\n"


def _run_caps(*extra):
    import subprocess, sys, pathlib
    src = pathlib.Path(__file__).resolve().parent.parent / "src"
    return subprocess.run(
        [sys.executable, "-m", "hs_uploader.cli", "capabilities", *extra],
        capture_output=True, text=True, env={"PYTHONPATH": str(src)})


def test_capabilities_bare_prints_3():
    # spec test 20: a v3.73-v3.76 sigmond reads this as int(stdout.strip()).
    out = _run_caps()
    assert out.stdout == "3\n"
    assert int(out.stdout.strip()) == 3


def test_capabilities_features_names_what_this_release_does():
    out = _run_caps("--features")
    assert out.returncode == 0
    assert out.stdout == "pipelines.d\nsend-log-v3\nswitch-files\n"


# ---- sink status (spec 6.2) ----------------------------------------------------------

import json as _json


def _station_dir(tmp_path, *, clients=("c1",)):
    etc = tmp_path / "etc"
    etc.mkdir()
    m = etc / "pipelines.toml"
    m.write_text("".join(
        f'[[pipeline]]\nname = "p-{c}"\nbuilder = "x:y"\nclient = "{c}"\n'
        for c in clients))
    return m


def test_sink_status_without_sinks_dir_says_so(tmp_path, capsys):
    m = _station_dir(tmp_path)
    assert main(["sink", "status", "--manifest", str(m)]) == 0
    out = capsys.readouterr().out
    assert "no switch files; the manifest's policy governs" in out
    assert "not enforced: the manifest's policy decides what ships" in out


def test_sink_status_lists_site_and_each_client(tmp_path, capsys):
    from hs_uploader import sinkswitch
    m = _station_dir(tmp_path, clients=("c1", "c2"))
    sinks = tmp_path / "etc" / "sinks"
    sinkswitch.write_switch(sinks / "c1.toml", {"state": "upload"})
    sinkswitch.write_switch(sinks / "site.toml", {"state": "fill"})
    sinkswitch.write_switch(sinks / "only-file.toml", {"state": "off"})
    (sinks / "c3.toml").write_text("state = [bad\n")
    assert main(["sink", "status", "--json", "--manifest", str(m)]) == 0
    d = _json.loads(capsys.readouterr().out)
    assert d["site"]["file"] == "fill" and d["site"]["effective"] == "fill"
    assert d["clients"]["c1"]["file"] == "upload"
    assert d["clients"]["c1"]["effective"] == "fill"      # site limits it
    assert d["clients"]["c2"]["file"] == "missing"
    assert d["clients"]["c2"]["effective"] == "off"
    assert d["clients"]["only-file"]["effective"] == "off"
    assert d["clients"]["c3"]["error"]
    assert d["enforced"] is False
    assert main(["sink", "status", "--manifest", str(m)]) == 0
    out = capsys.readouterr().out
    for word in ("site", "c1", "c2", "c3", "only-file",
                 "not enforced: the manifest's policy decides what ships"):
        assert word in out


def test_sink_status_sinks_flag_overrides_the_derived_directory(tmp_path, capsys):
    from hs_uploader import sinkswitch
    m = _station_dir(tmp_path)
    other = tmp_path / "elsewhere"
    sinkswitch.write_switch(other / "c1.toml", {"state": "upload"})
    assert main(["sink", "status", "--json", "--manifest", str(m),
                 "--sinks", str(other)]) == 0
    d = _json.loads(capsys.readouterr().out)
    assert d["clients"]["c1"]["effective"] == "upload"


def test_sink_status_counts_pipelines_without_a_client(tmp_path, capsys):
    m = _station_dir(tmp_path)
    m.write_text(m.read_text() + '[[pipeline]]\nname = "nc"\nbuilder = "x:y"\n')
    assert main(["sink", "status", "--manifest", str(m)]) == 0
    assert ("1 pipeline(s) carry no client; no switch applies to them"
            in capsys.readouterr().out)
    assert main(["sink", "status", "--json", "--manifest", str(m)]) == 0
    assert _json.loads(capsys.readouterr().out)["no_client_pipelines"] == 1


def test_sink_status_manifest_read_failure_exits_2(tmp_path, capsys):
    m = tmp_path / "missing.toml"
    assert main(["sink", "status", "--json", "--manifest", str(m)]) == 2
    assert _json.loads(capsys.readouterr().out)["manifest_read_failed"] is True
    assert main(["sink", "status", "--manifest", str(m)]) == 2
