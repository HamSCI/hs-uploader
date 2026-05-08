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
    store.advance_cursor(
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
