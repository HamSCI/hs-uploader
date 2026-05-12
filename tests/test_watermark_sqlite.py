"""SqliteWatermarkStore — cursors, deliverables, dead-letter, attempts."""

from __future__ import annotations

from hs_uploader.watermark import Deliverable, SqliteWatermarkStore


def test_cursor_round_trip(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    assert store.get_cursor("ch:wspr.spots", "wsprnet", "wspr.spots") == b""

    store.advance_cursor(
        "ch:wspr.spots", "wsprnet", "wspr.spots",
        cursor=b'{"time":"2026-05-08","tiebreak":"42"}',
        last_ack="2026-05-08T12:00:00+00:00",
    )
    got = store.get_cursor("ch:wspr.spots", "wsprnet", "wspr.spots")
    assert got == b'{"time":"2026-05-08","tiebreak":"42"}'


def test_cursor_advance_is_upsert(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    for ts, n in [("2026-05-08", "1"), ("2026-05-09", "2"), ("2026-05-10", "3")]:
        store.advance_cursor(
            "src", "dest", "tbl",
            cursor=ts.encode(), last_ack=ts,
        )
    assert store.get_cursor("src", "dest", "tbl") == b"2026-05-10"


def test_cursors_are_per_destination(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor(
        "ch:wspr.spots", "wsprdaemon", "wspr.spots",
        cursor=b"A", last_ack="t1",
    )
    store.advance_cursor(
        "ch:wspr.spots", "wsprnet", "wspr.spots",
        cursor=b"B", last_ack="t2",
    )
    assert store.get_cursor("ch:wspr.spots", "wsprdaemon", "wspr.spots") == b"A"
    assert store.get_cursor("ch:wspr.spots", "wsprnet", "wspr.spots") == b"B"


def test_reset_cursor(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor("s", "d", "t", cursor=b"X", last_ack="t1")
    assert store.reset_cursor("s", "d", "t") is True
    assert store.get_cursor("s", "d", "t") == b""
    # Second reset is a no-op return.
    assert store.reset_cursor("s", "d", "t") is False


def test_record_attempt_and_recent(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    for i in range(5):
        store.record_attempt(
            ts=f"2026-05-08T12:00:0{i}+00:00",
            source_id="s", dest_id="d", table="t",
            outcome="acked", records=10, bytes_=200, error=None,
        )
    rows = store.recent_attempts(limit=3)
    assert len(rows) == 3
    # Most-recent first.
    assert rows[0]["ts"] == "2026-05-08T12:00:04+00:00"


def test_deliverable_pop_due_returns_only_due(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.enqueue_deliverable(
        pipeline="p", payload_blob=b"old",
        enqueued_at="2026-05-08T11:00:00+00:00",
        next_attempt_at="2026-05-08T11:00:30+00:00",
    )
    store.enqueue_deliverable(
        pipeline="p", payload_blob=b"future",
        enqueued_at="2026-05-08T11:00:00+00:00",
        next_attempt_at="2026-05-08T13:00:00+00:00",
    )
    # Now=12:00 — only the first is due.
    d = store.pop_due_deliverable("p", now="2026-05-08T12:00:00+00:00")
    assert d is not None
    assert d.payload_blob == b"old"
    assert d.attempts == 0
    # The future one is still queued; another pop returns None.
    d2 = store.pop_due_deliverable("p", now="2026-05-08T12:00:00+00:00")
    assert d2 is None
    assert store.deliverable_count("p") == 1


def test_requeue_deliverable_preserves_id(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    did = store.enqueue_deliverable(
        pipeline="p", payload_blob=b"x",
        enqueued_at="2026-05-08T11:00:00+00:00",
        next_attempt_at="2026-05-08T11:00:30+00:00",
    )
    d = store.pop_due_deliverable("p", now="2026-05-08T12:00:00+00:00")
    assert d is not None and d.id == did
    # Requeue with bumped attempts and a fresh next-attempt time.
    store.requeue_deliverable(
        Deliverable(
            id=d.id,
            pipeline=d.pipeline,
            payload_blob=d.payload_blob,
            enqueued_at=d.enqueued_at,
            attempts=1,
            next_attempt_at="2026-05-08T11:01:00+00:00",
        )
    )
    d2 = store.pop_due_deliverable("p", now="2026-05-08T12:00:00+00:00")
    assert d2 is not None
    assert d2.id == did
    assert d2.attempts == 1


def test_dead_letter_count(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    assert store.dead_letter_count() == 0
    store.send_to_dead_letter(
        ts="2026-05-08T12:00:00+00:00",
        pipeline="p", payload_blob=b"oops",
        final_error="server returned 500",
    )
    assert store.dead_letter_count() == 1


def test_attempts_ring_buffer_trims(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    # Force the ring size down for the test.
    from hs_uploader.watermark import sqlite as sqlite_mod
    original = sqlite_mod._ATTEMPTS_RING_SIZE
    sqlite_mod._ATTEMPTS_RING_SIZE = 3
    try:
        for i in range(5):
            store.record_attempt(
                ts=f"t{i}", source_id="s", dest_id="d", table="t",
                outcome="acked", records=1, bytes_=None, error=None,
            )
        rows = store.recent_attempts(limit=10)
        assert len(rows) == 3
        # Newest survive.
        assert {r["ts"] for r in rows} == {"t4", "t3", "t2"}
    finally:
        sqlite_mod._ATTEMPTS_RING_SIZE = original


# ---- group-writable post-init -----------------------------------------


def test_group_writable_after_construct(tmp_path):
    """Watermark db + WAL/SHM sidecars are group-writable after the
    constructor returns.  Without this, the second HamSCI client to
    open the db (different system user, same supplementary group)
    gets "attempt to write a readonly database" — observed on bee1
    2026-05-12 during the wspr-uploader cutover."""
    import stat
    db_path = tmp_path / "w.db"
    SqliteWatermarkStore(db_path)
    mode = (db_path).stat().st_mode
    assert mode & stat.S_IWGRP, (
        f"main db mode {oct(mode & 0o7777)} missing group-write bit"
    )


def test_chmod_failure_is_silent(tmp_path):
    """Non-owner callers can't chmod — the constructor must not crash.
    Best-effort visibility, not control flow."""
    from unittest.mock import patch
    with patch("os.chmod", side_effect=PermissionError("not owner")):
        # No raise — the watermark store construct succeeds even if
        # chmod is unauthorized.
        SqliteWatermarkStore(tmp_path / "w.db")


def test_memory_db_no_chmod(tmp_path):
    """``:memory:`` paths have no on-disk file; chmod must skip cleanly."""
    # Just verify no exception.
    SqliteWatermarkStore(":memory:")
