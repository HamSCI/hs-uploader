"""Each source orders its own send records (sigmond plan-sink-control §6.2).

The store keeps a cursor as opaque bytes, and a byte compare ranks b"999"
above b"1000".  Only the source knows its encoding, so each source answers
``cursor_is_after(new, stored)`` in the order its own query uses.  An empty
``stored`` (no send record yet) and a value the source cannot decode both
answer True: v3.70 only warns, and a warning about an unreadable value would
measure nothing.
"""
from __future__ import annotations

import logging
import sqlite3

import pytest

from hs_uploader.sources import FileSpec, FileTreeSource, SqliteSource
from hs_uploader.sources.base import Source
from hs_uploader.sources.wspr_cycle import WsprCycleSource


def _sqlite() -> SqliteSource:
    # No connection config: the comparison never touches the database.
    return SqliteSource("psk", "spots", accepted_schema_versions=[1])


def _keep(tmp_path) -> FileTreeSource:
    return FileTreeSource(tmp_path, specs=[FileSpec("*")],
                          retention=FileTreeSource.KEEP)


def _delete_on_ack(tmp_path) -> FileTreeSource:
    return FileTreeSource(tmp_path, specs=[FileSpec("*")])


def _wspr(tmp_path) -> WsprCycleSource:
    return WsprCycleSource(db_path=tmp_path / "sink.db")


# ---- the protocol default ------------------------------------------------


def test_the_protocol_default_answers_true():
    class _Plain(Source):
        def source_id(self): return "plain"
        def health(self): return "ok"
        def iter_batches(self, cursor, limit): return iter(())

    assert _Plain().cursor_is_after(b"1", b"2") is True


# ---- SqliteSource: integer ids, the order of ``id > ?`` -------------------


def test_sqlite_ranks_1000_after_999():
    src = _sqlite()
    assert src.cursor_is_after(b"1000", b"999") is True
    assert src.cursor_is_after(b"999", b"1000") is False


def test_sqlite_an_equal_id_does_not_move_forward():
    assert _sqlite().cursor_is_after(b"42", b"42") is False


def test_sqlite_empty_stored_answers_true():
    assert _sqlite().cursor_is_after(b"1", b"") is True


def test_sqlite_undecodable_answers_true_and_logs_at_debug(caplog):
    src = _sqlite()
    with caplog.at_level(logging.DEBUG, logger="hs_uploader.sources.sqlite"):
        assert src.cursor_is_after(b"7", b"not-an-id") is True
        assert src.cursor_is_after(b"\xff", b"7") is True
    recs = [r for r in caplog.records if r.name == "hs_uploader.sources.sqlite"]
    assert recs and all(r.levelno == logging.DEBUG for r in recs)


# ---- FileTreeSource KEEP: integer nanoseconds ----------------------------


def test_keep_ranks_1000_after_999(tmp_path):
    src = _keep(tmp_path)
    assert src.cursor_is_after(b"1000", b"999") is True
    assert src.cursor_is_after(b"999", b"1000") is False


def test_keep_an_equal_mtime_does_not_move_forward(tmp_path):
    ns = b"1781583014421470123"
    assert _keep(tmp_path).cursor_is_after(ns, ns) is False


def test_keep_reads_a_legacy_float_seconds_record(tmp_path):
    # Stations still hold KEEP send records written as float seconds.
    # iter_batches reads them through _decode_keep_cursor; so must the check.
    src = _keep(tmp_path)
    legacy = b"1781583014.421470"
    assert src.cursor_is_after(b"1781583014421471000", legacy) is True   # 1 us later
    assert src.cursor_is_after(b"1781583014421469000", legacy) is False  # 1 us earlier


def test_keep_empty_stored_answers_true(tmp_path):
    assert _keep(tmp_path).cursor_is_after(b"1", b"") is True


@pytest.mark.parametrize("new,stored", [
    (b"1781583014421470123", b"garbage"),
    (b"garbage", b"1781583014421470123"),
    (b"1781583014421470123", b"\xff\xfe"),
    (b"1781583014421470123", b"inf"),   # float("inf") overflows int()
])
def test_keep_undecodable_answers_true_and_logs_at_debug(tmp_path, caplog, new, stored):
    src = _keep(tmp_path)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader.sources.files"):
        assert src.cursor_is_after(new, stored) is True
    recs = [r for r in caplog.records if r.name == "hs_uploader.sources.files"]
    assert recs and all(r.levelno == logging.DEBUG for r in recs)


# ---- FileTreeSource delete_on_ack: always forward ------------------------


@pytest.mark.parametrize("new,stored", [
    (b"<delete-on-ack>", b"<delete-on-ack>"),
    (b"<delete-on-ack>", b""),
    (b"999", b"1000"),
])
def test_delete_on_ack_always_answers_true(tmp_path, new, stored):
    # Its cursor never changes, so every write repeats the same value.
    assert _delete_on_ack(tmp_path).cursor_is_after(new, stored) is True


# ---- WsprCycleSource: text order, the order of its SQL -------------------


def test_wspr_cycle_a_later_cycle_moves_forward(tmp_path):
    src = _wspr(tmp_path)
    assert src.cursor_is_after(b"2026-10-07T12:02:00Z",
                               b"2026-10-07T12:00:00Z") is True
    assert src.cursor_is_after(b"2026-10-07T12:00:00Z",
                               b"2026-10-07T12:02:00Z") is False
    assert src.cursor_is_after(b"2026-10-07T12:00:00Z",
                               b"2026-10-07T12:00:00Z") is False


@pytest.mark.parametrize("new,stored", [
    (b"2026-10-07T12:02:00Z", b"2026-10-07T12:00:00Z"),
    (b"2026-10-07T12:00:00Z", b"2026-10-07T12:02:00Z"),
    (b"2026-10-07T12:00:00Z", b"2026-10-07T12:00:00Z"),
    (b"2026-10-07T12:00:00+00:00", b"2026-10-07T12:00:00Z"),
    (b"2026-10-07T12:00:00.5Z", b"2026-10-07T12:00:00Z"),
    (b"1000", b"999"),
])
def test_wspr_cycle_order_matches_the_sql_text_compare(tmp_path, new, stored):
    # iter_batches selects json_extract(payload_json, '$.time') > cursor,
    # a text compare.  Parsing the times would disagree with that query.
    sql_says = sqlite3.connect(":memory:").execute(
        "SELECT ? > ?", (new.decode("ascii"), stored.decode("ascii")),
    ).fetchone()[0]
    assert _wspr(tmp_path).cursor_is_after(new, stored) is bool(sql_says)


def test_wspr_cycle_empty_stored_answers_true(tmp_path):
    assert _wspr(tmp_path).cursor_is_after(b"2026-10-07T12:00:00Z", b"") is True


def test_wspr_cycle_undecodable_answers_true_and_logs_at_debug(tmp_path, caplog):
    src = _wspr(tmp_path)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader.sources.wspr_cycle"):
        assert src.cursor_is_after(b"\xff", b"2026-10-07T12:00:00Z") is True
    recs = [r for r in caplog.records if r.name == "hs_uploader.sources.wspr_cycle"]
    assert recs and all(r.levelno == logging.DEBUG for r in recs)
