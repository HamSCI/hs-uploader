"""Send records move forward only, enforced (sigmond spec-key-split-v373 §4.6).

advance_cursor reads the stored send record, asks the source's own order
``is_after(new, stored)``, and then, all inside one BEGIN IMMEDIATE
transaction:

| Comparison          | Action                   | Log                           | Returns           |
|---------------------|--------------------------|-------------------------------|-------------------|
| new after stored    | write                    | none                          | WRITTEN           |
| new equal to stored | refresh last_ack only    | none                          | REFRESHED         |
| new before stored   | refuse                   | WARNING, both values          | REFUSED           |
| comparison raises   | write                    | WARNING, both values, error   | WRITTEN_UNCHECKED |

A refused write logs one WARNING per key per hour; further ones inside the
hour log at DEBUG and add to a count; the first one after the hour logs one
WARNING with that count.  Core logs a refused advance as "acked, record held".
"""
from __future__ import annotations

import inspect
import json
import logging
import os
import sqlite3
import threading
import time

import pytest

from hs_uploader import Outcome, Pipeline, RetryPolicy, Uploader
from hs_uploader.core import _is_after_for
from hs_uploader.sources import FileSpec, FileTreeSource
from hs_uploader.sources.wspr_cycle import WsprCycleSource
from hs_uploader.watermark import Advance, SqliteWatermarkStore
from hs_uploader.watermark.sqlite import BACKWARD_WARN_INTERVAL_S
from tests.conftest import MemorySource
from tests.test_core_orchestration import _ident, _records
from tests.test_sqlite_source import _insert, _seed_table, _src

_STORE_LOG = "hs_uploader.watermark.sqlite"
_CORE_LOG = "hs_uploader.core"
_KEY = ("memory:test", "memory", "test.spots")
_PSK = ("sqlite:psk.spots@psk-recorder", "pskreporter", "psk.spots")


def _int_order(new: bytes, stored: bytes) -> bool:
    return not stored or int(new) > int(stored)


def _last_ack(store: SqliteWatermarkStore, key=_KEY) -> str:
    for row in store.all_cursors():
        if (row["source_id"], row["dest_id"], row["table_name"]) == tuple(key):
            return row["last_ack"]
    raise AssertionError(f"no send record for {key}")


def _warnings(caplog, name=_STORE_LOG) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.name == name and r.levelno == logging.WARNING]


def _debugs(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.name == _STORE_LOG and r.levelno == logging.DEBUG]


class _Clock:
    """A clock the test moves by hand, in seconds."""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


def _write(store: SqliteWatermarkStore, key, cursor: bytes, last_ack: str) -> Advance:
    return store.advance_cursor(*key, cursor=cursor, last_ack=last_ack,
                                is_after=_int_order)


# ---- the interface ------------------------------------------------------------


def test_advance_names_the_four_outcomes():
    assert [a.name for a in Advance] == [
        "WRITTEN", "REFRESHED", "REFUSED", "WRITTEN_UNCHECKED"]


def test_the_unchecked_write_left_the_public_interface():
    assert not hasattr(SqliteWatermarkStore, "advance_cursor_checked")
    params = inspect.signature(SqliteWatermarkStore.advance_cursor).parameters
    assert params["is_after"].default is inspect.Parameter.empty
    assert callable(getattr(SqliteWatermarkStore, "_write_cursor"))


# ---- one test per row of §4.6 ---------------------------------------------------


def test_after_writes_and_stays_quiet(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"999", last_ack="t1")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert _write(store, _KEY, b"1000", "t2") is Advance.WRITTEN
    assert store.get_cursor(*_KEY) == b"1000"
    assert _last_ack(store) == "t2"
    assert [r for r in caplog.records if r.name == _STORE_LOG] == []


def test_equal_refreshes_last_ack_only_and_stays_quiet(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"42", last_ack="t1")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert _write(store, _KEY, b"42", "t2") is Advance.REFRESHED
    assert store.get_cursor(*_KEY) == b"42"
    assert _last_ack(store) == "t2"
    assert [r for r in caplog.records if r.name == _STORE_LOG] == []


def test_equal_in_the_sources_order_keeps_the_stored_bytes(tmp_path, caplog):
    # b"042" and b"42" differ as bytes and tie as integers: the source's
    # order calls them equal, so only last_ack moves.
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"042", last_ack="t1")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert _write(store, _KEY, b"42", "t2") is Advance.REFRESHED
    assert store.get_cursor(*_KEY) == b"042"
    assert _last_ack(store) == "t2"
    assert _warnings(caplog) == []


def test_before_refuses_changes_nothing_and_warns_once(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_PSK, cursor=b"1000", last_ack="t1")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert _write(store, _PSK, b"999", "t2") is Advance.REFUSED
    assert store.get_cursor(*_PSK) == b"1000"
    assert _last_ack(store, _PSK) == "t1"
    (rec,) = _warnings(caplog)
    msg = rec.getMessage()
    for part in (*_PSK, "b'1000'", "b'999'", "refused"):
        assert part in msg, (part, msg)


def test_a_comparator_that_raises_writes_unchecked_and_warns(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"1", last_ack="t1")

    def broken(new: bytes, stored: bytes) -> bool:
        raise RuntimeError("comparator bug")

    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert store.advance_cursor(
            *_KEY, cursor=b"2", last_ack="t2", is_after=broken,
        ) is Advance.WRITTEN_UNCHECKED
    assert store.get_cursor(*_KEY) == b"2"
    assert _last_ack(store) == "t2"
    (rec,) = _warnings(caplog)
    msg = rec.getMessage()
    for part in ("comparator bug", "RuntimeError", "b'1'", "b'2'"):
        assert part in msg, (part, msg)


def test_the_first_write_on_a_key_needs_no_comparison(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")

    def never(new: bytes, stored: bytes) -> bool:
        raise AssertionError("nothing stored, nothing to compare")

    assert store.advance_cursor(
        *_KEY, cursor=b"7", last_ack="t1", is_after=never,
    ) is Advance.WRITTEN
    assert store.get_cursor(*_KEY) == b"7"


def test_reset_cursor_still_rewinds_unchecked(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    _write(store, _KEY, b"1000", "t1")
    assert store.reset_cursor(*_KEY) is True
    assert _write(store, _KEY, b"5", "t2") is Advance.WRITTEN
    assert store.get_cursor(*_KEY) == b"5"


# ---- two processes racing one key ------------------------------------------------


def test_two_connections_race(tmp_path):
    """Two store objects on one file stand in for two processes.

    A's comparator runs while B, on its own connection, advances the same
    key further.  With the read, the comparison and the write in one
    BEGIN IMMEDIATE transaction, B waits for A and then compares against
    A's write, so the record ends at B's value.  A read-then-write would let
    B write 20 inside A's comparison and A overwrite it with 10.
    """
    path = tmp_path / "wm.db"
    a = SqliteWatermarkStore(path)
    b = SqliteWatermarkStore(path)
    a._write_cursor(*_KEY, cursor=b"5", last_ack="t0")

    b_started = threading.Event()
    results: dict[str, Advance] = {}

    def b_writes() -> None:
        b_started.set()
        results["b"] = _write(b, _KEY, b"20", "tb")

    def slow_order(new: bytes, stored: bytes) -> bool:
        if "thread" not in results:
            results["thread"] = threading.Thread(target=b_writes)
            results["thread"].start()
            b_started.wait(5.0)
            time.sleep(0.3)          # B attempts its write in here
        return _int_order(new, stored)

    results["a"] = a.advance_cursor(*_KEY, cursor=b"10", last_ack="ta",
                                    is_after=slow_order)
    results["thread"].join(10.0)
    assert results["a"] is Advance.WRITTEN
    assert results["b"] is Advance.WRITTEN
    assert a.get_cursor(*_KEY) == b"20"
    assert b.get_cursor(*_KEY) == b"20"


def test_interleaved_writers_never_move_the_record_back(tmp_path):
    path = tmp_path / "wm.db"
    a = SqliteWatermarkStore(path)
    b = SqliteWatermarkStore(path)
    seen = []
    for i, (store, cursor) in enumerate(
            [(a, b"3"), (b, b"7"), (a, b"5"), (b, b"6"), (a, b"9"), (b, b"8")]):
        _write(store, _KEY, cursor, f"t{i}")
        seen.append(int(a.get_cursor(*_KEY)))
    assert seen == [3, 7, 7, 7, 9, 9]


# ---- one WARNING per key per hour ----------------------------------------------


def test_a_repeat_within_the_hour_logs_at_debug_and_still_refuses(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store._write_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert _write(store, _KEY, b"999", "t1") is Advance.REFUSED   # warns
        clock.t += 3599.0
        assert _write(store, _KEY, b"998", "t2") is Advance.REFUSED   # inside the hour
    assert len(_warnings(caplog)) == 1
    (debug,) = _debugs(caplog)
    assert "stored b'1000', new b'998'" in debug.getMessage()
    assert "(1 since the last warning)" in debug.getMessage()
    assert store.get_cursor(*_KEY) == b"1000"
    assert _last_ack(store) == "t0"


def test_after_the_hour_one_warning_counts_the_refusals_since_the_last(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store._write_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        _write(store, _KEY, b"999", "t1")          # the first: warns at t = 1000
        for cursor in (b"998", b"997", b"996"):    # three more, at DEBUG
            clock.t += 600.0
            _write(store, _KEY, cursor, "t2")
        clock.t = 1000.0 + 3600.0                  # the hour has passed
        _write(store, _KEY, b"990", "t3")          # one WARNING counts four
        clock.t += 1.0
        _write(store, _KEY, b"989", "t4")          # a new window: quiet again
    first, summary = _warnings(caplog)
    assert "stored b'1000', new b'999'" in first.getMessage()
    msg = summary.getMessage()
    assert msg.startswith("send record (%s, %s, %s) " % _KEY)
    assert "does not move forward: 4 times since the last warning" in msg
    assert "the latest stored b'1000', new b'990'" in msg
    assert store.get_cursor(*_KEY) == b"1000"
    assert _last_ack(store) == "t0"


@pytest.mark.parametrize("other", [
    ("sqlite:psk.spots@meteor-scatter", _PSK[1], _PSK[2]),   # another source_id
    (_PSK[0], "pskreporter-udp", _PSK[2]),                   # another dest_id
    (_PSK[0], _PSK[1], "psk.msk144"),                        # another table
])
def test_each_key_keeps_its_own_window(tmp_path, caplog, other):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    for key in (_PSK, other):
        store._write_cursor(*key, cursor=b"100", last_ack="t0")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        _write(store, _PSK, b"99", "t1")      # _PSK's first: warns
        clock.t += 1800.0
        _write(store, other, b"99", "t2")     # other's first: warns, inside _PSK's hour
        _write(store, _PSK, b"98", "t3")      # inside _PSK's hour: quiet
        clock.t += 1800.0                     # _PSK's hour has passed, other's has not
        _write(store, _PSK, b"97", "t4")      # _PSK: one WARNING counts two
        _write(store, other, b"98", "t5")     # inside other's hour: quiet
    warned = [r.getMessage() for r in _warnings(caplog)]
    assert len(warned) == 3
    assert warned[0].startswith("send record (%s, %s, %s) " % _PSK)
    assert warned[1].startswith("send record (%s, %s, %s) " % other)
    assert warned[2].startswith("send record (%s, %s, %s) " % _PSK)
    assert "2 times since the last warning" in warned[2]
    assert store.get_cursor(*_PSK) == b"100"
    assert store.get_cursor(*other) == b"100"


def test_every_backward_write_is_refused_whatever_it_logs(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store._write_cursor(*_KEY, cursor=b"100", last_ack="t0")
    steps = [(0.0, b"99"), (10.0, b"98"), (3600.0, b"97"), (5.0, b"96"), (7200.0, b"95")]
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        for i, (step, cursor) in enumerate(steps, start=1):
            clock.t += step
            assert _write(store, _KEY, cursor, f"t{i}") is Advance.REFUSED
            assert store.get_cursor(*_KEY) == b"100"
            assert _last_ack(store) == "t0"
    levels = [r.levelname for r in caplog.records if r.name == _STORE_LOG]
    assert levels == ["WARNING", "DEBUG", "WARNING", "DEBUG", "WARNING"]


# ---- core: all three advance sites use the checked call --------------------


class _SpyStore(SqliteWatermarkStore):
    """Counts checked advances, and unchecked writes made from outside one."""

    def __init__(self, path):
        super().__init__(path)
        self.checked: list[tuple] = []
        self.results: list[Advance] = []
        self.bare = 0
        self._in_checked = False

    def _write_cursor(self, *a, **kw):
        if not self._in_checked:
            self.bare += 1
        return super()._write_cursor(*a, **kw)

    def advance_cursor(self, source_id, dest_id, table, *, cursor,
                       last_ack, is_after):
        self.checked.append((source_id, dest_id, table, cursor, is_after))
        self._in_checked = True
        try:
            got = super().advance_cursor(
                source_id, dest_id, table, cursor=cursor, last_ack=last_ack,
                is_after=is_after,
            )
            self.results.append(got)
            return got
        finally:
            self._in_checked = False


class _IntOrderSource(MemorySource):
    """MemorySource that ranks its cursors as integers, as SqliteSource does."""

    def cursor_is_after(self, new: bytes, stored: bytes) -> bool:
        return _int_order(new, stored)


def _pipe(tmp_path, transport, src, **kw):
    wm = _SpyStore(tmp_path / "wm.db")
    return Pipeline(name="test", source=src, transport=transport, watermark=wm,
                    identity=_ident(), **kw), wm


def test_a_first_attempt_ack_uses_the_checked_call(tmp_path, memory_transport):
    src = _IntOrderSource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    Uploader([pipe]).pump()
    assert wm.bare == 0
    assert [c[:4] for c in wm.checked] == [(*_KEY, b"3")]
    assert wm.checked[0][4] == src.cursor_is_after
    assert wm.results == [Advance.WRITTEN]
    assert wm.get_cursor(*_KEY) == b"3"


def test_a_partial_ack_uses_the_checked_call(tmp_path, memory_transport):
    src = _IntOrderSource(records=_records(5))
    memory_transport.next_outcomes = [
        Outcome.partial_ack(accepted_cursor=b"3", rejected=(), reason="2 rejected"),
    ]
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    Uploader([pipe]).pump()
    assert wm.bare == 0
    assert [c[:4] for c in wm.checked] == [(*_KEY, b"3")]
    assert wm.checked[0][4] == src.cursor_is_after


def test_a_replay_ack_uses_the_checked_call(tmp_path, memory_transport):
    src = _IntOrderSource(records=_records(2))
    pipe, wm = _pipe(tmp_path, memory_transport, src,
                     retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5))
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    assert wm.checked == [] and wm.bare == 0

    far = time.time() + 86_400.0
    memory_transport.next_outcomes = [Outcome.acked()]
    Uploader([pipe], now_fn=lambda: far).pump()
    assert wm.bare == 0
    assert [c[:4] for c in wm.checked] == [(*_KEY, b"2")]
    assert wm.checked[0][4] == src.cursor_is_after


def test_a_source_without_the_method_gets_always_forward(tmp_path, memory_transport, caplog):
    # MemorySource, like any source written before v3.70, lacks cursor_is_after.
    src = MemorySource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        Uploader([pipe]).pump()
    (call,) = wm.checked
    assert call[4](b"1", b"2") is True
    assert wm.get_cursor(*_KEY) == b"3"
    assert _warnings(caplog) == []


def test_core_logs_acked_record_held(tmp_path, memory_transport, caplog):
    # The stored record reads b"10".  MemorySource filters with a byte compare,
    # so it yields b"2" and b"3"; the ack would move the record from 10 to 3.
    src = _IntOrderSource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    wm._write_cursor(*_KEY, cursor=b"10", last_ack="t0")
    wm.bare = 0
    with caplog.at_level(logging.INFO):
        Uploader([pipe]).pump()
    assert wm.bare == 0
    assert wm.results == [Advance.REFUSED]
    assert wm.get_cursor(*_KEY) == b"10"
    (rec,) = _warnings(caplog)             # the store's WARNING, both values
    assert "b'10'" in rec.getMessage() and "b'3'" in rec.getMessage()
    held = [r.getMessage() for r in caplog.records
            if r.name == _CORE_LOG and "acked, record held" in r.getMessage()]
    assert len(held) == 1, held
    assert held[0].startswith("test: acked, record held"), held[0]
    assert "b'3'" in held[0]
    # The destination took the batch, so the attempt still records an ack.
    (attempt,) = wm.recent_attempts(limit=1)
    assert attempt["outcome"] == "acked"


def test_core_logs_a_refused_replay_ack_as_record_held(tmp_path, memory_transport, caplog):
    src = _IntOrderSource(records=_records(2))
    pipe, wm = _pipe(tmp_path, memory_transport, src,
                     retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5))
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    wm._write_cursor(*_KEY, cursor=b"50", last_ack="t0")   # another writer went on

    far = time.time() + 86_400.0
    memory_transport.next_outcomes = [Outcome.acked()]
    with caplog.at_level(logging.INFO, logger=_CORE_LOG):
        Uploader([pipe], now_fn=lambda: far).pump()
    assert wm.results == [Advance.REFUSED]
    assert wm.get_cursor(*_KEY) == b"50"
    assert any("acked, record held" in r.getMessage()
               for r in caplog.records if r.name == _CORE_LOG)


def _core_held(caplog) -> list[tuple[str, str]]:
    return [(r.levelname, r.getMessage()) for r in caplog.records
            if r.name == _CORE_LOG and "acked, record held" in r.getMessage()]


def test_core_logs_record_held_once_per_key_per_window(tmp_path, memory_transport, caplog):
    # The stored record reads b"10" and every pump re-ships b"2", b"3": each
    # ack gets refused.  One INFO per key per window; DEBUG between; the
    # first INFO after the window counts the refusals since the last one.
    clock = _Clock()
    src = _IntOrderSource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    wm._write_cursor(*_KEY, cursor=b"10", last_ack="t0")
    up = Uploader([pipe], clock=clock)
    with caplog.at_level(logging.DEBUG, logger=_CORE_LOG):
        for _ in range(3):                       # inside one window
            up.pump()
            clock.t += 60.0
        clock.t = 1000.0 + BACKWARD_WARN_INTERVAL_S
        up.pump()                                # the window has passed
        clock.t += 1.0
        up.pump()                                # a new window: DEBUG again
    assert wm.results == [Advance.REFUSED] * 5
    got = _core_held(caplog)
    assert [lvl for lvl, _ in got] == ["INFO", "DEBUG", "DEBUG", "INFO", "DEBUG"], got
    assert got[0][1].startswith("test: acked, record held: send record (%s, %s, %s)" % _KEY)
    assert "3 times since the last INFO" in got[3][1], got[3][1]
    assert "(1 since the last INFO)" in got[4][1], got[4][1]


def test_the_core_window_is_per_key_and_on_the_monotonic_clock():
    default = inspect.signature(Uploader.__init__).parameters["clock"].default
    assert default is time.monotonic


def test_a_written_advance_never_says_record_held(tmp_path, memory_transport, caplog):
    src = _IntOrderSource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    with caplog.at_level(logging.DEBUG, logger=_CORE_LOG):
        Uploader([pipe]).pump()
    assert not any("record held" in r.getMessage() for r in caplog.records)


# ---- the GRAPE spool: kept files are checked too (review N1) --------------------


def _spool(tmp_path, retention):
    spool = tmp_path / "spool"
    spool.mkdir()
    return FileTreeSource(spool, specs=[FileSpec("*")], retention=retention)


def test_filetree_keep_spool_is_checked(tmp_path, caplog):
    src = _spool(tmp_path, FileTreeSource.KEEP)
    key = (src.source_id(), "psws", "grape.files")
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    later = b"1781583005000000000"
    earlier = b"1781583001000000000"
    store._write_cursor(*key, cursor=later, last_ack="t0")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        got = store.advance_cursor(*key, cursor=earlier, last_ack="t1",
                                   is_after=_is_after_for(src))
    assert got is Advance.REFUSED
    assert store.get_cursor(*key) == later
    assert len(_warnings(caplog)) == 1
    # A legacy float-seconds record compares by the same clock.
    assert store.advance_cursor(*key, cursor=b"1781583010.5", last_ack="t2",
                                is_after=_is_after_for(src)) is Advance.WRITTEN


def test_a_delete_on_ack_spool_answers_after_always(tmp_path):
    src = _spool(tmp_path, FileTreeSource.DELETE_ON_ACK)
    key = (src.source_id(), "psws", "grape.files")
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*key, cursor=b"<delete-on-ack>", last_ack="t0")
    assert store.advance_cursor(*key, cursor=b"<delete-on-ack>", last_ack="t1",
                                is_after=_is_after_for(src)) is Advance.WRITTEN
    assert _last_ack(store, key) == "t1"


# ---- the check's own failures -----------------------------------------------------


def _edit_cursor_as_text(path, key, value: str) -> None:
    # A hand edit in the sqlite3 shell stores the cursor as TEXT.  get_cursor
    # does bytes(row["cursor"]), and bytes("5") raises TypeError.
    conn = sqlite3.connect(path)
    conn.execute(
        "UPDATE watermarks SET cursor = ? "
        "WHERE source_id = ? AND dest_id = ? AND table_name = ?",
        (value, *key),
    )
    conn.commit()
    conn.close()


def _cursor_type(path, key) -> str:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT typeof(cursor) FROM watermarks "
            "WHERE source_id = ? AND dest_id = ? AND table_name = ?", key,
        ).fetchone()[0]
    finally:
        conn.close()


def test_a_text_typed_stored_cursor_writes_unchecked(tmp_path, caplog):
    path = tmp_path / "wm.db"
    store = SqliteWatermarkStore(path)
    store._write_cursor(*_KEY, cursor=b"5", last_ack="t0")
    _edit_cursor_as_text(path, _KEY, "5")
    assert _cursor_type(path, _KEY) == "text"      # the premise of this test

    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert _write(store, _KEY, b"6", "t1") is Advance.WRITTEN_UNCHECKED

    assert store.get_cursor(*_KEY) == b"6"
    assert _last_ack(store) == "t1"
    assert _cursor_type(path, _KEY) == "blob"
    (rec,) = _warnings(caplog)
    msg = rec.getMessage()
    assert "send record (%s, %s, %s)" % _KEY in msg
    assert "forward check raised TypeError" in msg
    assert "does not move forward" not in msg


class _RaisingFilter(logging.Filter):
    """A log filter that fails, as a broken handler setup might."""

    def filter(self, record):
        raise RuntimeError("log filter bug")


def test_a_logging_failure_changes_no_decision(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    log = logging.getLogger(_STORE_LOG)
    flt = _RaisingFilter()
    log.addFilter(flt)

    def broken(new: bytes, stored: bytes) -> bool:
        raise RuntimeError("comparator bug")

    try:
        assert _write(store, _KEY, b"999", "t1") is Advance.REFUSED
        assert store.get_cursor(*_KEY) == b"1000"
        assert store.advance_cursor(*_KEY, cursor=b"7", last_ack="t2",
                                    is_after=broken) is Advance.WRITTEN_UNCHECKED
        assert store.get_cursor(*_KEY) == b"7"
    finally:
        log.removeFilter(flt)
    # The store's connection holds no open transaction afterwards.
    assert store._conn.in_transaction is False


def test_a_comparator_that_keeps_raising_logs_one_warning_per_window(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store._write_cursor(*_KEY, cursor=b"0", last_ack="t")

    def broken(new: bytes, stored: bytes) -> bool:
        raise RuntimeError("comparator bug")

    def write(cursor: bytes) -> Advance:
        return store.advance_cursor(
            *_KEY, cursor=cursor, last_ack="t", is_after=broken,
        )

    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert write(b"1") is Advance.WRITTEN_UNCHECKED   # the first: warns
        clock.t += 60.0
        assert write(b"2") is Advance.WRITTEN_UNCHECKED   # inside the hour: DEBUG
        assert write(b"3") is Advance.WRITTEN_UNCHECKED
        clock.t += BACKWARD_WARN_INTERVAL_S
        assert write(b"4") is Advance.WRITTEN_UNCHECKED   # after the hour: warns
        assert store.get_cursor(*_KEY) == b"4"
    levels = [r.levelname for r in caplog.records if r.name == _STORE_LOG]
    assert levels == ["WARNING", "DEBUG", "DEBUG", "WARNING"]
    assert all("comparator bug" in r.getMessage()
               for r in caplog.records if r.name == _STORE_LOG)


def test_a_database_error_rolls_back_and_raises(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"1", last_ack="t0")
    store._conn.execute("DROP TABLE watermarks")
    store._conn.commit()
    with pytest.raises(sqlite3.OperationalError):
        _write(store, _KEY, b"2", "t1")
    assert store._conn.in_transaction is False


class _RollbackFails:
    """The store's connection, except that rollback() raises."""

    def __init__(self, conn):
        self._c = conn

    def rollback(self):
        raise RuntimeError("rollback bug")

    def __getattr__(self, name):
        return getattr(self._c, name)


def test_a_failed_rollback_keeps_the_original_error(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store._write_cursor(*_KEY, cursor=b"1", last_ack="t0")
    store._conn.execute("DROP TABLE watermarks")
    store._conn.commit()
    real = store._conn
    store._conn = _RollbackFails(real)
    try:
        with pytest.raises(sqlite3.OperationalError):
            _write(store, _KEY, b"2", "t1")
    finally:
        store._conn = real
        real.rollback()


# ---- the warning windows count from zero, and the texts stay exact -------------


def test_each_window_counts_from_zero_after_a_summary(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store._write_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        _write(store, _KEY, b"999", "t1")                # the first: warns
        for cursor in range(998, 992, -1):               # three full windows
            clock.t += BACKWARD_WARN_INTERVAL_S / 2
            _write(store, _KEY, str(cursor).encode(), "t")
    summaries = [r.getMessage() for r in _warnings(caplog)][1:]
    assert len(summaries) == 3
    assert all("does not move forward: 2 times since the last warning" in m
               for m in summaries), summaries
    counts = [r.getMessage() for r in _debugs(caplog)]
    assert len(counts) == 3
    assert all(m.endswith("(1 since the last warning)") for m in counts), counts


def test_the_warning_and_debug_texts_are_exact(tmp_path, caplog):
    # An operator greps the journal for "does not move forward" and sums the counts.
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    key = ("s", "d", "t")
    store._write_cursor(*key, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        _write(store, key, b"999", "t1")                 # the first form
        clock.t += 10.0
        _write(store, key, b"998", "t2")                 # a repeat: DEBUG
        clock.t += BACKWARD_WARN_INTERVAL_S
        _write(store, key, b"997", "t3")                 # the summary: 2 times
        clock.t += BACKWARD_WARN_INTERVAL_S
        _write(store, key, b"996", "t4")                 # the summary: 1 time
    got = [(r.levelname, r.getMessage()) for r in caplog.records
           if r.name == _STORE_LOG]
    assert got == [
        ("WARNING",
         "send record (s, d, t) does not move forward: stored b'1000', new b'999'; "
         "refused, record held (repeats on this key log at DEBUG for 3600 s, "
         "then one WARNING counts them)"),
        ("DEBUG",
         "send record (s, d, t) again fails to move forward: stored b'1000', "
         "new b'998' (1 since the last warning)"),
        ("WARNING",
         "send record (s, d, t) does not move forward: 2 times since the last "
         "warning, the latest stored b'1000', new b'997'; refused, record held"),
        ("WARNING",
         "send record (s, d, t) does not move forward: 1 time since the last "
         "warning, the latest stored b'1000', new b'996'; refused, record held"),
    ]
    assert all("does not move forward" in m for lvl, m in got if lvl == "WARNING")
    assert all("does not move forward" not in m for lvl, m in got if lvl == "DEBUG")


def test_the_window_is_3600_seconds_on_the_monotonic_clock():
    assert BACKWARD_WARN_INTERVAL_S == 3600.0
    default = inspect.signature(SqliteWatermarkStore.__init__).parameters["clock"].default
    assert default is time.monotonic


# ---- a real source's own cursors stay silent -----------------------------------


def _wspr_row(conn, table: str, cycle: str) -> None:
    conn.execute(
        "INSERT INTO pending_uploads"
        "(target_db, target_table, schema_version, payload_json, queued_at) "
        "VALUES('wspr', ?, 2, ?, '')",
        (table, json.dumps({"time": cycle, "rx_call": "AC0G/B4", "band": "20"})),
    )


def _sqlite_source(tmp_path):
    # Twelve rows, two per pump: the ids run 2, 4, ... 12, and 8 -> 10 is
    # where a byte compare would call the record backward.
    conn = sqlite3.connect(tmp_path / "sink.db", timeout=10.0)
    _seed_table(conn)
    for i in range(12):
        _insert(conn, target_db="psk", target_table="spots", payload={"i": i})
    conn.close()
    return _src(tmp_path / "sink.db"), 6, 2


def _keep_source(tmp_path):
    spool = tmp_path / "spool"
    spool.mkdir()
    for i in range(5):
        f = spool / f"f{i}.txt"
        f.write_text("x")
        ns = 1_781_583_000_000_000_000 + i * 1_000_000_000
        os.utime(f, ns=(ns, ns))
    return (FileTreeSource(spool, specs=[FileSpec("*")],
                           retention=FileTreeSource.KEEP), 3, 2)


def _delete_on_ack_source(tmp_path):
    spool = tmp_path / "spool"
    spool.mkdir()
    for i in range(5):
        (spool / f"f{i}.txt").write_text("x")
    return FileTreeSource(spool, specs=[FileSpec("*")]), 3, 2


def _wspr_source(tmp_path):
    conn = sqlite3.connect(tmp_path / "sink.db")
    _seed_table(conn)
    for minute in (0, 2, 4, 6):
        cycle = f"2026-01-01T12:{minute:02d}:00Z"
        _wspr_row(conn, "spots", cycle)
        _wspr_row(conn, "noise", cycle)
    conn.commit()
    conn.close()
    return WsprCycleSource(db_path=tmp_path / "sink.db", start_at="beginning"), 4, 1000


@pytest.mark.parametrize("build", [
    _sqlite_source, _keep_source, _delete_on_ack_source, _wspr_source,
], ids=["sqlite", "file-keep", "file-delete-on-ack", "wspr-cycle"])
def test_a_real_sources_own_cursors_stay_silent(tmp_path, memory_transport, caplog, build):
    src, batches, batch_limit = build(tmp_path)
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(name="test", source=src, transport=memory_transport,
                    watermark=wm, identity=_ident(), batch_limit=batch_limit)
    assert _is_after_for(src) == src.cursor_is_after
    with caplog.at_level(logging.DEBUG):
        up = Uploader([pipe])
        for _ in range(batches + 2):
            up.pump()
    assert len(memory_transport.shipped) == batches
    key = (src.source_id(), "memory", "test.spots")
    assert wm.get_cursor(*key) == memory_transport.shipped[-1].cursor_after
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    assert [r for r in caplog.records
            if r.name == _STORE_LOG and r.levelno == logging.DEBUG] == []
    assert not any("record held" in r.getMessage() for r in caplog.records)
