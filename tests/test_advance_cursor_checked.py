"""The warn-only forward check on send records (sigmond plan-sink-control §10.4 item 4).

advance_cursor_checked asks the source whether a write moves the send record
forward.  When it does not, the store logs the key and both values, then
writes exactly as advance_cursor does.  The first such write on a key logs a
WARNING; for the next hour further ones log at DEBUG and add to a count; the
first one after the hour logs one WARNING with that count.  v3.70 changes
nothing the station sends; the warnings measure how often a shared key runs
backward before v3.71 enforces the rule.
"""
from __future__ import annotations

import inspect
import json
import logging
import os
import sqlite3
import time

import pytest

from hs_uploader import Outcome, Pipeline, RetryPolicy, Uploader
from hs_uploader.core import _is_after_for
from hs_uploader.sources import FileSpec, FileTreeSource
from hs_uploader.sources.wspr_cycle import WsprCycleSource
from hs_uploader.watermark import SqliteWatermarkStore
from hs_uploader.watermark.sqlite import BACKWARD_WARN_INTERVAL_S
from tests.conftest import MemorySource
from tests.test_core_orchestration import _ident, _records
from tests.test_sqlite_source import _insert, _seed_table, _src

_STORE_LOG = "hs_uploader.watermark.sqlite"
_KEY = ("memory:test", "memory", "test.spots")
_PSK = ("sqlite:psk.spots", "pskreporter", "psk.spots")


def _int_order(new: bytes, stored: bytes) -> bool:
    return not stored or int(new) > int(stored)


def _last_ack(store: SqliteWatermarkStore) -> str:
    (row,) = store.all_cursors()
    return row["last_ack"]


def _warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.name == _STORE_LOG and r.levelno == logging.WARNING]


def _debugs(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.name == _STORE_LOG and r.levelno == logging.DEBUG]


class _Clock:
    """A clock the test moves by hand, in seconds."""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


def _write(store: SqliteWatermarkStore, key, cursor: bytes, last_ack: str) -> bool:
    return store.advance_cursor_checked(*key, cursor=cursor, last_ack=last_ack,
                                        is_after=_int_order)


# ---- the store ---------------------------------------------------------------


def test_a_forward_write_returns_true_and_stays_quiet(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor("s", "d", "t", cursor=b"999", last_ack="t1")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert store.advance_cursor_checked(
            "s", "d", "t", cursor=b"1000", last_ack="t2", is_after=_int_order,
        ) is True
    assert store.get_cursor("s", "d", "t") == b"1000"
    assert _last_ack(store) == "t2"
    assert _warnings(caplog) == []


def test_a_backward_write_warns_with_key_and_both_values_and_still_writes(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor("sqlite:psk.spots", "pskreporter", "psk.spots",
                         cursor=b"1000", last_ack="t1")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert store.advance_cursor_checked(
            "sqlite:psk.spots", "pskreporter", "psk.spots",
            cursor=b"999", last_ack="t2", is_after=_int_order,
        ) is False
    # v3.70 warns only: the write happens exactly as advance_cursor makes it.
    assert store.get_cursor("sqlite:psk.spots", "pskreporter", "psk.spots") == b"999"
    assert _last_ack(store) == "t2"
    (rec,) = _warnings(caplog)
    msg = rec.getMessage()
    for part in ("sqlite:psk.spots", "pskreporter", "psk.spots", "b'1000'", "b'999'"):
        assert part in msg, (part, msg)


def test_a_write_that_stands_still_warns_too(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor("s", "d", "t", cursor=b"42", last_ack="t1")
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert store.advance_cursor_checked(
            "s", "d", "t", cursor=b"42", last_ack="t2", is_after=_int_order,
        ) is False
    assert len(_warnings(caplog)) == 1
    assert _last_ack(store) == "t2"


def test_the_first_write_hands_the_comparator_an_empty_stored_value(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    seen: list[tuple[bytes, bytes]] = []

    def spy(new: bytes, stored: bytes) -> bool:
        seen.append((new, stored))
        return True

    assert store.advance_cursor_checked(
        "s", "d", "t", cursor=b"7", last_ack="t1", is_after=spy,
    ) is True
    assert seen == [(b"7", b"")]
    assert store.get_cursor("s", "d", "t") == b"7"


def test_a_comparator_that_raises_never_blocks_the_write(tmp_path, caplog):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor("s", "d", "t", cursor=b"1", last_ack="t1")

    def broken(new: bytes, stored: bytes) -> bool:
        raise RuntimeError("comparator bug")

    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        assert store.advance_cursor_checked(
            "s", "d", "t", cursor=b"2", last_ack="t2", is_after=broken,
        ) is True
    assert store.get_cursor("s", "d", "t") == b"2"
    assert any("comparator bug" in r.getMessage() for r in _warnings(caplog))


# ---- one WARNING per key per hour --------------------------------------------


def test_a_repeat_within_the_hour_logs_at_debug_and_still_writes(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store.advance_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert _write(store, _KEY, b"999", "t1") is False     # the first: warns
        clock.t += 3599.0
        assert _write(store, _KEY, b"998", "t2") is False     # inside the hour
    assert len(_warnings(caplog)) == 1
    (debug,) = _debugs(caplog)
    assert "stored b'999', new b'998'" in debug.getMessage()
    assert "(1 since the last warning)" in debug.getMessage()
    assert store.get_cursor(*_KEY) == b"998"
    assert _last_ack(store) == "t2"


def test_after_the_hour_one_warning_counts_the_writes_since_the_last(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store.advance_cursor(*_KEY, cursor=b"1000", last_ack="t0")
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
    assert "the latest stored b'996', new b'990'" in msg
    assert store.get_cursor(*_KEY) == b"989"
    assert _last_ack(store) == "t4"


@pytest.mark.parametrize("other", [
    ("sqlite:psk.spots@meteor-scatter", _PSK[1], _PSK[2]),   # another source_id
    (_PSK[0], "pskreporter-udp", _PSK[2]),                   # another dest_id
    (_PSK[0], _PSK[1], "psk.msk144"),                        # another table
])
def test_each_key_keeps_its_own_window(tmp_path, caplog, other):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    for key in (_PSK, other):
        store.advance_cursor(*key, cursor=b"100", last_ack="t0")
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
    assert store.get_cursor(*_PSK) == b"97"
    assert store.get_cursor(*other) == b"98"


def test_every_write_goes_ahead_whatever_it_logs(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store.advance_cursor(*_KEY, cursor=b"100", last_ack="t0")
    steps = [(0.0, b"99"), (10.0, b"98"), (3600.0, b"97"), (5.0, b"96"), (7200.0, b"95")]
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        for i, (step, cursor) in enumerate(steps, start=1):
            clock.t += step
            assert _write(store, _KEY, cursor, f"t{i}") is False
            assert store.get_cursor(*_KEY) == cursor
            assert _last_ack(store) == f"t{i}"
    levels = [r.levelname for r in caplog.records if r.name == _STORE_LOG]
    assert levels == ["WARNING", "DEBUG", "WARNING", "DEBUG", "WARNING"]


# ---- core: all three advance sites use the checked call --------------------


class _SpyStore(SqliteWatermarkStore):
    """Counts checked advances, and bare advance_cursor calls made from outside one."""

    def __init__(self, path):
        super().__init__(path)
        self.checked: list[tuple] = []
        self.bare = 0
        self._in_checked = False

    def advance_cursor(self, *a, **kw):
        if not self._in_checked:
            self.bare += 1
        return super().advance_cursor(*a, **kw)

    def advance_cursor_checked(self, source_id, dest_id, table, *, cursor,
                               last_ack, is_after):
        self.checked.append((source_id, dest_id, table, cursor, is_after))
        self._in_checked = True
        try:
            return super().advance_cursor_checked(
                source_id, dest_id, table, cursor=cursor, last_ack=last_ack,
                is_after=is_after,
            )
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


def test_core_warns_on_a_backward_ack_and_still_writes(tmp_path, memory_transport, caplog):
    # The stored record reads b"10".  MemorySource filters with a byte compare,
    # so it yields b"2" and b"3", and the ack moves the record from 10 to 3.
    src = _IntOrderSource(records=_records(3))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    wm.advance_cursor(*_KEY, cursor=b"10", last_ack="t0")
    wm.bare = 0
    with caplog.at_level(logging.WARNING, logger=_STORE_LOG):
        Uploader([pipe]).pump()
    assert wm.bare == 0
    assert wm.get_cursor(*_KEY) == b"3"
    (rec,) = _warnings(caplog)
    assert "b'10'" in rec.getMessage() and "b'3'" in rec.getMessage()


# ---- the check never decides whether the write happens ----------------------


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


def test_a_text_typed_stored_cursor_does_not_block_the_write(tmp_path, caplog):
    path = tmp_path / "wm.db"
    store = SqliteWatermarkStore(path)
    store.advance_cursor(*_KEY, cursor=b"5", last_ack="t0")
    _edit_cursor_as_text(path, _KEY, "5")
    assert _cursor_type(path, _KEY) == "text"      # the premise of this test

    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        # The read raises, so the check cannot run: it counts as forward.
        assert _write(store, _KEY, b"6", "t1") is True

    # advance_cursor made the write, as it would have without the check.
    assert store.get_cursor(*_KEY) == b"6"
    assert _last_ack(store) == "t1"
    assert _cursor_type(path, _KEY) == "blob"
    (rec,) = _warnings(caplog)
    msg = rec.getMessage()
    assert "send record (%s, %s, %s)" % _KEY in msg
    assert "forward check raised TypeError" in msg
    # Task 10 greps for this phrase to count backward writes: keep it out.
    assert "does not move forward" not in msg


class _RaisingFilter(logging.Filter):
    """A log filter that fails, as a broken handler setup might."""

    def filter(self, record):
        raise RuntimeError("log filter bug")


def test_a_logging_failure_does_not_block_the_write(tmp_path):
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    store.advance_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    log = logging.getLogger(_STORE_LOG)
    flt = _RaisingFilter()
    log.addFilter(flt)
    try:
        # The write moves backward, so the store tries to log, and the log
        # call raises.  The answer is the comparator's; the write still happens.
        assert _write(store, _KEY, b"999", "t1") is False
    finally:
        log.removeFilter(flt)
    assert store.get_cursor(*_KEY) == b"999"
    assert _last_ack(store) == "t1"


def test_a_comparator_that_keeps_raising_logs_one_warning_per_window(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)

    def broken(new: bytes, stored: bytes) -> bool:
        raise RuntimeError("comparator bug")

    def write(cursor: bytes) -> bool:
        return store.advance_cursor_checked(
            *_KEY, cursor=cursor, last_ack="t", is_after=broken,
        )

    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        assert write(b"1") is True                  # the first: warns
        clock.t += 60.0
        assert write(b"2") is True                  # inside the hour: DEBUG
        assert write(b"3") is True
        clock.t += BACKWARD_WARN_INTERVAL_S
        assert write(b"4") is True                  # after the hour: warns again
        assert store.get_cursor(*_KEY) == b"4"
    levels = [r.levelname for r in caplog.records if r.name == _STORE_LOG]
    assert levels == ["WARNING", "DEBUG", "DEBUG", "WARNING"]
    assert all("comparator bug" in r.getMessage()
               for r in caplog.records if r.name == _STORE_LOG)


# ---- the warning windows count from zero, and the texts stay as Task 10 reads them


def test_each_window_counts_from_zero_after_a_summary(tmp_path, caplog):
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    store.advance_cursor(*_KEY, cursor=b"1000", last_ack="t0")
    with caplog.at_level(logging.DEBUG, logger=_STORE_LOG):
        _write(store, _KEY, b"999", "t1")                # the first: warns
        for cursor in range(998, 992, -1):               # three full windows
            clock.t += BACKWARD_WARN_INTERVAL_S / 2
            _write(store, _KEY, str(cursor).encode(), "t")
    summaries = [r.getMessage() for r in _warnings(caplog)][1:]
    assert len(summaries) == 3
    # Each summary counts the DEBUG write and itself, and nothing from before.
    assert all("does not move forward: 2 times since the last warning" in m
               for m in summaries), summaries
    counts = [r.getMessage() for r in _debugs(caplog)]
    assert len(counts) == 3
    assert all(m.endswith("(1 since the last warning)") for m in counts), counts


def test_the_warning_and_debug_texts_are_exact(tmp_path, caplog):
    # Task 10 greps the journal for these lines and sums their counts.
    clock = _Clock()
    store = SqliteWatermarkStore(tmp_path / "wm.db", clock=clock)
    key = ("s", "d", "t")
    store.advance_cursor(*key, cursor=b"1000", last_ack="t0")
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
         "writing it anyway (v3.70 warns only; repeats on this key log at DEBUG "
         "for 3600 s, then one WARNING counts them)"),
        ("DEBUG",
         "send record (s, d, t) again fails to move forward: stored b'999', "
         "new b'998' (1 since the last warning)"),
        ("WARNING",
         "send record (s, d, t) does not move forward: 2 times since the last "
         "warning, the latest stored b'998', new b'997'; writing it anyway "
         "(v3.70 warns only)"),
        ("WARNING",
         "send record (s, d, t) does not move forward: 1 time since the last "
         "warning, the latest stored b'997', new b'996'; writing it anyway "
         "(v3.70 warns only)"),
    ]
    # A grep for the phrase finds WARNINGs alone.
    assert all("does not move forward" in m for lvl, m in got if lvl == "WARNING")
    assert all("does not move forward" not in m for lvl, m in got if lvl == "DEBUG")


def test_the_window_is_3600_seconds_on_the_monotonic_clock():
    assert BACKWARD_WARN_INTERVAL_S == 3600.0
    # A wall clock would let a host-clock step stretch or cut a window.
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
    # The run did real work: every batch shipped, and the record moved with it.
    assert len(memory_transport.shipped) == batches
    key = (src.source_id(), "memory", "test.spots")
    assert wm.get_cursor(*key) == memory_transport.shipped[-1].cursor_after
    # And it stayed silent: normal operation never reads as a backward write.
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    assert [r for r in caplog.records
            if r.name == _STORE_LOG and r.levelno == logging.DEBUG] == []
