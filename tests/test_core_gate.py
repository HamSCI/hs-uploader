"""The gate in the pump; the held path (spec sink-2b-part2 §3.2, §3.3; tests 3-5).

A stand-in transport records every call in one shared log, so a test can
show both that a held pipeline sent nothing and, by a positive control in
the same harness, that the same pipeline sends once the gate opens.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from hs_uploader import Outcome, Pipeline, Record, RetryPolicy, StationIdentity, Uploader
from hs_uploader import sinkswitch
from hs_uploader.gate import SinkGate
from hs_uploader.watermark import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport

SRC, DEST, TABLE = "memory:test", "memory", "test.spots"
FAR = time.time() + 86_400.0  # every queued retry is due by then


def _ident():
    return StationIdentity(call="AC0G", grid="EM38ww")


def _rec(i):
    return (Record(table=TABLE,
                   time=datetime(2026, 5, 8, 12, 0, i, tzinfo=timezone.utc),
                   columns={"i": i}),
            str(i).encode())


class LoggedTransport(MemoryTransport):
    """MemoryTransport that also appends each call to a shared log, and
    runs an optional hook after each call (to flip a switch mid-pump)."""

    def __init__(self, log, after=None):
        super().__init__()
        self.log = log
        self.after = after or (lambda kind: None)

    def ship(self, batch, identity):
        self.log.append(("ship", tuple(r.columns["i"] for r in batch.records)))
        out = super().ship(batch, identity)
        self.after("ship")
        return out

    def replay(self, payload_blob, identity):
        self.log.append(("replay", payload_blob))
        out = super().replay(payload_blob, identity)
        self.after("replay")
        return out


class CountingSource(MemorySource):
    def __init__(self, records):
        super().__init__(records=records)
        self.reads = 0

    def iter_batches(self, cursor, limit):
        self.reads += 1
        yield from super().iter_batches(cursor, limit)


class Switch:
    """A gate whose answer a test (or a transport hook) flips."""

    def __init__(self, send):
        self.send = send
        self.asked = 0

    def __call__(self, pipe):
        self.asked += 1
        return self.send


def _store_snapshot(wm):
    c = wm._conn
    return {
        "attempts": [tuple(r) for r in c.execute("SELECT * FROM attempts ORDER BY rowid")],
        "deliverables": [tuple(r) for r in c.execute("SELECT * FROM deliverables ORDER BY id")],
        "watermarks": [tuple(r) for r in c.execute("SELECT * FROM watermarks ORDER BY source_id")],
        "dead_letter": wm.dead_letter_count(),
    }


def _pipe_with_retry_and_rows(tmp_path, log, **kw):
    """A pipeline holding one queued retry (records 1-2) and a source with
    two more rows (3-4) behind it."""
    src = CountingSource(records=[_rec(1), _rec(2)])
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    tr = LoggedTransport(log)
    pipe = Pipeline(name="test", source=src, transport=tr, watermark=wm,
                    identity=_ident(), client="psk-recorder",
                    retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5), **kw)
    tr.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    assert wm.deliverable_count("test") == 1
    src._records += [_rec(3), _rec(4)]
    log.clear()
    src.reads = 0
    return pipe, src, wm, tr


# -- test 3: a held pipeline touches nothing, then resumes where it stopped --

@pytest.mark.parametrize("pumps", [1, 100])
def test_held_pipeline_touches_nothing_then_resumes(tmp_path, pumps):
    log = []
    pipe, src, wm, _ = _pipe_with_retry_and_rows(tmp_path, log)
    before = _store_snapshot(wm)
    (d_id, _, _, _, attempts, next_at, *_rest) = before["deliverables"][0]
    gate = Switch(False)
    up = Uploader([pipe], now_fn=lambda: FAR, gate=gate)

    for _ in range(pumps):
        assert up.pump() is False
    assert gate.asked >= pumps
    assert log == []                       # no transport call
    assert src.reads == 0                  # never reads its source
    assert _store_snapshot(wm) == before   # no attempt row; retry and cursor as they stood
    row = wm._conn.execute("SELECT id, attempts, next_attempt_at FROM deliverables").fetchone()
    assert tuple(row) == (d_id, attempts, next_at)
    assert wm.get_cursor(SRC, DEST, TABLE) == b""

    gate.send = True                       # release: the retry first, then the source
    assert up.pump() is True
    assert log == [("replay", b"2"), ("ship", (3, 4))]
    assert wm.deliverable_count("test") == 0
    assert wm.get_cursor(SRC, DEST, TABLE) == b"4"
    assert len(_store_snapshot(wm)["attempts"]) > len(before["attempts"])


def test_open_gate_control_sends(tmp_path):
    """Positive control: the same harness, gate open, records the sends."""
    log = []
    pipe, src, wm, _ = _pipe_with_retry_and_rows(tmp_path, log)
    before = _store_snapshot(wm)
    up = Uploader([pipe], now_fn=lambda: FAR, gate=Switch(True))
    assert up.pump() is True
    assert log == [("replay", b"2"), ("ship", (3, 4))]
    assert _store_snapshot(wm) != before


def test_held_from_first_pump_never_reads_its_source(tmp_path):
    """No retry queued: the check at the top of the pump keeps a held
    pipeline from opening a batch at all (spec §3.5 relies on it: a
    start_at=now source anchors at its first read).  Control: the same
    pipeline reads and ships once released."""
    log = []
    src = CountingSource(records=[_rec(1), _rec(2)])
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(name="test", source=src, transport=LoggedTransport(log),
                    watermark=wm, identity=_ident(), client="psk-recorder")
    gate = Switch(False)
    up = Uploader([pipe], gate=gate)
    for _ in range(100):
        up.pump()
    assert (src.reads, log, wm.get_cursor(SRC, DEST, TABLE)) == (0, [], b"")
    assert _store_snapshot(wm)["attempts"] == []
    gate.send = True
    assert up.pump() is True
    assert src.reads == 1 and log == [("ship", (1, 2))]


def test_held_never_dead_letters(tmp_path):
    """Nothing attempts while held, so nothing reaches dead-letter however
    long the hold lasts, even past max_attempts pumps."""
    log = []
    pipe, _, wm, _ = _pipe_with_retry_and_rows(tmp_path, log)
    clock = [FAR]
    up = Uploader([pipe], now_fn=lambda: clock[0], gate=Switch(False))
    for _ in range(20):
        clock[0] += 3600.0
        up.pump()
    assert wm.dead_letter_count() == 0
    assert wm.deliverable_count("test") == 1
    assert log == []


# -- test 4: a switch change counts at the next send, not the next pump --

def _two_retries(tmp_path, log, gate):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    tr = LoggedTransport(log, after=lambda kind: setattr(gate, "send", False))
    pipe = Pipeline(name="test", source=CountingSource(records=[]), transport=tr,
                    watermark=wm, identity=_ident(), client="psk-recorder")
    for i, at in ((1, "2026-01-01T00:00:01Z"), (2, "2026-01-01T00:00:02Z")):
        wm.enqueue_deliverable(pipeline="test", payload_blob=str(i).encode(),
                               enqueued_at=at, next_attempt_at=at, source_id=SRC,
                               dest_id=DEST, table=TABLE, cursor_after=str(i).encode())
    return pipe, wm


def test_flip_between_replays_stops_the_second(tmp_path):
    log, gate = [], Switch(True)
    pipe, wm = _two_retries(tmp_path, log, gate)
    second = wm._conn.execute("SELECT * FROM deliverables WHERE payload_blob=?",
                              (b"2",)).fetchone()
    Uploader([pipe], now_fn=lambda: FAR, gate=gate).pump()
    assert log == [("replay", b"1")]
    assert wm.deliverable_count("test") == 1
    assert tuple(wm._conn.execute("SELECT * FROM deliverables").fetchone()) == tuple(second)


def test_flip_between_replays_control(tmp_path):
    """Control: with no flip, both retries replay in one pump."""
    log, gate = [], Switch(True)
    pipe, wm = _two_retries(tmp_path, log, gate)
    pipe.transport.after = lambda kind: None
    Uploader([pipe], now_fn=lambda: FAR, gate=gate).pump()
    assert log == [("replay", b"1"), ("replay", b"2")]


def _drain_mode(tmp_path, log, gate, flip=True):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    after = (lambda kind: setattr(gate, "send", False)) if flip else None
    tr = LoggedTransport(log, after=after)
    src = CountingSource(records=[_rec(1), _rec(2), _rec(3)])
    pipe = Pipeline(name="test", source=src, transport=tr, watermark=wm,
                    identity=_ident(), client="psk-recorder",
                    batch_limit=1, max_records_per_pump=10)
    return pipe, wm


def test_flip_between_drain_batches_stops_the_second(tmp_path):
    log, gate = [], Switch(True)
    pipe, wm = _drain_mode(tmp_path, log, gate)
    Uploader([pipe], gate=gate).pump()
    assert log == [("ship", (1,))]
    assert wm.get_cursor(SRC, DEST, TABLE) == b"1"


def test_flip_between_drain_batches_control(tmp_path):
    """Control: with no flip, drain mode ships all three batches in one pump."""
    log, gate = [], Switch(True)
    pipe, wm = _drain_mode(tmp_path, log, gate, flip=False)
    Uploader([pipe], gate=gate).pump()
    assert log == [("ship", (1,)), ("ship", (2,)), ("ship", (3,))]
    assert wm.get_cursor(SRC, DEST, TABLE) == b"3"


# -- test 5: Uploader() with no gate behaves as part 1 (the shims' path, V15) --

@pytest.fixture
def holding_tree(tmp_path, monkeypatch):
    """A sinks directory with the site at off and the client at fill, and a
    standing marker, at the paths the system defaults resolve to."""
    sinks = tmp_path / "sinks"
    sinks.mkdir()
    (sinks / "site.toml").write_text('state = "off"\n')
    (sinks / "wspr-recorder.toml").write_text('state = "fill"\n')
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(state))
    monkeypatch.setattr(sinkswitch, "DEFAULT_SINKS_DIR", sinks)
    sinkswitch.marker_path().write_text("carried over\n")
    return sinks, sinkswitch.marker_path()


def _shim_pipelines(tmp_path, log):
    """Two pipelines, as wspr-recorder's shim builds them (wsprdaemon and
    wsprnet), each naming a governed client."""
    out = []
    for name in ("wsprdaemon", "wsprnet"):
        out.append(Pipeline(name=name, source=CountingSource(records=[_rec(1), _rec(2)]),
                            transport=LoggedTransport(log),
                            watermark=SqliteWatermarkStore(tmp_path / f"{name}.db"),
                            identity=_ident(), client="wspr-recorder"))
    return out


def test_no_gate_ships_as_part1(tmp_path, holding_tree):
    log, outcomes = [], []
    pipes = _shim_pipelines(tmp_path, log)
    # Exactly the shim's call (wspr-recorder hs_uploader_shim.py:345).
    up = Uploader(pipes, on_batch_outcome=lambda p, b, o: outcomes.append(p.name))
    assert up.pump() is True
    assert sorted(log) == [("ship", (1, 2)), ("ship", (1, 2))]
    assert sorted(outcomes) == ["wsprdaemon", "wsprnet"]
    for p in pipes:
        assert p.watermark.get_cursor(SRC, DEST, TABLE) == b"2"


def test_no_gate_control_tree_holds(tmp_path, holding_tree):
    """Control: the same tree, given to a real SinkGate, holds every pipeline,
    so the test above proves the default ignores it."""
    sinks, marker = holding_tree
    log = []
    pipes = _shim_pipelines(tmp_path, log)
    up = Uploader(pipes, gate=SinkGate(sinks, marker).may_send)
    assert up.pump() is False
    assert log == []
