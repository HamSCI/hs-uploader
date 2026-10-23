"""The starting record (spec sink-2b-part2 §3.5, test 6).

A legacy entry with ``start_at = "now"`` fixes its starting point at the
first pump that reads its source, and keeps it in memory only.  A pipeline
held from its first pump never reads its source, so on a fresh station that
goes off, then fill, then upload, the first real pump would anchor at the
raise and skip the whole fill backlog.  So while the marker stands, the
daemon writes a starting record at each build, and when the marker first
appears, for every governed pipeline whose source starts at now and whose
key holds no record.

Every test that asserts an absence carries a positive control in the same
harness.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hs_uploader import daemon, sinkswitch
from hs_uploader.config import StationIdentity
from hs_uploader.core import Pipeline, Uploader
from hs_uploader.gate import SINK_GATE_ORIGIN, SinkGate
from hs_uploader.sources import wspr_cycle
from hs_uploader.sources.sqlite import SqliteSource, _ConnectionConfig
from hs_uploader.sources.wspr_cycle import WsprCycleSource
from hs_uploader.watermark.sqlite import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport

CLIENT = "psk-recorder"

# The queue as the v3.70+ writer makes it: producer and local columns.
_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL,
    producer        TEXT NOT NULL DEFAULT '',
    local           INTEGER NOT NULL DEFAULT 0
)
"""


def _make_sink(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(_DDL)
    conn.commit()
    conn.close()


def _put(path: Path, n: int, *, db="psk", table="spots", producer=CLIENT,
         local=0, time_iso="2026-10-23T12:00:00Z") -> int:
    conn = sqlite3.connect(path)
    cur = conn.execute(
        "INSERT INTO pending_uploads (target_db, target_table, schema_version, "
        "payload_json, queued_at, producer, local) VALUES (?,?,?,?,?,?,?)",
        (db, table, 2, json.dumps({"n": n, "time": time_iso}),
         "2026-10-23T12:00:00+00:00", producer, local))
    conn.commit()
    conn.close()
    return cur.lastrowid


def _psk_source(sink: Path, start_at="now") -> SqliteSource:
    return SqliteSource(database="psk", table="spots",
                        accepted_schema_versions=[2],
                        config=_ConnectionConfig(path=str(sink)),
                        start_at=start_at, producer=CLIENT)


def _transport(*tables):
    t = MemoryTransport(tables[0] if tables else "psk.spots",
                        accepts_versions=(1, 2))
    for extra in tables[1:]:
        t.ACCEPTS[extra] = [1, 2]
    return t


def _pipe(source, store, *, name="psk", client=CLIENT, governed=True,
          origin="legacy", transport=None):
    return Pipeline(name=name, source=source,
                    transport=transport or _transport(),
                    watermark=store,
                    identity=StationIdentity(call="AC0G", grid="EM38ww"),
                    client=client, governed=governed, origin=origin)


def _key(p):
    return (p.source_id(), p.dest_id(), p.transport.primary_table())


def _raw(db: Path, key):
    """The stored row's (cursor, last_ack), read beside the store."""
    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT cursor, last_ack FROM watermarks "
            "WHERE source_id=? AND dest_id=? AND table_name=?", key).fetchone()
    finally:
        conn.close()
    return None if row is None else (bytes(row[0]), row[1])


def _shipped(transport):
    return [r.columns["n"] for b in transport.shipped for r in b.records]


# ---- initial_cursor: the source's own encoding, byte for byte -----------------


def test_sqlite_initial_cursor_equals_a_part1_pump_cursor(tmp_path):
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    for n in (1, 2, 3):
        _put(sink, n)
    _put(sink, 90, local=1)                    # local rows never ship
    _put(sink, 91, producer="meteor-scatter")  # outside the producer scope
    store = SqliteWatermarkStore(wm)
    initial = _psk_source(sink).initial_cursor()
    # A part 1 pump that ships every row present now (start at beginning);
    # it deletes what it sent, so the answer is taken first.
    p = _pipe(_psk_source(sink, start_at=None), store)
    Uploader([p]).pump_until_idle()
    assert _shipped(p.transport) == [1, 2, 3]
    pumped = store.get_cursor(*_key(p))
    assert pumped == b"3"
    assert initial == pumped


def test_sqlite_initial_cursor_keeps_an_anchor_a_pump_already_fixed(tmp_path):
    # A first pump that read the source fixed its anchor in memory; the
    # starting record must be that anchor, not a later max(id).
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    _put(sink, 1)
    src = _psk_source(sink)
    p = _pipe(src, SqliteWatermarkStore(wm))
    Uploader([p]).pump()                       # anchors at id 1, ships nothing
    assert _shipped(p.transport) == []
    _put(sink, 2)
    assert src.initial_cursor() == b"1"
    assert _psk_source(sink).initial_cursor() == b"2"   # control: fresh source


def test_sqlite_initial_cursor_none_from_beginning_zero_on_fresh_sink(tmp_path):
    sink = tmp_path / "sink.db"
    assert _psk_source(sink, start_at=None).initial_cursor() is None
    assert _psk_source(sink).initial_cursor() == b"0"   # no file yet
    assert not sink.exists()                            # and it made none
    _make_sink(sink)
    assert _psk_source(sink).initial_cursor() == b"0"   # empty queue
    _put(sink, 1)
    assert _psk_source(sink).initial_cursor() == b"1"


class _Frozen(datetime):
    FIXED = datetime(2026, 10, 23, 12, 5, 30, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.FIXED


def test_wspr_initial_cursor_equals_a_part1_pump_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr(wspr_cycle, "datetime", _Frozen)
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    # Cycles 12:00 and 12:02 are complete; 12:04 is the one in progress.
    for n, t in ((1, "12:00"), (2, "12:02"), (3, "12:04")):
        _put(sink, n, db="wspr", table="spots", time_iso=f"2026-10-23T{t}:00Z")
        _put(sink, n, db="wspr", table="noise", time_iso=f"2026-10-23T{t}:00Z")
    store = SqliteWatermarkStore(wm)
    p = _pipe(WsprCycleSource(db_path=sink, start_at="beginning"), store,
              transport=_transport("wspr.spots", "wspr.noise"))
    Uploader([p]).pump_until_idle()
    assert _shipped(p.transport) == [1, 1, 2, 2]
    pumped = store.get_cursor(*_key(p))
    assert pumped == b"2026-10-23T12:02:00Z"
    assert WsprCycleSource(db_path=sink, start_at="now").initial_cursor() == pumped
    assert WsprCycleSource(db_path=sink, start_at="beginning").initial_cursor() is None


# ---- write_starting_records ----------------------------------------------------


def test_held_now_pipeline_without_a_record_gets_one(tmp_path):
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    _put(sink, 1)
    _put(sink, 2)
    store = SqliteWatermarkStore(wm)
    p = _pipe(_psk_source(sink), store)
    assert _raw(wm, _key(p)) is None
    written = daemon.write_starting_records([p], store)
    assert written == ["|".join(_key(p))]
    assert store.get_cursor(*_key(p)) == b"2"


def test_a_key_that_holds_a_record_stays_untouched(tmp_path):
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    for n in (1, 2, 3, 4):
        _put(sink, n)
    store = SqliteWatermarkStore(wm)
    old = _pipe(_psk_source(sink), store, name="old",
                transport=_transport("psk.spots"))
    old.transport.name = "memory-old"
    fresh = _pipe(_psk_source(sink), store, name="fresh")
    store._write_cursor(*_key(old), cursor=b"2", last_ack="2026-01-01T00:00:00")
    before = _raw(wm, _key(old))
    written = daemon.write_starting_records([old, fresh], store)
    assert _raw(wm, _key(old)) == before == (b"2", "2026-01-01T00:00:00")
    # Positive control: the same call wrote the key that held none.
    assert written == ["|".join(_key(fresh))]
    assert store.get_cursor(*_key(fresh)) == b"4"


def test_a_record_written_after_the_check_still_stays(tmp_path):
    # Another process writes the key between the pass's read and its write:
    # advance_cursor's order must still leave that record's cursor alone.
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    for n in (1, 2, 3, 4):
        _put(sink, n)

    class Racing(SqliteWatermarkStore):
        def get_cursor(self, *key):
            got = super().get_cursor(*key)
            if not got:
                self._write_cursor(*key, cursor=b"2", last_ack="other")
            return got

    store = Racing(wm)
    p = _pipe(_psk_source(sink), store)
    assert daemon.write_starting_records([p], store) == []
    assert _raw(wm, _key(p))[0] == b"2"
    # Positive control: without the race the same call writes.
    q = _pipe(_psk_source(sink), SqliteWatermarkStore(tmp_path / "wm2.db"))
    assert daemon.write_starting_records([q], q.watermark) != []
    assert q.watermark.get_cursor(*_key(q)) == b"4"


def test_beginning_and_ungoverned_pipelines_get_none(tmp_path):
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    _put(sink, 1)
    store = SqliteWatermarkStore(wm)

    def mk(name, **kw):
        p = _pipe(_psk_source(sink, kw.pop("start_at", "now")), store,
                  name=name, **kw)
        p.transport.name = f"mem-{name}"
        return p

    beginning = mk("beginning", start_at=None)
    ungoverned = mk("ungoverned", governed=False, origin=SINK_GATE_ORIGIN)
    heartbeat = mk("heartbeat", client="sigmond")
    no_init = _pipe(MemorySource(), store, name="memsrc")
    # A client file cannot opt out (plan ruling 2): the gate judges it, so
    # it gets a record, as does a plain governed one (positive controls).
    claims = mk("claims", governed=False, origin="pipelines.d/psk-recorder.toml")
    plain = mk("plain")
    pipes = [beginning, ungoverned, heartbeat, no_init, claims, plain]
    written = daemon.write_starting_records(pipes, store)
    assert written == ["|".join(_key(claims)), "|".join(_key(plain))]
    for p in (beginning, ungoverned, heartbeat, no_init):
        assert _raw(wm, _key(p)) is None, p.name


def test_a_failing_source_is_skipped_and_the_rest_written(tmp_path, caplog):
    sink, wm = tmp_path / "sink.db", tmp_path / "wm.db"
    _make_sink(sink)
    _put(sink, 1)
    store = SqliteWatermarkStore(wm)
    bad = _pipe(_psk_source(sink), store, name="bad")
    bad.transport.name = "mem-bad"
    bad.source.initial_cursor = lambda: 1 / 0
    good = _pipe(_psk_source(sink), store, name="good")
    assert daemon.write_starting_records([bad, good], store) == [
        "|".join(_key(good))]
    assert "ZeroDivisionError" in caplog.text


# ---- the daemon: the pass at each build, and when the marker appears ----------------


def _builder(*, identity, watermark, config, name):
    src = SqliteSource(database="psk", table="spots",
                       accepted_schema_versions=[2],
                       config=_ConnectionConfig(path=os.environ["SIGMOND_SQLITE_PATH"]),
                       start_at=config.get("start_at") or None, producer=CLIENT)
    TRANSPORTS[name] = t = _transport()
    return [Pipeline(name=name, source=src, transport=t,
                     watermark=watermark, identity=identity)]


TRANSPORTS: dict = {}

_MANIFEST = textwrap.dedent("""
    [identity]
    call = "AC0G/S"
    grid = "EM38ww"
    ssh_key_file = "/etc/hs-uploader/keys/id_ed25519"

    [daemon]
    pump_interval_sec = 0.01
    wake = false

    [[pipeline]]
    name = "psk"
    builder = "fake:fn"
    client = "psk-recorder"
    [pipeline.config]
    start_at = "now"
""")


@pytest.fixture
def station(tmp_path, monkeypatch):
    sink = tmp_path / "sink.db"
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(sink))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: _builder)
    TRANSPORTS.clear()
    (tmp_path / "state").mkdir()
    etc = tmp_path / "etc"
    etc.mkdir()
    manifest = etc / "pipelines.toml"
    manifest.write_text(_MANIFEST)
    _make_sink(sink)

    class S:
        pass

    s = S()
    s.sink, s.manifest, s.sinks = sink, manifest, etc / "sinks"
    s.marker = sinkswitch.marker_path()
    s.wm = tmp_path / "state" / "watermarks.db"

    def switch(name, state):
        sinkswitch.write_switch(s.sinks / f"{name}.toml",
                                {"state": state, "set_by": "test"})

    s.switch = switch
    s.mark = lambda: sinkswitch.write_marker(s.marker, ["test"])
    return s


def _rebuilder(station, marker=True):
    store = SqliteWatermarkStore(station.wm)
    r = daemon.Rebuilder(station.manifest, daemon.load_manifest(station.manifest),
                         store, marker=station.marker if marker else None)
    return r, store


def test_build_writes_a_record_only_while_the_marker_stands(station):
    _put(station.sink, 1)
    r, store = _rebuilder(station)
    running, _b, _r = r.start()
    key = _key(running[0])
    assert _raw(station.wm, key) is None          # no marker, no record
    station.mark()
    _put(station.sink, 2)
    r2, _ = _rebuilder(station)
    r2.start()
    assert store.get_cursor(*key) == b"2"         # marker: the build wrote it


def test_not_held_without_the_marker_matches_part1(station):
    # No marker: the pass writes nothing, and the pump does exactly what a
    # part 1 Uploader over the same source does (anchor at its first pump).
    _put(station.sink, 1)
    r, store = _rebuilder(station)
    running, _b, _r = r.start()
    assert store.all_cursors() == []
    up = Uploader(running, inline_single=False)
    up.pump_until_idle()
    _put(station.sink, 2)
    _put(station.sink, 3)
    up.pump_until_idle()
    got = (_shipped(TRANSPORTS["psk"]), store.get_cursor(*_key(running[0])))

    ref_store = SqliteWatermarkStore(station.wm.with_name("ref.sqlite"))
    ref_sink = station.sink.with_name("ref-sink.db")
    _make_sink(ref_sink)
    _put(ref_sink, 1)
    ref = _pipe(_psk_source(ref_sink), ref_store)
    ref_up = Uploader([ref])
    ref_up.pump_until_idle()
    _put(ref_sink, 2)
    _put(ref_sink, 3)
    ref_up.pump_until_idle()
    assert got == (_shipped(ref.transport), ref_store.get_cursor(*_key(ref)))
    assert got == ([2, 3], b"3")


def test_off_fill_upload_ships_the_fill_rows_oldest_first(station):
    _put(station.sink, 100)                  # history before the build
    station.switch("site", "upload")
    station.switch(CLIENT, "off")
    station.mark()
    r, store = _rebuilder(station)
    running, _b, _r = r.start()
    gate = SinkGate(r.sdir, station.marker)
    up = Uploader(running, inline_single=False, gate=gate.may_send)
    t = TRANSPORTS["psk"]
    up.pump_until_idle()                     # off: held from its first pump
    station.switch(CLIENT, "fill")
    for n in (1, 2, 3):
        _put(station.sink, n)                # stored during fill
    up.pump_until_idle()
    assert t.shipped == []                   # held
    station.switch(CLIENT, "upload")
    up.pump_until_idle()
    assert _shipped(t) == [1, 2, 3]          # the history row 100 stays unsent


def test_a_rebuild_writes_the_record_for_a_pipeline_it_adds(station):
    station.switch("site", "upload")
    station.switch(CLIENT, "fill")
    station.mark()
    station.manifest.write_text(_MANIFEST.split("[[pipeline]]")[0])
    pdir = station.manifest.parent / "pipelines.d"
    pdir.mkdir()
    r, store = _rebuilder(station)
    r.start()
    up = Uploader([], inline_single=False)
    (pdir / "psk-recorder.toml").write_text(textwrap.dedent("""
        [[pipeline]]
        name = "psk"
        builder = "fake:fn"
        client = "psk-recorder"
        [pipeline.config]
        start_at = "now"
    """))
    _put(station.sink, 7)
    assert r.due()
    assert store.all_cursors() == []
    r.rebuild(up)
    assert [p.name for p in up.pipelines] == ["psk"]
    assert store.get_cursor(*_key(up.pipelines[0])) == b"1"


@pytest.fixture
def signals():
    saved = {s: signal.getsignal(s)
             for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    signal.signal(signal.SIGHUP, lambda *_a: None)
    yield
    for s, h in saved.items():
        signal.signal(s, h)


def test_marker_appearing_runs_the_pass_before_the_next_held_pump(
        station, monkeypatch, signals):
    _put(station.sink, 100)
    station.switch("site", "upload")
    station.switch(CLIENT, "fill")
    seen: list = []        # the stored record at the start of each pump
    script = [
        lambda: (station.mark(), _put(station.sink, 1), _put(station.sink, 2)),
        None,
    ]

    class Scripted(Uploader):
        def __init__(self, pipelines, **kw):
            gate = SinkGate(station.sinks, station.marker)
            super().__init__(pipelines, gate=gate.may_send, **kw)

        def pump(self):
            n = len(seen)
            p = self.pipelines[0]
            seen.append(_raw(station.wm, _key(p)))
            super().pump()
            if script[n] is not None:
                script[n]()
            if n + 1 >= len(script):
                os.kill(os.getpid(), signal.SIGTERM)
            return False

    monkeypatch.setattr(daemon, "Uploader", Scripted)
    assert daemon.run(station.manifest) == 0
    assert seen[0] is None                   # before the marker: no record
    assert seen[1] is not None               # it appeared: the pass ran first
    assert seen[1][0] == b"1"                # the first pump's anchor (row 100)
    assert TRANSPORTS["psk"].shipped == []   # held under fill throughout
