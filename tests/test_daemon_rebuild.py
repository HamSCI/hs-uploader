"""Rebuild on change, and the store check at each rebuild (spec sink-2b §3.4, §3.5).

Tests 6, 7 and 8 (rebuild half).  The daemon stats ``pipelines.d/`` before
each pump, rebuilds between pumps when the signature moved or SIGHUP came,
and keeps the legacy manifest as it read it at start.  A rebuild keeps every
unchanged ``Pipeline`` object and its executor, closes a removed pipeline's
source on that pipeline's own thread, and runs the store check again.

The pipelines here come from a stand-in builder entry (``builder =
"fake:fn"``), so each one's source and transport record which thread touched
them.
"""

from __future__ import annotations

import logging
import os
import signal
import textwrap
import threading
import time
from datetime import datetime, timezone

import pytest

from hs_uploader import daemon
from hs_uploader.core import Outcome, Pipeline, Record, RecordBatch, Uploader
from hs_uploader.watermark.sqlite import SqliteWatermarkStore

from conftest import MemoryTransport


# ---- stand-in pipelines ---------------------------------------------------------


class TrackSource:
    """Yields one record once; records the threads that read and closed it."""

    def __init__(self, name, producer=None):
        self._id = f"track:{name}"
        self.producer = producer
        self.threads: list[str] = []
        self.closed_on: str | None = None

    def source_id(self):
        return self._id

    def health(self):
        return "ok"

    def iter_batches(self, cursor, limit):
        self.threads.append(threading.current_thread().name)
        if cursor == b"":
            yield RecordBatch(
                records=(Record(table="test.spots",
                                time=datetime(2026, 10, 21, tzinfo=timezone.utc),
                                columns={"x": 1}),),
                cursor_after=b"1")

    def commit(self, token):
        return None

    def close(self):
        self.closed_on = threading.current_thread().name


class TrackTransport(MemoryTransport):
    def __init__(self, name):
        super().__init__()
        self.name = f"mem:{name}"
        self.closed_on: str | None = None

    def close(self):
        self.closed_on = threading.current_thread().name


BUILT: list[str] = []
SOURCES: dict[str, list[TrackSource]] = {}


def _fake_builder(*, identity, watermark, config, name):
    BUILT.append(name)
    src = TrackSource(name, config.get("producer") or None)
    SOURCES.setdefault(name, []).append(src)
    return [Pipeline(name=name,
                     source=src,
                     transport=TrackTransport(name),
                     watermark=watermark, identity=identity)]


def _entry(name, *, v=1, client=None, producer=""):
    client_line = f'client = "{client}"\n' if client else ""
    return textwrap.dedent(f"""
        [[pipeline]]
        name = "{name}"
        builder = "fake:fn"
        {client_line}
        [pipeline.config]
        v = {v}
        producer = "{producer}"
    """)


_HEAD = textwrap.dedent("""
    [identity]
    call = "AC0G/S"
    grid = "EM38ww"
    ssh_key_file = "/etc/hs-uploader/keys/id_ed25519"

    [daemon]
    pump_interval_sec = 0.01
""")


@pytest.fixture
def station(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: _fake_builder)
    BUILT.clear()
    SOURCES.clear()
    etc = tmp_path / "etc"
    etc.mkdir()

    class S:
        manifest = etc / "pipelines.toml"
        pdir = etc / "pipelines.d"
        db = tmp_path / "watermarks.db"

        def legacy(self, *entries, wake="false"):
            head = _HEAD + (f"wake = {wake}\n" if wake is not None else "")
            self.manifest.write_text(head + "".join(entries))

        def file(self, name, text):
            self.pdir.mkdir(exist_ok=True)
            (self.pdir / name).write_text(text)

        def drop(self, name):
            (self.pdir / name).unlink()

        def runner(self):
            wm = SqliteWatermarkStore(self.db)
            r = daemon.Rebuilder(self.manifest,
                                 daemon.load_manifest(self.manifest), wm)
            running, _built, _refused = r.start()
            return r, Uploader(running, inline_single=False), wm

        def rows(self):
            return {row["name"]: row for row in
                    SqliteWatermarkStore(self.db).pipeline_states()}

    return S()


def _names(pipes):
    return [p.name for p in pipes]


def _by_name(up, name):
    return next(p for p in up.pipelines if p.name == name)


def _executor(up, pipe):
    return next(ex for p, ex in zip(up.pipelines, up._pump_executors)
                if p is pipe)


# ---- the run loop: stat before each pump, SIGHUP, legacy manifest -----------------


@pytest.fixture
def signals():
    """Restore the handlers run() installs; ignore SIGHUP meanwhile, so a
    daemon that forgot its handler fails the test instead of killing pytest."""
    saved = {s: signal.getsignal(s)
             for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    signal.signal(signal.SIGHUP, lambda *_a: None)
    yield
    for s, h in saved.items():
        signal.signal(s, h)


def _run_scripted(monkeypatch, station, script):
    """Run the daemon; after pump n, call script[n]; stop after the last.

    Returns the pipeline list each pump saw.
    """
    seen: list[list[Pipeline]] = []

    class Scripted(Uploader):
        def pump(self):
            n = len(seen)
            seen.append(list(self.pipelines))
            super().pump()
            if n < len(script) and script[n] is not None:
                script[n]()
            if n + 1 >= len(script):
                os.kill(os.getpid(), signal.SIGTERM)
            return False

    monkeypatch.setattr(daemon, "Uploader", Scripted)
    assert daemon.run(station.manifest) == 0
    return seen


def test_changed_file_rebuilds_within_one_pump(station, monkeypatch, signals):
    station.legacy(_entry("leg", client="legacy-x"))
    station.file("a.toml", _entry("a1"))
    seen = _run_scripted(monkeypatch, station, [
        lambda: station.file("a.toml", _entry("a1") + _entry("a2")),
        None,
    ])
    assert _names(seen[0]) == ["leg", "a1"]
    assert _names(seen[1]) == ["leg", "a1", "a2"]
    assert seen[1][1] is seen[0][1]                 # a1 unchanged, kept


def test_sighup_rebuilds_with_no_file_change(station, monkeypatch, signals):
    station.legacy(_entry("leg", client="legacy-x"))
    station.file("a.toml", _entry("a1"))
    calls = []
    real = daemon.Rebuilder.rebuild

    def counted(self, uploader):
        calls.append(len(seen_ref))
        return real(self, uploader)

    monkeypatch.setattr(daemon.Rebuilder, "rebuild", counted)
    seen_ref: list = []
    seen = _run_scripted(monkeypatch, station, [
        lambda: seen_ref.append(0),
        lambda: (seen_ref.append(1), os.kill(os.getpid(), signal.SIGHUP)),
        None,
    ])
    assert len(seen) == 3
    assert calls == [2]           # nothing before the SIGHUP, one after


def test_legacy_manifest_edit_changes_nothing_even_with_sighup(
        station, monkeypatch, signals):
    station.legacy(_entry("leg", client="legacy-x"))
    seen = _run_scripted(monkeypatch, station, [
        lambda: station.legacy(_entry("leg", client="legacy-x", v=2),
                               _entry("leg2", client="legacy-y")),
        lambda: os.kill(os.getpid(), signal.SIGHUP),
        None,
    ])
    assert [_names(s) for s in seen] == [["leg"], ["leg"], ["leg"]]
    assert seen[2][0] is seen[0][0]
    assert BUILT == ["leg"]


def test_idle_daemon_recovers_when_the_file_is_fixed(
        station, monkeypatch, signals):
    # C15/C17: the sole client's file broken at start leaves the daemon idle;
    # a corrected file must bring it back without a restart.
    station.legacy(_entry("leg", client="psk-recorder"))
    station.file("psk-recorder.toml", "[[pipeline]\nname = ")
    seen = _run_scripted(monkeypatch, station, [
        lambda: station.file("psk-recorder.toml", _entry("psk-a")),
        None,
    ])
    assert _names(seen[0]) == []
    assert _names(seen[1]) == ["psk-a"]
    assert set(station.rows()) == {"psk-a"}


class _FakeListener:
    started: list = []
    stopped: list = []

    def __init__(self, on_wake, path=None):
        self.path = path or "fake.sock"

    def start(self):
        _FakeListener.started.append(self)
        return True

    def stop(self, timeout=2.0):
        _FakeListener.stopped.append(self)


def test_wake_socket_bound_when_a_new_set_first_needs_it(
        station, monkeypatch, signals):
    _FakeListener.started, _FakeListener.stopped = [], []
    monkeypatch.setattr(daemon, "WakeListener", _FakeListener)
    # Only pipelines named "sink*" read a sink, for this test.
    monkeypatch.setattr(daemon, "_needs_wake",
                        lambda pipes: any(p.name.startswith("sink")
                                          for p in pipes))
    station.legacy(_entry("spool", client="grape"), wake=None)
    stopped_while_running = []
    seen = _run_scripted(monkeypatch, station, [
        lambda: station.file("psk.toml", _entry("sink-psk")),
        lambda: station.drop("psk.toml"),
        lambda: stopped_while_running.append(len(_FakeListener.stopped)),
        None,
    ])
    assert _names(seen[0]) == ["spool"]
    assert _names(seen[1]) == ["spool", "sink-psk"]
    assert _names(seen[2]) == ["spool"]
    assert len(_FakeListener.started) == 1          # bound once, at the rebuild
    assert stopped_while_running == [0]             # never unbound while running


def test_wake_socket_bound_by_a_rebuild_after_an_idle_start(
        station, monkeypatch, signals):
    _FakeListener.started, _FakeListener.stopped = [], []
    monkeypatch.setattr(daemon, "WakeListener", _FakeListener)
    monkeypatch.setattr(daemon, "_needs_wake",
                        lambda pipes: any(p.name.startswith("sink")
                                          for p in pipes))
    station.legacy(_entry("leg", client="psk"), wake=None)
    station.file("psk.toml", "[[pipeline]\nname = ")
    seen = _run_scripted(monkeypatch, station, [
        lambda: station.file("psk.toml", _entry("sink-psk")),
        None,
    ])
    assert _names(seen[1]) == ["sink-psk"]
    assert len(_FakeListener.started) == 1


# ---- the rebuild itself ---------------------------------------------------------------


def test_unchanged_pipeline_keeps_object_and_executor(station):
    station.legacy(_entry("leg", client="legacy-x"))
    station.file("a.toml", _entry("a1"))
    station.file("b.toml", _entry("b1"))
    r, up, _ = station.runner()
    up.pump()
    leg, a1, b1 = (_by_name(up, n) for n in ("leg", "a1", "b1"))
    ex_leg, ex_a1, ex_b1 = (_executor(up, p) for p in (leg, a1, b1))

    station.file("b.toml", _entry("b1", v=22))
    assert r.due()
    assert r.rebuild(up) is True
    assert _names(up.pipelines) == ["leg", "a1", "b1"]
    assert _by_name(up, "leg") is leg and _executor(up, leg) is ex_leg
    assert _by_name(up, "a1") is a1 and _executor(up, a1) is ex_a1
    new_b1 = _by_name(up, "b1")
    assert new_b1 is not b1 and _executor(up, new_b1) is not ex_b1
    # the replaced b1 closed on its own thread; its executor is shut down
    assert b1.source.closed_on == b1.source.threads[0]
    assert b1.source.closed_on.startswith("hs-uploader-b1")
    with pytest.raises(RuntimeError):
        ex_b1.submit(lambda: None)
    assert leg.source.closed_on is None and a1.source.closed_on is None
    assert not r.due()


def test_removed_pipeline_closes_on_its_own_thread(station):
    station.legacy()
    station.file("a.toml", _entry("a1"))
    station.file("b.toml", _entry("b1"))
    r, up, _ = station.runner()
    up.pump()
    b1 = _by_name(up, "b1")
    ex_b1 = _executor(up, b1)
    station.drop("b.toml")
    assert r.rebuild(up) is True
    assert _names(up.pipelines) == ["a1"]
    assert b1.source.closed_on == b1.source.threads[0]
    assert b1.transport.closed_on == b1.source.threads[0]
    with pytest.raises(RuntimeError):
        ex_b1.submit(lambda: None)
    assert set(station.rows()) == {"a1"}


def test_a_pipeline_pumps_on_one_thread_for_its_life(station):
    # The daemon's Uploader never pumps a lone pipeline inline: when a second
    # arrives, the first must not move to a new thread (its sqlite connection
    # is pinned to the thread that opened it).
    station.legacy()
    station.file("a.toml", _entry("a1"))
    wm = SqliteWatermarkStore(station.db)
    r = daemon.Rebuilder(station.manifest,
                         daemon.load_manifest(station.manifest), wm)
    running, _, _ = r.start()
    up = daemon._new_uploader(running)
    up.pump()
    a1 = _by_name(up, "a1")
    station.file("b.toml", _entry("b1"))
    r.rebuild(up)
    up.pump()
    station.drop("a.toml")
    r.rebuild(up)
    threads = set(a1.source.threads) | {a1.source.closed_on}
    assert len(a1.source.threads) == 2 and len(threads) == 1
    assert threads.pop().startswith("hs-uploader-a1")


def test_queued_retry_survives_a_rebuild(station):
    station.legacy()
    station.file("a.toml", _entry("a1"))
    clock = [time.time()]
    wm = SqliteWatermarkStore(station.db)
    r = daemon.Rebuilder(station.manifest,
                         daemon.load_manifest(station.manifest), wm)
    running, _, _ = r.start()
    up = Uploader(running, inline_single=False, now_fn=lambda: clock[0])
    a1 = _by_name(up, "a1")
    a1.transport.next_outcomes = [Outcome.retry_later("link down")]
    up.pump()
    assert wm.deliverable_count("a1") == 1

    station.file("c.toml", _entry("c1"))
    assert r.rebuild(up) is True
    assert _by_name(up, "a1") is a1
    clock[0] += 86_400
    up.pump()
    assert a1.transport.replayed and wm.deliverable_count("a1") == 0


def test_retry_under_a_removed_pipeline_waits_and_replays_on_return(station):
    station.legacy()
    station.file("a.toml", _entry("a1"))
    station.file("b.toml", _entry("b1"))
    clock = [time.time()]
    wm = SqliteWatermarkStore(station.db)
    r = daemon.Rebuilder(station.manifest,
                         daemon.load_manifest(station.manifest), wm)
    running, _, _ = r.start()
    up = Uploader(running, inline_single=False, now_fn=lambda: clock[0])
    _by_name(up, "b1").transport.next_outcomes = [Outcome.retry_later("down")]
    up.pump()
    assert wm.deliverable_count("b1") == 1

    station.drop("b.toml")
    r.rebuild(up)
    clock[0] += 86_400
    up.pump()
    assert wm.deliverable_count("b1") == 1          # waits in the store

    station.file("b.toml", _entry("b1"))
    r.rebuild(up)
    up.pump()
    assert _by_name(up, "b1").transport.replayed
    assert wm.deliverable_count("b1") == 0


def test_identical_rebuild_changes_nothing_and_logs_one_debug(station, caplog):
    station.legacy(_entry("leg", client="legacy-x"))
    station.file("a.toml", _entry("a1"))
    station.file("bad.toml", "[[pipeline]\nname = ")
    r, up, _ = station.runner()
    before = list(up.pipelines)
    executors = list(up._pump_executors)
    stamps = {n: row["updated_at"] for n, row in station.rows().items()}
    station.file("a.toml.new", "side file")            # moves the signature
    assert r.due()
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        assert r.rebuild(up) is False
    assert all(p is q for p, q in zip(up.pipelines, before))
    assert len(up.pipelines) == len(before)
    assert up._pump_executors == executors
    msgs = [(rec.levelno, rec.getMessage()) for rec in caplog.records]
    assert [m for lvl, m in msgs if lvl >= logging.INFO] == []
    assert len([m for lvl, m in msgs if lvl == logging.DEBUG]) == 1
    assert {n: row["updated_at"] for n, row in station.rows().items()} == stamps
    assert not r.due()


# ---- test 7: the store check at rebuild ---------------------------------------------


def test_producer_file_on_a_version_1_store_is_refused_at_rebuild(station):
    station.legacy(_entry("leg", client="legacy-x"))
    wm = SqliteWatermarkStore(station.db)
    wm._conn.execute("PRAGMA user_version = 1")
    wm._conn.commit()
    r, up, _ = station.runner()
    station.file("psk-recorder.toml",
                 _entry("psk-recorder-pskreporter", producer="psk-recorder"))
    assert r.rebuild(up) is True
    assert _names(up.pipelines) == ["leg"]
    rows = station.rows()
    reason = rows["psk-recorder-pskreporter"]["refused_reason"]
    assert "version 1" in reason
    assert rows["psk-recorder-pskreporter"]["client"] == "psk-recorder"
    assert rows["leg"]["refused_reason"] == ""


def test_send_log_follows_the_store_version_read_at_rebuild(station):
    station.legacy(_entry("leg", client="legacy-x"))
    wm0 = SqliteWatermarkStore(station.db)            # born at version 3
    wm0._conn.execute("PRAGMA user_version = 2")
    wm0._conn.commit()
    r, up, wm = station.runner()
    up.pump()                                         # ships at version 2
    last = lambda: wm._conn.execute(  # noqa: E731
        "SELECT count(*) FROM last_send").fetchone()[0]
    assert last() == 0

    wm0._conn.execute("PRAGMA user_version = 3")      # `migrate` ran
    wm0._conn.commit()
    station.file("n.toml", _entry("n1"))
    r.rebuild(up)
    up.pump()
    assert [tuple(row) for row in wm._conn.execute(
        "SELECT pipeline FROM last_send")] == [("n1",)]


# ---- test 8: a file broken on rebuild ---------------------------------------------------


def test_file_broken_on_rebuild_keeps_last_good_build(station, caplog):
    station.legacy(_entry("leg", client="legacy-x"))
    station.file("a.toml", _entry("a1"))
    r, up, _ = station.runner()
    a1 = _by_name(up, "a1")

    def errors():
        return [rec for rec in caplog.records if rec.levelno >= logging.ERROR]

    with caplog.at_level(logging.INFO):
        station.file("a.toml", "[[pipeline]\nname = ")
        r.rebuild(up)
        assert _names(up.pipelines) == ["leg", "a1"]
        assert _by_name(up, "a1") is a1
        rows = station.rows()
        assert rows["pipelines.d/a.toml"]["refused_reason"]
        assert rows["pipelines.d/a.toml"]["client"] == "a"
        assert rows["a1"]["refused_reason"] == ""
        assert len(errors()) == 1

        station.file("z.toml.new", "side")             # side file: no repeat
        r.rebuild(up)
        assert len(errors()) == 1

        station.file("a.toml", "[[pipeline]\nname = 1\n[")   # new signature
        r.rebuild(up)
        assert len(errors()) == 2
        assert _by_name(up, "a1") is a1

    up.pump()                                         # still pumping
    station.file("a.toml", _entry("a1", v=5))
    r.rebuild(up)
    assert "pipelines.d/a.toml" not in station.rows()
    assert _by_name(up, "a1") is not a1


def test_file_failing_to_build_on_rebuild_keeps_last_good_build(station):
    station.legacy()
    station.file("a.toml", _entry("a1"))
    r, up, _ = station.runner()
    a1 = _by_name(up, "a1")
    # a1 now collides with a2 inside the same file: refused whole.
    station.file("a.toml", _entry("a1", v=2) + _entry("a1", v=3))
    r.rebuild(up)
    assert _names(up.pipelines) == ["a1"] and _by_name(up, "a1") is a1
    assert "collides" in station.rows()["pipelines.d/a.toml"]["refused_reason"]


def test_a_new_file_refused_at_rebuild_closes_what_it_built(station):
    station.legacy(_entry("leg", client="legacy-x"))
    r, up, _ = station.runner()
    station.file("dup.toml", _entry("ok-1") + _entry("leg"))
    BUILT.clear()
    r.rebuild(up)
    assert _names(up.pipelines) == ["leg"]
    assert "ok-1" in BUILT
    # built, refused, never pumped: closed at once, on the main thread
    assert SOURCES["ok-1"][-1].closed_on == threading.main_thread().name
    assert "pipelines.d/dup.toml" in station.rows()


def test_a_file_silences_and_revives_legacy_entries_at_rebuild(station):
    station.legacy(_entry("leg-a", client="a"), _entry("leg-b", client="b"))
    r, up, _ = station.runner()
    leg_b = _by_name(up, "leg-b")
    station.file("a.toml", _entry("a1"))
    r.rebuild(up)
    assert _names(up.pipelines) == ["leg-b", "a1"]
    assert _by_name(up, "leg-b") is leg_b
    station.drop("a.toml")
    r.rebuild(up)
    assert _names(up.pipelines) == ["leg-a", "leg-b"]
    assert _by_name(up, "leg-b") is leg_b


# ---- Uploader.replace_pipelines -------------------------------------------------------


def test_replace_pipelines_closes_shared_objects_only_when_unused(station):
    station.legacy()
    station.file("a.toml", _entry("a1") + _entry("a2"))
    r, up, _ = station.runner()
    a1, a2 = _by_name(up, "a1"), _by_name(up, "a2")
    a2.transport = a1.transport                       # shared, as a builder may
    up.pump()
    up.replace_pipelines([a1])
    assert a2.source.closed_on is not None
    assert a1.transport.closed_on is None             # a1 still uses it
    assert up.pipelines == [a1]


def _ft(name, root, *, client):
    """A generic (filetree -> psws_dataset) legacy entry."""
    return textwrap.dedent(f"""
        [[pipeline]]
        name = "{name}"
        client = "{client}"
        [pipeline.source]
        type = "filetree"
        root = "{root}"
        patterns = ["OBS*"]
        retention = "keep"
        match_dirs = true
        table = "grape.dataset"
        [pipeline.transport]
        type = "psws_dataset"
        instrument_id = "{name}"
        table = "grape.dataset"
    """)


def test_generic_legacy_entries_silenced_and_revived_at_rebuild(
        station, tmp_path):
    for d in ("g1", "g2"):
        (tmp_path / d).mkdir()
    station.legacy(_ft("g1", tmp_path / "g1", client="a"),
                   _ft("g2", tmp_path / "g2", client="b"))
    r, up, _ = station.runner()
    g2 = _by_name(up, "g2")
    station.file("a.toml", _entry("a1"))
    r.rebuild(up)
    assert _names(up.pipelines) == ["g2", "a1"] and _by_name(up, "g2") is g2
    station.drop("a.toml")
    r.rebuild(up)
    assert _names(up.pipelines) == ["g1", "g2"] and _by_name(up, "g2") is g2
    assert station.rows()["g1"]["client"] == "a"


# ---- fix round 1 ---------------------------------------------------------------------


class _Flaky(Exception):
    pass


def _fail_refresh(monkeypatch, times):
    """Make the store's version read fail ``times`` times, then work."""
    real = SqliteWatermarkStore.refresh_schema_version
    left = [times]

    def flaky(self):
        if left[0] > 0:
            left[0] -= 1
            raise _Flaky("database is locked")
        return real(self)

    monkeypatch.setattr(SqliteWatermarkStore, "refresh_schema_version", flaky)


def test_failed_rebuild_keeps_the_signature_and_closes_what_it_built(
        station, monkeypatch):
    # I1: a rebuild that raises must leave due() true, the running set and
    # the build untouched, and close the pipelines it made new.
    station.legacy(_entry("leg", client="legacy-x"))
    r, up, _ = station.runner()
    before = list(up.pipelines)
    _fail_refresh(monkeypatch, 1)
    station.file("n.toml", _entry("n1"))
    with pytest.raises(_Flaky):
        r.rebuild(up)
    assert r.due()
    assert up.pipelines == before and "n1" not in station.rows()
    assert SOURCES["n1"][0].closed_on == threading.main_thread().name
    assert r.rebuild(up) is True
    assert _names(up.pipelines) == ["leg", "n1"]
    assert not r.due()


def test_failed_rebuild_retries_every_pump_and_logs_once(
        station, monkeypatch, signals, caplog):
    station.legacy(_entry("leg", client="legacy-x"))

    def break_then_add():
        _fail_refresh(monkeypatch, 2)
        station.file("n.toml", _entry("n1"))

    with caplog.at_level(logging.ERROR):
        seen = _run_scripted(monkeypatch, station,
                             [break_then_add, None, None, None])
    assert [_names(s) for s in seen] == [
        ["leg"], ["leg"], ["leg"], ["leg", "n1"]]
    failed = [rec for rec in caplog.records
              if "rebuild failed" in rec.getMessage()]
    assert len(failed) == 1


def test_failed_sighup_rebuild_retries_without_a_file_change(
        station, monkeypatch, signals):
    # A SIGHUP whose rebuild fails is not lost: the next pump tries again.
    station.legacy(_entry("leg", client="legacy-x"))
    calls = []
    real = daemon.Rebuilder.rebuild

    def counted(self, uploader):
        calls.append(1)
        return real(self, uploader)

    monkeypatch.setattr(daemon.Rebuilder, "rebuild", counted)

    def hup_while_broken():
        _fail_refresh(monkeypatch, 1)
        os.kill(os.getpid(), signal.SIGHUP)

    _run_scripted(monkeypatch, station, [hup_while_broken, None, None])
    assert len(calls) == 2            # failed once, retried once, then quiet


_HUP_DURING_START = textwrap.dedent('''
    import os, signal, sys
    sys.path.insert(0, sys.argv[1])
    from hs_uploader import daemon
    from hs_uploader.core import Uploader

    real_start = daemon.Rebuilder.start

    def start(self):
        os.kill(os.getpid(), signal.SIGHUP)     # arrives during the build
        return real_start(self)

    daemon.Rebuilder.start = start
    rebuilds = []
    real_rebuild = daemon.Rebuilder.rebuild

    def rebuild(self, up):
        rebuilds.append(1)
        return real_rebuild(self, up)

    daemon.Rebuilder.rebuild = rebuild

    class Once(Uploader):
        def pump(self):
            os.kill(os.getpid(), signal.SIGTERM)
            return False

    daemon.Uploader = Once
    rc = daemon.run(sys.argv[2])
    print("rc", rc, "rebuilds", len(rebuilds))
''')


def test_sighup_during_start_does_not_kill_the_daemon(station, tmp_path):
    import subprocess
    import sys
    from pathlib import Path
    (tmp_path / "g1").mkdir()
    station.legacy(_ft("g1", tmp_path / "g1", client="a"))
    script = tmp_path / "hup.py"
    script.write_text(_HUP_DURING_START)
    src = Path(daemon.__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-I", str(script), str(src), str(station.manifest)],
        capture_output=True, text=True, timeout=60,
        env={**os.environ, "HS_UPLOADER_STATE_DIR": str(tmp_path)})
    assert proc.returncode == 0, (proc.returncode, proc.stderr[-2000:])
    assert "rc 0 rebuilds 1" in proc.stdout


def _fd_count():
    return len(os.listdir("/proc/self/fd"))


def test_sqlite_source_closes_on_its_worker_thread_and_leaks_no_fd(
        station, tmp_path, monkeypatch):
    # M2: a real SqliteSource on a temporary sink, removed by a rebuild.
    import sqlite3
    from hs_uploader.sink.writer import PENDING_UPLOADS_DDL
    from hs_uploader.sources.sqlite import SqliteSource

    sink = tmp_path / "sink.db"
    with sqlite3.connect(sink) as c:
        c.executescript(PENDING_UPLOADS_DDL)

    opened_on: dict[str, list] = {}

    def sqlite_builder(*, identity, watermark, config, name):
        src = SqliteSource.from_env(
            database="sigmond", table="psk.spots",
            accepted_schema_versions=[1],
            env={"SIGMOND_SQLITE_PATH": str(sink)})
        real_connect = src._connect

        def connect():
            if src._conn is None:
                opened_on.setdefault(name, []).append(
                    threading.current_thread().name)
            return real_connect()

        src._connect = connect
        real_close = src.close

        def close():
            src.closed_on = threading.current_thread().name
            src.closed_conn = src._conn
            real_close()

        src.close = close
        SOURCES.setdefault(name, []).append(src)
        return [Pipeline(name=name, source=src,
                         transport=TrackTransport(name),
                         watermark=watermark, identity=identity)]

    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: sqlite_builder)
    station.legacy()
    station.file("keep.toml", _entry("keep"))
    r, up, _ = station.runner()

    counts = []
    for i in range(20):
        station.file("s.toml", _entry("sq", v=i))
        r.rebuild(up)
        up.pump()
        src = _by_name(up, "sq").source
        assert src._conn is not None                    # it really connected
        station.drop("s.toml")
        r.rebuild(up)
        assert src.closed_on == opened_on["sq"][-1]
        assert src.closed_on.startswith("hs-uploader-sq")
        assert src._conn is None
        with pytest.raises(sqlite3.ProgrammingError):   # closed, not leaked
            src.closed_conn.execute("SELECT 1")
        counts.append(_fd_count())
    assert counts[-1] <= counts[2], counts


# ---- test 19 and the switch report (spec 6.2, 9.2) ---------------------------------
# Without the carry-over marker the switches stay unenforced (gate rule 3), and
# the report says so; with it standing the report says "enforced".


def _sw(station, client, state):
    from hs_uploader import sinkswitch
    sinkswitch.write_switch(station.manifest.parent / "sinks" / f"{client}.toml",
                            {"state": state})


@pytest.fixture(autouse=False)
def fresh_switch_state():
    from hs_uploader import sinkswitch
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()
    yield
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()


def _would(caplog):
    return [r.getMessage() for r in caplog.records
            if r.getMessage().startswith("sink switch for ")]


NOT_ENFORCED = "(not enforced: no carry-over marker)"


def test_switch_reading_off_does_not_stop_the_pipeline_shipping(
        station, caplog, fresh_switch_state):
    """Test 19: without the marker the daemon reports the switch and
    enforces nothing."""
    station.legacy(_entry("leg", client="c1"))
    _sw(station, "c1", "off")
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        r, up, wm = station.runner()
        up.pump_until_idle()
    p = _by_name(up, "leg")
    assert p.transport.shipped, "an off switch must not stop the send"
    assert _would(caplog) == [f"sink switch for c1 reads off {NOT_ENFORCED}"]


def test_switch_report_logs_changes_only_and_errors_once(
        station, caplog, fresh_switch_state):
    station.legacy(_entry("leg", client="c1"))
    _sw(station, "c1", "upload")
    sinks = station.manifest.parent / "sinks"
    (sinks / "site.toml").write_text("state = [broken\n")
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        r, up, _ = station.runner()
        for _i in range(3):                      # three rebuilds, same answer
            station.file("x.toml.new", str(_i))
            r.rebuild(up)
    assert _would(caplog) == [f"sink switch for c1 reads off {NOT_ENFORCED}"]
    errors = [x for x in caplog.records if x.levelno == logging.ERROR]
    assert len(errors) == 1
    # the answer changes: one more line
    (sinks / "site.toml").unlink()
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        r.rebuild(up)
    assert _would(caplog) == [f"sink switch for c1 reads upload {NOT_ENFORCED}"]


def test_switch_report_says_enforced_when_the_marker_stands(
        station, caplog, fresh_switch_state):
    from hs_uploader.sinkswitch import marker_path
    station.legacy(_entry("leg", client="c1"))
    _sw(station, "c1", "fill")
    marker = marker_path()
    wm = SqliteWatermarkStore(station.db)
    r = daemon.Rebuilder(station.manifest,
                         daemon.load_manifest(station.manifest), wm,
                         marker=marker)
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        r.start()
        assert _would(caplog) == [
            f"sink switch for c1 reads fill {NOT_ENFORCED}"]
        caplog.clear()
        marker.write_text("carried over\n")    # the marker appears
        for _ in range(100):                    # a hundred pumps: one line
            r.marker_check()
        assert _would(caplog) == ["sink switch for c1 reads fill (enforced)"]
        for i in range(3):                      # three rebuilds: none more
            station.file("x.toml.new", str(i))
            r.report_switches()
        assert _would(caplog) == ["sink switch for c1 reads fill (enforced)"]
        caplog.clear()
        marker.unlink()                         # positive control: it goes
        for _ in range(100):
            r.marker_check()
    assert _would(caplog) == [f"sink switch for c1 reads fill {NOT_ENFORCED}"]


def test_no_sinks_directory_with_marker_says_no_policy_line(
        station, caplog, fresh_switch_state):
    """With the marker standing a missing sinks/ is a fault the gate reports;
    "the manifest's policy governs" would be false."""
    from hs_uploader.sinkswitch import marker_path
    station.legacy(_entry("leg", client="c1"))
    marker = marker_path()
    wm = SqliteWatermarkStore(station.db)
    r = daemon.Rebuilder(station.manifest,
                         daemon.load_manifest(station.manifest), wm,
                         marker=marker)
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        marker.write_text("carried over\n")
        r.start()
        assert not any("policy governs" in x.getMessage()
                       for x in caplog.records)
        marker.unlink()                         # control: without it, it says so
        r.report_switches()
    assert [x.getMessage() for x in caplog.records].count(
        "no switch files; the manifest's policy governs") == 1


def test_no_sinks_directory_logs_one_info_line(
        station, caplog, fresh_switch_state):
    station.legacy(_entry("leg", client="c1"), _entry("old"))
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        r, up, _ = station.runner()
        station.file("x.toml.new", "1")
        r.rebuild(up)
    msgs = [x.getMessage() for x in caplog.records]
    assert msgs.count(
        "no switch files; the manifest's policy governs") == 1
    assert _would(caplog) == []


def test_ungoverned_only_client_gets_no_would_read_line(
        station, caplog, fresh_switch_state):
    station.legacy(_entry("leg", client="c1").replace(
        'builder = "fake:fn"', 'builder = "fake:fn"\ngoverned = false'))
    _sw(station, "c1", "off")
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        station.runner()
    assert _would(caplog) == []
