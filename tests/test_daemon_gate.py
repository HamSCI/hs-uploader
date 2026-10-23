"""The daemon passes the gate; each transition logs once (spec sink-2b-part2
§3.2, §9.2, test 7; plan ruling 6).

Every test that asserts an absence (no line over a hundred pumps) carries a
positive control in the same harness: the line that does appear when the
condition changes.
"""
from __future__ import annotations

import logging
import os
import signal
import textwrap
from datetime import datetime, timezone

import pytest

from hs_uploader import daemon, gate as gate_mod, sinkswitch
from hs_uploader.core import Pipeline, Record, Uploader
from hs_uploader.config import StationIdentity
from hs_uploader.gate import REMIND_S, SinkGate
from hs_uploader.pipelines_d import sinks_dir_for
from hs_uploader.sinkswitch import marker_path
from hs_uploader.watermark.sqlite import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport

TABLE = "test.spots"


def _rec(i):
    return (Record(table=TABLE,
                   time=datetime(2026, 5, 8, 12, 0, i, tzinfo=timezone.utc),
                   columns={"i": i}),
            str(i).encode())


@pytest.fixture(autouse=True)
def fresh_switch_state():
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()
    yield
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()


@pytest.fixture
def tree(tmp_path):
    sinks = tmp_path / "sinks"
    sinks.mkdir()
    marker = tmp_path / "state" / "carry-over-v1.done"
    marker.parent.mkdir()
    marker.write_text("carried over\n")
    return sinks, marker


def sw(sinks, name, state):
    sinkswitch.write_switch(sinks / f"{name}.toml", {"state": state})


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def mkpipe(name, client, wm, n=3):
    return Pipeline(name=name, source=MemorySource(records=[_rec(i) for i in range(1, n + 1)],
                                                   source_id=f"memory:{name}"),
                    transport=MemoryTransport(), watermark=wm,
                    identity=StationIdentity(call="AC0G", grid="EM38ww"),
                    client=client, origin=f"pipelines.d/{client or 'x'}.toml")


def hsu_lines(caplog, level=logging.INFO):
    """Every line any hs_uploader logger wrote at exactly `level`."""
    return [r.getMessage() for r in caplog.records
            if r.name.startswith("hs_uploader") and r.levelno == level]


# ---- one INFO line per pipeline per transition -------------------------------

def test_hundred_held_pumps_log_nothing_more_then_one_line_per_transition(
        tree, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "fill")
    wm = SqliteWatermarkStore(":memory:")
    p = mkpipe("p1", "c1", wm)
    up = Uploader([p], gate=SinkGate(sinks, marker).may_send)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        up.pump()
        assert hsu_lines(caplog) == ["pipeline p1 held: sink filling (client)"]
        caplog.clear()
        for _ in range(100):
            up.pump()
        assert hsu_lines(caplog) == []
        assert p.transport.shipped == []
        # positive control: the switch rises; exactly one line, and it ships
        sw(sinks, "c1", "upload")
        for _ in range(100):
            up.pump()
        assert hsu_lines(caplog) == ["pipeline p1 sending: sink uploading"]
        assert p.transport.shipped
        caplog.clear()
        sw(sinks, "site", "fill")   # the site lowers it: one held line
        for _ in range(100):
            up.pump()
    assert hsu_lines(caplog) == ["pipeline p1 held: sink filling (site)"]


def test_start_logs_one_line_per_held_pipeline(tree, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "fill")
    sw(sinks, "c2", "off")
    sw(sinks, "c3", "upload")
    wm = SqliteWatermarkStore(":memory:")
    pipes = [mkpipe("p1", "c1", wm), mkpipe("p2", "c2", wm),
             mkpipe("p3", "c3", wm)]
    up = Uploader(pipes, inline_single=False, gate=SinkGate(sinks, marker).may_send)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            up.pump()
    held = sorted(m for m in hsu_lines(caplog) if " held: " in m)
    assert held == ["pipeline p1 held: sink filling (client)",
                    "pipeline p2 held: sink off (client)"]
    assert not any(" sending: " in m for m in hsu_lines(caplog))
    assert pipes[2].transport.shipped        # the unheld one ships


def test_transition_lines_are_capped(tree, caplog, monkeypatch):
    sinks, marker = tree
    sw(sinks, "c1", "fill")
    monkeypatch.setattr(gate_mod, "SAID_CAP", 3)
    g = SinkGate(sinks, marker)
    wm = SqliteWatermarkStore(":memory:")
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for i in range(10):
            assert not g.may_send(mkpipe(f"p{i}", "c1", wm))
    assert len(g._last) == 3
    assert len([m for m in hsu_lines(caplog) if " held: " in m]) == 3


# ---- rule 4: once per pipeline for the daemon's life --------------------------


def _fake_builder(*, identity, watermark, config, name):
    return [Pipeline(name=name,
                     source=MemorySource(records=[_rec(1)], source_id=f"memory:{name}"),
                     transport=MemoryTransport(), watermark=watermark,
                     identity=identity)]


def _entry(name, client=None):
    line = f'client = "{client}"\n' if client else ""
    return textwrap.dedent(f"""
        [[pipeline]]
        name = "{name}"
        builder = "fake:fn"
        {line}
        [pipeline.config]
        v = 1
    """)


_HEAD = textwrap.dedent("""
    [identity]
    call = "AC0G/S"
    grid = "EM38ww"

    [daemon]
    pump_interval_sec = 0.01
    wake = false
""")


@pytest.fixture
def station(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: _fake_builder)
    etc = tmp_path / "etc"
    etc.mkdir()
    (etc / "sinks").mkdir()

    class S:
        manifest = etc / "pipelines.toml"
        pdir = etc / "pipelines.d"
        sinks = etc / "sinks"

        def legacy(self, *entries):
            self.manifest.write_text(_HEAD + "".join(entries))

        def file(self, name, text):
            self.pdir.mkdir(exist_ok=True)
            (self.pdir / name).write_text(text)

        def mark(self):
            marker_path().write_text("carried over\n")

    return S()


@pytest.fixture
def signals():
    saved = {s: signal.getsignal(s)
             for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    signal.signal(signal.SIGHUP, lambda *_a: None)
    yield
    for s, h in saved.items():
        signal.signal(s, h)


def _run_loop(monkeypatch, station, script):
    """Run the daemon's real loop; after pump n call script[n]; stop after
    the last.  Returns each Uploader the daemon built (one, it should be)."""
    made: list[Uploader] = []
    pumps = [0]

    class Scripted(Uploader):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            made.append(self)

        def pump(self):
            n = pumps[0]
            pumps[0] += 1
            out = super().pump()
            if n < len(script) and script[n] is not None:
                script[n]()
            if n + 1 >= len(script):
                os.kill(os.getpid(), signal.SIGTERM)
            return out

    monkeypatch.setattr(daemon, "Uploader", Scripted)
    assert daemon.run(station.manifest) == 0
    return made


def test_rule4_line_once_per_pipeline_across_pumps_and_rebuilds(
        station, monkeypatch, signals, caplog):
    station.legacy(_entry("old"), _entry("old2"))   # neither names a client
    station.mark()
    script = [None] * 30
    script[3] = lambda: station.file("c1.toml", _entry("a1"))
    script[9] = lambda: station.file("c2.toml", _entry("b1"))
    script[15] = lambda: station.file("c1.toml", _entry("a1") + _entry("a2"))
    with caplog.at_level(logging.INFO, logger="hs_uploader"):
        made = _run_loop(monkeypatch, station, script)
    assert len(made) == 1
    rebuilt = [m for m in hsu_lines(caplog) if "daemon: rebuilt" in m]
    assert len(rebuilt) == 3                 # the rebuilds really ran
    rule4 = [m for m in hsu_lines(caplog) if "names no client" in m]
    assert sorted(rule4) == [
        "pipeline old names no client; its manifest's policy governs it",
        "pipeline old2 names no client; its manifest's policy governs it"]
    # a1, a2 (c1) and b1 (c2) have no switch file: each held line once
    held = sorted(m for m in hsu_lines(caplog) if " held: " in m)
    assert held == ["pipeline a1 held: no switch file (client)",
                    "pipeline a2 held: no switch file (client)",
                    "pipeline b1 held: no switch file (client)"]


# ---- a fault: once, then a reminder every 6 h ----------------------------------

def test_broken_site_toml_errors_once_then_reminds_after_six_hours(tree, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "upload")
    (sinks / "site.toml").write_text("state = [broken\n")
    clock = Clock()
    wm = SqliteWatermarkStore(":memory:")
    p = mkpipe("p1", "c1", wm)
    up = Uploader([p], gate=SinkGate(sinks, marker, clock=clock).may_send)

    def errors():
        return hsu_lines(caplog, logging.ERROR)

    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        up.pump()
        assert len(errors()) == 1 and "site.toml" in errors()[0]
        for _ in range(100):
            clock.t += 60.0                  # 100 minutes pass, under 6 h
            up.pump()
        assert len(errors()) == 1
        clock.t = 1000.0 + REMIND_S - 1.0    # just short: still none
        up.pump()
        assert len(errors()) == 1
        clock.t = 1000.0 + REMIND_S + 1.0    # past 6 h: one reminder
        for _ in range(100):
            up.pump()
        assert len(errors()) == 2
        assert "reminder" in errors()[1] and "site.toml" in errors()[1]
        clock.t += REMIND_S + 1.0            # and again 6 h later
        for _ in range(100):
            up.pump()
        assert len(errors()) == 3
    assert p.transport.shipped == []         # held throughout
    held = [m for m in hsu_lines(caplog) if " held: " in m]
    assert held == ["pipeline p1 held: switch unreadable (site)"]


def test_cleared_fault_forgets_its_clock(tree, caplog):
    """A fault that clears and comes back logs once (its first sight, from
    sinkswitch), never a first sight plus a stale reminder."""
    sinks, marker = tree
    sw(sinks, "c1", "upload")
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    p = mkpipe("p1", "c1", wm)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        (sinks / "site.toml").write_text("state = [broken\n")
        assert not g.may_send(p)
        (sinks / "site.toml").unlink()
        assert g.may_send(p)
        clock.t += 2 * REMIND_S
        (sinks / "site.toml").write_text("state = [broken again\n")
        assert not g.may_send(p)
    assert len(hsu_lines(caplog, logging.ERROR)) == 2
    assert not any("reminder" in m for m in hsu_lines(caplog, logging.ERROR))


def test_missing_sinks_with_marker_is_a_fault(tmp_path, caplog):
    marker = tmp_path / "carry-over-v1.done"
    marker.write_text("x\n")
    sinks = tmp_path / "sinks"               # never made
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            assert not g.may_send(mkpipe("p1", "c1", wm))
        errs = hsu_lines(caplog, logging.ERROR)
        assert len(errs) == 1 and "missing" in errs[0]
        clock.t += REMIND_S + 1
        assert not g.may_send(mkpipe("p1", "c1", wm))
        assert len(hsu_lines(caplog, logging.ERROR)) == 2
        # control: the directory appears with an upload switch; it sends
        sinks.mkdir()
        sw(sinks, "c1", "upload")
        assert g.may_send(mkpipe("p1", "c1", wm))


def test_gate_exception_reminds_after_six_hours(tree, monkeypatch, caplog):
    sinks, marker = tree
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")

    def boom(*a, **k):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(gate_mod, "effective_state", boom)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            assert not g.may_send(mkpipe("p1", "c1", wm))
        assert len(hsu_lines(caplog, logging.ERROR)) == 1
        clock.t += REMIND_S + 1
        for _ in range(100):
            assert not g.may_send(mkpipe("p1", "c1", wm))
    errs = hsu_lines(caplog, logging.ERROR)
    assert len(errs) == 2 and "reminder" in errs[1]


# ---- the daemon builds ONE gate and every Uploader passes it ---------------------

def test_daemon_gates_with_the_marker_standing(station, monkeypatch):
    station.legacy(_entry("leg", client="c1"))
    station.mark()
    sw(station.sinks, "c1", "fill")
    made = []
    real = daemon.Uploader

    class Spy(real):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            made.append(self)

    monkeypatch.setattr(daemon, "Uploader", Spy)
    assert daemon.run(station.manifest, once=True) == 0
    up = made[0]
    g = up._gate.__self__                   # the daemon passes SinkGate.may_send
    assert isinstance(g, SinkGate) and up._gate == g.may_send
    # the same sinks/ and marker that `sink status` and the writer read
    assert g.sinks_dir == sinks_dir_for(station.manifest)
    assert g.marker == marker_path()
    assert up.pipelines[0].transport.shipped == []   # held
    # positive control: the same daemon with the switch raised ships
    sw(station.sinks, "c1", "upload")
    made.clear()
    assert daemon.run(station.manifest, once=True) == 0
    assert made[0].pipelines[0].transport.shipped


def test_daemon_keeps_one_gate_across_rebuilds(station, monkeypatch, signals):
    station.legacy(_entry("leg", client="c1"))
    station.mark()
    sw(station.sinks, "c1", "fill")
    gates = []
    script = [None] * 6
    script[1] = lambda: station.file("c1.toml", _entry("a1"))
    real_replace = Uploader.replace_pipelines

    def spy(self, new):
        gates.append(self._gate)
        return real_replace(self, new)

    monkeypatch.setattr(Uploader, "replace_pipelines", spy)
    made = _run_loop(monkeypatch, station, script)
    assert len(made) == 1 and len(gates) == 1
    assert isinstance(made[0]._gate.__self__, SinkGate)
    assert gates[0].__self__ is made[0]._gate.__self__
    assert all(p.transport.shipped == [] for p in made[0].pipelines)


# ---- fix round 1 ---------------------------------------------------------------

def _two(wm):
    # Bad_Name fails valid_client_name: effective_state returns before it
    # reads site.toml or looks for sinks/, so it must judge neither.
    return mkpipe("good", "good", wm), mkpipe("bad", "Bad_Name", wm)


def test_bad_client_name_does_not_clear_the_missing_sinks_fault(tmp_path, caplog):
    marker = tmp_path / "carry-over-v1.done"
    marker.write_text("x\n")
    clock = Clock()
    g = SinkGate(tmp_path / "sinks", marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    good, bad = _two(wm)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            clock.t += 1
            assert not g.may_send(good)
            assert not g.may_send(bad)
    missing = [m for m in hsu_lines(caplog, logging.ERROR) if "is missing" in m]
    assert len(missing) == 1


def test_bad_client_name_does_not_reset_the_site_reminder(tree, caplog):
    sinks, marker = tree
    sw(sinks, "good", "upload")
    (sinks / "site.toml").write_text("state = [broken\n")
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    good, bad = _two(wm)
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            clock.t += REMIND_S / 49          # a reminder at pumps 50 and 99
            assert not g.may_send(good)
            assert not g.may_send(bad)
    site = [m for m in hsu_lines(caplog, logging.ERROR) if "site.toml" in m]
    assert len(site) == 1 + 2                 # first sight, then each 6 h
    assert all("reminder" in m for m in site[1:])


def test_gate_exception_that_clears_logs_a_new_outbreak_as_first_sight(
        tree, monkeypatch, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "upload")
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    p = mkpipe("p1", "c1", wm)
    real = gate_mod.effective_state

    def boom(*a, **k):
        raise RuntimeError("disk on fire")

    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        monkeypatch.setattr(gate_mod, "effective_state", boom)
        for _ in range(10):
            assert not g.may_send(p)
        monkeypatch.setattr(gate_mod, "effective_state", real)
        assert g.may_send(p)                  # it clears
        clock.t += REMIND_S + 1               # the 6 h clock governs a recurrence
        monkeypatch.setattr(gate_mod, "effective_state", boom)
        for _ in range(10):
            assert not g.may_send(p)
    errs = [m for m in hsu_lines(caplog, logging.ERROR) if "gate failed" in m]
    assert len(errs) == 2
    assert not any("reminder" in m for m in errs)


def _flaky(real):
    n = iter(range(10**6))

    def flaky(*a, **k):
        if next(n) % 2:
            raise OSError("transient")
        return real(*a, **k)
    return flaky


def test_alternating_gate_fault_logs_one_error_and_no_transition_lines(
        tree, monkeypatch, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "upload")
    clock = Clock()
    g = SinkGate(sinks, marker, clock=clock)
    wm = SqliteWatermarkStore(":memory:")
    p = mkpipe("p1", "c1", wm)
    real = gate_mod.effective_state
    monkeypatch.setattr(gate_mod, "effective_state", _flaky(real))
    with caplog.at_level(logging.DEBUG, logger="hs_uploader"):
        for _ in range(100):
            clock.t += 30.0                   # 50 min in all, under 6 h
            g.may_send(p)
        errs = [m for m in hsu_lines(caplog, logging.ERROR)]
        assert len(errs) == 1
        infos = hsu_lines(caplog)
        assert len([m for m in infos if " held: " in m]) <= 1
        assert [m for m in infos if " sending: " in m] == []
        # positive control: a real switch change still logs, once each way
        caplog.clear()
        monkeypatch.setattr(gate_mod, "effective_state", real)
        sw(sinks, "c1", "fill")
        for _ in range(10):
            assert not g.may_send(p)
        sw(sinks, "c1", "upload")
        for _ in range(10):
            assert g.may_send(p)
    assert hsu_lines(caplog) == ["pipeline p1 held: sink filling (client)",
                                 "pipeline p1 sending: sink uploading"]


def test_transition_cap_warns_once(tree, caplog, monkeypatch):
    sinks, marker = tree
    sw(sinks, "c1", "fill")
    monkeypatch.setattr(gate_mod, "SAID_CAP", 3)
    g = SinkGate(sinks, marker)
    wm = SqliteWatermarkStore(":memory:")
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(5):
            for i in range(10):
                g.may_send(mkpipe(f"p{i}", "c1", wm))
    warns = [m for m in hsu_lines(caplog, logging.WARNING) if "transition" in m]
    assert len(warns) == 1


def test_forget_drops_removed_pipelines(tree, caplog):
    sinks, marker = tree
    sw(sinks, "c1", "fill")
    g = SinkGate(sinks, marker)
    wm = SqliteWatermarkStore(":memory:")
    a, b = mkpipe("a", "c1", wm), mkpipe("b", "c1", wm)
    g.may_send(a), g.may_send(b)
    g.forget([a])
    assert set(g._last) == {gate_mod._key(a)}


def test_daemon_rebuild_forgets_removed_pipelines(station, monkeypatch, signals):
    station.legacy(_entry("leg", client="c1"))
    station.mark()
    sw(station.sinks, "c1", "fill")
    script = [None] * 6
    script[0] = lambda: station.file("c2.toml", _entry("x1"))
    script[2] = lambda: (station.pdir / "c2.toml").unlink()
    made = _run_loop(monkeypatch, station, script)
    g = made[0]._gate.__self__
    assert [k[1] for k in g._last] == ["leg"]
