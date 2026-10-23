"""SinkGate: may_send rules 1-5 (spec sink-2b-part2 §3.1, tests 2 and 11).

Every hold case has a send control beside it and every send case a hold
control, so the gate can neither leak nor wrongly hold unnoticed.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import pytest

from hs_uploader import gate as gate_mod
from hs_uploader.core import Pipeline
from hs_uploader.gate import SINK_GATE_ORIGIN, Decision, SinkGate
from hs_uploader.transports.discard import DiscardTransport
from hs_uploader.transports.heartbeat_sftp import HeartbeatSftp

PSK_ORIGIN = "pipelines.d/psk-recorder.toml"


class StubTransport:
    name = "stub"
    ACCEPTS = {"t": [1]}


def pipe(name="p", *, client="psk-recorder", origin="legacy", governed=True,
         transport=None):
    return Pipeline(name=name, source=object(),
                    transport=transport if transport is not None else StubTransport(),
                    watermark=object(), identity=object(), client=client,
                    governed=governed, origin=origin)


def put(d, name, state):
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.toml").write_text(f'state = "{state}"\n')


@pytest.fixture
def tree(tmp_path):
    sinks = tmp_path / "sinks"
    sinks.mkdir()
    marker = tmp_path / "state" / "carry-over-v1.done"
    marker.parent.mkdir()
    marker.write_text("carried over\n")
    return sinks, marker


def gate_lines(caplog, level):
    return [r for r in caplog.records
            if r.name == gate_mod.log.name and r.levelno == level]


def test_origin_constant():
    assert SINK_GATE_ORIGIN == "pipelines.d/sigmond.toml"


def test_daemon_reexports_sinkgate():
    from hs_uploader import daemon
    assert daemon.SinkGate is SinkGate


# ---- rule 1 -----------------------------------------------------------------

def test_rule1_sigmond_file_ungoverned_sends(tree):
    sinks, marker = tree
    put(sinks, "site", "off")  # no sigmond.toml switch file
    g = SinkGate(sinks, marker)
    hb = pipe("heartbeat", client="sigmond", origin=SINK_GATE_ORIGIN,
              governed=False, transport=HeartbeatSftp(host="h"))
    d = g.decide(hb)
    assert d == Decision(True, 1, d.reason) and g.may_send(hb)
    # control: the same pipeline governed holds (no sigmond switch file)
    hb.governed = True
    assert not g.may_send(hb)


# ---- ruling 2: governed = false only from sigmond.toml ----------------------

@pytest.mark.parametrize("origin", [PSK_ORIGIN, "legacy"])
def test_ruling2_ungoverned_elsewhere_judged_by_switches(tree, caplog, origin):
    sinks, marker = tree
    put(sinks, "psk-recorder", "fill")
    g = SinkGate(sinks, marker)
    p = pipe("psk", origin=origin, governed=False)
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(100):
            assert not g.may_send(p)
    assert g.decide(p).rule == 5
    warns = gate_lines(caplog, logging.WARNING)
    assert len(warns) == 1, [r.getMessage() for r in warns]
    assert "psk" in warns[0].getMessage()
    # control: the switch raised to upload, the same pipeline sends
    put(sinks, "psk-recorder", "upload")
    assert g.may_send(p)


def test_ruling2_one_warning_per_pipeline(tree, caplog):
    sinks, marker = tree
    put(sinks, "psk-recorder", "fill")
    g = SinkGate(sinks, marker)
    a = pipe("a", origin=PSK_ORIGIN, governed=False)
    b = pipe("b", origin=PSK_ORIGIN, governed=False)
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(100):
            g.may_send(a)
            g.may_send(b)
    assert len(gate_lines(caplog, logging.WARNING)) == 2


# ---- rule 2: the legacy heartbeat -------------------------------------------

def test_rule2_legacy_heartbeat_by_client_sends(tree):
    sinks, marker = tree
    put(sinks, "site", "off")
    g = SinkGate(sinks, marker)
    p = pipe("hb", client="sigmond", origin="legacy")
    d = g.decide(p)
    assert d.send and d.rule == 2


@pytest.mark.parametrize("wrap", [False, True])
def test_rule2_legacy_heartbeat_by_transport_sends(tree, wrap):
    sinks, marker = tree
    put(sinks, "site", "off")
    put(sinks, "wspr-recorder", "off")
    g = SinkGate(sinks, marker)
    t = HeartbeatSftp(host="h")
    p = pipe("hb", client="wspr-recorder", origin="legacy",
             transport=DiscardTransport(t) if wrap else t)
    d = g.decide(p)
    assert d.send and d.rule == 2
    # control: the same entry with another transport holds
    assert not g.may_send(pipe("x", client="wspr-recorder", origin="legacy"))


def test_rule2_pipeline_file_cannot_claim_heartbeat(tree):
    sinks, marker = tree
    put(sinks, "site", "off")
    put(sinks, "psk-recorder", "upload")
    g = SinkGate(sinks, marker)
    by_client = pipe("a", client="sigmond", origin=PSK_ORIGIN)
    by_transport = pipe("b", client="psk-recorder", origin=PSK_ORIGIN,
                        transport=HeartbeatSftp(host="h"))
    for p in (by_client, by_transport):
        d = g.decide(p)
        assert not d.send and d.rule == 5


# ---- rule 3: the marker -----------------------------------------------------

def test_rule3_no_marker_sends_and_marker_counts_live(tree):
    sinks, marker = tree
    marker.unlink()
    put(sinks, "site", "off")
    put(sinks, "psk-recorder", "off")
    g = SinkGate(sinks, marker)
    p = pipe()
    d = g.decide(p)
    assert d.send and d.rule == 3
    # positive control: the marker appears; the same gate now holds
    marker.write_text("x\n")
    d = g.decide(p)
    assert not d.send and d.rule == 5
    # and vanishes again: sends, no restart
    marker.unlink()
    assert g.may_send(p)


def test_rule3_dangling_link_counts_as_marker(tree):
    sinks, marker = tree
    marker.unlink()
    marker.symlink_to(marker.parent / "nowhere")
    put(sinks, "psk-recorder", "fill")
    assert not SinkGate(sinks, marker).may_send(pipe())


# ---- rule 4: no client ------------------------------------------------------

def test_rule4_no_client_sends_one_info(tree, caplog):
    sinks, marker = tree
    put(sinks, "site", "off")
    g = SinkGate(sinks, marker)
    p = pipe("old", client="")
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(100):
            d = g.decide(p)
            assert d.send and d.rule == 4 and d.reason == "no client"
    infos = [r.getMessage() for r in gate_lines(caplog, logging.INFO)]
    assert infos == ["pipeline old names no client; its manifest's policy "
                     "governs it"]


# ---- rule 5: the switches ---------------------------------------------------

NINE = {  # (site, client) -> (effective, the switch that sets it); written out
    ("off", "off"): ("off", "site"), ("off", "fill"): ("off", "site"),
    ("off", "upload"): ("off", "site"),
    ("fill", "off"): ("off", "client"), ("fill", "fill"): ("fill", "site"),
    ("fill", "upload"): ("fill", "site"),
    ("upload", "off"): ("off", "client"), ("upload", "fill"): ("fill", "client"),
    ("upload", "upload"): ("upload", None),
}
VERB = {"off": "off", "fill": "filling"}


@pytest.mark.parametrize("site,client", sorted(NINE))
def test_rule5_nine_pairs(tree, site, client):
    sinks, marker = tree
    put(sinks, "site", site)
    put(sinks, "psk-recorder", client)
    d = SinkGate(sinks, marker).decide(pipe())
    state, who = NINE[(site, client)]
    assert d.rule == 5
    assert d.send == (state == "upload")
    # off and fill both hold, so the reason carries which one the gate read
    want = "sink uploading" if who is None else f"sink {VERB[state]} ({who})"
    assert d.reason == want


def test_rule5_reasons(tree):
    sinks, marker = tree
    g = SinkGate(sinks, marker)
    put(sinks, "site", "fill")
    put(sinks, "psk-recorder", "upload")
    assert g.decide(pipe()).reason == "sink filling (site)"
    put(sinks, "site", "upload")
    put(sinks, "psk-recorder", "off")
    assert g.decide(pipe()).reason == "sink off (client)"
    put(sinks, "psk-recorder", "upload")
    assert g.decide(pipe()).reason == "sink uploading"
    (sinks / "site.toml").write_text("state = [\n")
    assert g.decide(pipe()).reason == "switch unreadable (site)"


def test_rule5_missing_site_with_client_upload_sends(tree):
    sinks, marker = tree
    put(sinks, "psk-recorder", "upload")
    assert SinkGate(sinks, marker).may_send(pipe())


FAIL_REASON = {
    "missing": "no switch file (client)",
    "broken": "switch unreadable (client)",
    "unknown": "switch unreadable (client)",
    "no-sinks": "no switch file (client)",  # sinks/ missing: no client file
    "bad-name": "bad client name",
}


@pytest.mark.parametrize("case", sorted(FAIL_REASON))
def test_rule5_fails_closed(tree, case):
    sinks, marker = tree
    put(sinks, "site", "upload")
    put(sinks, "psk-recorder", "upload")
    client = "psk-recorder"
    g = SinkGate(sinks, marker)
    assert g.may_send(pipe(client=client))  # control: sends before the fault
    if case == "missing":
        (sinks / "psk-recorder.toml").unlink()
    elif case == "broken":
        (sinks / "psk-recorder.toml").write_text("state = [\n")
    elif case == "unknown":
        put(sinks, "psk-recorder", "burst")
    elif case == "no-sinks":
        for f in sinks.iterdir():
            f.unlink()
        sinks.rmdir()
    elif case == "bad-name":
        client = "Bad/Name"
    d = g.decide(pipe(client=client))
    assert not d.send and d.rule == 5
    assert d.reason == FAIL_REASON[case]


# ---- a fault inside the gate holds -----------------------------------------

def test_exception_holds_and_logs_once(tree, monkeypatch, caplog):
    sinks, marker = tree
    put(sinks, "psk-recorder", "upload")
    g = SinkGate(sinks, marker)
    assert g.may_send(pipe())  # control

    def boom(*a, **k):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(gate_mod, "effective_state", boom)
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(100):
            d = g.decide(pipe())
            assert not d.send and d.rule == 0
            assert not g.may_send(pipe())
    errs = gate_lines(caplog, logging.ERROR)
    assert len(errs) == 1 and "disk on fire" in errs[0].getMessage()


def test_fault_keyed_by_type_not_text(tree, monkeypatch, caplog):
    sinks, marker = tree
    g = SinkGate(sinks, marker)
    n = iter(range(10**6))

    def boom(*a, **k):
        raise RuntimeError(f"fault number {next(n)}")  # new text every call

    monkeypatch.setattr(gate_mod, "effective_state", boom)
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for _ in range(100):
            assert not g.may_send(pipe())
        assert len(gate_lines(caplog, logging.ERROR)) == 1
        # positive control: another exception TYPE is a new fault, logged once
        def boom2(*a, **k):
            raise KeyError("x")
        monkeypatch.setattr(gate_mod, "effective_state", boom2)
        for _ in range(100):
            assert not g.may_send(pipe())
    assert len(gate_lines(caplog, logging.ERROR)) == 2


def test_logged_keys_are_capped(tree, monkeypatch, caplog):
    sinks, marker = tree
    put(sinks, "psk-recorder", "fill")
    monkeypatch.setattr(gate_mod, "SAID_CAP", 5)
    g = SinkGate(sinks, marker)
    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        for i in range(50):
            for _ in range(3):
                assert not g.may_send(pipe(f"p{i}", origin=PSK_ORIGIN,
                                           governed=False))
    assert len(g._said) == 5
    # (the per-pipeline transition table has its own cap line; Task 6)
    warns = [r.getMessage() for r in gate_lines(caplog, logging.WARNING)
             if "transition" not in r.getMessage()]
    assert len(warns) == 6 and "will not be logged" in warns[-1]


def test_reason_words_the_deciding_read(tree, monkeypatch):
    """The reason comes from the Effective that decided, never a second read."""
    sinks, marker = tree
    put(sinks, "site", "upload")
    put(sinks, "psk-recorder", "fill")  # what a second read would see
    from hs_uploader.sinkswitch import Effective
    monkeypatch.setattr(gate_mod, "effective_state",
                        lambda *a, **k: Effective("off", "off", "upload", []))
    d = SinkGate(sinks, marker).decide(pipe())
    assert not d.send and d.reason == "sink off (client)"


# ---- rule 3 fails closed when the marker cannot be checked -----------------

@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores mode 000")
def test_unreadable_marker_dir_fails_closed(tree, caplog):
    sinks, marker = tree
    marker.unlink()
    put(sinks, "psk-recorder", "fill")
    g = SinkGate(sinks, marker)
    assert g.decide(pipe()).rule == 3  # control: no marker, readable dir: sends
    marker.parent.chmod(0o000)
    try:
        with caplog.at_level(logging.DEBUG):
            for _ in range(100):
                d = g.decide(pipe())
                assert not d.send and d.rule == 5
    finally:
        marker.parent.chmod(0o755)
    errs = [r for r in caplog.records if r.levelno == logging.ERROR
            and "marker" in r.getMessage()]
    assert len(errs) == 1
    assert g.decide(pipe()).rule == 3


# ---- X8: gate state under two threads --------------------------------------

class SlowSet(set):
    """Widens the gap between "seen?" and "add", where a race would land."""

    calls = 0

    def __contains__(self, key):
        SlowSet.calls += 1
        found = super().__contains__(key)
        time.sleep(0.002)
        return found


def test_once_state_is_thread_safe(tree, caplog):
    sinks, marker = tree
    put(sinks, "psk-recorder", "fill")
    g = SinkGate(sinks, marker)
    SlowSet.calls = 0
    g._said = SlowSet()
    pipes = [pipe(f"p{i}", origin=PSK_ORIGIN, governed=False)
             for i in range(10)]
    barrier = threading.Barrier(2)
    held = []

    def run():
        barrier.wait()
        held.extend(g.may_send(p) for p in pipes)

    with caplog.at_level(logging.DEBUG, logger=gate_mod.log.name):
        ts = [threading.Thread(target=run) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
    assert held == [False] * 20
    assert SlowSet.calls >= 20  # the gate really consulted the slow set
    # one WARNING per pipeline, never two, though both threads judged each
    assert len(gate_lines(caplog, logging.WARNING)) == len(pipes)
