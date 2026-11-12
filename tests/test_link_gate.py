"""LinkGate: hold network pipelines while a scheduled link is down.

The South Pole talks only during satellite passes.  Outside a window the
pipeline must not attempt at all; inside one it sends and the transport
judges the link; and no broken, stale or missing schedule may ever hold.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import time

import pytest

from hs_uploader import Pipeline, Record, StationIdentity, Uploader
from hs_uploader.link_gate import LinkGate, evaluate
from hs_uploader.transports.discard import DiscardTransport
from hs_uploader.watermark import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 11, 12, 0, 0, tzinfo=UTC)


def at(h, m=0):
    return T0 + dt.timedelta(hours=h, minutes=m)


def doc(**kw):
    """Two windows: 01:00-03:00 and 05:00-06:00, horizon 00:00-24:00."""
    d = {"version": 1, "fetched_at": T0.isoformat(),
         "horizon": [T0.isoformat(), at(24).isoformat()],
         "windows": [{"start": at(1).isoformat(), "end": at(3).isoformat(), "sats": ["TDRS-6"]},
                     {"start": at(5).isoformat(), "end": at(6).isoformat(), "sats": ["DSCS-B11"]}]}
    d.update(kw)
    return d


# -- evaluate -----------------------------------------------------------------

@pytest.mark.parametrize("now,up,scheduled", [
    (at(0, 30), False, True),    # before the first window
    (at(1), True, True),         # a window's start is inside it
    (at(2, 59), True, True),
    (at(3), False, True),        # a window's end is outside it
    (at(5, 30), True, True),
    (at(7), False, True),        # after the last window, inside the horizon
    (at(24), True, False),       # past the horizon: fail open
    (T0 - dt.timedelta(minutes=1), True, False),
])
def test_evaluate(now, up, scheduled):
    st = evaluate(doc(), now)
    assert (st.up, st.scheduled) == (up, scheduled)


def test_evaluate_reports_when_it_changes():
    assert evaluate(doc(), at(0, 30)).until == at(1)
    assert evaluate(doc(), at(2)).until == at(3)
    assert evaluate(doc(), at(4)).until == at(5)


def test_stale_schedule_fails_open():
    old = doc(fetched_at=(at(0) - dt.timedelta(days=9)).isoformat())
    st = evaluate(old, at(0, 30))
    assert st.up and not st.scheduled and "stale" in st.reason


def test_naive_timestamp_is_rejected():
    """No timezone could mean NZ local time, which would invert the gaps."""
    with pytest.raises(ValueError):
        evaluate(doc(fetched_at="2026-11-12T00:00:00"), at(0, 30))


# -- the gate and its file ----------------------------------------------------

class P:
    def __init__(self, transport=None):
        self.transport = transport or MemoryTransport()


def gate_for(tmp_path, content, now):
    path = tmp_path / "link-schedule.json"
    if content is not None:
        path.write_text(content if isinstance(content, str) else json.dumps(content))
    return LinkGate(path, now_fn=lambda: now[0], recheck_s=0)


def test_no_file_means_feature_off(tmp_path):
    g = gate_for(tmp_path, None, [at(0, 30)])
    assert g.state() is None
    assert g.may_send(P())


@pytest.mark.parametrize("content", ["{not json", json.dumps({"windows": []}), ""])
def test_unreadable_file_sends_and_warns_once(tmp_path, caplog, content):
    g = gate_for(tmp_path, content, [at(0, 30)])
    with caplog.at_level(logging.WARNING):
        assert all(g.may_send(P()) for _ in range(5))
    assert len([r for r in caplog.records if "unreadable" in r.message]) == 1


def test_holds_between_windows_and_sends_inside(tmp_path):
    now = [at(0, 30)]
    g = gate_for(tmp_path, doc(), now)
    assert not g.may_send(P())
    now[0] = at(1, 1)
    assert g.may_send(P())
    now[0] = at(4)
    assert not g.may_send(P())


def test_discard_transport_needs_no_link(tmp_path):
    g = gate_for(tmp_path, doc(), [at(0, 30)])
    assert g.may_send(P(DiscardTransport(MemoryTransport())))


def test_logs_each_transition_once(tmp_path, caplog):
    now = [at(0, 30)]
    g = gate_for(tmp_path, doc(), now)
    with caplog.at_level(logging.INFO):
        for t in (at(0, 30), at(0, 40), at(1, 30), at(2), at(3, 30), at(4)):
            now[0] = t
            for _ in range(3):          # three pipelines per pump
                g.may_send(P())
    lines = [r.message for r in caplog.records if r.message.startswith("link ")]
    assert [l.split()[1] for l in lines] == ["DOWN", "UP", "DOWN"]


def test_rewritten_file_is_picked_up(tmp_path):
    now = [at(0, 30)]
    g = gate_for(tmp_path, doc(), now)
    assert not g.may_send(P())
    p = tmp_path / "link-schedule.json"
    d = doc()
    d["windows"].insert(0, {"start": at(0).isoformat(), "end": at(1).isoformat(), "sats": []})
    p.write_text(json.dumps(d))
    os.utime(p, ns=(time.time_ns(), time.time_ns() + 10**9))
    assert g.may_send(P())


def test_sink_gate_asks_the_link_only_when_it_would_send(tmp_path):
    from hs_uploader.gate import SinkGate
    asked = []
    sg = SinkGate(tmp_path / "sinks", tmp_path / "marker")
    sg.link = lambda p: asked.append(p) or False
    sg.decide = lambda p: type("D", (), {"send": False})()
    assert sg.may_send("held") is False and asked == []
    sg.decide = lambda p: type("D", (), {"send": True})()
    assert sg.may_send("open") is False and asked == ["open"]


# -- in the pump --------------------------------------------------------------

def _rec(i):
    return (Record(table="test.spots",
                   time=dt.datetime(2026, 11, 12, 0, 0, i, tzinfo=UTC),
                   columns={"i": i}), str(i).encode())


def test_gap_holds_without_attempts_then_ships_the_backlog(tmp_path):
    now = [at(0, 30)]
    g = gate_for(tmp_path, doc(), now)
    tr = MemoryTransport()
    src = MemorySource(records=[_rec(1), _rec(2)])
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(name="spots", source=src, transport=tr, watermark=wm,
                    identity=StationIdentity(call="AI6VN", grid="CM87"))
    up = Uploader([pipe], gate=g.may_send)

    for _ in range(10):
        up.pump()
    assert tr.shipped == []                         # not one attempt in the gap
    assert wm._conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 0

    src._records.append(_rec(3))                    # data keeps arriving
    now[0] = at(1, 0)                               # the pass begins
    up.pump()
    assert [r.columns["i"] for b in tr.shipped for r in b.records] == [1, 2, 3]
