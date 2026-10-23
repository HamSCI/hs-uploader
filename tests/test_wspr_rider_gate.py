"""The psk rider gate in WsprCycleSource (spec-sink-2b-part2 §5, test 10).

wspr-recorder's `wspr-wsprdaemon` pipeline pulls each cycle's `psk.spots`
rows into the WSPR tar (`include_psk`).  While the carry-over marker stands,
a psk row rides only when its producer's effective state reads `upload` at
build time.  The producer comes from the row's `producer` column, or from
`infer_producer` when that reads `''` or the sink has no such column.  The
cycle's WSPR spots and noise stay exactly as they were.

The rule checks the state when the tar ships (Michael's answer 1): a row
stored while its producer read `fill` rides once the producer reads `upload`,
if its cycle has not shipped yet.  The cycle's send record advances when the
tar ships, so a rider left out then never rides later.

Every test that asserts an absence also shows, in the same harness, the
thing present under the one changed condition (Global Constraint 1).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import types
from pathlib import Path

import pytest

import hs_uploader.sinkswitch as sinkswitch
import hs_uploader.sources.wspr_cycle as wspr_cycle
from hs_uploader import daemon
from hs_uploader import Pipeline, StationIdentity, Uploader
from hs_uploader.pipeline_factory import build_entry
from hs_uploader.pipelines_d import sinks_dir_for
from hs_uploader.sink import PENDING_UPLOADS_DDL, SqliteConfig, Writer
from hs_uploader.sources.wspr_cycle import WsprCycleSource
from hs_uploader.watermark.sqlite import SqliteWatermarkStore

from conftest import MemoryTransport

LOGGER = "hs_uploader.sources.wspr_cycle"
C1 = "2026-01-05T00:00:00Z"
C2 = "2026-01-05T00:02:00Z"
BEFORE = b"2026-01-01T00:00:00Z"   # a stored send record before every cycle

# A sink as v3.69 and older made it: no `producer`, no `local` column.
_OLD_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
)
"""


# ---- harness ---------------------------------------------------------------


@pytest.fixture
def tree(tmp_path):
    """A scratch sinks/, a standing marker, and an empty sink."""
    sinks = tmp_path / "sinks"
    sinks.mkdir()
    marker = tmp_path / "state" / sinkswitch.MARKER_NAME
    marker.parent.mkdir()
    marker.write_text("carry-over done\n")
    db = tmp_path / "sink.db"
    with sqlite3.connect(db) as c:
        c.executescript(PENDING_UPLOADS_DDL)
    return types.SimpleNamespace(sinks=sinks, marker=marker, db=db,
                                 tmp=tmp_path)


def _switch(sinks, name, state):
    """Write sinks/<name>.toml with a new inode, so read_switch's cache misses."""
    path = sinks / f"{name}.toml"
    tmp = sinks / f".{name}.tmp"
    tmp.write_text(f'client = "{name}"\nstate = "{state}"\n')
    os.replace(tmp, path)


def _add(db, target_db, table, payload, *, producer=None):
    """Insert one row; `producer=None` leaves a sink without the column alone."""
    with sqlite3.connect(db) as c:
        if producer is None:
            c.execute(
                "INSERT INTO pending_uploads (target_db, target_table, "
                "schema_version, payload_json, queued_at) "
                "VALUES (?,?,2,?,'2026-01-05T00:00:30Z')",
                (target_db, table, json.dumps(payload)))
        else:
            c.execute(
                "INSERT INTO pending_uploads (target_db, target_table, "
                "schema_version, payload_json, queued_at, producer) "
                "VALUES (?,?,2,?,'2026-01-05T00:00:30Z',?)",
                (target_db, table, json.dumps(payload), producer))


def _cycle(db, cycle=C1, *, ft8="psk-recorder", msk="meteor-scatter",
           wspr="wspr-recorder"):
    """One cycle: 2 WSPR spots, 1 noise, 2 FT8 and 2 MSK144 psk rows."""
    _add(db, "wspr", "spots", {"time": cycle, "k": "s1", "snr": -21}, producer=wspr)
    _add(db, "psk", "spots", {"time": cycle, "k": "ft8-a", "mode": "FT8"}, producer=ft8)
    _add(db, "wspr", "noise", {"time": cycle, "k": "n1", "rms": -130.5}, producer=wspr)
    _add(db, "psk", "spots", {"time": cycle, "k": "msk-a", "mode": "MSK144"}, producer=msk)
    _add(db, "wspr", "spots", {"time": cycle, "k": "s2", "snr": -5}, producer=wspr)
    _add(db, "psk", "spots", {"time": cycle, "k": "ft8-b", "mode": "FT8"}, producer=ft8)
    _add(db, "psk", "spots", {"time": cycle, "k": "msk-b", "mode": "msk144"}, producer=msk)


def _src(t, **kw):
    return WsprCycleSource(db_path=t.db, include_psk=True, sinks_dir=t.sinks,
                           marker=t.marker, **kw)


def _batches(src, cursor=BEFORE):
    return list(src.iter_batches(cursor, 1000))


def _keys(batch, table=None):
    return [r.columns["k"] for r in batch.records
            if table is None or r.table == table]


def _psk(src, cursor=BEFORE):
    return [k for b in _batches(src, cursor) for k in _keys(b, "psk.spots")]


def _wspr_bytes(batch):
    """The batch's WSPR records, as bytes, in order."""
    return [json.dumps([r.table, r.time.isoformat(), r.columns], sort_keys=True)
            for r in batch.records if r.table.startswith("wspr.")]


def _rider_lines(caplog):
    return [r for r in caplog.records
            if r.name == LOGGER and "wsprdaemon tar" in r.getMessage()]


# ---- the rule ---------------------------------------------------------------


def test_fill_rows_stay_out_upload_rows_ride_and_wspr_rows_are_untouched(tree):
    _cycle(tree.db)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    (got,) = _batches(_src(tree))
    assert _keys(got, "psk.spots") == ["msk-a", "msk-b"]
    # The unfiltered batch: the same source with no marker standing.
    (bare,) = _batches(WsprCycleSource(
        db_path=tree.db, include_psk=True, sinks_dir=tree.sinks,
        marker=tree.tmp / "absent"))
    assert _keys(bare, "psk.spots") == ["ft8-a", "msk-a", "ft8-b", "msk-b"]
    assert _wspr_bytes(got) == _wspr_bytes(bare)
    assert _keys(got, "wspr.spots") == ["s1", "s2"]
    assert _keys(got, "wspr.noise") == ["n1"]
    assert got.cursor_after == bare.cursor_after == C1.encode()
    assert got.commit_token == bare.commit_token


@pytest.mark.parametrize("client,site,rides", [
    ("upload", None, True),
    ("upload", "upload", True),
    ("fill", None, False),
    ("off", None, False),
    ("upload", "fill", False),        # the stricter of the two
    ("upload", "off", False),
    ("missing", None, False),         # no client file: fails closed (§5: only upload)
    ("broken", None, False),          # a broken file reads off
    ("upload", "broken", False),
])
def test_a_row_rides_only_while_its_producer_reads_upload(tree, client, site, rides):
    _cycle(tree.db)
    _switch(tree.sinks, "meteor-scatter", "upload")
    if client == "broken":
        (tree.sinks / "psk-recorder.toml").write_text("state = [\n")
    elif client != "missing":
        _switch(tree.sinks, "psk-recorder", client)
    if site == "broken":
        (tree.sinks / "site.toml").write_text("state = \"sideways\"\n")
    elif site is not None:
        _switch(tree.sinks, "site", site)
    ft8 = [k for k in _psk(_src(tree)) if k.startswith("ft8")]
    assert ft8 == (["ft8-a", "ft8-b"] if rides else [])


def test_rows_stored_with_no_producer_are_judged_by_inference(tree):
    _cycle(tree.db, ft8="", msk="")
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    src = _src(tree)
    assert _psk(src) == ["msk-a", "msk-b"]
    # Control: the inferred producer, not the empty name, decides.
    _switch(tree.sinks, "psk-recorder", "upload")
    _switch(tree.sinks, "meteor-scatter", "fill")
    assert _psk(src) == ["ft8-a", "ft8-b"]


def test_a_named_producer_wins_over_inference(tree):
    # An FT8 row a client named meteor-scatter is judged as meteor-scatter.
    _cycle(tree.db, ft8="meteor-scatter")
    _switch(tree.sinks, "psk-recorder", "upload")
    _switch(tree.sinks, "meteor-scatter", "fill")
    assert _psk(_src(tree)) == []
    _switch(tree.sinks, "meteor-scatter", "upload")
    assert _psk(_src(tree)) == ["ft8-a", "msk-a", "ft8-b", "msk-b"]


def test_no_marker_keeps_every_row_and_the_marker_counts_live(tree):
    _cycle(tree.db)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    tree.marker.unlink()
    src = _src(tree)
    assert _psk(src) == ["ft8-a", "msk-a", "ft8-b", "msk-b"]
    # Positive control: the marker appears, the same source drops FT8.
    tree.marker.write_text("carry-over done\n")
    assert _psk(src) == ["msk-a", "msk-b"]
    # And it goes again: every row rides again.
    tree.marker.unlink()
    assert _psk(src) == ["ft8-a", "msk-a", "ft8-b", "msk-b"]


def test_a_sink_without_the_producer_column_filters_by_inference(tmp_path, tree):
    old = tmp_path / "old.db"
    with sqlite3.connect(old) as c:
        c.executescript(_OLD_DDL)
    _cycle(old, ft8=None, msk=None, wspr=None)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    src = WsprCycleSource(db_path=old, include_psk=True, sinks_dir=tree.sinks,
                          marker=tree.marker)
    (got,) = _batches(src)
    assert _keys(got, "psk.spots") == ["msk-a", "msk-b"]
    assert _keys(got, "wspr.spots") == ["s1", "s2"]
    _switch(tree.sinks, "psk-recorder", "upload")
    assert _psk(src) == ["ft8-a", "msk-a", "ft8-b", "msk-b"]


def test_the_shim_built_source_filters_with_the_default_paths(tree, monkeypatch):
    # wspr-recorder/wspr_recorder/hs_uploader_shim.py:739-746 at b523524:
    #     WsprCycleSource(db_path=self._sink_db,
    #                     expected_reporters=self._merge_reporters,
    #                     backstop_sec=self._merge_backstop_sec,
    #                     include_psk=True)
    # It passes neither sinks_dir nor marker.  Point the defaults at scratch.
    monkeypatch.setattr(sinkswitch, "DEFAULT_SINKS_DIR", tree.sinks)
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tree.marker.parent))
    _cycle(tree.db)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    src = WsprCycleSource(db_path=tree.db, expected_reporters=set(),
                          backstop_sec=90.0, include_psk=True)
    assert _psk(src) == ["msk-a", "msk-b"]
    # Control: the same source, the producer at upload, rides.
    _switch(tree.sinks, "psk-recorder", "upload")
    assert _psk(src) == ["ft8-a", "msk-a", "ft8-b", "msk-b"]


def test_without_include_psk_no_rider_is_read(tree):
    _cycle(tree.db)
    _switch(tree.sinks, "psk-recorder", "upload")
    _switch(tree.sinks, "meteor-scatter", "upload")
    src = WsprCycleSource(db_path=tree.db, sinks_dir=tree.sinks, marker=tree.marker)
    (got,) = _batches(src)
    assert _keys(got) == ["s1", "n1", "s2"]


# ---- the log ----------------------------------------------------------------


def test_a_hundred_cycles_log_one_line_per_producer_decision(tree, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    cycles = [f"2026-01-05T{(i * 2) // 60:02d}:{(i * 2) % 60:02d}:00Z"
              for i in range(100)]
    for c in cycles:
        _cycle(tree.db, c)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    src = _src(tree)
    got = _batches(src)
    assert len(got) == 100 and all(_keys(b, "psk.spots") == ["msk-a", "msk-b"]
                                   for b in got)
    lines = _rider_lines(caplog)
    assert len(lines) == 1
    assert "psk-recorder" in lines[0].getMessage()
    assert "fill" in lines[0].getMessage()
    assert lines[0].levelno == logging.INFO
    # The per-cycle line counts what it left out.
    ready = [r.getMessage() for r in caplog.records
             if r.name == LOGGER and " ready (" in r.getMessage()]
    assert len(ready) == 100
    assert all(m.endswith("2 psk, 2 psk left out)") for m in ready)

    # Positive control: the decision changes, one more line, then silence.
    caplog.clear()
    _switch(tree.sinks, "psk-recorder", "upload")
    again = _batches(src)
    assert all(_keys(b, "psk.spots") == ["ft8-a", "msk-a", "ft8-b", "msk-b"]
               for b in again)
    lines = _rider_lines(caplog)
    assert len(lines) == 1 and "psk-recorder" in lines[0].getMessage()
    assert "again" in lines[0].getMessage()


def test_a_failing_check_leaves_the_riders_out_and_logs_once(tree, monkeypatch,
                                                             caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    for i in range(100):
        _cycle(tree.db, f"2026-01-05T{(i * 2) // 60:02d}:{(i * 2) % 60:02d}:00Z")
    _switch(tree.sinks, "psk-recorder", "upload")
    _switch(tree.sinks, "meteor-scatter", "upload")

    def boom(*_a, **_k):
        raise RuntimeError("disk on fire, " + os.urandom(4).hex())

    monkeypatch.setattr(sinkswitch, "effective_state", boom)
    got = _batches(_src(tree))
    assert len(got) == 100
    assert all(_keys(b) == ["s1", "n1", "s2"] for b in got)   # WSPR still ships
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "RuntimeError" in errors[0].getMessage()
    # Control: once the check works, the riders ride.
    monkeypatch.undo()
    assert _keys(_batches(_src(tree))[0], "psk.spots") == [
        "ft8-a", "msk-a", "ft8-b", "msk-b"]


# ---- ship time, and the send record ------------------------------------------


def test_a_row_stored_at_fill_rides_once_its_producer_reads_upload(tree):
    """Ship-time rule only (Michael's answer 1): what a producer held at
    `fill` rides out once it reads `upload`, while its cycle has not shipped."""
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    _switch(tree.sinks, "wspr-recorder", "upload")
    cfg = SqliteConfig(path=str(tree.db))
    psk = Writer("psk", "spots", config=cfg, sinks_dir=tree.sinks,
                 marker=tree.marker, producer="psk-recorder")
    wspr = Writer("wspr", "spots", config=cfg, sinks_dir=tree.sinks,
                  marker=tree.marker, producer="wspr-recorder")
    psk.insert([{"time": C1, "k": "ft8-a", "mode": "FT8"}])   # stored at fill
    wspr.insert([{"time": C1, "k": "s1"}])
    psk.flush()
    wspr.flush()
    src = _src(tree)
    assert _psk(src) == []                       # built now: left out
    _switch(tree.sinks, "psk-recorder", "upload")
    assert _psk(src) == ["ft8-a"]                # built after the raise: rides


def _pipe(src, store):
    t = MemoryTransport("wspr.spots", accepts_versions=(1, 2))
    t.ACCEPTS.update({"wspr.noise": [1, 2], "psk.spots": [1, 2]})
    return Pipeline(name="wspr-wsprdaemon", source=src, transport=t,
                    watermark=store,
                    identity=StationIdentity(call="AC0G", grid="EM38ww"),
                    client="wspr-recorder")


def test_a_rider_left_out_when_its_cycle_ships_never_rides_later(tree):
    """ENFORCEMENT GATE: the send record advances when the tar ships."""
    _cycle(tree.db, C1)
    _switch(tree.sinks, "psk-recorder", "fill")
    _switch(tree.sinks, "meteor-scatter", "upload")
    store = SqliteWatermarkStore(tree.tmp / "wm.db")
    p = _pipe(_src(tree, start_at="beginning"), store)
    up = Uploader([p])
    up.pump_until_idle()
    assert [_keys(b, "psk.spots") for b in p.transport.shipped] == [["msk-a", "msk-b"]]
    key = (p.source_id(), p.dest_id(), p.transport.primary_table())
    assert store.get_cursor(*key) == C1.encode()

    _switch(tree.sinks, "psk-recorder", "upload")
    _cycle(tree.db, C2)
    up.pump_until_idle()
    shipped = [b.cursor_after for b in p.transport.shipped]
    assert shipped == [C1.encode(), C2.encode()]      # C1 never ships twice
    assert _keys(p.transport.shipped[1], "psk.spots") == [
        "ft8-a", "msk-a", "ft8-b", "msk-b"]
    ft8_c1 = [r for b in p.transport.shipped for r in b.records
              if r.table == "psk.spots" and r.columns["time"] == C1
              and r.columns["mode"] == "FT8"]
    assert ft8_c1 == []

    # Control: the C1 FT8 rows still sit in the sink, and a send record
    # that has not passed C1 would carry them now.
    fresh = _pipe(_src(tree, start_at="beginning"),
                  SqliteWatermarkStore(tree.tmp / "wm2.db"))
    Uploader([fresh]).pump_until_idle()
    assert _keys(fresh.transport.shipped[0], "psk.spots") == [
        "ft8-a", "msk-a", "ft8-b", "msk-b"]


# ---- the daemon passes its paths -----------------------------------------------


_ENTRY = {
    "name": "wspr-wsprdaemon",
    "client": "wspr-recorder",
    "source": {"type": "wspr_cycle", "include_psk": True},
    "transport": {"type": "wsprdaemon_tar", "servers": ["gw1.example"],
                  "receiver": "AC0G"},
}


def test_build_entry_passes_the_paths_to_the_source(tree):
    store = SqliteWatermarkStore(tree.tmp / "wm.db")
    p = build_entry(_ENTRY, base_identity={}, watermark=store,
                    sinks_dir=tree.sinks, marker=tree.marker)
    assert p.source.sinks_dir == tree.sinks
    assert p.source.marker == tree.marker
    # Left out, the source reads the library's defaults at each cycle.
    q = build_entry(_ENTRY, base_identity={}, watermark=store)
    assert q.source.sinks_dir is None and q.source.marker is None


def test_the_daemon_gives_the_rider_its_own_sinks_dir_and_marker(tree, monkeypatch):
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tree.tmp))
    etc = tree.tmp / "etc"
    etc.mkdir()
    manifest = etc / "pipelines.toml"
    manifest.write_text(
        '[identity]\ncall = "AC0G"\ngrid = "EM38ww"\n\n'
        "[[pipeline]]\n"
        'name = "wspr-wsprdaemon"\n'
        '[pipeline.source]\ntype = "wspr_cycle"\ninclude_psk = true\n'
        f'db_path = "{tree.db}"\n'
        '[pipeline.transport]\ntype = "wsprdaemon_tar"\n'
        'servers = ["gw1.example"]\nreceiver = "AC0G"\n')
    (etc / "pipelines.d").mkdir()
    (etc / "pipelines.d" / "wspr-recorder.toml").write_text(
        'client = "wspr-recorder"\n\n[[pipeline]]\n'
        'name = "wspr-cycle-2"\n'
        '[pipeline.source]\ntype = "wspr_cycle"\ninclude_psk = true\n'
        f'db_path = "{tree.db}"\nstart_at = "beginning"\n'
        '[pipeline.transport]\ntype = "wsprdaemon_tar"\n'
        'servers = ["gw2.example"]\nreceiver = "AC0G"\n')
    store = SqliteWatermarkStore(tree.tmp / "wm.db")
    r = daemon.Rebuilder(manifest, daemon.load_manifest(manifest), store,
                         marker=tree.marker)
    running, _built, refused = r.start()
    assert refused == []
    sources = {p.name: p.source for p in running}
    assert set(sources) == {"wspr-wsprdaemon", "wspr-cycle-2"}
    for s in sources.values():
        assert s.sinks_dir == sinks_dir_for(manifest) == etc / "sinks"
        assert s.marker == tree.marker
