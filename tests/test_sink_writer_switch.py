"""The sink writer's switch check (spec-sink-2b-part2 §4, tests 8 and 9).

`Writer.insert()` drops a row when the writer is not local, the carry-over
marker stands, `sinks/<producer>.toml` exists, and `effective_state(producer)`
reads `off`.  `fill` and `upload` store.  Each row's producer comes from
`self.producer or infer_producer(...)`, as `flush()` names it, so one batch
can split by producer.

Every test that asserts a row stores also shows, in the same harness, the one
changed condition under which the row drops (Global Constraint 1).
"""

import logging
import os
import sqlite3
import sys
import types

import pytest

import hs_uploader.sinkswitch as sinkswitch
from hs_uploader.sink import SqliteConfig, Writer
from hs_uploader.sink.writer import HEALTH_OK

LOGGER = "sigmond.hamsci_sink"


@pytest.fixture
def tree(tmp_path):
    """A scratch sinks/ directory and marker path; the marker stands."""
    sinks = tmp_path / "sinks"
    sinks.mkdir()
    marker = tmp_path / "state" / sinkswitch.MARKER_NAME
    marker.parent.mkdir()
    marker.write_text("carry-over done\n")
    return types.SimpleNamespace(sinks=sinks, marker=marker, db=tmp_path / "sink.db")


def _switch(sinks, name, state):
    """Write sinks/<name>.toml with a new inode, so read_switch's cache misses."""
    path = sinks / f"{name}.toml"
    tmp = sinks / f".{name}.tmp"
    tmp.write_text(f'client = "{name}"\nstate = "{state}"\n')
    os.replace(tmp, path)


def _writer(t, *, database="psk", table="spots", **kw):
    return Writer(database, table, config=SqliteConfig(path=str(t.db)),
                  sinks_dir=t.sinks, marker=t.marker, **kw)


def _stored(t):
    """(producer, mode-or-None) for every stored row, in id order."""
    if not t.db.exists():
        return []
    with sqlite3.connect(t.db) as c:
        return [(p, m) for p, m in c.execute(
            "SELECT producer, json_extract(payload_json, '$.mode') "
            "FROM pending_uploads ORDER BY id")]


def _put(w, rows):
    w.insert(rows)
    w.flush()


# ---- test 8 -----------------------------------------------------------------

@pytest.mark.parametrize("state,kept", [("off", 0), ("fill", 3), ("upload", 3)])
def test_off_drops_fill_and_upload_store(tree, state, kept):
    _switch(tree.sinks, "psk-recorder", state)
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "FT8"}] * 3)
    assert len(_stored(tree)) == kept
    assert w.dropped == ({"psk-recorder": 3} if state == "off" else {})
    assert w.health == HEALTH_OK          # off keeps the writer healthy


def test_a_mixed_psk_batch_splits_by_producer(tree):
    _switch(tree.sinks, "psk-recorder", "upload")
    _switch(tree.sinks, "meteor-scatter", "off")
    w = _writer(tree)                     # names no producer: inferred per row
    _put(w, [{"mode": "FT8"}, {"mode": "MSK144"}, {"mode": "FT4"}, {"mode": "msk144"}])
    assert _stored(tree) == [("psk-recorder", "FT8"), ("psk-recorder", "FT4")]
    assert w.dropped == {"meteor-scatter": 2}


def test_rows_with_no_named_producer_are_judged_by_inference(tree):
    w = _writer(tree, database="wspr")    # infer_producer -> wspr-recorder
    _switch(tree.sinks, "wspr-recorder", "upload")
    _put(w, [{"mode": "W2"}])
    assert _stored(tree) == [("wspr-recorder", "W2")]
    _switch(tree.sinks, "wspr-recorder", "off")        # control: now it drops
    _put(w, [{"mode": "W2"}])
    assert _stored(tree) == [("wspr-recorder", "W2")]
    assert w.dropped == {"wspr-recorder": 1}


def test_a_producer_with_no_switch_file_stores(tree):
    _switch(tree.sinks, "site", "off")    # even under a site at off
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1
    _switch(tree.sinks, "psk-recorder", "upload")      # control: the file exists,
    _put(w, [{"mode": "FT8"}])                         # the site ceiling drops it
    assert len(_stored(tree)) == 1
    assert w.dropped == {"psk-recorder": 1}


def test_an_unknown_producer_stores_and_reads_no_switch(tree, caplog):
    _switch(tree.sinks, "site", "off")
    w = _writer(tree, database="renamed")              # infer_producer -> ''
    with caplog.at_level(logging.DEBUG):
        _put(w, [{"mode": "FT8"}])
    assert _stored(tree) == [("", "FT8")]
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_no_marker_stores(tree):
    _switch(tree.sinks, "psk-recorder", "off")
    tree.marker.unlink()
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1
    tree.marker.write_text("x\n")                      # control: the marker stands
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1
    assert w.dropped == {"psk-recorder": 1}


def test_a_local_writer_stores(tree):
    _switch(tree.sinks, "psk-recorder", "off")
    w = _writer(tree, producer="psk-recorder", local=True)
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1
    assert w.dropped == {}
    other = _writer(tree, producer="psk-recorder")     # control: not local, drops
    _put(other, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1
    assert other.dropped == {"psk-recorder": 1}


def test_a_switch_change_counts_at_the_next_insert(tree):
    w = _writer(tree, producer="psk-recorder")
    _switch(tree.sinks, "psk-recorder", "off")
    _put(w, [{"mode": "A"}])
    _switch(tree.sinks, "psk-recorder", "fill")
    _put(w, [{"mode": "B"}])
    _switch(tree.sinks, "psk-recorder", "off")
    _put(w, [{"mode": "C"}])
    _switch(tree.sinks, "psk-recorder", "upload")
    _put(w, [{"mode": "D"}])
    assert [m for _, m in _stored(tree)] == ["B", "D"]
    assert w.dropped == {"psk-recorder": 2}


def test_the_check_runs_at_insert_not_at_flush(tree):
    """A row judged at insert keeps that verdict whatever the switch says at flush."""
    w = _writer(tree, producer="psk-recorder")
    _switch(tree.sinks, "psk-recorder", "upload")
    w.insert([{"mode": "kept"}])
    _switch(tree.sinks, "psk-recorder", "off")
    w.insert([{"mode": "gone"}])
    _switch(tree.sinks, "psk-recorder", "upload")
    w.flush()
    assert [m for _, m in _stored(tree)] == ["kept"]


def test_a_broken_switch_file_reads_off_and_drops(tree):
    """Spec §4.1 drops whenever the file exists and effective_state reads off;
    design §3.4 reads any unreadable or unparsable file as off."""
    (tree.sinks / "psk-recorder.toml").write_text("state = [broken\n")
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "FT8"}])
    assert _stored(tree) == []
    _switch(tree.sinks, "psk-recorder", "fill")        # control: a sound file stores
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1


def test_defaults_read_the_library_paths(tree, monkeypatch):
    """sinks_dir=None and marker=None mean DEFAULT_SINKS_DIR and marker_path()."""
    monkeypatch.setattr(sinkswitch, "DEFAULT_SINKS_DIR", tree.sinks)
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tree.marker.parent))
    _switch(tree.sinks, "psk-recorder", "off")
    w = Writer("psk", "spots", config=SqliteConfig(path=str(tree.db)),
               producer="psk-recorder")
    _put(w, [{"mode": "FT8"}])
    assert _stored(tree) == []
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tree.sinks))   # no marker there
    _put(w, [{"mode": "FT8"}])
    assert len(_stored(tree)) == 1


def test_a_thousand_inserts_at_off_log_one_line(tree, caplog):
    _switch(tree.sinks, "psk-recorder", "off")
    w = _writer(tree, producer="psk-recorder", batch_rows=10_000)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for _ in range(1000):
            w.insert([{"mode": "FT8"}, {"mode": "FT4"}])
    lines = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert lines == ["hamsci_sink: sink switch reads off for psk-recorder: "
                     "storing nothing for repositories"]
    assert caplog.records[0].levelno == logging.INFO
    assert w.dropped == {"psk-recorder": 2000}
    # Control: the same harness sees each later transition, once.
    caplog.clear()
    _switch(tree.sinks, "psk-recorder", "fill")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for _ in range(100):
            w.insert([{"mode": "FT8"}])
    _switch(tree.sinks, "psk-recorder", "off")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for _ in range(100):
            w.insert([{"mode": "FT8"}])
    lines = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert lines == [
        "hamsci_sink: sink switch no longer reads off for psk-recorder: storing again",
        "hamsci_sink: sink switch reads off for psk-recorder: "
        "storing nothing for repositories",
    ]


def test_dropped_counts_are_capped_with_one_warning(tree, monkeypatch, caplog):
    from hs_uploader.sink import writer as writer_mod
    monkeypatch.setattr(writer_mod, "_PRODUCER_CAP", 3)
    names = [f"client-{i}" for i in range(6)]
    for n in names:
        _switch(tree.sinks, n, "off")
    w = _writer(tree)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for _ in range(5):
            for n in names:
                w.producer = n
                w.insert([{"mode": "x"}])
    assert sorted(w.dropped) == names[:3]
    assert _stored(tree) == []                         # past the cap rows still drop
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1 and "3 producers" in warns[0].getMessage()


def test_a_failing_switch_read_stores_and_logs_once(tree, monkeypatch, caplog):
    def boom(*a, **k):
        raise RuntimeError("switch reader broke")
    _switch(tree.sinks, "psk-recorder", "off")
    monkeypatch.setattr(sinkswitch, "effective_state", boom)
    w = _writer(tree, producer="psk-recorder")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for _ in range(100):
            w.insert([{"mode": "FT8"}])
    w.flush()
    assert len(_stored(tree)) == 100
    errors = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(errors) == 1 and "RuntimeError" in errors[0].getMessage()
    assert errors[0].levelno == logging.ERROR
    # Control: once the reminder interval has passed, the same harness
    # sees the fault again.
    from hs_uploader.sink import writer as writer_mod
    monkeypatch.setattr(writer_mod, "_REMIND_S", 0.0)
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        w.insert([{"mode": "FT8"}])
    assert [r.levelno for r in caplog.records] == [logging.ERROR]


# ---- test 9: the guarded import ---------------------------------------------

def test_hs_uploader_unimportable_stores_and_raises_nothing(tree, monkeypatch):
    _switch(tree.sinks, "psk-recorder", "off")
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "control"}])                     # control: enforced, drops
    assert _stored(tree) == []
    for name in [k for k in sys.modules if k == "hs_uploader" or k.startswith("hs_uploader.")]:
        if name != "hs_uploader.sink" and not name.startswith("hs_uploader.sink."):
            monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "hs_uploader", None)   # import hs_uploader fails
    _put(w, [{"mode": "FT8"}])
    assert _stored(tree) == [("psk-recorder", "FT8")]


@pytest.mark.parametrize("missing", ["marker_stands", "effective_state",
                                     "marker_path", "DEFAULT_SINKS_DIR",
                                     "valid_client_name"])
def test_a_sinkswitch_lacking_a_name_stores_and_raises_nothing(tree, monkeypatch, missing):
    _switch(tree.sinks, "psk-recorder", "off")
    w = _writer(tree, producer="psk-recorder")
    _put(w, [{"mode": "control"}])                     # control: enforced, drops
    assert _stored(tree) == []
    old = types.ModuleType("hs_uploader.sinkswitch")
    for name in ("marker_stands", "effective_state", "marker_path",
                 "DEFAULT_SINKS_DIR", "valid_client_name"):
        if name != missing:
            setattr(old, name, getattr(sinkswitch, name))
    monkeypatch.setitem(sys.modules, "hs_uploader.sinkswitch", old)
    _put(w, [{"mode": "FT8"}])
    assert _stored(tree) == [("psk-recorder", "FT8")]
