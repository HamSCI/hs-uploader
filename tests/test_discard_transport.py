"""Discard mode — acknowledge everything, ship nothing, leave no backlog.

sigmond's ``discard`` uploads mode (sigmond tasks/plan-upload-control.md),
for bench provisioning of a machine bound for another site.  What must hold:

* nothing reaches the network while discard runs;
* the cursor the REAL destination reads advances, so ending discard ships
  nothing recorded on the bench;
* a delete-on-ack spool empties as it would after a real upload;
* a deliverable queued before discard began gets dropped, not retried.
"""
from __future__ import annotations

import os

from hs_uploader.core import Pipeline, Uploader
from hs_uploader.pipeline_factory import build_pipelines
from hs_uploader.sources import FileSpec, FileTreeSource
from hs_uploader.transports.discard import DiscardTransport
from hs_uploader.watermark.sqlite import SqliteWatermarkStore
from tests.conftest import MemorySource, MemoryTransport
from tests.test_core_orchestration import _ident, _records
from tests.test_pipeline_factory import _manifest


def _pipe(wm, transport, src):
    return Pipeline(name="test", source=src, transport=transport, watermark=wm,
                    identity=_ident())


def test_discard_ships_nothing_and_advances_the_real_cursor(tmp_path):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    real = MemoryTransport()
    Uploader([_pipe(wm, DiscardTransport(real), MemorySource(records=_records(5)))]).pump()
    assert real.shipped == []
    assert wm.get_cursor("memory:test", real.name, "test.spots") == b"5"


def test_ending_discard_ships_only_what_came_after(tmp_path):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    records = _records(8)
    Uploader([_pipe(wm, DiscardTransport(MemoryTransport()),
                    MemorySource(records=records[:5]))]).pump()
    # The machine reaches its site; discard ends; three new records arrive.
    real = MemoryTransport()
    Uploader([_pipe(wm, real, MemorySource(records=records))]).pump()
    shipped = [r.columns["i"] for b in real.shipped for r in b.records]
    assert shipped == [6, 7, 8]


def test_a_deliverable_queued_before_discard_is_dropped(tmp_path):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    wm.enqueue_deliverable(pipeline="test", payload_blob=b"old",
                           enqueued_at="2026-10-01T00:00:00+00:00",
                           next_attempt_at="1970-01-01T00:00:00+00:00",
                           cursor_after=b"1")
    real = MemoryTransport()
    Uploader([_pipe(wm, DiscardTransport(real), MemorySource(records=_records(0)))]).pump()
    assert real.replayed == []
    assert wm.deliverable_count("test") == 0
    assert wm.dead_letter_count() == 0


def test_a_delete_on_ack_spool_empties(tmp_path):
    spool = tmp_path / "spool"
    spool.mkdir()
    for i in range(3):
        f = spool / f"mag_{i}.zip"
        f.write_bytes(b"x")
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))
    src = FileTreeSource(spool, specs=[FileSpec(pattern="*.zip", parser=None,
                                                table="test.spots")])
    real = MemoryTransport()
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    Uploader([_pipe(wm, DiscardTransport(real), src)]).pump()
    assert real.shipped == []
    assert list(spool.glob("*.zip")) == []


def test_manifest_discard_flag_wraps_under_the_real_name(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    plain = build_pipelines(_manifest(tmp_path),
                            watermark=SqliteWatermarkStore(":memory:"))
    m = _manifest(tmp_path)
    for entry in m["pipeline"]:
        entry["discard"] = True
    wrapped = build_pipelines(m, watermark=SqliteWatermarkStore(":memory:"))
    assert all(isinstance(p.transport, DiscardTransport) for p in wrapped)
    assert [p.transport.name for p in wrapped] == [p.transport.name for p in plain]
    assert not any(isinstance(p.transport, DiscardTransport) for p in plain)
