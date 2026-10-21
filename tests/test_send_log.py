"""The send log names its pipeline and client, and keeps last_send (spec §4.3, test 12)."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

import pytest

from hs_uploader import (
    Outcome, Pipeline, Record, RetryPolicy, StationIdentity, Uploader,
)
from hs_uploader.transports.discard import DiscardTransport
from hs_uploader.watermark import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport
from test_watermark_schema import _strip_to_v2_shape, _user_version


def _ident():
    return StationIdentity(call="AC0G", grid="EM38ww")


def _records(n):
    return [(Record(table="test.spots",
                    time=datetime(2026, 5, 8, 12, 0, i, tzinfo=timezone.utc),
                    columns={"i": i}), str(i).encode())
            for i in range(1, n + 1)]


def _pipe(wm, transport, n=3, **kw):
    return Pipeline(name="pipe-a", client="client-a",
                    source=MemorySource(records=_records(n)),
                    transport=transport, watermark=wm, identity=_ident(), **kw)


def _attempts(db):
    c = sqlite3.connect(db)
    try:
        return c.execute("SELECT outcome, pipeline, client FROM attempts "
                         "ORDER BY rowid").fetchall()
    finally:
        c.close()


def _last_send(db):
    c = sqlite3.connect(db)
    try:
        return c.execute("SELECT client, pipeline, outcome, ts, records "
                         "FROM last_send ORDER BY client, pipeline").fetchall()
    finally:
        c.close()


def _v2_db(tmp_path):
    db = tmp_path / "wm.db"
    SqliteWatermarkStore(db).close()
    _strip_to_v2_shape(db)
    c = sqlite3.connect(db)
    c.execute("PRAGMA user_version = 2")
    c.commit()
    c.close()
    return db


def _v3_db(tmp_path):
    db = tmp_path / "wm.db"
    SqliteWatermarkStore(db).close()
    return db


def test_v2_store_writes_old_columns_and_no_last_send(tmp_path):
    db = _v2_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    Uploader([_pipe(wm, MemoryTransport())]).pump()
    wm.close()
    c = sqlite3.connect(db)
    cols = [r[1] for r in c.execute("PRAGMA table_info(attempts)")]
    assert "pipeline" not in cols and "client" not in cols
    assert c.execute("SELECT count(*) FROM attempts").fetchone()[0] == 1
    assert c.execute("SELECT count(*) FROM last_send").fetchone()[0] == 0
    c.close()


def test_opening_a_v2_store_changes_neither_columns_nor_version(tmp_path):
    db = _v2_db(tmp_path)
    SqliteWatermarkStore(db).close()
    c = sqlite3.connect(db)
    cols = [r[1] for r in c.execute("PRAGMA table_info(attempts)")]
    c.close()
    assert "pipeline" not in cols and "client" not in cols
    assert _user_version(db) == 2


def test_v3_store_writes_pipeline_and_client(tmp_path):
    """All nine outcomes the core records carry pipe.name and pipe.client."""
    db = _v3_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    wm.close()
    # First attempts: acked, partial_ack, retry_later, permanent (4 sites).
    rej = (_records(3)[-1][0],)
    for outcome in (Outcome.acked(),
                    Outcome.partial_ack(b"1", rej, "one rejected"),
                    Outcome.retry_later("blip"),
                    Outcome.permanent_failure("bad")):
        t = MemoryTransport()
        t.next_outcomes = [outcome]
        wm2 = SqliteWatermarkStore(tmp_path / ("first-%s.db" % outcome.kind))
        Uploader([_pipe(wm2, t, n=3)]).pump()
        wm2.close()
        rows = _attempts(tmp_path / ("first-%s.db" % outcome.kind))
        assert rows == [(outcome.kind, "pipe-a", "client-a")]
    # Replays: acked (with cursor), retry_later, permanent, partial_ack read
    # as acked (4 sites), and dead_letter when the attempt budget runs out.
    cases = [(Outcome.acked(), 5), (Outcome.retry_later("x"), 5),
             (Outcome.permanent_failure("y"), 5),
             (Outcome.partial_ack(b"1", (), "z"), 5),
             (Outcome.retry_later("again"), 1)]
    seen = set()
    for i, (outcome, max_attempts) in enumerate(cases):
        rdb = tmp_path / ("replay-%d.db" % i)
        wm2 = SqliteWatermarkStore(rdb)
        t2 = MemoryTransport()
        p2 = Pipeline(name="pipe-a", client="client-a",
                      source=MemorySource(records=_records(1)),
                      transport=t2, watermark=wm2, identity=_ident(),
                      retry=RetryPolicy(base=1.0, cap_sec=1.0,
                                        max_attempts=max_attempts))
        t2.next_outcomes = [Outcome.retry_later("seed")]
        Uploader([p2]).pump()
        t2.next_outcomes = [outcome]
        Uploader([p2], now_fn=lambda: time.time() + 86_400.0).pump()
        wm2.close()
        rows = _attempts(rdb)
        assert len(rows) >= 2, rows          # the seed, the replay, maybe a re-ship
        assert all(r[1:] == ("pipe-a", "client-a") for r in rows), rows
        seen.add(rows[1][0])
    assert seen == {"acked", "retry_later", "permanent", "dead_letter"}


def test_last_send_on_acked_and_partial_ack_only(tmp_path):
    db = _v3_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    kw = dict(source_id="s", dest_id="d", table="t", bytes_=None, error=None,
              pipeline="p", client="c")
    wm.record_attempt(ts="T1", outcome="acked", records=5, **kw)
    assert _last_send(db) == [("c", "p", "acked", "T1", 5)]
    wm.record_attempt(ts="T2", outcome="partial_ack", records=3, **kw)
    assert _last_send(db) == [("c", "p", "partial_ack", "T2", 3)]
    for i, bad in enumerate(("retry_later", "permanent", "dead_letter")):
        wm.record_attempt(ts="T%d" % (3 + i), outcome=bad, records=1, **kw)
    assert _last_send(db) == [("c", "p", "partial_ack", "T2", 3)]
    wm.close()


def test_last_send_same_transaction(tmp_path):
    db = _v3_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    kw = dict(source_id="s", dest_id="d", table="t", bytes_=None, error=None,
              pipeline="p", client="c")
    wm.record_attempt(ts="T1", outcome="acked", records=5, **kw)
    # Make the attempt INSERT fail after last_send exists: drop its table.
    wm._conn.execute("DROP TABLE attempts")
    wm._conn.execute("CREATE TABLE attempts(x)")
    with pytest.raises(sqlite3.Error):
        wm.record_attempt(ts="T2", outcome="acked", records=9, **kw)
    wm.close()
    assert _last_send(db) == [("c", "p", "acked", "T1", 5)]


def test_discard_writes_no_last_send(tmp_path):
    db = _v3_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    Uploader([_pipe(wm, DiscardTransport(MemoryTransport()))]).pump()
    wm.close()
    assert _attempts(db) == [("acked", "pipe-a", "client-a")]
    assert _last_send(db) == []


def test_ordinary_transport_writes_last_send_through_the_core(tmp_path):
    db = _v3_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    Uploader([_pipe(wm, MemoryTransport())]).pump()
    wm.close()
    rows = _last_send(db)
    assert [r[:3] + r[4:] for r in rows] == [("client-a", "pipe-a", "acked", 3)]


@pytest.mark.parametrize("make", [_v2_db, _v3_db])
def test_existing_callers_unchanged(tmp_path, make):
    db = make(tmp_path)
    wm = SqliteWatermarkStore(db)
    wm.record_attempt(ts="T", source_id="s", dest_id="d", table="t",
                      outcome="acked", records=1, bytes_=None, error=None)
    wm.close()
    c = sqlite3.connect(db)
    assert c.execute("SELECT outcome FROM attempts").fetchall() == [("acked",)]
    c.close()


def test_refresh_follows_the_version(tmp_path):
    db = _v2_db(tmp_path)
    wm = SqliteWatermarkStore(db)
    assert wm.refresh_schema_version() == 2 and wm._v3 is False
    wm._conn.execute("ALTER TABLE attempts ADD COLUMN pipeline TEXT NOT NULL DEFAULT ''")
    wm._conn.execute("ALTER TABLE attempts ADD COLUMN client TEXT NOT NULL DEFAULT ''")
    wm._conn.execute("PRAGMA user_version = 3")
    assert wm.refresh_schema_version() == 3 and wm._v3 is True
    wm.close()
