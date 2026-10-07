"""D15: a retry that fails its replay keeps its key, cursor_after and commit_token.

The requeue rebuilt the deliverable field by field and left five fields out
(core.py, the retry_later branch of _handle_outcome).  A retry that failed
one replay lost its key, its cursor_after and its commit token.  When it
finally acknowledged, it advanced no send record and cleaned nothing up, and
the next drain sent its rows a second time (sigmond plan-sink-control D15,
§10.4 item 6).
"""
from __future__ import annotations

import time

from hs_uploader import Outcome, Pipeline, RecordBatch, RetryPolicy, Uploader
from hs_uploader.watermark import SqliteWatermarkStore
from tests.conftest import MemorySource
from tests.test_core_orchestration import _ident, _records

_KEY = ("memory:test", "memory", "test.spots")


class _TokenSource(MemorySource):
    """MemorySource whose batches carry a commit token; records each commit()."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.commits: list[bytes] = []

    def iter_batches(self, cursor: bytes, limit: int):
        for batch in super().iter_batches(cursor, limit):
            yield RecordBatch(records=batch.records,
                              cursor_after=batch.cursor_after,
                              commit_token=b"tok:" + batch.cursor_after)

    def commit(self, commit_token: bytes) -> None:
        self.commits.append(commit_token)


def _queued(wm: SqliteWatermarkStore) -> list[tuple]:
    return [
        (r["source_id"], r["dest_id"], r["table_name"], bytes(r["cursor_after"]),
         bytes(r["commit_token"]), r["attempts"])
        for r in wm._conn.execute(
            "SELECT source_id, dest_id, table_name, cursor_after, commit_token, "
            "attempts FROM deliverables ORDER BY id")
    ]


def test_retry_failing_twice_then_acked_advances_to_its_cursor(tmp_path, memory_transport):
    src = _TokenSource(records=_records(2))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(name="test", source=src, transport=memory_transport,
                    watermark=wm, identity=_ident(),
                    retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5))
    clock = [time.time()]
    up = Uploader([pipe], now_fn=lambda: clock[0])

    # 1. The first attempt fails: the retry queues with its key.
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    up.pump()
    assert _queued(wm) == [(*_KEY, b"2", b"tok:2", 0)]

    # 2. Its replay fails too.  The requeued row must still carry the key,
    #    cursor_after and commit token; only the attempt count moves.
    clock[0] += 3600.0
    memory_transport.next_outcomes = [Outcome.retry_later("still down")]
    up.pump()
    assert _queued(wm) == [(*_KEY, b"2", b"tok:2", 1)]

    # 3. The next replay acknowledges.  A second outcome waits behind it: if
    #    the drain sends the rows again, that send fails, so the send record
    #    can reach b"2" only through the retry's own cursor_after.
    clock[0] += 3600.0
    memory_transport.next_outcomes = [Outcome.acked(),
                                      Outcome.retry_later("a re-send")]
    up.pump()
    assert wm.get_cursor(*_KEY) == b"2"
    assert src.commits == [b"tok:2"]
    assert wm.deliverable_count("test") == 0
    assert len(memory_transport.shipped) == 1      # sent once, never re-sent
    assert len(memory_transport.replayed) == 2
