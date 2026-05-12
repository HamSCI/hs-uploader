"""End-to-end orchestration: Pipeline + Uploader + MemoryTransport.

These tests exercise the cursor-advance, retry-deliverable, and
dead-letter machinery without any network or real source.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hs_uploader import (
    Outcome,
    Pipeline,
    Record,
    RetryPolicy,
    StationIdentity,
    Uploader,
)
from hs_uploader.watermark import SqliteWatermarkStore

from conftest import MemorySource, MemoryTransport


def _ident() -> StationIdentity:
    return StationIdentity(call="AC0G", grid="EM38ww")


def _records(n: int):
    """n records with cursors b'1', b'2', ..."""
    out = []
    for i in range(1, n + 1):
        out.append(
            (
                Record(
                    table="test.spots",
                    time=datetime(2026, 5, 8, 12, 0, i, tzinfo=timezone.utc),
                    columns={"i": i},
                ),
                str(i).encode(),
            )
        )
    return out


def test_drains_one_batch_and_advances_cursor(tmp_path, memory_transport):
    src = MemorySource(records=_records(3))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test",
        source=src,
        transport=memory_transport,
        watermark=wm,
        identity=_ident(),
    )
    up = Uploader([pipe])

    did = up.pump()
    assert did is True
    assert len(memory_transport.shipped) == 1
    assert len(memory_transport.shipped[0].records) == 3
    # Cursor advanced to the last record's cursor.
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b"3"

    # Second pump: nothing new.
    did2 = up.pump()
    assert did2 is False


def test_retry_later_persists_deliverable(tmp_path, memory_transport):
    src = MemorySource(records=_records(2))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    memory_transport.next_outcomes = [Outcome.retry_later("server hiccup")]
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )
    Uploader([pipe]).pump()

    # Cursor did NOT advance — retry hasn't acked yet.
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b""
    # Deliverable was queued.
    assert wm.deliverable_count("test") == 1


def test_replay_ack_advances_cursor_and_commits(tmp_path, memory_transport):
    """Replay-ack must advance the cursor and call source.commit —
    correctness fix beyond Phase 1's first-attempt-only logic."""
    src = MemorySource(records=_records(2))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
        retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5),
    )

    # First pump: ship returns retry_later → deliverable queued, cursor
    # has NOT advanced.
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b""

    # Second pump in fast-forward time: replay returns acked → cursor
    # MUST now advance to b"2" (the cursor stored on the deliverable).
    import time
    far = time.time() + 86_400.0
    up = Uploader([pipe], now_fn=lambda: far)
    memory_transport.next_outcomes = [Outcome.acked()]
    up.pump()

    assert wm.get_cursor("memory:test", "memory", "test.spots") == b"2"
    assert wm.deliverable_count("test") == 0


def test_replay_acks_clears_deliverable(tmp_path, memory_transport):
    """First attempt fails, replay succeeds → deliverable consumed."""
    src = MemorySource(records=_records(2))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
        retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=5),
    )

    # First pump: ship returns retry_later → deliverable queued.
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    assert wm.deliverable_count("test") == 1

    # Second pump: deliverable is due (next_attempt_at = now + 1s but
    # we drive the clock with now_fn).  Use a fake clock that's far
    # in the future to make the deliverable due.
    import time
    far_future = time.time() + 10_000
    up_fast_clock = Uploader([pipe], now_fn=lambda: far_future)
    # ship is empty after the first batch since cursor didn't advance —
    # but the deliverable replay drives transport.replay.
    memory_transport.next_outcomes = [Outcome.acked()]
    up_fast_clock.pump()

    assert wm.deliverable_count("test") == 0
    assert len(memory_transport.replayed) == 1


def test_retry_exhaustion_dead_letters(tmp_path, memory_transport):
    src = MemorySource(records=_records(1))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
        retry=RetryPolicy(base=1.0, cap_sec=1.0, max_attempts=3),
    )

    # Initial ship → retry_later.
    memory_transport.next_outcomes = [Outcome.retry_later("blip")]
    Uploader([pipe]).pump()
    assert wm.deliverable_count("test") == 1

    # Drive replays.  The clock cell starts well after the first
    # pump's wallclock-anchored ``next_attempt_at`` and advances by
    # one hour per iteration so each requeued (now + 1s) deadline is
    # always past on the next pump.
    import time
    clock = [time.time() + 86_400.0]
    def fake_now():
        return clock[0]
    up = Uploader([pipe], now_fn=fake_now)

    for _ in range(3):
        clock[0] += 3600.0
        memory_transport.next_outcomes = [Outcome.retry_later("still bad")]
        up.pump()

    # After max_attempts retries, the deliverable should be dead-lettered.
    assert wm.deliverable_count("test") == 0
    assert wm.dead_letter_count() == 1


def test_partial_ack_advances_to_accepted_cursor(tmp_path, memory_transport):
    src = MemorySource(records=_records(5))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    rejected = [_records(5)[-1][0], _records(5)[-2][0]]  # last two
    memory_transport.next_outcomes = [
        Outcome.partial_ack(
            accepted_cursor=b"3",
            rejected=tuple(rejected),
            reason="2 spots rejected",
        )
    ]
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )
    Uploader([pipe]).pump()

    # Cursor advanced to the partial-ack accepted point, not all the way.
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b"3"


def test_permanent_failure_dead_letters_immediately(tmp_path, memory_transport):
    src = MemorySource(records=_records(1))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    memory_transport.next_outcomes = [
        Outcome.permanent_failure("auth rejected")
    ]
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )
    Uploader([pipe]).pump()

    assert wm.deliverable_count("test") == 0
    assert wm.dead_letter_count() == 1
    # Cursor did not advance.
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b""


def test_on_batch_outcome_callback_fires_per_first_attempt(tmp_path, memory_transport):
    """The optional callback gives the consumer (psk-recorder shim) a
    per-batch hook to count `RecordBatch.records` so its journal log
    is accurate regardless of source backend (file / SQLite / CH)."""
    src = MemorySource(records=_records(5))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )

    seen: list[tuple[str, int, str]] = []

    def cb(pipeline, batch, outcome):
        seen.append((pipeline.name, len(batch.records), outcome.kind))

    up = Uploader([pipe], on_batch_outcome=cb)
    up.pump()

    assert seen == [("test", 5, "acked")]


def test_on_batch_outcome_callback_fires_on_retry_later(tmp_path, memory_transport):
    """retry_later is a valid first-attempt outcome — the callback
    must fire so the shim can log "we tried but it's being retried"
    rather than miss the event entirely."""
    src = MemorySource(records=_records(2))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    memory_transport.next_outcomes = [Outcome.retry_later("server hiccup")]
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )

    seen = []
    Uploader(
        [pipe],
        on_batch_outcome=lambda p, b, o: seen.append(o.kind),
    ).pump()

    assert seen == ["retry_later"]


def test_on_batch_outcome_callback_exception_does_not_break_pump(tmp_path, memory_transport):
    """A buggy callback must not crash the pump loop — visibility hooks
    are best-effort, not control flow."""
    src = MemorySource(records=_records(3))
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    pipe = Pipeline(
        name="test", source=src, transport=memory_transport,
        watermark=wm, identity=_ident(),
    )

    def boom(*_a, **_kw):
        raise RuntimeError("callback bug")

    up = Uploader([pipe], on_batch_outcome=boom)
    # Should not raise — the bad callback gets swallowed and logged.
    assert up.pump() is True
    # And the underlying batch still acked + advanced the cursor.
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b"3"


def test_retry_policy_delay_for():
    rp = RetryPolicy(base=2.0, cap_sec=60.0, max_attempts=10)
    assert rp.delay_for(0) == 1.0
    assert rp.delay_for(1) == 2.0
    assert rp.delay_for(5) == 32.0
    assert rp.delay_for(6) == 60.0   # capped
    assert rp.delay_for(20) == 60.0  # still capped
    assert rp.delay_for(-1) == 0.0   # negative tolerated
