"""Core types for hs-uploader.

The shape of the library is `Source -> Record -> Transport`, with a
``WatermarkStore`` persisting per-(source, destination, table) cursors and
retry deliverables.  Everything in this module is protocol-agnostic; the
concrete sources, transports, and watermark backends live in their
respective subpackages.

The public surface here is small on purpose: ``Record``, ``RecordBatch``,
``Outcome``, ``BatchPolicy``, ``RetryPolicy``, ``Pipeline``, ``Uploader``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
)

if TYPE_CHECKING:
    from .sources.base import Source
    from .transports.base import Transport
    from .watermark.base import WatermarkStore
    from .config import StationIdentity

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------- Record


@dataclass(frozen=True)
class Record:
    """One unit of data flowing from a Source into a Transport.

    `table` is the logical source table name (e.g. ``"wspr.spots"``); it
    is the routing key transports use to decide whether they accept this
    record (see ``Transport.ACCEPTS``).

    `time` is the canonical observation time in UTC.  It is what the CH
    source orders by and what the watermark cursor is keyed on.

    `columns` is a row-shaped payload (str → JSON-compatible value) for
    spot transports.  ``payload_path`` is set instead for dataset-shaped
    transports (PSWS Digital RF) where the actual bytes live on disk.
    Exactly one of the two is meaningful; transports declare which they
    expect.
    """

    table: str
    time: datetime
    columns: Mapping[str, Any] = field(default_factory=dict)
    dedup_key: Optional[bytes] = None
    payload_path: Optional[Path] = None


@dataclass(frozen=True)
class RecordBatch:
    """A batch of records plus the cursor advance proposed if the
    transport ACKs successfully, plus an optional ``commit_token`` the
    source uses to perform any cleanup (e.g. delete acked files) once
    the batch has been delivered upstream.

    Both ``cursor_after`` and ``commit_token`` are opaque to everything
    but the source that emitted them.  The orchestrator persists them
    alongside the deliverable so a retry-then-replay path arrives at
    the same final ack semantics as a first-attempt ack.
    """

    records: Sequence[Record]
    cursor_after: bytes  # opaque to everything but the source
    commit_token: bytes = b""  # source-specific cleanup payload

    def __len__(self) -> int:
        return len(self.records)


# -------------------------------------------------------------------- Outcome


@dataclass(frozen=True)
class Outcome:
    """Transport-level result of one ``ship()`` call.

    Use the ``ACKED`` / ``RETRY_LATER`` / ``PARTIAL_ACK`` / ``PERMANENT_FAILURE``
    factory classmethods rather than instantiating directly — the kind
    field is normalised that way and the orchestrator's match logic
    stays simple.
    """

    kind: str  # one of: "acked", "retry_later", "partial_ack", "permanent"
    reason: str = ""
    accepted_cursor: Optional[bytes] = None
    rejected: Sequence[Record] = ()

    @classmethod
    def acked(cls) -> "Outcome":
        return cls(kind="acked")

    @classmethod
    def retry_later(cls, reason: str) -> "Outcome":
        return cls(kind="retry_later", reason=reason)

    @classmethod
    def partial_ack(
        cls, accepted_cursor: bytes, rejected: Sequence[Record], reason: str = ""
    ) -> "Outcome":
        return cls(
            kind="partial_ack",
            reason=reason,
            accepted_cursor=accepted_cursor,
            rejected=tuple(rejected),
        )

    @classmethod
    def permanent_failure(cls, reason: str) -> "Outcome":
        return cls(kind="permanent", reason=reason)

    @property
    def succeeded(self) -> bool:
        return self.kind in ("acked", "partial_ack")


# ------------------------------------------------------------------- Policies


@dataclass(frozen=True)
class BatchPolicy:
    """Transport-level constraints on how many records to ship in one go.

    Transports advertise these so the orchestrator can chunk a Source
    stream without each transport re-implementing the chunking logic.
    """

    max_records: int = 1000
    max_bytes: Optional[int] = None
    min_records_for_burst: int = 1
    stability_window_sec: float = 0.0


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with a cap, plus an attempt limit.

    Used by the ``Uploader`` when a Transport returns ``RETRY_LATER`` —
    the deliverable is enqueued and re-tried at ``base ** attempts``
    seconds (capped at ``cap_sec``) until ``max_attempts`` is reached,
    after which the deliverable goes to dead-letter.
    """

    base: float = 2.0
    cap_sec: float = 300.0
    max_attempts: int = 12

    @classmethod
    def exponential(
        cls, base: float = 2.0, cap_sec: float = 300.0, max_attempts: int = 12
    ) -> "RetryPolicy":
        return cls(base=base, cap_sec=cap_sec, max_attempts=max_attempts)

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait before attempt N (0-indexed)."""
        if attempt < 0:
            return 0.0
        return min(self.base**attempt, self.cap_sec)


# ------------------------------------------------------------------- Pipeline


@dataclass
class Pipeline:
    """One source bound to one transport with a watermark slot.

    A pipeline is the atomic unit of uploading: one cursor, one retry
    state, one transport endpoint.  Multiple pipelines per Uploader are
    fine and cheap; the same source can feed two transports
    independently (e.g. ``wspr.spots`` to wsprdaemon AND wsprnet).
    """

    name: str
    source: "Source"
    transport: "Transport"
    watermark: "WatermarkStore"
    identity: "StationIdentity"
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    fallback_transport: Optional["Transport"] = None
    batch_limit: int = 1000

    def source_id(self) -> str:
        return self.source.source_id()

    def dest_id(self, transport: Optional["Transport"] = None) -> str:
        t = transport or self.transport
        return t.name


# ------------------------------------------------------------------ Uploader


_NowFn = Callable[[], float]


class Uploader:
    """Orchestrates one or more pipelines.

    Two entry points:

    * ``pump()`` — do one pass over every pipeline (drain ready
      deliverables, then drain the source up to the batch limit).
      Returns ``True`` if any work happened, ``False`` if everything was
      idle.  Designed for daemon use: ``while running: pump(); sleep(N)``.

    * ``pump_until_idle(max_passes)`` — keep calling ``pump()`` until it
      returns ``False`` (or the pass count is hit).  Designed for cron
      use: a single shell invocation drains the queue and exits.

    Optional ``on_batch_outcome`` is a callback invoked after each
    first-attempt ship (``acked`` / ``partial_ack`` / ``retry_later`` /
    ``permanent``) with ``(pipeline, batch, outcome)``.  Useful for
    operator visibility — e.g., the psk-recorder shim uses it to count
    ``RecordBatch.records`` per-mode (ft8/ft4) for its journal log
    line, which works uniformly across CH / SQLite / file sources
    (spool-dir delta counting only worked for the file source).
    Defaults to no-op; existing callers are unaffected.
    """

    def __init__(
        self,
        pipelines: Sequence[Pipeline],
        *,
        now_fn: _NowFn = time.time,
        on_batch_outcome: Optional[Callable[["Pipeline", "RecordBatch", "Outcome"], None]] = None,
    ):
        self.pipelines: list[Pipeline] = list(pipelines)
        self._now = now_fn
        self._on_batch_outcome = on_batch_outcome or (lambda *_a, **_kw: None)
        # Per-pipeline dedicated executor (max_workers=1) so each
        # pipeline's pump always runs on the same worker thread.  This
        # is required because SqliteSource holds a sqlite3.Connection
        # that's pinned to its creator thread — bouncing across pool
        # workers raises ProgrammingError.  A persistent thread per
        # pipeline also lets us run the SFTP-to-wsprdaemon and
        # HTTP-to-wsprnet uploads in parallel (the user-stated goal),
        # since each pump iteration submits one task per pipeline to
        # its own executor and joins them at the end.
        from concurrent.futures import ThreadPoolExecutor
        self._pump_executors: list[ThreadPoolExecutor] = [
            ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix=f"hs-uploader-{p.name}",
            )
            for p in self.pipelines
        ]

    def close(self) -> None:
        for ex in getattr(self, "_pump_executors", []):
            ex.shutdown(wait=False)

    # -- public --

    def pump(self) -> bool:
        # Parallel pipeline drain.  Each pipeline owns its own source,
        # transport, and watermark store — no shared mutable state
        # between them — so they can run concurrently.  This matters
        # most when one pipeline is slow (e.g., wsprdaemon-tar's SFTP
        # round-trip vs. wsprnet's HTTP POST): without parallelism the
        # 60 s pump interval can be eaten by one pipeline's network
        # latency, starving the others.
        #
        # Per-pipeline state isolation:
        #   - SqliteWatermarkStore opens a fresh connection per call
        #     (thread-safe).
        #   - SqliteSource keeps a sqlite3.Connection pinned to its
        #     creator thread; we honor that by giving each pipeline a
        #     dedicated single-worker executor (see __init__).
        #   - Transports hold per-instance buffers but are not shared.
        # The on_batch_outcome callback IS shared — caller must make it
        # thread-safe if it mutates state (see hs_uploader_shim's
        # callback which uses simple += under the GIL).
        if len(self.pipelines) <= 1:
            # Skip the executor hop for the one-pipeline case.
            did_work = False
            for pipe in self.pipelines:
                did_work |= self._pump_one(pipe)
            return did_work

        futures = [
            ex.submit(self._pump_one, pipe)
            for ex, pipe in zip(self._pump_executors, self.pipelines)
        ]
        # Materialise every future's result BEFORE folding with any().
        # `any(f.result() for f in futures)` short-circuits the moment the
        # first future returns True, leaving the slower pipelines still
        # running in their executors while pump() returns.  The shim's
        # ``shipped wsprdaemon=N wsprnet=M`` log line fires right after
        # pump() returns, so any pipeline that hadn't yet completed when
        # the short-circuit triggered shows zero in that pump's tally
        # even though its records were ack'd a few seconds later.
        # Observed B4-100 2026-05-15: wsprnet=0 logged at 13:22:34.229
        # while wsprnet's on_batch_outcome fired at 13:22:37.743 with
        # 38/38 added — the actual ship was healthy, the log lied.
        results = [f.result() for f in futures]
        return any(results)

    def _pump_one(self, pipe: "Pipeline") -> bool:
        """Drain one pipeline (deliverables first, then source)."""
        did_work = self._drain_deliverables(pipe)
        # Don't drain the source while deliverables are queued —
        # the cursor hasn't advanced, so a fresh source pull would
        # re-ship the same records the deliverable already owns.
        # Wait for the in-flight batch to ack (or dead-letter)
        # before pulling the next slice.
        if pipe.watermark.deliverable_count(pipe.name) > 0:
            return did_work
        return did_work | self._drain_source(pipe)

    def pump_until_idle(self, max_passes: int = 100) -> int:
        passes = 0
        while passes < max_passes:
            if not self.pump():
                break
            passes += 1
        return passes

    # -- internals --

    def _drain_deliverables(self, pipe: Pipeline) -> bool:
        """Retry any deliverables whose backoff window has elapsed.

        Each pipeline's watermark store may have queued deliverables
        from previous failed attempts.  Walk them, retry those whose
        ``next_attempt_at`` is past, and either ack them or re-schedule.
        """
        now = self._now()
        any_attempted = False
        while True:
            deliverable = pipe.watermark.pop_due_deliverable(
                pipe.name, now=_iso(now)
            )
            if deliverable is None:
                break
            any_attempted = True
            outcome = pipe.transport.replay(deliverable.payload_blob, pipe.identity)
            self._handle_outcome(pipe, deliverable, outcome, now)
        return any_attempted

    def _drain_source(self, pipe: Pipeline) -> bool:
        """Pull a fresh batch from the source and ship it."""
        cursor = pipe.watermark.get_cursor(
            pipe.source_id(), pipe.dest_id(), pipe.transport.primary_table()
        )
        for batch in pipe.source.iter_batches(
            cursor=cursor,
            limit=min(pipe.batch_limit, pipe.transport.batch_policy().max_records),
        ):
            if not batch.records:
                continue
            outcome = pipe.transport.ship(batch, pipe.identity)
            now = self._now()
            self._handle_first_attempt(pipe, batch, outcome, now)
            # Stop after one batch per pump-pass to keep cadence bounded.
            return True
        return False

    def _handle_first_attempt(
        self,
        pipe: Pipeline,
        batch: RecordBatch,
        outcome: Outcome,
        now: float,
    ) -> None:
        # Fire the operator-supplied callback up front so it observes
        # every outcome kind (acked, partial_ack, retry_later, permanent)
        # uniformly.  Exceptions in user code are swallowed to keep the
        # pump loop alive — the callback is for visibility, not control.
        try:
            self._on_batch_outcome(pipe, batch, outcome)
        except Exception:  # noqa: BLE001
            logger.exception("on_batch_outcome callback raised; ignored")
        ts = _iso(now)
        table = pipe.transport.primary_table()
        if outcome.kind == "acked":
            pipe.watermark.advance_cursor(
                pipe.source_id(), pipe.dest_id(), table,
                cursor=batch.cursor_after, last_ack=ts,
            )
            pipe.source.commit(batch.commit_token)
            pipe.watermark.record_attempt(
                ts=ts, source_id=pipe.source_id(), dest_id=pipe.dest_id(),
                table=table, outcome="acked",
                records=len(batch), bytes_=None, error=None,
            )
            return
        if outcome.kind == "partial_ack":
            # Advance only to the partially-accepted cursor; commit the
            # whole token (sources that need finer-grained "which rejected
            # records did NOT get acked" can encode that into commit_token,
            # but the v1 file source either acks all or none).
            pipe.watermark.advance_cursor(
                pipe.source_id(), pipe.dest_id(), table,
                cursor=outcome.accepted_cursor or batch.cursor_after,
                last_ack=ts,
            )
            pipe.source.commit(batch.commit_token)
            pipe.watermark.record_attempt(
                ts=ts, source_id=pipe.source_id(), dest_id=pipe.dest_id(),
                table=table, outcome="partial_ack",
                records=len(batch) - len(outcome.rejected),
                bytes_=None, error=outcome.reason or None,
            )
            return
        if outcome.kind == "retry_later":
            payload = pipe.transport.serialize_for_retry(batch, pipe.identity)
            pipe.watermark.enqueue_deliverable(
                pipeline=pipe.name,
                payload_blob=payload,
                enqueued_at=ts,
                next_attempt_at=_iso(now + pipe.retry.delay_for(0)),
                source_id=pipe.source_id(),
                dest_id=pipe.dest_id(),
                table=table,
                cursor_after=batch.cursor_after,
                commit_token=batch.commit_token,
            )
            pipe.watermark.record_attempt(
                ts=ts, source_id=pipe.source_id(), dest_id=pipe.dest_id(),
                table=table, outcome="retry_later",
                records=len(batch), bytes_=len(payload),
                error=outcome.reason or None,
            )
            return
        if outcome.kind == "permanent":
            payload = pipe.transport.serialize_for_retry(batch, pipe.identity)
            pipe.watermark.send_to_dead_letter(
                ts=ts, pipeline=pipe.name,
                payload_blob=payload,
                final_error=outcome.reason or "permanent_failure",
            )
            pipe.watermark.record_attempt(
                ts=ts, source_id=pipe.source_id(), dest_id=pipe.dest_id(),
                table=table, outcome="permanent",
                records=len(batch), bytes_=None,
                error=outcome.reason or None,
            )
            return
        raise RuntimeError(f"Unhandled outcome kind: {outcome.kind!r}")

    def _handle_outcome(
        self,
        pipe: Pipeline,
        deliverable: Any,
        outcome: Outcome,
        now: float,
    ) -> None:
        ts = _iso(now)
        if outcome.kind == "acked":
            # Replay-ack advances the cursor and triggers source cleanup
            # using the (cursor_after, commit_token) tuple captured at
            # enqueue time.  This is the correctness path that v1 was
            # missing: without it, a CH source's cursor would never
            # advance for a batch that succeeded only on retry.
            if deliverable.cursor_after:
                pipe.watermark.advance_cursor(
                    deliverable.source_id or pipe.source_id(),
                    deliverable.dest_id or pipe.dest_id(),
                    deliverable.table or pipe.transport.primary_table(),
                    cursor=deliverable.cursor_after,
                    last_ack=ts,
                )
            pipe.source.commit(deliverable.commit_token)
            pipe.watermark.record_attempt(
                ts=ts,
                source_id=pipe.source_id(),
                dest_id=pipe.dest_id(),
                table=pipe.transport.primary_table(),
                outcome="acked",
                records=None,
                bytes_=len(deliverable.payload_blob),
                error=None,
            )
            return
        if outcome.kind == "retry_later":
            attempts = deliverable.attempts + 1
            if attempts >= pipe.retry.max_attempts:
                pipe.watermark.send_to_dead_letter(
                    ts=ts,
                    pipeline=pipe.name,
                    payload_blob=deliverable.payload_blob,
                    final_error=outcome.reason or "max_attempts",
                )
                pipe.watermark.record_attempt(
                    ts=ts,
                    source_id=pipe.source_id(),
                    dest_id=pipe.dest_id(),
                    table=pipe.transport.primary_table(),
                    outcome="dead_letter",
                    records=None,
                    bytes_=len(deliverable.payload_blob),
                    error=outcome.reason or None,
                )
                return
            from .watermark.base import Deliverable
            pipe.watermark.requeue_deliverable(
                Deliverable(
                    id=deliverable.id,
                    pipeline=deliverable.pipeline,
                    payload_blob=deliverable.payload_blob,
                    enqueued_at=deliverable.enqueued_at,
                    attempts=attempts,
                    next_attempt_at=_iso(now + pipe.retry.delay_for(attempts)),
                )
            )
            pipe.watermark.record_attempt(
                ts=ts,
                source_id=pipe.source_id(),
                dest_id=pipe.dest_id(),
                table=pipe.transport.primary_table(),
                outcome="retry_later",
                records=None,
                bytes_=len(deliverable.payload_blob),
                error=outcome.reason or None,
            )
            return
        if outcome.kind == "permanent":
            pipe.watermark.send_to_dead_letter(
                ts=ts,
                pipeline=pipe.name,
                payload_blob=deliverable.payload_blob,
                final_error=outcome.reason or "permanent_failure",
            )
            pipe.watermark.record_attempt(
                ts=ts,
                source_id=pipe.source_id(),
                dest_id=pipe.dest_id(),
                table=pipe.transport.primary_table(),
                outcome="permanent",
                records=None,
                bytes_=len(deliverable.payload_blob),
                error=outcome.reason or None,
            )
            return
        # partial_ack from a deliverable replay is ill-defined — treat as
        # acked, surface a warning.  In practice transports won't emit
        # partial_ack from ``replay`` (they emit it from ``ship`` on first
        # attempt and then we only enqueue the rejected suffix).
        logger.warning(
            "deliverable replay produced unexpected outcome %s for pipeline %s; "
            "acking",
            outcome.kind, pipe.name,
        )
        pipe.watermark.record_attempt(
            ts=ts,
            source_id=pipe.source_id(),
            dest_id=pipe.dest_id(),
            table=pipe.transport.primary_table(),
            outcome="acked",
            records=None,
            bytes_=len(deliverable.payload_blob),
            error=None,
        )


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(
        timespec="seconds"
    )
