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
from dataclasses import dataclass, field, replace
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

import threading

from .watermark.base import Advance
from .watermark.sqlite import BACKWARD_WARN_INTERVAL_S

if TYPE_CHECKING:
    from .sources.base import Source
    from .transports.base import Transport
    from .watermark.base import WatermarkStore
    from .config import StationIdentity

logger = logging.getLogger(__name__)

GATE_FAULT_CAP = 64  # bound on the exception types a raising gate logs


# --------------------------------------------------------------------- Record


@dataclass(frozen=True)
class Record:
    """One unit of data flowing from a Source into a Transport.

    `table` is the logical source table name (e.g. ``"wspr.spots"``); it
    is the routing key transports use to decide whether they accept this
    record (see ``Transport.ACCEPTS``).

    `time` is the canonical observation time in UTC — the timestamp
    transports stamp onto outgoing records.

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
    n_bytes: int = 0  # transport payload size in bytes (e.g. tar bytes); 0 when N/A

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
    # Per-pump record budget across multiple batches.  Default ``None``
    # preserves the historical "one batch per pump-pass" cadence —
    # safe for sources with batch_limit-sized batches (typically
    # 30-1000 records).  Set explicitly when batches are deliberately
    # small (e.g. wsprnet diagnostic mode with one spot per POST):
    # the orchestrator then drains the source until ``max_records_per_pump``
    # records have shipped or the source is exhausted, instead of
    # stopping after one batch.
    max_records_per_pump: Optional[int] = None
    # The client package that owns this pipeline (manifest ``client``).
    # The daemon records it in ``pipeline_state``; "" means unnamed.
    client: str = ""
    # Manifest ``governed`` (spec sink-2b §3.2).  The daemon's switch report reads it; nothing else does.
    governed: bool = True
    # Where the entry came from: "legacy" or "pipelines.d/<file>" (§3.6).
    origin: str = "legacy"

    def source_id(self) -> str:
        return self.source.source_id()

    def dest_id(self, transport: Optional["Transport"] = None) -> str:
        t = transport or self.transport
        return t.name


# ------------------------------------------------------------------ Uploader


_NowFn = Callable[[], float]


def _pump_executor(pipe: "Pipeline"):
    from concurrent.futures import ThreadPoolExecutor
    return ThreadPoolExecutor(max_workers=1,
                              thread_name_prefix=f"hs-uploader-{pipe.name}")


def _close_objects(name: str, objs: Sequence[Any]) -> None:
    """Close each object that has ``close()``; log and go on past a failure."""
    for obj in objs:
        close = getattr(obj, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception:
            logger.exception("%s: closing %s failed", name,
                             type(obj).__name__)


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
    line, which works uniformly across SQLite / file sources
    (spool-dir delta counting only worked for the file source).
    Defaults to no-op; existing callers are unaffected.
    """

    def __init__(
        self,
        pipelines: Sequence[Pipeline],
        *,
        now_fn: _NowFn = time.time,
        on_batch_outcome: Optional[Callable[["Pipeline", "RecordBatch", "Outcome"], None]] = None,
        clock: Callable[[], float] = time.monotonic,
        inline_single: bool = True,
        gate: Optional[Callable[["Pipeline"], bool]] = None,
    ):
        self.pipelines: list[Pipeline] = list(pipelines)
        # ``gate(pipe)`` answers "may this pipeline send now?" (spec
        # sink-2b-part2 §3.2).  None sends always: the in-process shims of
        # psk-recorder, meteor-scatter and wspr-recorder build their own
        # Uploader with no gate and behave exactly as before.  Only the
        # daemon passes one (a SinkGate).  Never default this to the
        # system's switch files.
        self._gate = gate
        # Exception types a gate has raised, each logged once (bounded:
        # keyed by type, never by message, and capped at GATE_FAULT_CAP).
        self._gate_faults: set[str] = set()
        self._gate_faults_full = False
        self._gate_lock = threading.Lock()
        # ``inline_single`` pumps a lone pipeline on the caller's thread.
        # The daemon passes False: its set changes at a rebuild, and a
        # pipeline must keep one thread for its life (a SqliteSource's
        # connection is pinned to the thread that opened it).
        self._inline_single = inline_single
        self._now = now_fn
        # "acked, record held" windows, one per send-record key:
        # key -> [window start on `clock`, refusals since the last INFO].
        # Pipelines pump on their own threads, hence the lock.
        self._clock = clock
        self._held_windows: dict[tuple[str, str, str], list] = {}
        self._held_lock = threading.Lock()
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
        self._pump_executors = [_pump_executor(p) for p in self.pipelines]

    def close(self) -> None:
        for ex in getattr(self, "_pump_executors", []):
            ex.shutdown(wait=False)

    def replace_pipelines(self, new: Sequence[Pipeline]) -> None:
        """Swap the running set between pumps (spec sink-2b §3.4 step 5).

        Call it only between pumps: ``pump()`` joins every future before it
        returns, so no pipeline is mid-pump here.  A pipeline object in
        ``new`` that already runs keeps its executor, and so its thread and
        its source's open connection.  Each removed pipeline's source, then
        its transports, close on that pipeline's own executor thread (an
        object a kept pipeline still holds stays open), and its executor
        shuts down.  Each added pipeline gets a new executor.  Queued
        retries need nothing here: they live in the store, keyed by
        pipeline name.
        """
        new = list(new)
        keep = {id(p) for p in new}
        held = {id(obj) for p in new
                for obj in (p.source, p.transport, p.fallback_transport)
                if obj is not None}
        closed: set[int] = set()
        executors = {}
        # A lone pipeline pumped inline ran on the caller's thread; close it
        # there.  (Pass ``inline_single=False`` to any Uploader whose set
        # may grow: an inline pipeline cannot move onto an executor.)
        inline = self._inline_single and len(self.pipelines) <= 1
        for pipe, ex in zip(self.pipelines, self._pump_executors):
            if id(pipe) in keep:
                executors[id(pipe)] = ex
                continue
            objs = []
            for obj in (pipe.source, pipe.transport, pipe.fallback_transport):
                if obj is None or id(obj) in held or id(obj) in closed:
                    continue
                closed.add(id(obj))
                objs.append(obj)
            try:
                if inline:
                    _close_objects(pipe.name, objs)
                else:
                    ex.submit(_close_objects, pipe.name, objs).result()
            except Exception:
                logger.exception("%s: closing the removed pipeline failed",
                                 pipe.name)
            ex.shutdown(wait=True)
        self.pipelines = new
        self._pump_executors = [executors.get(id(p)) or _pump_executor(p)
                                for p in new]

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
        if self._inline_single and len(self.pipelines) <= 1:
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

    def _may_send(self, pipe: "Pipeline") -> bool:
        """Ask the gate; a gate that raises holds (fail closed).

        Raising out of here would abort ``pump()`` with other pipelines'
        futures unjoined (``replace_pipelines`` relies on every future
        having finished) and repeat on every pump.  So hold instead, and
        log at ERROR once per exception type, within GATE_FAULT_CAP types.
        """
        if self._gate is None:
            return True
        try:
            return bool(self._gate(pipe))
        except Exception as e:  # noqa: BLE001 — fail closed
            self._note_gate_fault(pipe, type(e).__name__)
            return False

    def _note_gate_fault(self, pipe: "Pipeline", kind: str) -> None:
        with self._gate_lock:
            if kind in self._gate_faults:
                return
            if len(self._gate_faults) >= GATE_FAULT_CAP:
                if self._gate_faults_full:
                    return
                self._gate_faults_full = True
                first = False
            else:
                self._gate_faults.add(kind)
                first = True
        if first:
            logger.error("%s: the sink gate raised %s; holding (fail closed). "
                         "Further %s faults stay silent until restart",
                         pipe.name, kind, kind)
        else:
            logger.error("sink gate faults reached %d exception types; new "
                         "types now hold without logging", GATE_FAULT_CAP)

    def _pump_one(self, pipe: "Pipeline") -> bool:
        """Drain one pipeline (deliverables first, then source).

        A held pipeline returns here without work (spec §3.3): it pops no
        retry, reads no source, ships nothing and writes no attempt row,
        so its send record and queued retries stand exactly as they were
        and it resumes where it stopped.  It logs nothing; the gate logs
        transitions.
        """
        if not self._may_send(pipe):
            return False
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
            # Ask before each pop, so a switch that changes mid-pump stops
            # the next replay and leaves its retry queued untouched.
            if not self._may_send(pipe):
                break
            deliverable = pipe.watermark.pop_due_deliverable(
                pipe.name, now=_iso(now)
            )
            if deliverable is None:
                break
            any_attempted = True
            outcome = pipe.transport.replay(deliverable.payload_blob, pipe.identity)
            self._handle_outcome(pipe, deliverable, outcome, now)
            if outcome.kind == "retry_later":
                # The destination is failing RIGHT NOW: the next due
                # deliverable will fail the same way, so draining the rest
                # this pump only turns a backlog into a connection storm
                # (hs-uploader#4: 1,176 retries at ~4 conn/s against gw2
                # after a link came back).  One probe per pump interval is
                # the rate cap; the backlog waits for the next pump.
                logger.info(
                    "%s: destination failing (%s) — deferring the remaining "
                    "%d queued deliverable(s) to the next pump",
                    pipe.name, outcome.reason,
                    pipe.watermark.deliverable_count(pipe.name),
                )
                break
        return any_attempted

    def _drain_source(self, pipe: Pipeline) -> bool:
        """Pull batches from the source and ship them, up to the
        pipeline's per-pump record budget.

        Historical behaviour (``max_records_per_pump is None``): ship
        ONE batch then return.  Keeps the pump cadence predictable for
        large-batch transports — one wsprdaemon tar per pump, one
        wsprnet POST of up to 999 spots per pump, etc.

        Drain-mode (``max_records_per_pump`` set): keep shipping
        batches until either the budget is exhausted or
        ``iter_batches`` runs out of work.  Designed for small-batch
        diagnostic modes (e.g. ``WsprNet(max_spots_per_upload=1)`` —
        one spot per POST) where the historical one-batch-per-pump
        cap would crater throughput to 1 record / pump-interval and
        the queue would grow without bound.

        The drain loop re-queries ``iter_batches`` between iterations
        because the watermark advances inside ``_handle_first_attempt``
        — the next call sees the next slice of pending work.
        """
        budget = pipe.max_records_per_pump
        shipped = 0
        any_shipped = False
        while budget is None or shipped < budget:
            # Ask before reading the next batch, so a hold arriving after a
            # batch stops the drain without reading one batch ahead.
            if not self._may_send(pipe):
                return any_shipped
            cursor = pipe.watermark.get_cursor(
                pipe.source_id(), pipe.dest_id(),
                pipe.transport.primary_table(),
            )
            progressed = False
            for batch in pipe.source.iter_batches(
                cursor=cursor,
                limit=min(
                    pipe.batch_limit,
                    pipe.transport.batch_policy().max_records,
                ),
            ):
                if not batch.records:
                    continue
                # Ask before each batch, so a drain-mode pipeline stops
                # between batches when its switch changes mid-pump.
                if not self._may_send(pipe):
                    return any_shipped
                outcome = pipe.transport.ship(batch, pipe.identity)
                now = self._now()
                self._handle_first_attempt(pipe, batch, outcome, now)
                shipped += len(batch.records)
                any_shipped = True
                progressed = True
                if not outcome.succeeded:
                    # A failed first attempt leaves the cursor where it was
                    # (retry_later queued a deliverable; permanent went to
                    # dead-letter without advancing), so re-querying the
                    # source can only re-yield the SAME batch.  Continuing
                    # the drain loop here is the hs-uploader#4 hot loop:
                    # 20000/17 = 1,176 duplicate deliverables of one wspr
                    # cycle in 47 s.  Stop; the next pump (or the queued
                    # deliverable's retry) picks it up.
                    return True
                if budget is None:
                    # Historical mode: one batch per pump and out.
                    return True
                if shipped >= budget:
                    return True
            if not progressed:
                # Source exhausted — no more work.
                return any_shipped
        return any_shipped

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
            self._advance(pipe, pipe.source_id(), pipe.dest_id(), table,
                          batch.cursor_after, ts)
            pipe.source.commit(batch.commit_token)
            pipe.watermark.record_attempt(
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
            self._advance(pipe, pipe.source_id(), pipe.dest_id(), table,
                          outcome.accepted_cursor or batch.cursor_after, ts)
            pipe.source.commit(batch.commit_token)
            pipe.watermark.record_attempt(
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
                ts=ts, source_id=pipe.source_id(), dest_id=pipe.dest_id(),
                table=table, outcome="permanent",
                records=len(batch), bytes_=None,
                error=outcome.reason or None,
            )
            return
        raise RuntimeError(f"Unhandled outcome kind: {outcome.kind!r}")

    def _advance(
        self, pipe: Pipeline, source_id: str, dest_id: str, table: str,
        cursor: bytes, last_ack: str,
    ) -> Advance:
        """Advance one send record after an ack, forward only (spec §4.6).

        A refused write means the destination took the batch while the
        stored record already lies further on; the record stays put.  That
        gets logged as "acked, record held", never as a plain ack, through
        ``_log_record_held``.
        """
        result = pipe.watermark.advance_cursor(
            source_id, dest_id, table, cursor=cursor, last_ack=last_ack,
            is_after=_is_after_for(pipe.source),
        )
        if result is Advance.REFUSED:
            self._log_record_held(pipe, (source_id, dest_id, table), cursor)
        return result

    def _log_record_held(
        self, pipe: Pipeline, key: tuple[str, str, str], cursor: bytes,
    ) -> None:
        """Log a refused advance without flooding the journal.

        The first refusal on a key logs at INFO and opens a window of
        ``BACKWARD_WARN_INTERVAL_S`` on ``self._clock``.  Inside the
        window each further one adds to the key's count and logs at
        DEBUG.  The first one after the window logs one INFO with the
        count since the last INFO (itself included), then opens a new
        window.  Every line says "acked, record held".  A logging
        failure never reaches the pump.
        """
        try:
            with self._held_lock:
                now = self._clock()
                window = self._held_windows.get(key)
                if window is None:
                    self._held_windows[key] = [now, 0]
                    logger.info(
                        "%s: acked, record held: send record (%s, %s, %s) "
                        "stays ahead of %r (repeats on this key log at DEBUG "
                        "for %d s, then one INFO counts them)",
                        pipe.name, *key, cursor, BACKWARD_WARN_INTERVAL_S,
                    )
                    return
                window[1] += 1
                if now - window[0] < BACKWARD_WARN_INTERVAL_S:
                    logger.debug(
                        "%s: acked, record held again: send record "
                        "(%s, %s, %s) stays ahead of %r (%d since the last "
                        "INFO)", pipe.name, *key, cursor, window[1],
                    )
                    return
                logger.info(
                    "%s: acked, record held: send record (%s, %s, %s) "
                    "refused %d %s since the last INFO, the latest new %r",
                    pipe.name, *key, window[1],
                    "time" if window[1] == 1 else "times", cursor,
                )
                self._held_windows[key] = [now, 0]
        except Exception:  # noqa: BLE001 — logging must not stop the pump
            pass

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
            # missing: without it, a source's cursor would never
            # advance for a batch that succeeded only on retry.
            if deliverable.cursor_after:
                self._advance(
                    pipe,
                    deliverable.source_id or pipe.source_id(),
                    deliverable.dest_id or pipe.dest_id(),
                    deliverable.table or pipe.transport.primary_table(),
                    deliverable.cursor_after, ts,
                )
            pipe.source.commit(deliverable.commit_token)
            pipe.watermark.record_attempt(
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
                    pipeline=pipe.name, client=pipe.client,
                    last_send=getattr(pipe.transport, "ships", True),
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
            # Change only the attempt count and the next attempt time; keep
            # every other field, the key, cursor_after and commit_token
            # among them.  Rebuilding the row field by field once dropped
            # those five (D15): the retry's eventual ack then advanced no
            # send record, cleaned nothing up, and the rows went out again.
            pipe.watermark.requeue_deliverable(
                replace(
                    deliverable,
                    attempts=attempts,
                    next_attempt_at=_iso(now + pipe.retry.delay_for(attempts)),
                )
            )
            pipe.watermark.record_attempt(
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
                pipeline=pipe.name, client=pipe.client,
                last_send=getattr(pipe.transport, "ships", True),
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
            pipeline=pipe.name, client=pipe.client,
            last_send=getattr(pipe.transport, "ships", True),
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


def _always_after(new: bytes, stored: bytes) -> bool:
    return True


def _is_after_for(source: "Source") -> Callable[[bytes, bytes], bool]:
    """The source's own cursor order, for ``WatermarkStore.advance_cursor``.

    A source written before v3.70 (or a test double) may lack
    ``cursor_is_after``.  The concrete sources do not subclass the
    ``Source`` protocol, so they inherit no default; such a source gets
    "always after", which never refuses a write.
    """
    fn = getattr(source, "cursor_is_after", None)
    return fn if callable(fn) else _always_after
