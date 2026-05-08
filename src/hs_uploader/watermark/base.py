"""WatermarkStore protocol — persisted state for hs-uploader.

Owns three concerns:

1. **Cursors.**  Per-(source, destination, table) opaque cursor
   bytes.  Advances atomically on successful ack.

2. **Attempts.**  Ring-buffered audit log of shipping attempts.  Used
   for ``hs-uploader status`` and for forensic debugging.  Bounded
   (last 10k entries by default).

3. **Deliverables.**  In-flight batches that returned ``RETRY_LATER``.
   Their formatted payloads are persisted so retries are byte-stable
   across restarts.

Plus a separate **dead-letter** table for deliverables that exhausted
their retry budget — the cursor is NOT advanced; an operator must
intervene.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class Deliverable:
    """One row out of the watermark deliverables table."""

    id: int
    pipeline: str
    payload_blob: bytes
    enqueued_at: str  # ISO8601
    attempts: int
    next_attempt_at: str


@runtime_checkable
class WatermarkStore(Protocol):
    """Persistent state for hs-uploader.

    All methods take string ids (source_id, dest_id, table) so
    implementations don't need to know about Source/Transport types.
    """

    # --- cursors ---

    def get_cursor(self, source_id: str, dest_id: str, table: str) -> bytes:
        """Returns the last-acked cursor for this triple, or empty
        bytes if nothing has been shipped yet (the source's
        responsibility to interpret an empty cursor as "from the
        beginning").
        """
        ...

    def advance_cursor(
        self,
        source_id: str,
        dest_id: str,
        table: str,
        *,
        cursor: bytes,
        last_ack: str,
    ) -> None:
        ...

    # --- attempts (audit log) ---

    def record_attempt(
        self,
        *,
        ts: str,
        source_id: str,
        dest_id: str,
        table: str,
        outcome: str,
        records: Optional[int],
        bytes_: Optional[int],
        error: Optional[str],
    ) -> None:
        ...

    # --- deliverables (retry queue) ---

    def enqueue_deliverable(
        self,
        *,
        pipeline: str,
        payload_blob: bytes,
        enqueued_at: str,
        next_attempt_at: str,
    ) -> int:
        """Returns the deliverable id."""
        ...

    def pop_due_deliverable(
        self, pipeline: str, *, now: str
    ) -> Optional[Deliverable]:
        """Atomically claim the oldest deliverable for ``pipeline``
        whose ``next_attempt_at <= now``.  Returns None if nothing is
        due.
        """
        ...

    def deliverable_count(self, pipeline: Optional[str] = None) -> int:
        """Number of currently-queued deliverables, optionally filtered
        by pipeline name.  The orchestrator uses this to gate fresh
        source-draining on a pipeline that already has in-flight work.
        """
        ...

    def requeue_deliverable(self, deliverable: Deliverable) -> None:
        """Re-insert a previously-popped deliverable.  The caller
        constructs a fresh ``Deliverable`` carrying the updated
        ``attempts`` and ``next_attempt_at`` while preserving the
        original ``id``, ``pipeline``, ``payload_blob``, and
        ``enqueued_at``.
        """
        ...

    # --- dead letter ---

    def send_to_dead_letter(
        self,
        *,
        ts: str,
        pipeline: str,
        payload_blob: bytes,
        final_error: str,
    ) -> None:
        ...
