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

import enum
from dataclasses import dataclass
from typing import Callable, Optional, Protocol, runtime_checkable


class Advance(enum.Enum):
    """What ``WatermarkStore.advance_cursor`` did with one write (spec §4.6).

    ``WRITTEN``: the new cursor lies after the stored one; both fields written.
    ``REFRESHED``: the two tie in the source's order; ``last_ack`` alone moved.
    ``REFUSED``: the new cursor lies before the stored one; nothing changed.
    ``WRITTEN_UNCHECKED``: the comparison raised; written without the check.
    """

    WRITTEN = "written"
    REFRESHED = "refreshed"
    REFUSED = "refused"
    WRITTEN_UNCHECKED = "written_unchecked"


@dataclass(frozen=True)
class Deliverable:
    """One row out of the watermark deliverables table.

    ``cursor_after`` and ``commit_token`` carry the source-specific
    ack semantics across the deliverable lifecycle so a replay-ack
    can advance the cursor and trigger source cleanup just like a
    first-attempt-ack would have.

    ``source_id``, ``dest_id``, and ``table`` are stored alongside the
    deliverable so the orchestrator can advance the right watermark
    on replay-ack without needing to keep the original Pipeline
    in-memory.
    """

    id: int
    pipeline: str
    payload_blob: bytes
    enqueued_at: str  # ISO8601
    attempts: int
    next_attempt_at: str
    source_id: str = ""
    dest_id: str = ""
    table: str = ""
    cursor_after: bytes = b""
    commit_token: bytes = b""


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
        is_after: Callable[[bytes, bytes], bool],
    ) -> Advance:
        """Move the send record forward only (spec §4.6), in one
        transaction that reads the stored record, asks
        ``is_after(cursor, stored)`` and writes.  After: write
        (``WRITTEN``).  Equal in the source's order: refresh
        ``last_ack`` only (``REFRESHED``).  Before: change nothing and
        log a WARNING with both values (``REFUSED``; one WARNING per key
        per hour, a count at DEBUG between).  The comparison raises:
        write and log the error (``WRITTEN_UNCHECKED``).  No public
        method writes a send record unchecked; ``reset_cursor`` deletes
        one (``SqliteWatermarkStore.reset_cursor``, the deliberate rewind).
        """
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
        pipeline: str = "",
        client: str = "",
        last_send: bool = True,
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
        source_id: str = "",
        dest_id: str = "",
        table: str = "",
        cursor_after: bytes = b"",
        commit_token: bytes = b"",
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
        passes a copy of the popped ``Deliverable`` that changes only
        ``attempts`` and ``next_attempt_at`` (``dataclasses.replace``
        does this).  The store writes every field back, the key,
        ``cursor_after`` and ``commit_token`` included, so the retry's
        eventual ack still advances its send record and commits to its
        source (D15).
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
