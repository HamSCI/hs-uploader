"""Source protocol — the abstraction over "where records come from".

A Source emits ``RecordBatch``es starting from an opaque ``cursor``
(bytes).  The cursor is meaningful only to the source that produced it;
the watermark store treats it as opaque blob.  This keeps the
orchestrator out of the cursor-format business and lets each source
pick its own (filename+offset, time+tiebreak hash, etc.).
"""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable

from ..core import RecordBatch


@runtime_checkable
class Source(Protocol):
    """Yields batches of records starting strictly after ``cursor``.

    Implementations MUST be deterministic for a given cursor: calling
    ``iter_batches(cursor=X, limit=N)`` twice from the same on-disk
    state should yield the same first batch.  This is what makes the
    "what's owed = a query, not a queue" model safe across restarts.

    ``health`` is a free-form string the orchestrator can surface for
    operator visibility.  ``"ok"`` is happy path; everything else
    (``"unreachable"``, ``"stale-schema"``, ``"degraded"``) gets
    promoted to a `validate --json` issue by the consuming client.
    """

    def source_id(self) -> str:
        """Stable identity for this source (used as a watermark key)."""
        ...

    def health(self) -> str:
        ...

    def iter_batches(self, cursor: bytes, limit: int) -> Iterator[RecordBatch]:
        ...
