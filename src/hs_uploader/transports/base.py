"""Transport protocol — the abstraction over "where records go".

A Transport accepts a ``RecordBatch`` and reports an ``Outcome``.  It
declares which tables (and which schema versions of those tables) it
accepts so the orchestrator can prevent shipping records to a transport
that doesn't speak their format.

Two ship paths:

* ``ship(batch, identity)`` — first-attempt shipping.  Builds the
  payload and pushes it.  This is what Pipelines call on fresh batches
  pulled from the source.

* ``replay(payload_blob, identity)`` — retry of a previously-failed
  batch from the watermark deliverables queue.  The payload was
  serialized at first-attempt time via ``serialize_for_retry`` so
  payloads whose rebuild is non-deterministic (Digital RF datasets
  whose bytes are verified server-side) can be retried byte-identically.
"""

from __future__ import annotations

from typing import Mapping, Protocol, runtime_checkable

from ..core import BatchPolicy, Outcome, RecordBatch


@runtime_checkable
class Transport(Protocol):
    name: str
    """Stable id for this transport instance, used as a watermark key.
    Different instances of the same transport class targeting different
    destinations MUST have different ``name``s
    (e.g. ``"wsprdaemon-tar-sftp:gw1"``).
    """

    ACCEPTS: Mapping[str, list[int]]
    """Table -> accepted schema versions.  E.g. ``{"wspr.spots": [3]}``.
    Strict matching: a producer at v4 talking to a transport that
    declares v3 is refused; the source surfaces a stale-schema issue.
    """

    def primary_table(self) -> str:
        """The single table this transport's pipeline reads from.

        v1 keeps it simple: one transport = one table for cursor
        purposes.  Multi-table aggregation (e.g. wsprdaemon's tar
        bundling spots + noise) is handled by the source side composing
        a single iter_batches stream.
        """
        ...

    def batch_policy(self) -> BatchPolicy:
        ...

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        ...

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        """Render the payload for a retry deliverable.

        Default implementations may return the batch's pickle bytes;
        transports whose payload is non-trivial to rebuild (Digital RF)
        return the actual bytes-on-the-wire so retries are byte-stable.
        """
        ...

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        """Retry a previously-failed deliverable.

        The blob is exactly what ``serialize_for_retry`` produced.
        """
        ...
