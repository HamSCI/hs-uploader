"""DiscardTransport — acknowledge every batch and ship nothing.

For a station in sigmond's ``discard`` uploads mode: bench provisioning of a
machine bound for another site, or diagnostics.  The recorders run, the
uploader runs, and nothing leaves the host.

It wraps the pipeline's REAL transport rather than replacing it, and borrows
its ``name``, ``ACCEPTS``, table and batch policy.  The watermark store keys
cursors on ``transport.name``, so every ack advances the cursor the real
destination will read when discard ends.  A delete-on-ack source empties as it
would after a real upload.  Leaving discard therefore ships nothing recorded
on the bench: the cursor already stands at the present.  A stand-in with its
own name would leave the real cursor where it was, and the first pump after
discard would ship everything still on disk.

The inner transport never runs: neither ``ship`` nor ``replay`` reaches it, so
no network connection, key or credential comes into play.
"""

from __future__ import annotations

import logging
from typing import Mapping

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


class DiscardTransport:
    def __init__(self, inner):
        self.inner = inner
        self.name: str = inner.name
        self.ACCEPTS: Mapping[str, list[int]] = inner.ACCEPTS
        self.discarded_records = 0

    def primary_table(self) -> str:
        return self.inner.primary_table()

    def batch_policy(self) -> BatchPolicy:
        return self.inner.batch_policy()

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        self.discarded_records += len(batch.records)
        logger.debug("%s: discarded %d record(s) (discard mode)",
                     self.name, len(batch.records))
        return Outcome.acked()

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        return b""

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        # A deliverable queued before discard began: drop it too.
        return Outcome.acked()


def wrap_if_discard(entry: Mapping, transport):
    """Wrap ``transport`` when the manifest entry says ``discard = true``."""
    if not entry.get("discard"):
        return transport
    logger.warning("pipeline %s: DISCARD mode — acknowledges without shipping "
                   "to %s", entry.get("name", "?"), transport.name)
    return DiscardTransport(transport)
