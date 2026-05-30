"""Cycle-aligned multi-table source for wsprdaemon.org uploads.

Reads from sink.db's ``pending_uploads`` table, treating
``(target_db='wspr', target_table IN ('spots','noise'))`` as a single
logical stream grouped by the WSPR-cycle timestamp embedded in
``payload_json.time``.  Yields **one RecordBatch per cycle**, oldest
first, so the downstream ``WsprdaemonTarSftp`` transport can build a
single tar containing parallel ``wsprdaemon/spots/...`` and
``wsprdaemon/noise/...`` subtrees — matching the v3 server's expected
tar layout.

Why this instead of two SqliteSources (one per table)?  Running spots
and noise in two separate pipelines uploads two tars per cycle, racing
on the SFTP filename and confusing the v3 server's per-tar processing.
Bundling them into one tar per cycle is the design intent.

Shippability gate
-----------------

A cycle is only emitted when it is older than the current 2-minute
WSPR boundary — i.e., not the cycle currently being decoded and
written into the db.  This sidesteps the half-written-cycle race even
when the uploader is woken eagerly by SIGUSR1 from the producer.

Missing-noise tolerance
-----------------------

WSPR noise rows land in sink.db within milliseconds of decoder start;
spots arrive 0-20 s later.  By the time SIGUSR1 fires from spot_sink
the noise is *always* present.  But if a cycle has spots and no noise
(or vice versa) we still ship it — with a WARNING — rather than block
forever waiting for data that may never come.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

from ..core import Record, RecordBatch

logger = logging.getLogger(__name__)


# WSPR cycle = 2 min, aligned to even minute (00, 02, 04, ...).
WSPR_CYCLE_SECONDS = 120

HEALTH_OK = "ok"
HEALTH_NOOP = "noop"

# The "table" label we expose to watermark store callers + transports.
# Treated as the primary_table for routing; this source covers both
# wspr.spots and wspr.noise so neither would be quite right.
_LOGICAL_TABLE = "wspr.cycle"


class WsprCycleSource:
    """Multi-table cycle-aligned source backed by sink.db.

    The watermark cursor is the ISO-8601 timestamp of the last cycle
    *shipped*.  Cycle ordering is ASCII-comparable (strict ISO Z
    suffix) so the cursor doubles as a sort key.
    """

    # Transports use this to decide whether to accept records from
    # this source.  Both subtables travel together.
    ACCEPTS = {"wspr.spots": [1, 2], "wspr.noise": [1, 2]}

    def __init__(
        self,
        *,
        db_path: Path | str = "/var/lib/sigmond/sink.db",
        ship_buffer_sec: int = 0,
        start_at: str = "now",
        expected_reporters: Optional[set] = None,
        backstop_sec: float = 90.0,
    ):
        """``db_path`` is the sink.db path (must be readable by the
        uploader's runtime user).  ``ship_buffer_sec`` is a defensive
        subtraction from the cycle floor — for clock skew between
        producer and uploader.  Default 0 because we already rely on
        the producer's SIGUSR1 to wake us only after a cycle is
        committed.

        ``expected_reporters`` enables event-driven cross-receiver
        gating for the multi-RX merge: a cycle is only shipped once
        every listed reporter (rx_call, e.g. ``AC0G/B5``) has written
        its per-cycle ``wspr.noise`` completion rows, OR the cycle is
        older than ``backstop_sec`` past its end (so a dead receiver
        can't stall uploads).  Empty/None → no gating (single-receiver
        behaviour: ship every past cycle).  See wspr_completion.py.

        ``start_at`` controls behaviour the first time the source is
        polled with an empty watermark cursor:

        * ``"now"`` (default): treat the cycle floor at first-pump as
          the anchor and only ship cycles emitted *after* that point.
          Safe default — a fresh install won't try to replay every
          historical row sitting in sink.db.
        * ``"beginning"``: ship every cycle in sink.db from the
          earliest row forward.  Useful for backfills.
        """
        self.db_path = Path(db_path)
        self.ship_buffer_sec = int(ship_buffer_sec)
        self.start_at = start_at
        self.expected_reporters = set(expected_reporters or ())
        self.backstop_sec = float(backstop_sec)
        self._conn: Optional[sqlite3.Connection] = None
        # Cached at-first-pump anchor so the "now" mode is stable across
        # multiple iter_batches calls before the first cycle ships.
        self._start_at_anchor: Optional[str] = None

    # -- Source protocol ----------------------------------------------------

    def source_id(self) -> str:
        return f"sqlite:{_LOGICAL_TABLE}"

    def health(self) -> str:
        if not self.db_path.exists():
            return HEALTH_NOOP
        return HEALTH_OK

    def iter_batches(self, cursor: bytes, limit: int) -> Iterator[RecordBatch]:
        """Yield one RecordBatch per shippable cycle, oldest first."""
        conn = self._connect()
        cursor_iso = self._effective_cursor(cursor)

        if self.expected_reporters:
            # Event-driven cross-RX gate: only ship cycles up to the
            # newest contiguous-complete one (all expected receivers'
            # noise rows present, or past the backstop).  Replaces the
            # plain cycle-floor cutoff with completion awareness.
            from .wspr_completion import shippable_ceiling
            ceiling_iso = shippable_ceiling(
                conn,
                cursor_iso=cursor_iso,
                expected=self.expected_reporters,
                backstop_sec=self.backstop_sec,
            )
            if ceiling_iso is None:
                return
        else:
            # Single-receiver: ship everything before the in-progress
            # cycle (original behaviour).
            ceiling_iso = self._cycle_floor_iso(datetime.now(timezone.utc))

        # Find distinct cycles past the cursor and at/before the ceiling.
        # `json_extract` lets us pull the cycle timestamp without parsing
        # every payload in Python.  The comparison is `<=` for the
        # completion ceiling (an exact shippable cycle) but the floor
        # path passes the in-progress cycle start, which is exclusive —
        # use `<` there.  Unify by always treating ceiling as inclusive
        # of complete cycles: when gated, ceiling_iso is a real cycle we
        # WANT to include, so `<=`.  When ungated, ceiling_iso is the
        # in-progress floor, exclusive, so `<`.
        op = "<=" if self.expected_reporters else "<"
        sql = (
            "SELECT DISTINCT json_extract(payload_json, '$.time') AS cycle "
            "FROM pending_uploads "
            "WHERE target_db = 'wspr' "
            "  AND target_table IN ('spots', 'noise') "
            "  AND json_extract(payload_json, '$.time') > ? "
            f"  AND json_extract(payload_json, '$.time') {op} ? "
            "ORDER BY cycle ASC"
        )
        cycles = [row[0] for row in conn.execute(sql, (cursor_iso, ceiling_iso))]
        if not cycles:
            return

        for cycle_iso in cycles:
            batch = self._build_batch_for_cycle(conn, cycle_iso)
            if batch is not None:
                yield batch

    def commit(self, commit_token: bytes) -> None:
        """Per-cycle commit hook.

        No-op in this source — row cleanup is deferred to
        ``smd storage trim`` (24 h retention).  Doing it here would
        race the wsprnet pipeline, which also reads ``wspr.spots``."""

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # -- internals ----------------------------------------------------------

    def _effective_cursor(self, cursor: bytes) -> str:
        """Resolve the cursor used in the SQL > comparison.

        Non-empty cursor → use it verbatim (resume from where we left off).
        Empty cursor + start_at='now' → cache the current cycle floor on
        first call and reuse it forever (so we never replay the historical
        backlog that the wsprdaemon-noise pipeline has been pruning).
        Empty cursor + start_at='beginning' → use empty string (ship all).
        """
        if cursor:
            return cursor.decode("ascii")
        if self.start_at == "beginning":
            return ""
        # start_at == "now"
        if self._start_at_anchor is None:
            # Anchor at the cycle BEFORE the current floor so the first
            # complete cycle still gets shipped.  Mechanism: SQL filter
            # is ``cycle > anchor AND cycle < floor``, strict on both
            # ends.  Floor moves up to the next cycle on the next pump,
            # at which point the previously-current cycle becomes
            # complete and matches the now-widened range.
            floor_dt = datetime.now(timezone.utc).replace(
                second=0, microsecond=0,
            )
            floor_dt -= timedelta(minutes=floor_dt.minute % 2)
            anchor_dt = floor_dt - timedelta(minutes=2)
            self._start_at_anchor = anchor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(
                "WsprCycleSource: empty watermark + start_at='now' → "
                "anchored at %s (first cycle to ship: %s on next pump)",
                self._start_at_anchor,
                floor_dt.strftime("%H:%M:%SZ"),
            )
        return self._start_at_anchor

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        return self._conn

    def _cycle_floor_iso(self, now: datetime) -> str:
        """Return ISO timestamp of the START of the current
        in-progress WSPR cycle.  Cycles older than this are safe to
        ship; cycles ≥ this might still be receiving rows."""
        # Round `now` down to the previous even minute.
        floor = now.replace(second=0, microsecond=0)
        floor -= timedelta(minutes=floor.minute % 2)
        # Optional safety margin (default 0).
        floor -= timedelta(seconds=self.ship_buffer_sec)
        return floor.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _build_batch_for_cycle(
        self, conn: sqlite3.Connection, cycle_iso: str,
    ) -> Optional[RecordBatch]:
        """Fetch every spots+noise row for one cycle, build a mixed
        RecordBatch, return None if zero rows (race with smd storage
        trim, harmless)."""
        records: list[Record] = []
        n_spots = 0
        n_noise = 0
        for target_table, payload_json in conn.execute(
            "SELECT target_table, payload_json "
            "FROM pending_uploads "
            "WHERE target_db = 'wspr' "
            "  AND target_table IN ('spots', 'noise') "
            "  AND json_extract(payload_json, '$.time') = ? "
            "ORDER BY id ASC",
            (cycle_iso,),
        ):
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "WsprCycleSource: corrupt payload in cycle %s: %s",
                    cycle_iso, exc,
                )
                continue
            # Logical table name the transport routes on.
            tname = f"wspr.{target_table}"
            try:
                rec_time = datetime.fromisoformat(
                    payload["time"].replace("Z", "+00:00")
                )
            except (KeyError, ValueError):
                rec_time = datetime.fromisoformat(
                    cycle_iso.replace("Z", "+00:00")
                )
            records.append(Record(
                table=tname,
                time=rec_time,
                columns=payload,
            ))
            if target_table == "spots":
                n_spots += 1
            else:
                n_noise += 1

        if not records:
            return None

        if n_noise == 0:
            logger.warning(
                "WsprCycleSource: cycle %s shipping spots-only "
                "(%d spots, 0 noise rows) — noise unexpectedly missing",
                cycle_iso, n_spots,
            )
        elif n_spots == 0:
            logger.warning(
                "WsprCycleSource: cycle %s shipping noise-only "
                "(%d noise, 0 spot rows) — spots unexpectedly missing",
                cycle_iso, n_noise,
            )
        else:
            logger.info(
                "WsprCycleSource: cycle %s ready (%d spots, %d noise)",
                cycle_iso, n_spots, n_noise,
            )

        return RecordBatch(
            records=tuple(records),
            cursor_after=cycle_iso.encode("ascii"),
            commit_token=b"",  # commit() is a no-op
        )
