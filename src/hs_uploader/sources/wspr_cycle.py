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
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

from .. import sinkswitch
from ..core import Record, RecordBatch
from ..sink import writer as _sink_writer
from ..sink.writer import infer_producer
from ._local_rows import LocalRowFilter

logger = logging.getLogger(__name__)


# WSPR cycle = 2 min, aligned to even minute (00, 02, 04, ...).
WSPR_CYCLE_SECONDS = 120

HEALTH_OK = "ok"
HEALTH_NOOP = "noop"

# The "table" label we expose to watermark store callers + transports.
# Treated as the primary_table for routing; this source covers both
# wspr.spots and wspr.noise so neither would be quite right.
_LOGICAL_TABLE = "wspr.cycle"

# The psk rider gate (spec sink-2b-part2 §5).  Producers whose decision
# is remembered for the once-per-change INFO line, and exception types
# remembered for the once-per-6-h ERROR line, are each capped.
_RIDER_CAP = 64
_REMIND_S = 6 * 3600.0
# The clock the reminder reads; tests replace it.
_clock = time.monotonic


class WsprCycleSource:
    """Multi-table cycle-aligned source backed by sink.db.

    The watermark cursor is the ISO-8601 timestamp of the last cycle
    *shipped*.  Cycle ordering is ASCII-comparable (strict ISO Z
    suffix) so the cursor doubles as a sort key.
    """

    # Transports use this to decide whether to accept records from
    # this source.  Both subtables travel together.  psk.spots is
    # listed too (safe — the transport only routes records it's given;
    # see include_psk below for the gating).
    ACCEPTS = {"wspr.spots": [1, 2], "wspr.noise": [1, 2], "psk.spots": [1, 2]}

    def __init__(
        self,
        *,
        db_path: Optional[Path | str] = None,
        ship_buffer_sec: int = 0,
        start_at: str = "now",
        expected_reporters: Optional[set] = None,
        backstop_sec: float = 90.0,
        include_psk: bool = False,
        sinks_dir: Optional[Path | str] = None,
        marker: Optional[Path | str] = None,
    ):
        """``db_path`` is the sink.db path (default: ``sink_path()``; must be readable by the
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

        ``sinks_dir`` and ``marker`` feed the psk rider gate (spec
        sink-2b-part2 §5).  While the carry-over marker stands, a psk row
        rides only when its producer's effective state reads ``upload``
        when the cycle's batch is built.  ``None`` means the library's
        defaults, ``sinkswitch.DEFAULT_SINKS_DIR`` and
        ``sinkswitch.marker_path()``, read at each cycle.  The gate stays on
        by default, so wspr-recorder's in-process shim, which passes
        neither, filters too.  The daemon passes the paths it uses.
        """
        if db_path is None:
            db_path = (_sink_writer.sink_path()
                       or _sink_writer.LEGACY_SINK_PATH)
        self.db_path = Path(db_path)
        self.ship_buffer_sec = int(ship_buffer_sec)
        self.start_at = start_at
        self.expected_reporters = set(expected_reporters or ())
        self.backstop_sec = float(backstop_sec)
        # When True, also pull this cycle's psk.spots (FT8/FT4/...) rows
        # into the same RecordBatch so they ride in the per-cycle
        # wsprdaemon tar.  Default False so existing callers are
        # unaffected (no separate psk pipeline needed when True).
        self.include_psk = bool(include_psk)
        self.sinks_dir = None if sinks_dir is None else Path(sinks_dir)
        self.marker = None if marker is None else Path(marker)
        # producer -> True while its psk rows are left out (capped).
        self._left_out: dict[str, bool] = {}
        self._cap_said = False
        # exception type -> monotonic time of its last ERROR line (capped).
        self._check_failed: dict[str, float] = {}
        # `producer` column probe, once per connection: (conn, present).
        self._producer_col: Optional[tuple[sqlite3.Connection, bool]] = None
        self._conn: Optional[sqlite3.Connection] = None
        # Cached at-first-pump anchor so the "now" mode is stable across
        # multiple iter_batches calls before the first cycle ships.
        self._start_at_anchor: Optional[str] = None
        # `local` column probe, once per connection (spec §5.2).
        self._local_rows = LocalRowFilter()

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
        if not self._ensure_ready(conn):
            return
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
        local_sql = "".join(
            f"  AND {c} " for c in self._local_rows.clause(conn)
        )
        sql = (
            "SELECT DISTINCT json_extract(payload_json, '$.time') AS cycle "
            "FROM pending_uploads "
            "WHERE target_db = 'wspr' "
            "  AND target_table IN ('spots', 'noise') "
            + local_sql +
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

    def cursor_is_after(self, new: bytes, stored: bytes) -> bool:
        """Compare as text, the order of the SQL filter
        ``json_extract(payload_json, '$.time') > ?`` in ``iter_batches``.
        Parsing the times could disagree with that query when a producer
        writes ``+00:00`` or fractional seconds, so the compare stays
        lexical.  An empty ``stored`` answers True; so does a value that
        is not ASCII (logged at DEBUG)."""
        if not stored:
            return True
        try:
            return new.decode("ascii") > stored.decode("ascii")
        except UnicodeDecodeError as exc:
            logger.debug(
                "WsprCycleSource: cannot order send records %r and %r (%s); "
                "treating the new one as after", new, stored, exc,
            )
            return True

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
            floor_dt, self._start_at_anchor = _now_anchor(
                datetime.now(timezone.utc))
            logger.info(
                "WsprCycleSource: empty watermark + start_at='now' → "
                "anchored at %s (first cycle to ship: %s on next pump)",
                self._start_at_anchor,
                floor_dt.strftime("%H:%M:%SZ"),
            )
        return self._start_at_anchor

    def initial_cursor(self) -> Optional[bytes]:
        """The send record the first pump would anchor at, encoded as
        ``cursor_after`` is (spec sink-2b-part2 §3.5).  ``None`` for
        ``start_at = "beginning"``, which anchors nowhere.

        An anchor a pump already fixed wins; otherwise the anchor
        ``_effective_cursor`` would fix now: the last complete cycle before
        the current cycle floor, so the cycle in progress still ships.
        Reads no database and changes no state.
        """
        if self.start_at == "beginning":
            return None
        anchor = self._start_at_anchor
        if anchor is None:
            _floor, anchor = _now_anchor(datetime.now(timezone.utc))
        return _encode_cursor(anchor)

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        return self._conn

    def _ensure_ready(self, conn: sqlite3.Connection) -> bool:
        """Check that the queue table exists.

        Returns ``True`` if ``pending_uploads`` is present (ready to read);
        ``False`` if the producer hasn't flushed yet (table absent).
        Raises on actual SQLite connection failures (file unreadable,
        corruption, etc.).

        Mirrors ``SqliteSource._ensure_ready`` -- see the note there.  This
        source needs it for the same reason: bring-up starts the uploader
        before the recorders exist, so on a greenfield station we poll a
        database that sqlite3.connect() has just created empty.  Without the
        guard that is an unhandled OperationalError once per pump cycle
        (17 tracebacks on AC0G-B4's first boot, appliance v3.21) -- alarming
        during a first boot, and competing with the real diagnostics at
        exactly the moment an operator is watching.

        Not cached: unlike SqliteSource this source is long-lived across the
        transition from "no producer" to "producer running", and the cost is
        one indexed sqlite_master lookup per pump.
        """
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='pending_uploads'"
        )
        return cur.fetchone() is not None

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
        local_sql = "".join(
            f"  AND {c} " for c in self._local_rows.clause(conn)
        )
        for target_table, payload_json in conn.execute(
            "SELECT target_table, payload_json "
            "FROM pending_uploads "
            "WHERE target_db = 'wspr' "
            "  AND target_table IN ('spots', 'noise') "
            + local_sql +
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

        # PSK rides along: pull this cycle's psk.spots rows (FT8/FT4/...)
        # for the same 2-minute window and append them as psk.spots
        # records so they ship inside the per-cycle wsprdaemon tar.
        # Gated by include_psk; no separate psk pipeline needed.
        n_psk = 0
        n_left_out = 0
        if self.include_psk:
            try:
                next_cycle_dt = datetime.fromisoformat(
                    cycle_iso.replace("Z", "+00:00")
                ) + timedelta(minutes=2)
                next_cycle_iso = next_cycle_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, AttributeError):
                next_cycle_iso = cycle_iso
            producer_sql = ("producer" if self._has_producer(conn)
                            else "''")
            riders: list[tuple[dict, str]] = []
            for payload_json, producer in conn.execute(
                f"SELECT payload_json, {producer_sql} FROM pending_uploads "
                "WHERE target_db = 'psk' AND target_table = 'spots' "
                + local_sql +
                "  AND json_extract(payload_json, '$.time') >= ? "
                "  AND json_extract(payload_json, '$.time') < ? "
                "ORDER BY id ASC",
                (cycle_iso, next_cycle_iso),
            ):
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "WsprCycleSource: corrupt psk payload in cycle %s: %s",
                        cycle_iso, exc,
                    )
                    continue
                riders.append((payload, producer or ""))
            for (payload, _producer), rides in zip(
                    riders, self._judge_riders(riders)):
                if not rides:
                    n_left_out += 1
                    continue
                try:
                    rec_time = datetime.fromisoformat(
                        payload["time"].replace("Z", "+00:00")
                    )
                except (KeyError, ValueError, AttributeError):
                    rec_time = datetime.fromisoformat(
                        cycle_iso.replace("Z", "+00:00")
                    )
                records.append(Record(
                    table="psk.spots",
                    time=rec_time,
                    columns=payload,
                ))
                n_psk += 1

        if not records:
            return None

        if n_noise == 0 and n_spots > 0:
            logger.warning(
                "WsprCycleSource: cycle %s shipping spots-only "
                "(%d spots, 0 noise rows, %d psk, %d psk left out) — noise "
                "unexpectedly missing",
                cycle_iso, n_spots, n_psk, n_left_out,
            )
        elif n_spots == 0 and n_noise > 0:
            logger.warning(
                "WsprCycleSource: cycle %s shipping noise-only "
                "(%d noise, 0 spot rows, %d psk, %d psk left out) — spots "
                "unexpectedly missing",
                cycle_iso, n_noise, n_psk, n_left_out,
            )
        else:
            logger.info(
                "WsprCycleSource: cycle %s ready (%d spots, %d noise, %d psk, "
                "%d psk left out)",
                cycle_iso, n_spots, n_noise, n_psk, n_left_out,
            )

        return RecordBatch(
            records=tuple(records),
            cursor_after=_encode_cursor(cycle_iso),
            commit_token=b"",  # commit() is a no-op
        )

    # -- the psk rider gate (spec sink-2b-part2 §5) -------------------------

    def _has_producer(self, conn: sqlite3.Connection) -> bool:
        """Whether ``pending_uploads`` has a ``producer`` column (v3.70+).

        Probed once per connection.  A sink without it infers the
        producer of every row."""
        if self._producer_col is None or self._producer_col[0] is not conn:
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(pending_uploads)")}
            self._producer_col = (conn, "producer" in cols)
        return self._producer_col[1]

    def _judge_riders(self, riders: list[tuple[dict, str]]) -> list[bool]:
        """For each (payload, producer column) a bool: does the row ride?

        While the carry-over marker stands, a row rides only when its
        producer's effective state reads ``upload`` now.  A missing or
        broken client file, or a stricter site file, reads below
        ``upload``, so the row stays out.  Without the marker every row
        rides.  The producer is the row's ``producer`` column, or
        ``infer_producer`` when that reads ``''``.

        The marker and each switch file are read at every call, never
        cached here, so a change counts at the next cycle built.  A check
        that raises leaves every rider of the cycle out (they still reach
        pskreporter.info through the producer's own pipeline) and logs
        at ERROR once per exception type, then every 6 h.
        """
        if not riders:
            return []
        try:
            marker = (self.marker if self.marker is not None
                      else sinkswitch.marker_path())
            enforced = sinkswitch.marker_stands(marker)
            if not enforced and not any(self._left_out.values()):
                return [True] * len(riders)
            sinks_dir = (self.sinks_dir if self.sinks_dir is not None
                         else sinkswitch.DEFAULT_SINKS_DIR)
            verdict: dict[str, bool] = {}
            out: list[bool] = []
            for payload, producer in riders:
                producer = producer or infer_producer("psk", "spots", payload)
                rides = verdict.get(producer)
                if rides is None:
                    state = None
                    if enforced:
                        state = sinkswitch.effective_state(
                            producer, sinks_dir=sinks_dir).state
                    rides = verdict[producer] = state in (None, "upload")
                    self._note_rider(producer, rides, state)
                out.append(rides)
            return out
        except Exception as exc:  # noqa: BLE001 — the WSPR rows still ship
            kind = type(exc).__name__
            now = _clock()
            last = self._check_failed.get(kind)
            if (last is None and len(self._check_failed) < _RIDER_CAP) or (
                    last is not None and now - last >= _REMIND_S):
                self._check_failed[kind] = now
                logger.error(
                    "WsprCycleSource: the psk rider check failed (%s); "
                    "leaving every psk row out of the wsprdaemon tar", kind)
            return [False] * len(riders)

    def _note_rider(self, producer: str, rides: bool,
                    state: Optional[str]) -> None:
        """One INFO line when a producer's rider decision changes, never
        per cycle or per row.  A producer rides until first left out."""
        left_out = not rides
        if self._left_out.get(producer, False) == left_out:
            return
        if producer not in self._left_out:
            if len(self._left_out) >= _RIDER_CAP:
                if not self._cap_said:
                    self._cap_said = True
                    logger.warning(
                        "WsprCycleSource: %d psk producers tracked; further "
                        "producers are judged but not logged", _RIDER_CAP)
                return
        self._left_out[producer] = left_out
        if left_out:
            logger.info(
                "WsprCycleSource: psk rows from %s stay out of the wsprdaemon "
                "tar: its sink switch reads %s, not upload", producer, state)
        else:
            logger.info(
                "WsprCycleSource: psk rows from %s ride in the wsprdaemon tar "
                "again", producer)


def _encode_cursor(cycle_iso: str) -> bytes:
    """A send record: the cycle's ISO text, ASCII."""
    return cycle_iso.encode("ascii")


def _now_anchor(now: datetime) -> tuple[datetime, str]:
    """(current cycle floor, anchor ISO) for ``start_at = "now"`` at ``now``.

    Anchor at the cycle BEFORE the current floor so the first complete
    cycle still gets shipped.  Mechanism: SQL filter is ``cycle > anchor
    AND cycle < floor``, strict on both ends.  Floor moves up to the next
    cycle on the next pump, at which point the previously-current cycle
    becomes complete and matches the now-widened range.
    """
    floor_dt = now.replace(second=0, microsecond=0)
    floor_dt -= timedelta(minutes=floor_dt.minute % 2)
    anchor_dt = floor_dt - timedelta(minutes=2)
    return floor_dt, anchor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
