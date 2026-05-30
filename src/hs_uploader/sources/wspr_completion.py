"""Event-driven cross-receiver cycle completion for the WSPR merge.

In a multi-RX merge fleet, several wspr-recorder processes decode the
SAME WSPR cycles from different receivers and write their spots+noise
into one shared sink (``/var/lib/sigmond/sink.db``).  A single merge
uploader then ships the union.  The hard part is knowing WHEN a cycle
is done across ALL receivers — the uploader runs in one process and is
woken only by its local receiver's commits, so a naive "pump on wake"
ships a partial cycle and the slower receivers' spots (and their
better-SNR copies) spill into the next post, degrading the merge.

The wsprdaemon-v3 answer was event-driven: each decoder writes a
completion entry per cycle (an empty one if it found no spots), and
the uploader fires when all decoders have reported.  Sigmond already
has that completion entry — the per-cycle ``wspr.noise`` rows.  Every
receiver writes one noise row per band every cycle, computed from the
WAV independent of whether any spot decoded, and (verified on B4-100
2026-05-30) the noise rows are written AFTER that receiver's spots for
the cycle.  So "this receiver's noise rows are present for cycle C"
reliably means "this receiver is completely done with C".

``shippable_ceiling`` turns that into a gate: walk cycles oldest-first
from the source's cursor and return the newest cycle such that every
cycle up to it is either complete (all expected receivers' noise rows
present) or older than the backstop (so one dead receiver can't stall
uploads forever).  The contiguous-prefix rule keeps the cursor-based
sources from skipping an older incomplete cycle while shipping a newer
complete one.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional


logger = logging.getLogger(__name__)

# Cycles we've already logged a backstop warning for, so a force-shipped
# cycle (e.g. a receiver that was restarting) doesn't re-warn on every
# pump while it's still inside the completion walk's lookback window.
# Bounded so it can't grow without limit; a genuinely-dead receiver
# re-warns once per new cycle, which is the desired "still down" signal.
_WARNED_CYCLES: "OrderedDict[str, None]" = None  # lazily initialised

# WSPR cycle length.  Cycles start on even UTC minutes; a cycle's spots
# carry its START time, and decode+write completes some seconds into
# the NEXT cycle.
CYCLE_SECONDS = 120


def parse_expected_reporters(raw: Optional[str]) -> set:
    """Parse WD_MERGE_REPORTERS (comma-separated rx_call list) → set.

    e.g. ``"AC0G/B4,AC0G/B5,AC0G/B6"`` → {"AC0G/B4","AC0G/B5","AC0G/B6"}.
    Empty / unset → empty set, which disables gating (single-receiver
    deployments ship per the source's normal floor, unchanged).
    """
    if not raw:
        return set()
    return {tok.strip() for tok in raw.split(",") if tok.strip()}


def _current_cycle_floor(now: datetime) -> datetime:
    """START of the in-progress WSPR cycle (previous even minute).

    Cycles at or after this are still being decoded/written and must
    never be shipped, regardless of completion state.
    """
    floor = now.replace(second=0, microsecond=0)
    return floor - timedelta(minutes=floor.minute % 2)


def cycle_complete(
    conn: sqlite3.Connection,
    cycle_iso: str,
    expected: Iterable[str],
) -> bool:
    """True when every expected receiver has noise rows for ``cycle_iso``.

    The noise rows are the completion marker (written after the
    receiver's spots).  ``expected`` is the set of rx_call values
    (``AC0G/B4`` etc.).
    """
    expected = set(expected)
    if not expected:
        return True
    rows = conn.execute(
        "SELECT DISTINCT json_extract(payload_json, '$.rx_call') "
        "FROM pending_uploads "
        "WHERE target_db = 'wspr' AND target_table = 'noise' "
        "  AND json_extract(payload_json, '$.time') = ?",
        (cycle_iso,),
    ).fetchall()
    present = {r[0] for r in rows if r[0]}
    return expected.issubset(present)


def shippable_ceiling(
    conn: sqlite3.Connection,
    *,
    cursor_iso: str,
    expected: Iterable[str],
    backstop_sec: float,
    now: Optional[datetime] = None,
) -> Optional[str]:
    """Newest cycle-iso shippable as a contiguous-complete prefix.

    Walks distinct WSPR cycles strictly after ``cursor_iso`` and
    strictly before the in-progress cycle, oldest first.  Returns the
    last cycle for which every cycle in the run is either:

      * complete — all ``expected`` receivers' noise rows present, or
      * past the backstop — the cycle ended more than ``backstop_sec``
        ago (force-ship so a dead receiver can't stall the queue).

    Stops at the first cycle that is neither, so the cursor never jumps
    a gap.  Returns ``None`` when nothing is shippable yet.

    ``expected`` empty → gating disabled: returns the newest
    already-past cycle (i.e. everything before the in-progress one),
    matching the un-gated single-receiver behaviour.
    """
    expected = set(expected)
    if now is None:
        now = datetime.now(timezone.utc)
    floor = _current_cycle_floor(now)
    floor_iso = floor.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Distinct cycles in (cursor, floor), oldest first.
    cycles = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT json_extract(payload_json, '$.time') AS cycle "
            "FROM pending_uploads "
            "WHERE target_db = 'wspr' "
            "  AND target_table IN ('spots', 'noise') "
            "  AND json_extract(payload_json, '$.time') > ? "
            "  AND json_extract(payload_json, '$.time') < ? "
            "ORDER BY cycle ASC",
            (cursor_iso, floor_iso),
        )
    ]
    if not cycles:
        return None
    # A cycle (start time C) is past the backstop once now is more than
    # CYCLE_SECONDS (its own length) + backstop_sec beyond C.
    force_before = now - timedelta(seconds=CYCLE_SECONDS + backstop_sec)
    ceiling: Optional[str] = None
    for cycle_iso in cycles:
        if not expected:
            ceiling = cycle_iso
            continue
        if cycle_complete(conn, cycle_iso, expected):
            ceiling = cycle_iso
            continue
        # Incomplete — force-ship only if past the backstop.
        try:
            c_dt = datetime.strptime(
                cycle_iso, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            # Unparseable cycle key — don't let it wedge the prefix.
            ceiling = cycle_iso
            continue
        if c_dt < force_before:
            if _warn_once(cycle_iso):
                missing = expected - _present_reporters(conn, cycle_iso)
                logger.warning(
                    "wspr-merge: cycle %s past %ds backstop with %d "
                    "receiver(s) missing (%s) — shipping anyway",
                    cycle_iso, int(backstop_sec), len(missing),
                    ",".join(sorted(missing)) or "?",
                )
            ceiling = cycle_iso
            continue
        # Incomplete and not yet past backstop → stop the prefix here.
        break
    return ceiling


def _warn_once(cycle_iso: str, _cap: int = 256) -> bool:
    """True the first time a cycle key is seen (for backstop warnings).

    Bounded FIFO so a long-running uploader can't leak memory; a
    genuinely-dead receiver still re-warns once per NEW cycle (each new
    cycle key is unseen), which is the intended "receiver still down"
    heartbeat — only the per-pump repeat for the SAME cycle is muted.
    """
    global _WARNED_CYCLES
    if _WARNED_CYCLES is None:
        _WARNED_CYCLES = OrderedDict()
    if cycle_iso in _WARNED_CYCLES:
        return False
    _WARNED_CYCLES[cycle_iso] = None
    while len(_WARNED_CYCLES) > _cap:
        _WARNED_CYCLES.popitem(last=False)
    return True


def _present_reporters(conn: sqlite3.Connection, cycle_iso: str) -> set:
    rows = conn.execute(
        "SELECT DISTINCT json_extract(payload_json, '$.rx_call') "
        "FROM pending_uploads "
        "WHERE target_db = 'wspr' AND target_table = 'noise' "
        "  AND json_extract(payload_json, '$.time') = ?",
        (cycle_iso,),
    ).fetchall()
    return {r[0] for r in rows if r[0]}
