"""The WSPR and FST4W modes, defined once.

A mode's name (W2, F2, F5, F15, F30) is its identity.  Numbers exist only
because two wire formats want them, and the two disagree:

- wsprnet.org's MEPT mode column: 2, 3, 5, 15, 30.
- wsprdaemon's pkt_mode, carried in field 17 of the wsprdaemon tar's spot
  lines and stored by the WD servers as ``code``: 2 for WSPR-2, and for
  FST4W the period in minutes plus one (3, 6, 16, 31).  It stays as the
  legacy wire code; on wsprnet's scale 15 means FST4W-900, while on
  wsprdaemon's it means WSPR-15, so the two scales must never mix.

wspr-recorder decodes and names modes from this table, and the transports
here translate a name to the number their destination wants.  A sink row
carries the name in ``mode`` and wsprdaemon's number in ``pkt_mode``;
``resolve`` reads the name first and falls back to the number, which
covers older rows that carry no name or a made-up one (wspr-recorder
wrote ``PKT16`` and ``PKT31`` for F15 and F30 until this table existed).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class WsprMode:
    name: str             # W2, F2, F5, F15, F30
    label: str            # WSPR-2, FST4W-120, ...
    period_s: int
    wsprnet_code: int     # wsprnet.org MEPT mode column
    wsprdaemon_code: int  # wsprdaemon pkt_mode (tar field 17, WD ``code``)


MODES: tuple[WsprMode, ...] = (
    WsprMode("W2",  "WSPR-2",      120,  2,  2),
    WsprMode("F2",  "FST4W-120",   120,  3,  3),
    WsprMode("F5",  "FST4W-300",   300,  5,  6),
    WsprMode("F15", "FST4W-900",   900, 15, 16),
    WsprMode("F30", "FST4W-1800", 1800, 30, 31),
)

BY_NAME: dict[str, WsprMode] = {m.name: m for m in MODES}
BY_WSPRDAEMON_CODE: dict[int, WsprMode] = {m.wsprdaemon_code: m for m in MODES}


def resolve(columns: Mapping[str, Any]) -> Optional[WsprMode]:
    """The mode of one spot row: its ``mode`` name, else its stored number.

    The stored number is wsprdaemon's: ``pkt_mode`` on sink rows, ``code``
    on legacy file-source rows.  None when neither names a known mode.
    """
    name = str(columns.get("mode") or "").strip().upper()
    if name in BY_NAME:
        return BY_NAME[name]
    code = columns.get("code")
    if code is None:
        code = columns.get("pkt_mode")
    try:
        return BY_WSPRDAEMON_CODE.get(int(code))
    except (TypeError, ValueError):
        return None
