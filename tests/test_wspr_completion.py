"""Tests for the event-driven cross-RX completion gate."""

import json
import sqlite3
from datetime import datetime, timezone

import hs_uploader.sources.wspr_completion as wc
from hs_uploader.sources.wspr_completion import (
    cycle_complete,
    parse_expected_reporters,
    shippable_ceiling,
)


def _sink():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE pending_uploads ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, target_db TEXT, "
        "target_table TEXT, schema_version INT, payload_json TEXT, "
        "queued_at TEXT)"
    )
    return conn


def _add(conn, tbl, cycle, rx, n=1):
    for _ in range(n):
        conn.execute(
            "INSERT INTO pending_uploads"
            "(target_db,target_table,schema_version,payload_json,queued_at) "
            "VALUES('wspr',?,2,?,'')",
            (tbl, json.dumps({"time": cycle, "rx_call": rx, "band": "20"})),
        )
    conn.commit()


EXP = parse_expected_reporters("AC0G/B4,AC0G/B5,AC0G/B6")


def test_parse_expected_reporters():
    assert parse_expected_reporters("AC0G/B4, AC0G/B5 ,AC0G/B6") == {
        "AC0G/B4", "AC0G/B5", "AC0G/B6",
    }
    assert parse_expected_reporters("") == set()
    assert parse_expected_reporters(None) == set()


def test_cycle_complete_requires_all_reporters_noise():
    conn = _sink()
    cyc = "2026-05-30T15:16:00Z"
    _add(conn, "noise", cyc, "AC0G/B4", 17)
    _add(conn, "noise", cyc, "AC0G/B5", 17)
    assert not cycle_complete(conn, cyc, EXP)   # B6 missing
    _add(conn, "noise", cyc, "AC0G/B6", 17)
    assert cycle_complete(conn, cyc, EXP)


def test_empty_expected_disables_gate():
    conn = _sink()
    assert cycle_complete(conn, "2026-05-30T15:16:00Z", set())


def test_ceiling_blocks_at_incomplete_recent_cycle():
    conn = _sink()
    now = datetime(2026, 5, 30, 15, 20, 30, tzinfo=timezone.utc)  # in-prog 15:20
    for rx in EXP:
        _add(conn, "noise", "2026-05-30T15:16:00Z", rx, 17)
    # 15:18 incomplete (B6 missing), recent → not past backstop
    _add(conn, "noise", "2026-05-30T15:18:00Z", "AC0G/B4", 17)
    _add(conn, "noise", "2026-05-30T15:18:00Z", "AC0G/B5", 17)
    c = shippable_ceiling(
        conn, cursor_iso="2026-05-30T15:10:00Z",
        expected=EXP, backstop_sec=90, now=now,
    )
    assert c == "2026-05-30T15:16:00Z"  # blocked at 15:18


def test_ceiling_force_ships_past_backstop():
    conn = _sink()
    for rx in EXP:
        _add(conn, "noise", "2026-05-30T15:16:00Z", rx, 17)
    _add(conn, "noise", "2026-05-30T15:18:00Z", "AC0G/B4", 17)
    _add(conn, "noise", "2026-05-30T15:18:00Z", "AC0G/B5", 17)
    # now well past 15:18's end (15:20) + 90s backstop
    now = datetime(2026, 5, 30, 15, 22, 0, tzinfo=timezone.utc)
    wc._WARNED_CYCLES = None  # reset warn-once state
    c = shippable_ceiling(
        conn, cursor_iso="2026-05-30T15:10:00Z",
        expected=EXP, backstop_sec=90, now=now,
    )
    assert c == "2026-05-30T15:18:00Z"  # force-shipped despite B6 missing


def test_ceiling_ships_all_when_complete():
    conn = _sink()
    now = datetime(2026, 5, 30, 15, 20, 30, tzinfo=timezone.utc)
    for cyc in ("2026-05-30T15:16:00Z", "2026-05-30T15:18:00Z"):
        for rx in EXP:
            _add(conn, "noise", cyc, rx, 17)
    c = shippable_ceiling(
        conn, cursor_iso="2026-05-30T15:10:00Z",
        expected=EXP, backstop_sec=90, now=now,
    )
    assert c == "2026-05-30T15:18:00Z"  # both complete → ceiling = newest


def test_ceiling_excludes_in_progress_cycle():
    conn = _sink()
    now = datetime(2026, 5, 30, 15, 20, 30, tzinfo=timezone.utc)  # 15:20 in-prog
    for rx in EXP:
        _add(conn, "noise", "2026-05-30T15:20:00Z", rx, 17)  # current cycle
    c = shippable_ceiling(
        conn, cursor_iso="2026-05-30T15:10:00Z",
        expected=EXP, backstop_sec=90, now=now,
    )
    assert c is None  # nothing older than the in-progress cycle


def test_warn_once_mutes_repeat_same_cycle():
    wc._WARNED_CYCLES = None
    assert wc._warn_once("2026-05-30T15:18:00Z") is True
    assert wc._warn_once("2026-05-30T15:18:00Z") is False  # muted
    assert wc._warn_once("2026-05-30T15:20:00Z") is True   # new cycle warns
