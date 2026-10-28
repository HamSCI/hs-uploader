"""Two ways a failed upload lied about itself, both measured on AC0G/B4.

2026-10-26..27: PSWS refused station S000170's key after a reflash.  For
about 24 hours hs-uploader retried 993 times and dead-lettered 82 times,
and in all that output:

  * the reason never appeared.  `sftp -q` suppresses ssh's warnings and
    diagnostics, so "Permission denied (publickey)" was never logged and the
    journal showed only "Connection closed" — an auth refusal reading
    exactly like a network blip; and
  * the damage was overstated by forty times.  The 82 dead letters covered
    TWO datasets, one day's magnetometer zip and one day's GRAPE day, each
    dead-lettered, re-queued and dead-lettered again.  "78 failed for good"
    described the retry cadence, not the loss.

Both are reporting defects rather than transfer defects, which is what made
them expensive: the uploader was behaving correctly and describing itself
wrongly.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hs_uploader import backlog
from hs_uploader.transports.psws_magnetometer import (
    PswsMagnetometerSftp, _diagnose,
)


# ── the reason must survive to the log ───────────────────────────────────────

REAL_REFUSAL = (
    "Warning: Permanently added 'pswsnetwork.eng.ua.edu' (ED25519) to the "
    "list of known hosts.\n"
    "S000170@pswsnetwork.eng.ua.edu: Permission denied (publickey,"
    "gssapi-keyex,gssapi-with-mic).\n"
    "Connection closed.\n"
    "Connection closed\n"
)


def test_the_REAL_refusal_reports_the_PERMISSION_line_AND_NOTHING_ELSE():
    """The tail of this output is 'Connection closed' — the line that made
    an auth refusal indistinguishable from a network blip for 24 hours.

    Asserting only that the reason is SOMEWHERE in the string is too weak:
    a dump of every line contains it too, and a dump is what we are
    replacing.  The returned value must BE the line that says why."""
    out = _diagnose(REAL_REFUSAL)
    assert out == ("S000170@pswsnetwork.eng.ua.edu: Permission denied "
                   "(publickey,gssapi-keyex,gssapi-with-mic).")


@pytest.mark.parametrize("line,wanted", [
    ("S000170@host: Permission denied (publickey).", "Permission denied"),
    ("Host key verification failed.", "Host key verification"),
    ("ssh: connect to host h port 22: Connection refused", "Connection refused"),
    ("ssh: Could not resolve hostname h: Name or service not known",
     "Could not resolve hostname"),
    ("remote open failed: Disk quota exceeded", "Disk quota exceeded"),
])
def test_each_distinguishable_failure_is_named(line, wanted):
    assert wanted.lower() in _diagnose(f"chatter\n{line}\nmore chatter").lower()


def test_output_with_NO_recognised_diagnostic_still_says_something():
    assert _diagnose("weird\nunfamiliar\noutput").strip()


def test_EMPTY_output_does_not_report_an_empty_reason():
    """A blank warning line is worse than none: it reads as 'we looked'."""
    assert _diagnose("") == "(no output from sftp)"
    assert _diagnose("   \n\n  ") == "(no output from sftp)"


def _argv_of_a_real_call(monkeypatch) -> list:
    """The argv sftp is actually invoked with.

    Reading the source text instead would pass on a comment that merely
    mentions the flag — which is exactly how an earlier version of this
    test let `LogLevel=QUIET` through.
    """
    seen = {}

    class _R:
        returncode = 0
        stdout = b""
        stderr = b""

    def fake_run(cmd, **kw):
        seen["cmd"] = list(cmd)
        return _R()

    monkeypatch.setattr(
        "hs_uploader.transports.psws_magnetometer.subprocess.run", fake_run)
    t = PswsMagnetometerSftp.__new__(PswsMagnetometerSftp)
    t.host = "h"
    t.connect_timeout_sec = 5
    t.xfer_timeout_sec = 30
    t.bandwidth_limit_kbps = None
    t._run_sftp("u", None, b"quit\n")
    return seen["cmd"]


def test_the_sftp_command_does_NOT_pass_q(monkeypatch):
    """-q is the switch that hid the reason.  It must not come back."""
    assert "-q" not in _argv_of_a_real_call(monkeypatch)


def test_the_sftp_command_keeps_ssh_DIAGNOSTICS_switched_on(monkeypatch):
    argv = _argv_of_a_real_call(monkeypatch)
    assert "LogLevel=ERROR" in argv
    assert not any("QUIET" in a.upper() for a in argv)


# ── dead letters must count datasets, not attempts ───────────────────────────

SCHEMA = """
CREATE TABLE dead_letter (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, pipeline TEXT NOT NULL,
    payload_blob BLOB NOT NULL, final_error TEXT NOT NULL);
CREATE TABLE deliverables (pipeline TEXT, next_attempt_at TEXT);
CREATE TABLE watermarks (source_id TEXT, dest_id TEXT, table_name TEXT,
    last_ack TEXT, cursor BLOB);
"""


def _store(tmp_path: Path, rows) -> Path:
    p = tmp_path / "watermarks.db"
    conn = sqlite3.connect(p)
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT INTO dead_letter(ts, pipeline, payload_blob, final_error) "
        "VALUES(?,?,?,?)", rows)
    conn.commit(); conn.close()
    return p


def _now():
    return datetime(2026, 10, 27, 20, 0, tzinfo=timezone.utc)


def _ts(minutes_ago: int) -> str:
    return (_now() - timedelta(minutes=minutes_ago)).isoformat()


def _pipeline(summary, name):
    return next(p for p in summary["pipelines"] if p["name"] == name)


def test_THE_REAL_SHAPE_82_attempts_over_TWO_datasets(tmp_path):
    """One mag zip and one GRAPE day, each re-dead-lettered 41 times."""
    rows = []
    for i in range(41):
        rows.append((_ts(i), "mag-psws", b"OBS2026-10-26-mag", "sftp rc=255"))
        rows.append((_ts(i), "mag-psws", b"OBS2026-10-26-grape", "sftp rc=255"))
    s = backlog.summarize(_store(tmp_path, rows), now=_now())
    p = _pipeline(s, "mag-psws")
    assert p["dead_letter_count"] == 82        # attempts, as before
    assert p["dead_letter_datasets"] == 2      # the number that means loss


def test_one_dataset_dead_lettered_ONCE_reads_as_one(tmp_path):
    s = backlog.summarize(
        _store(tmp_path, [(_ts(1), "p", b"only", "e")]), now=_now())
    p = _pipeline(s, "p")
    assert p["dead_letter_count"] == 1 and p["dead_letter_datasets"] == 1


def test_DISTINCT_datasets_are_not_collapsed_into_one(tmp_path):
    """The opposite error: under-reporting real loss."""
    rows = [(_ts(1), "p", f"ds{i}".encode(), "e") for i in range(7)]
    s = backlog.summarize(_store(tmp_path, rows), now=_now())
    assert _pipeline(s, "p")["dead_letter_datasets"] == 7


def test_the_same_payload_in_DIFFERENT_pipelines_counts_once_EACH(tmp_path):
    rows = [(_ts(1), "a", b"same", "e"), (_ts(1), "b", b"same", "e")]
    s = backlog.summarize(_store(tmp_path, rows), now=_now())
    assert _pipeline(s, "a")["dead_letter_datasets"] == 1
    assert _pipeline(s, "b")["dead_letter_datasets"] == 1


def test_the_RECENT_window_counts_datasets_too(tmp_path):
    """smd check reads the 24 h window; it inflated the same way."""
    rows = [(_ts(i), "p", b"one", "e") for i in range(0, 50, 10)]      # recent
    rows += [(_ts(60 * 24 * 9), "p", b"old", "e")]                     # stale
    s = backlog.summarize(_store(tmp_path, rows), now=_now())
    p = _pipeline(s, "p")
    assert p["dead_letter_recent_count"] == 5
    assert p["dead_letter_recent_datasets"] == 1
    assert p["dead_letter_datasets"] == 2


def test_an_EMPTY_dead_letter_table_reports_zero_not_an_error(tmp_path):
    s = backlog.summarize(_store(tmp_path, []), now=_now())
    assert s["readable"] is True
    assert all(p.get("dead_letter_datasets", 0) == 0
               for p in s.get("pipelines", []))
