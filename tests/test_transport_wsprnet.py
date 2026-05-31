"""WsprNet transport — MEPT line rendering + HTTP multipart POST.

Stubs ``urlopen`` so the test suite stays offline.  Verifies the
wire-level shape (multipart fields + boundary) bytes-equals what
``wsprdaemon-client/bin/wd-upload-wsprnet`` emits for the same input,
plus the row-to-MEPT mapping for the columns that flow through
PskReporterTcp's psk.spots-or-wspr.spots renderers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock

import pytest

from hs_uploader import StationIdentity
from hs_uploader.core import Outcome, Record, RecordBatch
from hs_uploader.transports import WsprNet
from hs_uploader.transports.wsprnet import (
    MAX_SPOTS_PER_UPLOAD,
    _build_multipart,
    _record_to_mept,
)


def _ident(call="AC0G/B1", grid="EM38ww"):
    return StationIdentity(call=call, grid=grid)


def _spot(
    *,
    when=datetime(2026, 5, 7, 0, 42, tzinfo=timezone.utc),
    tx_sign="KC1KOP",
    tx_loc="FN41",
    freq_mhz=18.106073,
    sync=1,
    snr=-22,
    dt=0.2,
    power=23,
    drift=0,
    code=2,    # 2 = WSPR-2 (W-mode); was 1 before but no real WSPR mode is 1
):
    return Record(
        table="wspr.spots",
        time=when,
        columns={
            "tx_sign":       tx_sign,
            "tx_loc":        tx_loc,
            "frequency_mhz": freq_mhz,
            "sync_quality":  sync,
            "snr":           snr,
            "dt":            dt,
            "power":         power,
            "drift":         drift,
            "code":          code,
        },
    )


# ---- row → MEPT ----


def test_record_to_mept_renders_canonical_line():
    """Byte-identical W-mode format vs v1 wd-decode line 345:
       "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d"
    """
    line = _record_to_mept(_spot())
    assert line == (
        "260507 0042  1.00 -22  0.20   18.1060730 KC1KOP         FN41   23  0    2"
    )


def test_record_to_mept_drops_grid_when_absent():
    """Compound calls (e.g. ``W1/AJ8S``) frequently have no grid;
    v1 still emits the grid field as left-padded empty (6 chars wide)
    matching wd-decode's `%-6s` format — wsprnet.org tolerates an
    empty grid column.  Drop-in compatibility means we emit the
    same wire bytes, not a different shape."""
    line = _record_to_mept(_spot(tx_sign="W1/AJ8S", tx_loc="", snr=-16,
                                 dt=0.8, freq_mhz=18.106155, code=2))
    assert line == (
        "260507 0042  1.00 -16  0.80   18.1061550 W1/AJ8S               23  0    2"
    )


def test_record_to_mept_skips_unresolved_hash():
    """wsprd emits ``<...>`` when a 22-bit hash can't be mapped to a
    real callsign — these are noise, not spots, and must not ship."""
    assert _record_to_mept(_spot(tx_sign="<...>")) is None


def test_record_to_mept_skips_missing_call():
    assert _record_to_mept(_spot(tx_sign="")) is None


def _v2_spot(
    *,
    when=datetime(2026, 5, 7, 0, 42, tzinfo=timezone.utc),
    callsign="KC1KOP",
    grid="FN41",
    freq_hz=18_106_073,
    sync_quality=1.0,
    snr_db=-22,
    dt=0.2,
    pwr_dbm=23,
    drift_hz_per_s=0.0,
    pkt_mode=2,
    mode="W2",
):
    """v2 sink.db row shape (wspr-recorder's `spot_to_row` output)."""
    return Record(
        table="wspr.spots",
        time=when,
        columns={
            "callsign":       callsign,
            "grid":           grid,
            "frequency_hz":   freq_hz,
            "sync_quality":   sync_quality,
            "snr_db":         snr_db,
            "dt":             dt,
            "pwr_dbm":        pwr_dbm,
            "drift_hz_per_s": drift_hz_per_s,
            "pkt_mode":       pkt_mode,
            "mode":           mode,
        },
    )


def test_v2_schema_row_renders_same_wire_bytes():
    """Regression: v2 sink.db row (callsign / frequency_hz / snr_db /
    pwr_dbm / drift_hz_per_s / pkt_mode / mode) must produce identical
    wire bytes to the equivalent legacy row.  Before this fix, v2
    rows produced empty bodies (silent acked) because the transport
    only knew the legacy field names — wsprnet.org received nothing
    after the Pipeline-v2 cutover."""
    legacy = _spot()
    v2 = _v2_spot()
    assert _record_to_mept(legacy) == _record_to_mept(v2)


def test_v2_drift_hz_per_s_converted_to_hz_per_min():
    """Producer stores drift as Hz/s float; wire format wants Hz/min
    integer.  Multiply by 60 and round."""
    r = _v2_spot(drift_hz_per_s=0.1)   # 0.1 Hz/s = 6 Hz/min
    line = _record_to_mept(r)
    # The drift field is the 10th whitespace-token in W-mode output.
    parts = line.split()
    assert parts[9] == "6"


def test_v2_fmode_uses_jt9_format():
    """F-mode (FST4W) rows use a different format: %5.1f for sync
    (one decimal) and %3.0f for snr (zero decimals), drift hardcoded
    to 0.  Matches v1 wd-decode line 466."""
    r = _v2_spot(mode="F2", pkt_mode=3,
                 sync_quality=77.0, snr_db=-1, freq_hz=10_140_101)
    line = _record_to_mept(r)
    # F-mode wire format from v1:
    #   "260514 1158  77.0  -1  0.20   10.1401010 KC1KOP         FN41   23  0    3"
    assert line == (
        "260507 0042  77.0  -1  0.20   10.1401010 KC1KOP         FN41   23  0    3"
    )


def test_v2_bracketed_resolved_callsign_passes_through():
    """Type-3 hash-resolved calls like ``<AK4MI>`` are valid spots
    (v1 wd-upload-wsprnet._has_hash_callsign drops only ``<...>``).
    The brackets ride on the wire; wsprnet.org's server strips them."""
    r = _v2_spot(callsign="<AK4MI>")
    line = _record_to_mept(r)
    assert "<AK4MI>" in line


def test_record_to_mept_falls_back_to_frequency_hz():
    """Some sources emit ``frequency`` (Hz) but not ``frequency_mhz``.
    The renderer should compute MHz when only Hz is present."""
    r = Record(
        table="wspr.spots",
        time=datetime(2026, 5, 7, 0, 42, tzinfo=timezone.utc),
        columns={
            "tx_sign":   "K1ABC",
            "tx_loc":    "FN42",
            "frequency": 14_097_000,  # Hz
            "snr":       -10,
            "dt":        0.1,
            "power":     23,
        },
    )
    line = _record_to_mept(r)
    assert "14.097000" in line


# ---- batch / sort ----


def test_ship_sorts_by_date_time_freq():
    """Canonical wsprnet sort order: (date, time, freq).  Out-of-order
    input must be reordered before the body goes on the wire — the
    central server uses the line order to break ties."""
    early = _spot(freq_mhz=14.097100, tx_sign="K1ABC", tx_loc="FN42")
    later_lower_freq = _spot(
        when=datetime(2026, 5, 7, 0, 44, tzinfo=timezone.utc),
        freq_mhz=7.040000, tx_sign="K2DEF", tx_loc="FN30",
    )
    later_higher_freq = _spot(
        when=datetime(2026, 5, 7, 0, 44, tzinfo=timezone.utc),
        freq_mhz=14.097200, tx_sign="K3GHI", tx_loc="EM34",
    )
    body = WsprNet()._build_mept_body([
        later_higher_freq, early, later_lower_freq,
    ])  # noqa: SLF001
    text = body.decode()
    lines = [l for l in text.splitlines() if l]
    assert len(lines) == 3
    assert lines[0].split()[6] == "K1ABC"   # earliest time
    assert lines[1].split()[6] == "K2DEF"   # later time, lower freq
    assert lines[2].split()[6] == "K3GHI"


def test_batch_policy_caps_at_999():
    """wsprnet's hard server-side limit per transaction.  Larger
    batches are silently truncated by the gateway — the BatchPolicy
    keeps the orchestrator from emitting them in the first place."""
    assert WsprNet().batch_policy().max_records == MAX_SPOTS_PER_UPLOAD
    assert MAX_SPOTS_PER_UPLOAD == 999


# ---- multipart wire shape ----


def test_multipart_body_matches_wd_upload_wsprnet_shape():
    """Bytes-equal to what wd-upload-wsprnet emits for the same input
    — diff-friendly for the wsprdaemon-client migration."""
    body = _build_multipart(
        version="WD_4.0",
        call="AC0G/B1",
        grid="EM38ww",
        allmept=b"260507 0042 1 -22 0.2 18.106073 KC1KOP FN41 23 0 1\n",
    )
    # The on-wire boundary is two literal hyphens then the constant
    # ``--------WD4MeptBoundary`` (matching wd-upload-wsprnet's ``b'--'
    # + boundary`` prefix).
    B = b"----------WD4MeptBoundary"
    expected = (
        B + b"\r\n"
        + b'Content-Disposition: form-data; name="version"\r\n\r\n'
        + b"WD_4.0\r\n"
        + B + b"\r\n"
        + b'Content-Disposition: form-data; name="call"\r\n\r\n'
        + b"AC0G/B1\r\n"
        + B + b"\r\n"
        + b'Content-Disposition: form-data; name="grid"\r\n\r\n'
        + b"EM38ww\r\n"
        + B + b"\r\n"
        + b'Content-Disposition: form-data; name="allmept"; '
        + b'filename="spots.txt"\r\n'
        + b"Content-Type: text/plain\r\n\r\n"
        + b"260507 0042 1 -22 0.2 18.106073 KC1KOP FN41 23 0 1\n\r\n"
        + B + b"--\r\n"
    )
    assert body == expected


# ---- ship / replay / outcomes ----


class _FakeResp:
    def __init__(self, status=200, body=b"200 OK 1 spots added"):
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_ship_returns_acked_on_2xx():
    captured: dict = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["body"] = req.data
        captured["headers"] = dict(req.header_items())
        return _FakeResp(status=200)

    t = WsprNet(urlopen=fake_urlopen, version="hs-uploader/0.1")
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert captured["url"] == "http://wsprnet.org/meptspots.php"
    # version field is prefixed "WD_" to match wd-upload-wsprnet.
    assert b'name="version"\r\n\r\nWD_hs-uploader/0.1' in captured["body"]
    assert b'name="call"\r\n\r\nAC0G/B1' in captured["body"]
    assert b'name="grid"\r\n\r\nEM38ww' in captured["body"]
    # Content-Type carries the boundary inline.
    ct = next(
        v for k, v in captured["headers"].items()
        if k.lower() == "content-type"
    )
    assert "multipart/form-data; boundary=" in ct


def test_ship_acks_on_5xx_no_retry():
    """Per policy (operator @rrobinett 2026-05-14): ANY HTTP response —
    2xx success or 4xx/5xx error — is ack'd, never retried.  Retrying
    a 5xx just resends identical payload to the same server for the
    same rejection."""
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(
            req.full_url, 502, "Bad Gateway",
            hdrs={}, fp=BytesIO(b"gateway error"),
        )

    t = WsprNet(urlopen=fake_urlopen)
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert "502" in outcome.reason


def test_ship_acks_on_network_error_no_retry():
    """Per policy (operator @rrobinett 2026-05-23): even network-level
    failures and timeouts are ack'd, not retried.  wsprnet.org has been
    observed responding 3-10 min after the POST body was sent — long
    after our upload_timeout_sec fires.  Retrying on timeout just
    re-uploads the same body, which the server processes a second time
    and reports as ``0/N added`` (the first delivery already populated
    its tables).  Net effect was multi-minute retry loops landing zero
    new spots.  Lose-the-occasional-true-outage is cheaper than spin-
    forever."""
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.URLError("connection refused")

    t = WsprNet(urlopen=fake_urlopen)
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert "connection refused" in outcome.reason


def test_ship_permanent_failure_when_call_missing():
    """wsprnet rejects unauthenticated uploads — surfacing as
    permanent_failure (not retry_later) lets the deliverable
    dead-letter immediately instead of beating up the gateway."""
    t = WsprNet(urlopen=lambda *a, **k: _FakeResp())
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, StationIdentity(call="", grid="EM38ww"))
    assert outcome.kind == "permanent"


def test_ship_acks_empty_batch_without_calling_server():
    """If every record was filtered (all unresolved hashes / missing
    call), there's nothing to POST.  Acking-without-shipping advances
    the watermark past the rows without burning a network round-trip."""
    calls = {"n": 0}

    def fake_urlopen(*args, **kwargs):
        calls["n"] += 1
        return _FakeResp()

    t = WsprNet(urlopen=fake_urlopen)
    batch = RecordBatch(
        records=(_spot(tx_sign="<...>"),),  # filtered
        cursor_after=b"",
    )
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert calls["n"] == 0


def test_serialize_for_retry_is_byte_stable():
    """The retry payload must be the exact body the first attempt
    sent — replay through the deliverables queue should never
    re-render against potentially-changed row data."""
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    t = WsprNet()
    blob = t.serialize_for_retry(batch, _ident())
    assert blob == t._build_mept_body(batch.records)  # noqa: SLF001
    # v1 wd-decode line-345 wire format — byte-identical match.
    assert blob.startswith(
        b"260507 0042  1.00 -22  0.20   18.1060730 KC1KOP         FN41"
    )


def test_replay_posts_the_stored_blob_verbatim():
    captured: dict = {}

    def fake_urlopen(req, timeout):
        captured["body"] = req.data
        return _FakeResp(status=200)

    t = WsprNet(urlopen=fake_urlopen)
    blob = b"260507 0042 1 -22 0.2 18.106073 KC1KOP FN41 23 0 1\n"
    outcome = t.replay(blob, _ident())
    assert outcome.kind == "acked"
    # The allmept body section is the blob, byte-for-byte.
    assert blob + b"\r\n--" in captured["body"]


# ---- ACCEPTS / primary_table ----


def test_accepts_wspr_spots_versions():
    """Updated 2026-05-23: producers write schema_version=2 (and
    we declare room for 3) — ACCEPTS lists the actually-supported
    versions rather than a single legacy value."""
    assert WsprNet().ACCEPTS == {"wspr.spots": [1, 2, 3]}


def test_primary_table_is_wspr_spots():
    assert WsprNet().primary_table() == "wspr.spots"


# ---- multi-source dedup (multi-RX888 plan, phase 5) ----


def test_dedup_collapses_duplicate_spots_keeping_max_snr():
    from hs_uploader.transports.wsprnet import dedup_records_for_wsprnet
    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    weak = _spot(when=when, tx_sign="K9XX", tx_loc="EN52",
                 freq_mhz=14.097100, snr=-22)
    strong = _spot(when=when, tx_sign="K9XX", tx_loc="EN52",
                   freq_mhz=14.097100, snr=-5)
    different_call = _spot(when=when, tx_sign="W1AW", tx_loc="FN31",
                           freq_mhz=14.097200, snr=-15)
    out = dedup_records_for_wsprnet([weak, strong, different_call])
    # K9XX collapses to one record — the -5 dB one (max SNR).  W1AW
    # passes through untouched as a different spot.
    snrs = sorted(r.columns["snr"] for r in out)
    calls = sorted(r.columns["tx_sign"] for r in out)
    assert calls == ["K9XX", "W1AW"]
    assert -5 in snrs and -15 in snrs
    assert -22 not in snrs


def test_dedup_quantizes_frequency_to_100hz():
    """Two receivers report the same spot at frequencies differing by
    < 100 Hz — should collapse to one.  Differing by > 100 Hz is treated
    as two distinct spots."""
    from hs_uploader.transports.wsprnet import dedup_records_for_wsprnet
    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    # 14.0971000 vs 14.0971002 — within the 0.0001 MHz quantum
    a = _spot(when=when, tx_sign="K9XX", freq_mhz=14.0971000, snr=-10)
    b = _spot(when=when, tx_sign="K9XX", freq_mhz=14.0971002, snr=-8)
    # 14.0971000 vs 14.0975000 — 400 Hz apart, distinct spots
    c = _spot(when=when, tx_sign="K9XX", freq_mhz=14.0975000, snr=-12)
    out = dedup_records_for_wsprnet([a, b, c])
    snrs = sorted(r.columns["snr"] for r in out)
    # a+b collapse (kept -8), c distinct
    assert -8 in snrs and -12 in snrs
    assert -10 not in snrs
    assert len(out) == 2


def test_dedup_uses_snr_db_when_snr_absent():
    """v2 sink.db rows carry ``snr_db`` not ``snr``.  Dedup must read
    either name to pick the max-SNR winner."""
    from hs_uploader.transports.wsprnet import dedup_records_for_wsprnet
    from hs_uploader.core import Record
    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    weak = Record(table="wspr.spots", time=when, columns={
        "callsign": "K9XX", "frequency_hz": 14097100, "snr_db": -22,
        "pwr_dbm": 23, "drift_hz_per_s": 0.0, "pkt_mode": 2,
    })
    strong = Record(table="wspr.spots", time=when, columns={
        "callsign": "K9XX", "frequency_hz": 14097100, "snr_db": -5,
        "pwr_dbm": 23, "drift_hz_per_s": 0.0, "pkt_mode": 2,
    })
    out = dedup_records_for_wsprnet([weak, strong])
    assert len(out) == 1
    assert out[0].columns["snr_db"] == -5


def test_dedup_preserves_input_order_for_ties():
    """When two records have identical SNR, keep the first (matches
    wsprnet_audit's INSERT OR IGNORE first-write-wins semantics)."""
    from hs_uploader.transports.wsprnet import dedup_records_for_wsprnet
    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    first = _spot(when=when, tx_sign="K9XX", freq_mhz=14.097100, snr=-10,
                  tx_loc="EN52")
    second = _spot(when=when, tx_sign="K9XX", freq_mhz=14.097100, snr=-10,
                   tx_loc="FN31")   # different grid — only kept if "first"
    out = dedup_records_for_wsprnet([first, second])
    assert len(out) == 1
    assert out[0].columns["tx_loc"] == "EN52"


def test_dedup_passes_through_malformed_records_unchanged():
    """Records that fail the dedup-shape check (missing time, missing
    call, etc.) must still appear in the output so ``_record_to_mept``
    can drop them at format time with its own filter logic.  This
    keeps dedup orthogonal to the existing filter rules."""
    from hs_uploader.transports.wsprnet import dedup_records_for_wsprnet
    from hs_uploader.core import Record
    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    good = _spot(when=when, tx_sign="K9XX", snr=-10)
    no_call = Record(table="wspr.spots", time=when, columns={
        "frequency_mhz": 14.097, "snr": -8,
    })
    no_time = Record(table="wspr.spots", time=None, columns={
        "tx_sign": "K9XX", "frequency_mhz": 14.097, "snr": -8,
    })
    out = dedup_records_for_wsprnet([good, no_call, no_time])
    # All three pass through (good as the deduped winner, the others
    # because they didn't match the dedup shape).
    assert len(out) == 3


def test_ship_collapses_duplicates_in_posted_body():
    """End-to-end through ``ship()``: a batch containing two copies of
    the same spot only emits ONE MEPT line in the posted body."""
    captured: dict = {}

    def fake_urlopen(req, timeout):
        captured["body"] = req.data
        return _FakeResp(status=200)

    when = datetime(2026, 5, 19, 21, 22, tzinfo=timezone.utc)
    weak = _spot(when=when, tx_sign="K9XX", freq_mhz=14.097100, snr=-22)
    strong = _spot(when=when, tx_sign="K9XX", freq_mhz=14.097100, snr=-5)
    batch = RecordBatch(records=(weak, strong), cursor_after=b"")
    WsprNet(urlopen=fake_urlopen).ship(batch, _ident())

    # Count MEPT lines in the body (each line starts with the 6-digit
    # YYMMDD prefix).
    n_lines = captured["body"].count(b"260519 2122 ")
    assert n_lines == 1
    # And the winning SNR is -5 (the strong record).
    assert b" -5 " in captured["body"]
    assert b" -22 " not in captured["body"]


# ----- async upload API (W1GJM) -----

class _JsonResp:
    def __init__(self, payload, status=200):
        import json as _json
        self.status = status
        self._body = _json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_async_submit_and_poll_done():
    """Submit returns a nonce; status poll returns done with counts."""
    calls = []

    def fake_urlopen(req, timeout):
        calls.append((req.method, req.full_url))
        if req.full_url.endswith("/upload"):
            return _JsonResp({"nonce": "abc123", "queued": True})
        # status poll
        return _JsonResp({
            "nonce": "abc123", "status": "done",
            "accepted": 48, "submitted": 50, "processing_ms": 1820,
        })

    t = WsprNet(
        urlopen=fake_urlopen,
        api_base_url="https://wsprnet.org/api/upload/v1",
        poll_interval_sec=0.01,
    )
    outcome = t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert outcome.kind == "acked"
    assert outcome.reason == "48/50 added"
    # Submit went to <base>/upload, then a status GET to <base>/status/<nonce>.
    assert calls[0] == ("POST", "https://wsprnet.org/api/upload/v1/upload")
    assert ("GET", "https://wsprnet.org/api/upload/v1/status/abc123") in calls


def test_async_status_failed_is_acked():
    def fake_urlopen(req, timeout):
        if req.full_url.endswith("/upload"):
            return _JsonResp({"nonce": "n1", "queued": True})
        return _JsonResp({"nonce": "n1", "status": "failed",
                          "accepted": 0, "submitted": 5})

    t = WsprNet(urlopen=fake_urlopen,
                api_base_url="https://wsprnet.org/api/upload/v1",
                poll_interval_sec=0.01)
    outcome = t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert outcome.kind == "acked"
    assert outcome.reason == "0/5 added"


def test_async_no_nonce_is_acked():
    def fake_urlopen(req, timeout):
        return _JsonResp({"queued": False, "error": "bad"})

    t = WsprNet(urlopen=fake_urlopen,
                api_base_url="https://wsprnet.org/api/upload/v1",
                poll_interval_sec=0.01)
    outcome = t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert outcome.kind == "acked"
    assert "no nonce" in outcome.reason


def test_async_poll_timeout_is_acked():
    """Status never reaches done within poll_max_sec → acked, not retried."""
    def fake_urlopen(req, timeout):
        if req.full_url.endswith("/upload"):
            return _JsonResp({"nonce": "n2", "queued": True})
        return _JsonResp({"nonce": "n2", "status": "processing"})

    t = WsprNet(urlopen=fake_urlopen,
                api_base_url="https://wsprnet.org/api/upload/v1",
                poll_interval_sec=0.01, poll_max_sec=0.05)
    outcome = t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert outcome.kind == "acked"
    assert "pending" in outcome.reason or "processing" in outcome.reason


def test_async_disabled_uses_legacy_url():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        return _FakeResp(status=200)

    t = WsprNet(urlopen=fake_urlopen)  # no api_base_url
    t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert captured["url"] == "http://wsprnet.org/meptspots.php"


def test_async_surfaces_rejections_in_reason():
    def fake_urlopen(req, timeout):
        if req.full_url.endswith("/upload"):
            return _JsonResp({"nonce": "n3", "queued": True})
        return _JsonResp({
            "nonce": "n3", "status": "done", "accepted": 48,
            "submitted": 50, "rejected": 2,
            "rejections": [{"call": "W4UK/P", "reason": "no_grid"},
                           {"call": "AB1CD", "reason": "no_grid"}],
        })
    t = WsprNet(urlopen=fake_urlopen,
                api_base_url="https://wsprnet.org/api/upload/v1",
                poll_interval_sec=0.01)
    oc = t.ship(RecordBatch(records=(_spot(),), cursor_after=b""), _ident())
    assert oc.kind == "acked"
    assert oc.reason == "48/50 added, 2 rejected"
