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
    code=1,
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
    line = _record_to_mept(_spot())
    assert line == "260507 0042 1 -22 0.2 18.106073 KC1KOP FN41 23 0 1"


def test_record_to_mept_drops_grid_when_absent():
    """Compound calls (e.g. ``W1/AJ8S``) frequently have no grid;
    wsprnet's MEPT format omits the grid field rather than embeds an
    empty placeholder.  Matches what wd-upload-wsprnet reads from the
    short ``_spots.txt`` files."""
    line = _record_to_mept(_spot(tx_sign="W1/AJ8S", tx_loc="", snr=-16,
                                 dt=0.8, freq_mhz=18.106155, code=1))
    assert line == "260507 0042 1 -16 0.8 18.106155 W1/AJ8S 23 0 1"


def test_record_to_mept_skips_unresolved_hash():
    """wsprd emits ``<...>`` when a 22-bit hash can't be mapped to a
    real callsign — these are noise, not spots, and must not ship."""
    assert _record_to_mept(_spot(tx_sign="<...>")) is None


def test_record_to_mept_skips_missing_call():
    assert _record_to_mept(_spot(tx_sign="")) is None


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


def test_ship_returns_retry_later_on_5xx():
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(
            req.full_url, 502, "Bad Gateway",
            hdrs={}, fp=BytesIO(b"gateway error"),
        )

    t = WsprNet(urlopen=fake_urlopen)
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "retry_later"
    assert "502" in outcome.reason


def test_ship_returns_retry_later_on_network_error():
    import urllib.error

    def fake_urlopen(req, timeout):
        raise urllib.error.URLError("connection refused")

    t = WsprNet(urlopen=fake_urlopen)
    batch = RecordBatch(records=(_spot(),), cursor_after=b"")
    outcome = t.ship(batch, _ident())
    assert outcome.kind == "retry_later"
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
    assert blob.startswith(b"260507 0042 1 -22 0.2 18.106073 KC1KOP FN41")


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


def test_accepts_wspr_spots_v1():
    assert WsprNet().ACCEPTS == {"wspr.spots": [1]}


def test_primary_table_is_wspr_spots():
    assert WsprNet().primary_table() == "wspr.spots"
