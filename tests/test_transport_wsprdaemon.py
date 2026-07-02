"""WsprdaemonTarSftp / WsprdaemonTarFtp tests.

We mock ``subprocess.run`` to capture SFTP argv + stdin without a real
network, and ``ftplib.FTP`` to capture FTP commands.  The tar layout is
asserted by reading back the bytes the transport built.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hs_uploader import StationIdentity
from hs_uploader.core import Record, RecordBatch
from hs_uploader.transports.wsprdaemon import (
    WsprdaemonTarFtp,
    WsprdaemonTarSftp,
    _arcname_for,
    _build_rx_site,
    build_wsprdaemon_tar,
    COMPRESSION_BZ2,
    COMPRESSION_ZSTD,
)


# Phase 2 PR 5: the transport now defaults to zstd compression. Tests
# that read tar bytes back must sniff the format rather than assume bz2.
_BZ2_MAGIC  = b"BZh"
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def _open_tar_blob(blob: bytes) -> tarfile.TarFile:
    """Return a TarFile open on `blob` regardless of whether it's bz2
    or zstd compressed. Mirrors wsprdaemon-server's sniff logic."""
    if blob.startswith(_ZSTD_MAGIC):
        import zstandard
        raw = zstandard.ZstdDecompressor().decompress(blob)
        return tarfile.open(fileobj=io.BytesIO(raw), mode="r:")
    if blob.startswith(_BZ2_MAGIC):
        return tarfile.open(fileobj=io.BytesIO(blob), mode="r:bz2")
    raise ValueError(f"unknown tar magic: {blob[:8]!r}")


def _ident(call="AC0G/B1", grid="EM38ww", key="/etc/hs-uploader/keys/id_ed25519"):
    return StationIdentity(call=call, grid=grid, ssh_key_file=key)


def _spool_with_files(tmp_path: Path) -> tuple[Path, list[Record]]:
    files = []
    layout = [
        ("HF1/14M/210508_1200_wd_spots.txt", "wd spot line 1"),
        ("HF1/14M/210508_1202_wd_spots.txt", "wd spot line 2"),
        ("HF1/14M/noise/20210508_120000_noise.txt", "noise line"),
    ]
    records = []
    for rel, body in layout:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
        files.append(p)
        records.append(
            Record(
                table="wspr.spots",
                time=__import__("datetime").datetime.now(
                    tz=__import__("datetime").timezone.utc,
                ),
                columns={},
                payload_path=p,
            )
        )
    return tmp_path, records


# ---- pure helpers ----


def test_rx_site_format():
    assert _build_rx_site("AC0G/B1", "EM38ww") == "AC0G=B1_EM38ww"
    assert _build_rx_site("K1ABC", "FN42aa") == "K1ABC_FN42aa"
    assert _build_rx_site("AC0G/B1", "") == "AC0G=B1"


def test_arcname_spot(tmp_path):
    root = tmp_path
    p = tmp_path / "HF1" / "14M" / "210508_1200_wd_spots.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()
    assert _arcname_for(p, root=root, rx_site="AC0G=B1_EM38ww") == (
        "wspr/spots/AC0G=B1_EM38ww/HF1/14M/210508_1200_wd_spots.txt"
    )


def test_arcname_noise_renamed(tmp_path):
    root = tmp_path
    p = tmp_path / "HF1" / "14M" / "noise" / "20210508_120000_noise.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()
    # Server expects YYMMDD_HHMM (no seconds).
    assert _arcname_for(p, root=root, rx_site="X_Y") == (
        "wspr/noise/X_Y/HF1/14M/210508_1200_noise.txt"
    )


# ---- tar layout ----


def test_tar_layout_includes_config_and_files(tmp_path):
    root, records = _spool_with_files(tmp_path)
    paths = [r.payload_path for r in records]
    blob = build_wsprdaemon_tar(
        paths, root=root, rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = sorted(tf.getnames())
    # Mandatory config first.
    assert "uploads_config.txt" in names
    # client_upload_info.txt absent on the SFTP path.
    assert "client_upload_info.txt" not in names
    # All three spool files mapped to canonical arcnames.
    assert any(
        n.endswith("HF1/14M/210508_1200_wd_spots.txt") for n in names
    )
    assert any("HF1/14M/210508_1200_noise.txt" in n for n in names)


def test_tar_with_client_info(tmp_path):
    root, records = _spool_with_files(tmp_path)
    paths = [r.payload_path for r in records]
    blob = build_wsprdaemon_tar(
        paths, root=root, rx_site="X_Y",
        client_info=("AC0G/B1", "ssh-ed25519 AAAAC3... fake"),
    )
    with _open_tar_blob(blob) as tf:
        info = tf.extractfile("client_upload_info.txt").read()
    assert b"reporter_id=AC0G/B1" in info
    assert b"ssh_public_key=ssh-ed25519 AAAAC3..." in info


# ---- WsprdaemonTarSftp ----


def test_sftp_ship_invokes_subprocess_with_correct_args(tmp_path):
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarSftp(
        servers=["gw1.wsprdaemon.org"],
        spool_root=root,
        upload_id="AC0G_B1",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    captured = {}
    def fake_run(cmd, input, capture_output, timeout):
        captured["cmd"] = cmd
        captured["input"] = input
        out = MagicMock()
        out.returncode = 0
        out.stdout = b""
        out.stderr = b""
        return out

    with patch("subprocess.run", side_effect=fake_run):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "acked"
    cmd = captured["cmd"]
    assert cmd[0] == "sftp"
    assert "-b" in cmd and "-" in cmd
    assert "BatchMode=yes" in " ".join(cmd)
    # SSH key option present.
    assert any("/etc/hs-uploader/keys/id_ed25519" in arg for arg in cmd)
    # Login as call-with-slash-replaced.
    assert cmd[-1] == "AC0G_B1@gw1.wsprdaemon.org"
    # Batch input has put + rename for .part-then-rename convention.
    text = captured["input"].decode()
    assert "put " in text
    assert text.count(".part") == 2  # .part appears in both put and rename src
    assert "rename " in text


def test_sftp_falls_through_servers_on_failure(tmp_path):
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarSftp(
        servers=["gw1", "gw2"],
        spool_root=root,
        upload_id="X",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    calls = []
    def fake_run(cmd, input, capture_output, timeout):
        calls.append(cmd[-1])  # the user@host arg
        out = MagicMock()
        out.returncode = 1   # fail every time
        out.stdout = b"connection refused"
        out.stderr = b""
        return out

    with patch("subprocess.run", side_effect=fake_run):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "retry_later"
    assert "AC0G_B1@gw1" in calls
    assert "AC0G_B1@gw2" in calls


def test_sftp_host_key_change_triggers_one_retry(tmp_path):
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarSftp(
        servers=["gw1"],
        spool_root=root,
        upload_id="X",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    attempts = [0]
    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        if cmd[0] == "ssh-keygen":
            # known_hosts cleanup — pretend it succeeded.
            out = MagicMock(); out.returncode = 0
            out.stdout = b""; out.stderr = b""
            return out
        attempts[0] += 1
        out = MagicMock()
        if attempts[0] == 1:
            out.returncode = 1
            out.stderr = b"REMOTE HOST IDENTIFICATION HAS CHANGED!"
            out.stdout = b""
        else:
            out.returncode = 0
            out.stderr = b""; out.stdout = b""
        return out

    with patch("subprocess.run", side_effect=fake_run):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "acked"
    assert attempts[0] == 2  # initial + one retry


def test_sftp_serialize_for_retry_is_deterministic(tmp_path):
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarSftp(
        servers=["gw1"],
        spool_root=root,
        upload_id="X",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    a = transport.serialize_for_retry(batch, _ident())
    b = transport.serialize_for_retry(batch, _ident())
    # Tarballs include mtime fields — they may differ on second build.
    # We only assert both are valid bzip2 tars containing the same set
    # of arcnames + the same uploads_config.txt body.
    def _names_and_config(blob):
        with _open_tar_blob(blob) as tf:
            names = sorted(tf.getnames())
            cfg = tf.extractfile("uploads_config.txt").read()
        return names, cfg
    assert _names_and_config(a) == _names_and_config(b)


# ---- WsprdaemonTarFtp ----


def test_ftp_ship_invokes_storbinary(tmp_path):
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarFtp(
        servers=["gw2.wsprdaemon.org"],
        spool_root=root,
        ftp_password="secret",
        upload_id="AC0G_B1",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    captured = {}
    fake_ftp = MagicMock()
    def fake_ftp_init(timeout=None):
        return fake_ftp

    def fake_storbinary(cmd, fh):
        captured["cmd"] = cmd
        captured["bytes"] = fh.read()

    fake_ftp.__enter__ = MagicMock(return_value=fake_ftp)
    fake_ftp.__exit__ = MagicMock(return_value=None)
    fake_ftp.storbinary.side_effect = fake_storbinary

    with patch("ftplib.FTP", fake_ftp_init):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "acked"
    fake_ftp.connect.assert_called_with("gw2.wsprdaemon.org")
    fake_ftp.login.assert_called_with(user="noisegraphs", passwd="secret")
    fake_ftp.set_pasv.assert_called_with(True)
    assert captured["cmd"].startswith("STOR upload/AC0G_B1_")
    assert captured["cmd"].endswith(".tbz")
    # Tar should contain client_upload_info.txt (FTP path adds it).
    with _open_tar_blob(captured["bytes"]) as tf:
        names = tf.getnames()
    assert "client_upload_info.txt" in names


def test_ftp_falls_through_servers_on_error(tmp_path):
    import ftplib
    root, records = _spool_with_files(tmp_path)
    transport = WsprdaemonTarFtp(
        servers=["gw1", "gw2"],
        spool_root=root,
        ftp_password="x",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    calls = []
    def fake_ftp_init(timeout=None):
        f = MagicMock()
        f.__enter__ = MagicMock(return_value=f)
        f.__exit__ = MagicMock(return_value=None)
        def conn(host):
            calls.append(host)
            raise OSError("network unreachable")
        f.connect.side_effect = conn
        return f

    with patch("ftplib.FTP", fake_ftp_init):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "retry_later"
    assert calls == ["gw1", "gw2"]


# ---- SqliteSource (records carrying `columns`) path ----

from hs_uploader.transports.wsprdaemon import (  # noqa: E402
    build_wsprdaemon_tar_from_records,
    _format_extended_line,
    _format_short_line,
    _derive_geo,
    _ts_from_iso,
)


def _row_v2(
    *,
    band: str = "30",
    mode: str = "W2",
    time: str = "2026-05-14T11:58:00Z",
    callsign: str = "VE7SAR",
    grid: str = "CN89",
    freq_hz: int = 10_140_125,
    snr: int = -22,
    dt: float = 0.27,
    drift_hz_per_s: float = 0.0,
    pwr_dbm: int = 33,
    sync_quality: float = 0.27,
    cycles: int = 1,
    jitter: int = 0,
    blocksize: int = 1,
    metric: float = 0.32,
    decodetype: int = 1,
    ipass: int = 0,
    nhardmin: int = 0,
    pkt_mode: int = 2,
) -> dict:
    return {
        "time": time, "band": band, "mode": mode,
        "callsign": callsign, "grid": grid,
        "frequency_hz": freq_hz, "snr_db": snr, "dt": dt,
        "drift_hz_per_s": drift_hz_per_s, "pwr_dbm": pwr_dbm,
        "sync_quality": sync_quality,
        "cycles": cycles, "jitter": jitter, "blocksize": blocksize,
        "metric": metric, "decodetype": decodetype, "ipass": ipass,
        "nhardmin": nhardmin, "pkt_mode": pkt_mode,
        "rx_call": "AC0G/B1", "rx_grid": "EM38ww",
        "schema_version": 2,
    }


def _record_from_row(row: dict):
    """SqliteSource yields a Record with columns set and payload_path=None."""
    from datetime import datetime, timezone
    return Record(
        table="wspr.spots",
        time=datetime.fromisoformat(row["time"].replace("Z", "+00:00")),
        columns=row,
        payload_path=None,
    )


def test_loc_to_lat_lon_matches_wd_extend_spots():
    """Geodesy: parity with wsprdaemon-client/bin/wd-extend-spots.

    EM38ww (AC0G's grid) should give the same lat/lon a v1 lookup
    produces — see wd-extend-spots._loc_to_lat_lon for the canonical
    formula.  We use the formula's output as ground truth here so any
    future drift is visible.
    """
    from hs_uploader.transports.wsprdaemon import _loc_to_lat_lon
    lat, lon = _loc_to_lat_lon("EM38ww")
    # Known-good values produced by wd-extend-spots' formula for EM38ww:
    assert abs(lat - 38.9375) < 0.01
    assert abs(lon - (-92.125)) < 0.01


def test_ts_from_iso_round_trip():
    d, h, fn = _ts_from_iso("2026-05-14T11:58:00Z")
    assert d == "260514"
    assert h == "1158"
    assert fn == "20260514_115800"


def test_extended_line_w2_has_34_whitespace_fields():
    line = _format_extended_line(_row_v2(), rx_call="AC0G/B1", rx_grid="EM38ww")
    # whitespace-split must yield 34 columns (the extended-format invariant).
    assert len(line.split()) == 34


def test_short_line_f2_has_11_whitespace_fields():
    line = _format_short_line(_row_v2(mode="F2", pkt_mode=3), w_mode=False)
    # 11-field MEPT
    assert len(line.split()) == 11
    # F-mode line hardcodes drift = 0 (10th field).
    parts = line.split()
    assert parts[9] == "0"


def test_tar_from_records_layout(tmp_path):
    rows = [
        _row_v2(band="30", mode="W2", callsign="VE7SAR"),
        _row_v2(band="30", mode="W2", callsign="N0STR", time="2026-05-14T11:58:00Z"),
        _row_v2(band="40", mode="F2", callsign="K2TQC",
                time="2026-05-14T11:58:00Z", pkt_mode=3),
    ]
    records = [_record_from_row(r) for r in rows]
    blob = build_wsprdaemon_tar_from_records(
        records,
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD",
        rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = set(tf.getnames())
    assert "uploads_config.txt" in names
    # W-mode rows are grouped into one _wd_spots.txt file
    assert any(
        n.startswith("wspr/spots/AC0G=B1_EM38ww/KA9Q_T3FD/30/")
        and n.endswith("_W2_20260514_115800_wd_spots.txt")
        for n in names
    )
    # F-mode rows produce a short _spots.txt file
    assert any(
        n.startswith("wspr/spots/AC0G=B1_EM38ww/KA9Q_T3FD/40/")
        and n.endswith("_F2_20260514_115800_spots.txt")
        for n in names
    )


def test_tar_from_records_per_band_grouping(tmp_path):
    """Two W-mode rows from different bands at the same cycle land in
    separate per-band files, not merged."""
    records = [
        _record_from_row(_row_v2(band="30", mode="W2", callsign="A")),
        _record_from_row(_row_v2(band="40", mode="W2", callsign="B")),
    ]
    blob = build_wsprdaemon_tar_from_records(
        records,
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD",
        rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        bands = sorted(
            n.split("/")[4] for n in tf.getnames()
            if n.startswith("wspr/spots/")
        )
    assert bands == ["30", "40"]


def test_tar_from_records_per_receiver_rx_site(tmp_path):
    """Multi-RX merge: rows from different receivers (distinct rx_call /
    radiod_id) on the same band+cycle land under per-receiver RX_SITE
    subtrees, even though the uploader's own identity is the bare base
    call.  This is what lets ONE merge-uploader post the union to
    wsprnet as `AC0G` while wsprdaemon.org still sees per-receiver
    diversity attribution (AC0G/B4, /B5, /B6)."""
    def _rx_row(rx_call, radiod_id, snr):
        r = _row_v2(band="20", mode="W2", callsign="K7GXB", snr=snr)
        r["rx_call"] = rx_call
        r["radiod_id"] = radiod_id
        return r

    records = [
        _record_from_row(_rx_row("AC0G/B4", "B4-100-rx888mk2-status.local", -13)),
        _record_from_row(_rx_row("AC0G/B5", "bee1-status.local", -9)),
        _record_from_row(_rx_row("AC0G/B6", "bee2-status.local", -11)),
    ]
    # Uploader identity is the BARE merge call — must NOT leak into the
    # per-row RX_SITE attribution.
    blob = build_wsprdaemon_tar_from_records(
        records,
        rx_call="AC0G", rx_grid="EM38ww",
        receiver="merge", rx_site="AC0G_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        spot_names = sorted(
            n for n in tf.getnames() if n.startswith("wspr/spots/")
        )
    # Three distinct RX_SITE subtrees, one per receiver, none under the
    # bare uploader identity.
    rx_sites = sorted(n.split("/")[2] for n in spot_names)
    assert rx_sites == ["AC0G=B4_EM38ww", "AC0G=B5_EM38ww", "AC0G=B6_EM38ww"]
    # RECEIVER segment derives from radiod_id (mDNS suffix stripped).
    receivers = sorted(n.split("/")[3] for n in spot_names)
    assert receivers == ["B4-100-rx888mk2", "bee1", "bee2"]
    # The bare uploader identity never appears as an RX_SITE.
    assert not any("/AC0G_EM38ww/" in n for n in spot_names)


def test_sftp_ship_via_sqlite_source(tmp_path):
    """End-to-end: WsprdaemonTarSftp with no spool_root, records carry
    columns; receiver= is passed; SFTP fakery confirms upload happens."""
    rows = [_row_v2(callsign="VE7SAR")]
    records = [_record_from_row(r) for r in rows]
    batch = RecordBatch(records=tuple(records), cursor_after=b"cursor1")

    transport = WsprdaemonTarSftp(
        servers=["gw1.example"],
        receiver="KA9Q_T3FD",
        # spool_root deliberately omitted — SqliteSource path takes over.
    )

    captured = {}
    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["stdin"] = kwargs.get("input", b"")
        result = MagicMock()
        result.returncode = 0
        result.stdout = b""
        result.stderr = b""
        return result

    with patch("subprocess.run", side_effect=fake_run):
        outcome = transport.ship(batch, _ident())

    assert outcome.kind == "acked"
    assert "sftp" in captured["cmd"][0]


def test_ship_falls_back_to_acked_when_batch_empty():
    """No payload_path AND no columns → nothing to do, acked."""
    transport = WsprdaemonTarSftp(servers=["gw1"], receiver="KA9Q_T3FD")
    # Record with empty columns + no payload_path: nothing shippable.
    empty = Record(
        table="wspr.spots",
        time=__import__("datetime").datetime.now(
            tz=__import__("datetime").timezone.utc,
        ),
        columns={}, payload_path=None,
    )
    batch = RecordBatch(records=(empty,), cursor_after=b"")
    outcome = transport.ship(batch, _ident())
    assert outcome.kind == "acked"


def _noise_row(
    *,
    band: str = "30",
    time: str = "2026-05-14T11:58:00Z",
    rms_noise_dbm: float = -85.5,
    fft_noise_dbm: float = -148.3,
    overload_count: int = 0,
) -> dict:
    return {
        "time": time, "band": band,
        "rms_noise_dbm": rms_noise_dbm,
        "fft_noise_dbm": fft_noise_dbm,
        "overload_count": overload_count,
        "rx_call": "AC0G/B1", "rx_grid": "EM38ww",
        "schema_version": 1,
    }


def _noise_record(row: dict):
    from datetime import datetime
    return Record(
        table="wspr.noise",
        time=datetime.fromisoformat(row["time"].replace("Z", "+00:00")),
        columns=row,
        payload_path=None,
    )


def test_noise_line_has_15_whitespace_fields():
    """v1's `_compute_noise_line` writes 15 columns; emit the same."""
    from hs_uploader.transports.wsprdaemon import _format_noise_line
    line = _format_noise_line(_noise_row())
    assert len(line.split()) == 15
    # Field 13 = rms_noise_dbm; field 14 = fft_noise_dbm.
    parts = line.split()
    assert abs(float(parts[12]) - (-85.5)) < 0.01
    assert abs(float(parts[13]) - (-148.3)) < 0.01
    assert parts[14] == "0"


def test_tar_from_records_includes_noise_files(tmp_path):
    """Mixed batch (spots + noise) lands them in their respective
    arcnames: wspr/spots/... and wspr/noise/..."""
    records = [
        _record_from_row(_row_v2(band="30", mode="W2", callsign="VE7SAR")),
        _noise_record(_noise_row(band="30")),
        _noise_record(_noise_row(band="40", rms_noise_dbm=-90.2)),
    ]
    blob = build_wsprdaemon_tar_from_records(
        records,
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD",
        rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = sorted(tf.getnames())
    # Two noise arcnames under wspr/noise/.../<band>/
    noise_arcs = [n for n in names if n.startswith("wspr/noise/")]
    assert len(noise_arcs) == 2
    assert any("/30/" in n and n.endswith("_noise.txt") for n in noise_arcs)
    assert any("/40/" in n and n.endswith("_noise.txt") for n in noise_arcs)
    # Spot arcname still present.
    assert any(n.startswith("wspr/spots/") for n in names)


def test_noise_only_batch_produces_only_noise_tar(tmp_path):
    """A batch with no spot rows (just noise) is valid — produces a
    noise-only tar.  wsprdaemon.org accepts it."""
    records = [_noise_record(_noise_row())]
    blob = build_wsprdaemon_tar_from_records(
        records,
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD",
        rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    assert any(n.startswith("wspr/noise/") for n in names)
    assert not any(n.startswith("wspr/spots/") for n in names)


def test_sqlite_path_requires_receiver_set():
    """If a SqliteSource record arrives but receiver wasn't configured,
    surface the misconfiguration as a permanent failure rather than
    tar-building with a bad arcname."""
    transport = WsprdaemonTarSftp(servers=["gw1"])  # no receiver=
    records = [_record_from_row(_row_v2())]
    batch = RecordBatch(records=tuple(records), cursor_after=b"")
    outcome = transport.ship(batch, _ident())
    # Outcome.permanent_failure() sets kind="permanent"; the cause
    # lives in .reason (not .message).
    assert outcome.kind == "permanent"
    assert "receiver=" in outcome.reason


# ---- Phase 2 PR 5: psk.spots records → ft8/ft4 JSONL trees + routing.json ----


def _psk_row(
    *,
    mode: str = "ft8",
    band: int = 20,
    time: str = "2026-05-18T14:30:15+00:00",
    frequency: int = 14_074_580,
    receiver: str = "KA9Q_RX888",
    tx_call: str = "K1ABC",
    grid: str = "FN42",
    score: int = 50,
    snr_db: int = -12,
    dt: float = 0.27,
    message: str = "AC0G/B1 K1ABC FN42",
    forward_to_pskreporter: bool = True,
    schema_version: int = 2,
) -> dict:
    return {
        "time": time, "mode": mode, "decoder_kind": "jt9",
        "frequency": frequency, "band": band, "receiver": receiver,
        "rx_sign": "AC0G/B1", "rx_loc": "EM38ww",
        "tx_call": tx_call, "grid": grid, "score": score,
        "snr_db": snr_db, "dt": dt, "message": message,
        "host_call": "AC0G/B1", "host_grid": "EM38ww",
        "radiod_id": "my-rx888",
        "processing_version": "psk-recorder/0.6.1+test",
        "forward_to_pskreporter": forward_to_pskreporter,
        "schema_version": schema_version,
    }


def _psk_record(row: dict):
    from datetime import datetime
    return Record(
        table="psk.spots",
        time=datetime.fromisoformat(row["time"].replace("Z", "+00:00")),
        columns=row,
        payload_path=None,
    )


def test_psk_records_emit_jsonl_under_mode_peer_dir():
    """An ft8 row should land at ft8/<RX_SITE>/<RECEIVER>/<BAND>/...jsonl,
    NOT under wspr/. Each mode is a peer of wspr at the tar root."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    import json as _j
    records = [_psk_record(_psk_row(mode="ft8", band=20))]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
        ft8_files = [n for n in names if n.startswith("ft8/")]
        assert len(ft8_files) == 1, names
        n = ft8_files[0]
        # RECEIVER segment is the cleaned radiod_id (the per-receiver
        # identity; the uploader-level `receiver` arg is only a fallback
        # when a row has no radiod_id/rx_source — see 80bccdb). _psk_row
        # sets radiod_id="my-rx888"; the production `receiver` column is
        # empty, so radiod_id is what distinguishes bee1/bee2/B4 on wd30.
        assert n.startswith("ft8/AC0G=B1_EM38ww/my-rx888/20/"), n
        assert n.endswith("_ft8.jsonl"), n
        content = tf.extractfile(n).read().decode().splitlines()
    assert len(content) == 1
    parsed = _j.loads(content[0])
    assert parsed["mode"] == "ft8"
    assert parsed["frequency"] == 14_074_580
    assert parsed["forward_to_pskreporter"] is True


def test_psk_records_emit_routing_json_at_root():
    """Routing.json appears at root only when psk records are present."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    import json as _j
    records = [_psk_record(_psk_row(forward_to_pskreporter=True))]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
        assert "routing.json" in names
        routing = _j.loads(tf.extractfile("routing.json").read())
    # All flags True → collapse to a single default entry.
    assert routing == {"default": {"forward_to_pskreporter": True}}


def test_no_routing_json_when_no_psk_records():
    """A pure-WSPR tar must not carry routing.json."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    records = [_record_from_row(_row_v2())]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    assert "routing.json" not in names


def test_psk_records_routing_collapses_when_all_forward_false():
    """Operator on PSK_DELIVERY_MODE=both: every row carries
    forward=False. routing.json must collapse to one default entry,
    not a verbose per-receiver list, so the wire payload stays small."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    import json as _j
    records = [_psk_record(_psk_row(forward_to_pskreporter=False))
               for _ in range(3)]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        routing = _j.loads(tf.extractfile("routing.json").read())
    assert routing == {"default": {"forward_to_pskreporter": False}}


def test_psk_records_routing_emits_per_receiver_overrides_when_mixed():
    """Two receivers (distinct radiod_id) with different forwarding intent
    → default plus the minority as an explicit override.

    The per-receiver key is ``<RX_SITE>/<radiod_id>``: in production the
    psk row's ``receiver`` column is empty and radiod_id carries the
    per-receiver identity (bee1/bee2/B4), so routing.json and the tar
    paths both discriminate on radiod_id (see 80bccdb)."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    import json as _j

    def _row(radiod_id, fwd):
        r = _psk_row(forward_to_pskreporter=fwd)
        r["radiod_id"] = radiod_id
        return r

    records = [
        _psk_record(_row("rxA", True)),
        _psk_record(_row("rxA", True)),
        _psk_record(_row("rxB", False)),
    ]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="rxA", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        routing = _j.loads(tf.extractfile("routing.json").read())
    assert routing["default"]["forward_to_pskreporter"] is True
    assert routing["AC0G=B1_EM38ww/rxB"]["forward_to_pskreporter"] is False
    # rxA matches default → omitted from override map.
    assert "AC0G=B1_EM38ww/rxA" not in routing


def test_mixed_batch_packs_wspr_and_ft8_into_one_tar():
    """A batch with both wspr.spots and psk.spots records must produce
    one tar carrying both mode subtrees + routing.json."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    records = [
        _record_from_row(_row_v2()),
        _psk_record(_psk_row(mode="ft8")),
        _psk_record(_psk_row(mode="ft4", time="2026-05-18T14:30:22+00:00")),
    ]
    blob = build_wsprdaemon_tar_from_records(
        records, rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_T3FD", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    assert any(n.startswith("wspr/spots/") for n in names)
    assert any(n.startswith("ft8/") for n in names)
    assert any(n.startswith("ft4/") for n in names)
    assert "routing.json" in names


# ---- band derivation for psk rows missing 'band' ----


def test_psk_band_derived_from_frequency_when_missing():
    """psk-recorder's ChTailer doesn't set 'band' on rows — the tar
    builder must derive it from `frequency` so the arcname segment is
    the canonical metre-band (20, 40, ...). Regression for the
    Phase 2 producer-side wire-up where band=0 paths were silently
    dropped by the wsprdaemon-server's band_str_to_meters() check.
    """
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    row = _psk_row(mode="ft8", frequency=14_074_580)
    row.pop("band", None)  # producer doesn't set it
    blob = build_wsprdaemon_tar_from_records(
        [_psk_record(row)],
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    ft8_files = [n for n in names if n.startswith("ft8/")]
    assert len(ft8_files) == 1, names
    # 14.074 MHz → 20m. Arcname segment must be "20", not "0".
    assert "/20/" in ft8_files[0], ft8_files[0]


def test_psk_band_zero_when_frequency_outside_ham_bands():
    """Edge case: frequency outside known ham bands → band=0, server
    will warn + skip. We don't want to error in the tar builder.
    """
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    row = _psk_row(mode="ft8", frequency=0)
    row.pop("band", None)
    blob = build_wsprdaemon_tar_from_records(
        [_psk_record(row)],
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    ft8_files = [n for n in names if n.startswith("ft8/")]
    assert len(ft8_files) == 1, names
    assert "/0/" in ft8_files[0], ft8_files[0]


def test_psk_explicit_band_wins_over_derived():
    """If the producer DOES set band explicitly, that value wins."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar_from_records
    # 14 MHz would derive to 20m, but producer says 17m.
    row = _psk_row(mode="ft8", frequency=14_074_580, band=17)
    blob = build_wsprdaemon_tar_from_records(
        [_psk_record(row)],
        rx_call="AC0G/B1", rx_grid="EM38ww",
        receiver="KA9Q_RX888", rx_site="AC0G=B1_EM38ww",
    )
    with _open_tar_blob(blob) as tf:
        names = tf.getnames()
    ft8_files = [n for n in names if n.startswith("ft8/")]
    assert "/17/" in ft8_files[0], ft8_files[0]


# ---- compression knob ----


def test_default_compression_is_bz2():
    """bz2 -9 is the default: smaller tars than zstd (B4-100 benchmark)
    and dependency-free, the operator's chosen trade (size over CPU)."""
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar
    blob = build_wsprdaemon_tar(
        [], root=Path("/tmp"), rx_site="X_Y",
    )
    # First 3 bytes are the bz2 magic (BZh).
    assert blob[:3] == _BZ2_MAGIC


def test_explicit_zstd_still_works():
    """zstd stays selectable for hosts that install the package and
    prefer speed over size."""
    pytest.importorskip("zstandard")
    from hs_uploader.transports.wsprdaemon import build_wsprdaemon_tar
    blob = build_wsprdaemon_tar(
        [], root=Path("/tmp"), rx_site="X_Y",
        compression="zstd", compression_level=9,
    )
    assert blob[:4] == _ZSTD_MAGIC


def test_transport_carries_compression_setting():
    """Constructor-level compression flows into the built tar."""
    from hs_uploader.transports.wsprdaemon import WsprdaemonTarSftp
    t = WsprdaemonTarSftp(servers=["gw1"], compression=COMPRESSION_ZSTD)
    assert t.compression == COMPRESSION_ZSTD
    t_default = WsprdaemonTarSftp(servers=["gw1"])
    assert t_default.compression == COMPRESSION_BZ2


def test_accepts_includes_psk_spots():
    """ACCEPTS must advertise psk.spots so the orchestrator routes the
    psk source to this transport."""
    from hs_uploader.transports.wsprdaemon import WsprdaemonTarSftp, WsprdaemonTarFtp
    assert "psk.spots" in WsprdaemonTarSftp.ACCEPTS
    assert "psk.spots" in WsprdaemonTarFtp.ACCEPTS
    # Schema version 2 = ch_tailer's current write tag.
    assert WsprdaemonTarSftp.ACCEPTS["psk.spots"] == [2]


# ---- station key auto-generation (gap #1: ensure_ssh_key was never called) ----


def test_ftp_bootstrap_autogenerates_key_and_ships_pubkey(tmp_path):
    """A fresh site (no keypair on disk) must generate one so the FTP
    bootstrap ships a real pubkey in client_upload_info.txt — an empty
    ssh_public_key means the gateway can never provision SFTP."""
    root, records = _spool_with_files(tmp_path)
    key = tmp_path / "keys" / "id_ed25519"
    ident = _ident(key=str(key))
    transport = WsprdaemonTarFtp(
        servers=["gw2"],
        spool_root=root,
        ftp_password="x",
        upload_id="AC0G_B1",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    captured = {}
    fake_ftp = MagicMock()
    fake_ftp.__enter__ = MagicMock(return_value=fake_ftp)
    fake_ftp.__exit__ = MagicMock(return_value=None)
    fake_ftp.storbinary.side_effect = (
        lambda cmd, fh: captured.__setitem__("bytes", fh.read())
    )

    with patch("ftplib.FTP", lambda timeout=None: fake_ftp):
        outcome = transport.ship(batch, ident)

    assert outcome.kind == "acked"
    # Keypair generated on first use (real ssh-keygen).
    assert key.exists()
    assert (tmp_path / "keys" / "id_ed25519.pub").exists()
    with _open_tar_blob(captured["bytes"]) as tf:
        info = tf.extractfile("client_upload_info.txt").read().decode()
    assert "ssh_public_key=ssh-ed25519 " in info


def test_sftp_upload_autogenerates_key(tmp_path):
    """The SFTP path ensures the key exists before invoking sftp -i."""
    root, records = _spool_with_files(tmp_path)
    key = tmp_path / "keys" / "id_ed25519"
    transport = WsprdaemonTarSftp(
        servers=["gw1"],
        spool_root=root,
        upload_id="AC0G_B1",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    captured = {}

    def fake_run(cmd, **kw):
        if cmd[0] == "ssh-keygen":
            # Stand in for real keygen so the global subprocess patch
            # doesn't hide the generation call.
            path = Path(cmd[cmd.index("-f") + 1])
            path.write_text("PRIVATE")
            Path(str(path) + ".pub").write_text("ssh-ed25519 FAKE k")
            return MagicMock(returncode=0)
        captured["cmd"] = cmd
        out = MagicMock()
        out.returncode = 0
        out.stdout = b""
        out.stderr = b""
        return out

    with patch("subprocess.run", side_effect=fake_run):
        outcome = transport.ship(batch, _ident(key=str(key)))

    assert outcome.kind == "acked"
    assert key.exists()
    assert any(str(key) in arg for arg in captured["cmd"])


def test_key_generation_failure_is_nonfatal(tmp_path):
    """Keygen failure (unwritable dir, no ssh-keygen) must not kill the
    pump — the ship proceeds with an empty pubkey as before."""
    root, records = _spool_with_files(tmp_path)

    class BrokenIdent(StationIdentity):
        def ensure_ssh_key(self):
            raise OSError("read-only key dir")

    ident = BrokenIdent(
        call="AC0G/B1", grid="EM38ww",
        ssh_key_file=str(tmp_path / "nope" / "id_ed25519"),
    )
    transport = WsprdaemonTarFtp(
        servers=["gw2"],
        spool_root=root,
        ftp_password="x",
        upload_id="AC0G_B1",
    )
    batch = RecordBatch(records=tuple(records), cursor_after=b"")

    captured = {}
    fake_ftp = MagicMock()
    fake_ftp.__enter__ = MagicMock(return_value=fake_ftp)
    fake_ftp.__exit__ = MagicMock(return_value=None)
    fake_ftp.storbinary.side_effect = (
        lambda cmd, fh: captured.__setitem__("bytes", fh.read())
    )

    with patch("ftplib.FTP", lambda timeout=None: fake_ftp):
        outcome = transport.ship(batch, ident)

    assert outcome.kind == "acked"
    with _open_tar_blob(captured["bytes"]) as tf:
        info = tf.extractfile("client_upload_info.txt").read().decode()
    assert "ssh_public_key=\n" in info
