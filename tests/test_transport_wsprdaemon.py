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
)


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
        "wsprdaemon/spots/AC0G=B1_EM38ww/HF1/14M/210508_1200_wd_spots.txt"
    )


def test_arcname_noise_renamed(tmp_path):
    root = tmp_path
    p = tmp_path / "HF1" / "14M" / "noise" / "20210508_120000_noise.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()
    # Server expects YYMMDD_HHMM (no seconds).
    assert _arcname_for(p, root=root, rx_site="X_Y") == (
        "wsprdaemon/noise/X_Y/HF1/14M/210508_1200_noise.txt"
    )


# ---- tar layout ----


def test_tar_layout_includes_config_and_files(tmp_path):
    root, records = _spool_with_files(tmp_path)
    paths = [r.payload_path for r in records]
    blob = build_wsprdaemon_tar(
        paths, root=root, rx_site="AC0G=B1_EM38ww",
    )
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:bz2") as tf:
        names = sorted(tf.getnames())
    # Mandatory config first.
    assert "wsprdaemon/uploads_config.txt" in names
    # client_upload_info.txt absent on the SFTP path.
    assert "wsprdaemon/client_upload_info.txt" not in names
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
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:bz2") as tf:
        info = tf.extractfile("wsprdaemon/client_upload_info.txt").read()
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
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:bz2") as tf:
            names = sorted(tf.getnames())
            cfg = tf.extractfile("wsprdaemon/uploads_config.txt").read()
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
    with tarfile.open(fileobj=io.BytesIO(captured["bytes"]), mode="r:bz2") as tf:
        names = tf.getnames()
    assert "wsprdaemon/client_upload_info.txt" in names


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
