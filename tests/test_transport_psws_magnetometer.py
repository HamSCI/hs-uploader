"""PswsMagnetometerSftp tests — no network; subprocess.run is mocked."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hs_uploader import StationIdentity
from hs_uploader.core import Record, RecordBatch
from hs_uploader.transports.psws_magnetometer import (
    PswsMagnetometerSftp,
    TABLE,
)


def _ident(station_id="S000082", key="/etc/hs-uploader/keys/id_ed25519"):
    return StationIdentity(
        call="AC0G", grid="EM38ww",
        station_id=station_id, ssh_key_file=key,
    )


def _zip_record(tmp_path: Path, date: str = "2026-05-12") -> tuple[Path, Record]:
    z = tmp_path / f"OBS{date}T00:00.zip"
    z.write_bytes(b"PK\x03\x04 ... fake zip body ...")
    rec = Record(
        table=TABLE,
        time=datetime.now(tz=timezone.utc),
        columns={},
        payload_path=z,
    )
    return z, rec


# ---- pure helpers / accessors ------------------------------------------------


def test_accepts_only_mag_daily_zip():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    assert t.ACCEPTS == {"mag.daily_zip": [1]}
    assert t.primary_table() == "mag.daily_zip"


def test_batch_policy_is_single_record():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    assert t.batch_policy().max_records == 1


def test_trigger_dir_name_shape():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    name = t._trigger_dir_name("OBS2026-05-12T00:00")
    # c<dataset>_#<instrument>_#<ts-with-dashes-not-colons>
    assert name.startswith("cOBS2026-05-12T00:00_#RM3100_#")
    ts_part = name.rsplit("_#", 1)[-1]
    # ISO compact: YYYY-MM-DDTHH-MM
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}$", ts_part), ts_part


def test_sftp_user_falls_back_to_identity_station_id():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    assert t._sftp_user(_ident("S000082")) == "S000082"


def test_sftp_user_override_wins():
    t = PswsMagnetometerSftp(instrument_id="RM3100", sftp_user="S111111")
    assert t._sftp_user(_ident("S000082")) == "S111111"


def test_sftp_user_missing_raises():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    with pytest.raises(ValueError, match="station_id is empty"):
        t._sftp_user(_ident(station_id=""))


def test_ssh_key_falls_back_to_identity():
    t = PswsMagnetometerSftp(instrument_id="RM3100")
    ident = _ident(key="/path/to/key")
    assert t._ssh_key(ident) == "/path/to/key"


def test_ssh_key_override_wins():
    t = PswsMagnetometerSftp(
        instrument_id="RM3100",
        ssh_key_file="/override/key",
    )
    ident = _ident(key="/identity/key")
    assert t._ssh_key(ident) == "/override/key"


# ---- ship() dry-run path -----------------------------------------------------


def test_dry_run_returns_acked_without_calling_sftp(tmp_path, caplog):
    z, rec = _zip_record(tmp_path)
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100", dry_run=True)

    with patch("subprocess.run") as run_mock, caplog.at_level("INFO"):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    run_mock.assert_not_called()
    assert any("[dry-run]" in r.message and "would upload" in r.message
               for r in caplog.records)


def test_empty_batch_acked():
    t = PswsMagnetometerSftp(instrument_id="RM3100", dry_run=True)
    outcome = t.ship(RecordBatch(records=[], cursor_after=b""), _ident())
    assert outcome.kind == "acked"


# ---- ship() live (mocked subprocess) -----------------------------------------


def _run_ok(*args, **kwargs):
    res = MagicMock()
    res.returncode = 0
    res.stdout = b""
    res.stderr = b""
    return res


def _run_fail(*args, **kwargs):
    res = MagicMock()
    res.returncode = 2
    res.stdout = b""
    res.stderr = b"Permission denied (publickey).\n"
    return res


def test_ship_invokes_sftp_with_expected_argv(tmp_path):
    z, rec = _zip_record(tmp_path)
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100")

    with patch("subprocess.run", side_effect=_run_ok) as run_mock:
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    run_mock.assert_called_once()
    args, kwargs = run_mock.call_args
    argv = args[0]
    assert argv[0] == "sftp"
    assert "BatchMode=yes" in argv
    assert "-i" in argv and "/etc/hs-uploader/keys/id_ed25519" in argv
    assert argv[-1] == "S000082@pswsnetwork.eng.ua.edu"


def test_ship_sftp_batch_input_has_put_rename_mkdir(tmp_path):
    z, rec = _zip_record(tmp_path, date="2026-05-12")
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100")

    captured = {}
    def _capture(*args, **kwargs):
        captured["input"] = kwargs["input"]
        res = MagicMock(); res.returncode = 0
        res.stdout = b""; res.stderr = b""
        return res

    with patch("subprocess.run", side_effect=_capture):
        t.ship(batch, _ident())

    body = captured["input"].decode()
    # Three operations: put .part, rename to final, mkdir trigger.
    assert 'put "' in body and '.part"' in body
    assert 'rename "' in body and "OBS2026-05-12T00:00.zip" in body
    assert 'mkdir "cOBS2026-05-12T00:00_#RM3100_#' in body
    assert body.rstrip().endswith("quit")


def test_ship_failure_returns_retry_later(tmp_path):
    z, rec = _zip_record(tmp_path)
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100")

    with patch("subprocess.run", side_effect=_run_fail):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "retry_later"
    assert "rc=2" in outcome.reason


def test_ship_missing_zip_permanent_failure(tmp_path):
    """payload_path got deleted between source-emit and ship()."""
    rec = Record(
        table=TABLE,
        time=datetime.now(tz=timezone.utc),
        columns={},
        payload_path=tmp_path / "OBS-nowhere.zip",  # does not exist
    )
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100", dry_run=True)

    outcome = t.ship(batch, _ident())
    assert outcome.kind == "permanent"


def test_ship_no_station_id_permanent_failure(tmp_path):
    z, rec = _zip_record(tmp_path)
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="RM3100", dry_run=True)
    # identity.station_id is "" and no override -> permanent failure
    outcome = t.ship(batch, _ident(station_id=""))
    assert outcome.kind == "permanent"
    assert "station_id" in outcome.reason


# ---- registration ------------------------------------------------------------


def test_transport_exported_from_transports_package():
    from hs_uploader.transports import PswsMagnetometerSftp as exported
    assert exported is PswsMagnetometerSftp


# ---- directory datasets (GRAPE) ---------------------------------------------


def _grape_dataset(tmp_path: Path, date: str = "2026-06-28") -> tuple[Path, Record]:
    """A GRAPE OBS<date>T00-00/ dataset directory (ch0/ + gap_summary.json)."""
    ds = tmp_path / f"OBS{date}T00-00"
    (ds / "ch0").mkdir(parents=True)
    (ds / "ch0" / "data@0.bin").write_bytes(b"\x00\x01\x02\x03")
    (ds / "ch0" / "drf_properties.h5").write_bytes(b"h5")
    (ds / "gap_summary.json").write_text("{}")
    rec = Record(
        table="grape.dataset",
        time=datetime.now(tz=timezone.utc),
        columns={},
        payload_path=ds,
    )
    return ds, rec


def test_custom_table_and_alias():
    from hs_uploader.transports.psws_magnetometer import PswsDatasetSftp
    assert PswsDatasetSftp is PswsMagnetometerSftp
    t = PswsDatasetSftp(instrument_id="367", table="grape.dataset")
    assert t.ACCEPTS == {"grape.dataset": [1]}
    assert t.primary_table() == "grape.dataset"


def test_dir_trigger_keys_off_directory_name(tmp_path):
    ds, _ = _grape_dataset(tmp_path)
    t = PswsMagnetometerSftp(instrument_id="367", table="grape.dataset")
    name = t._trigger_dir_name(ds.name)
    assert name.startswith("cOBS2026-06-28T00-00_#367_#")


def test_dir_upload_batch_has_recursive_mkdir_put_then_trigger(tmp_path):
    ds, rec = _grape_dataset(tmp_path, date="2026-06-28")
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="367", table="grape.dataset")

    captured = {}
    def _capture(*args, **kwargs):
        captured["input"] = kwargs["input"]
        res = MagicMock(); res.returncode = 0
        res.stdout = b""; res.stderr = b""
        return res

    with patch("subprocess.run", side_effect=_capture):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    body = captured["input"].decode()
    lines = body.splitlines()
    # Top-level dataset dir is created first, then the ch0 subdir, then puts.
    assert lines[0] == '-mkdir "OBS2026-06-28T00-00"'  # error-tolerant mkdir
    assert '-mkdir "OBS2026-06-28T00-00/ch0"' in body
    assert 'put "' in body and '/ch0/data@0.bin" "OBS2026-06-28T00-00/ch0/data@0.bin"' in body
    assert 'put "' in body and 'gap_summary.json" "OBS2026-06-28T00-00/gap_summary.json"' in body
    # Trigger dir comes after the data, and there is NO post-upload ls/verify.
    assert '-mkdir "cOBS2026-06-28T00-00_#367_#' in body
    assert "ls " not in body
    trig_idx = next(i for i, l in enumerate(lines) if l.startswith('-mkdir "cOBS'))
    put_idx = max(i for i, l in enumerate(lines) if l.startswith('put "'))
    assert trig_idx > put_idx  # trigger created only after all data is put
    assert lines[-1] == "quit"


def test_dir_upload_failure_retry_later(tmp_path):
    ds, rec = _grape_dataset(tmp_path)
    batch = RecordBatch(records=[rec], cursor_after=b"")
    t = PswsMagnetometerSftp(instrument_id="367", table="grape.dataset")
    with patch("subprocess.run", side_effect=_run_fail):
        outcome = t.ship(batch, _ident())
    assert outcome.kind == "retry_later"
