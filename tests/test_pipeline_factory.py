"""pipeline_factory + daemon composition — manifest → Pipeline objects."""

from __future__ import annotations

import pytest

from hs_uploader.core import Pipeline
from hs_uploader.pipeline_factory import build_pipelines
from hs_uploader.sources import FileTreeSource
from hs_uploader.sources.sqlite import SqliteSource
from hs_uploader.transports.pskreporter import PskReporterTcp
from hs_uploader.transports.psws_magnetometer import PswsDatasetSftp
from hs_uploader.transports.wsprdaemon import WsprdaemonTarFtp, WsprdaemonTarSftp
from hs_uploader.transports.wsprnet import WsprNet
from hs_uploader.sources.wspr_cycle import WsprCycleSource
from hs_uploader.watermark.sqlite import SqliteWatermarkStore


def _wm():
    return SqliteWatermarkStore(":memory:")


def _manifest(tmp_path):
    return {
        "identity": {
            "call": "AC0G/S", "grid": "EM38ww",
            "ssh_key_file": "/etc/hs-uploader/keys/id_ed25519",
        },
        "pipeline": [
            {
                "name": "grape-psws",
                "max_records_per_pump": 64,
                "source": {
                    "type": "filetree", "root": str(tmp_path),
                    "patterns": ["OBS*"], "retention": "keep",
                    "match_dirs": True, "table": "grape.dataset",
                },
                "transport": {
                    "type": "psws_dataset", "instrument_id": "367",
                    "table": "grape.dataset",
                },
                "identity": {"station_id": "S000418"},
            },
            {
                "name": "psk-pskreporter",
                "source": {
                    "type": "sqlite", "database": "psk", "table": "spots",
                    "accepted_schema_versions": [2],
                    "extra_where": [["tx_call", "!=", ""]],
                    "start_at": "now", "delete_on_commit": False,
                },
                "transport": {
                    "type": "pskreporter",
                    "decoding_software": "psk-recorder/0.1",
                },
            },
            {
                "name": "wspr-wsprnet",
                "source": {
                    "type": "sqlite", "database": "wspr", "table": "spots",
                    "accepted_schema_versions": [1, 2],
                },
                "transport": {
                    "type": "wsprnet",
                    "api_base_url": "https://wsprnet.org/api/upload/v1",
                },
            },
        ],
    }


def test_builds_all_generic_pipelines(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    pipes = build_pipelines(_manifest(tmp_path), watermark=_wm())
    assert [p.name for p in pipes] == ["grape-psws", "psk-pskreporter", "wspr-wsprnet"]
    assert all(isinstance(p, Pipeline) for p in pipes)


def test_grape_pipeline_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    grape = build_pipelines(_manifest(tmp_path), watermark=_wm())[0]
    assert isinstance(grape.source, FileTreeSource)
    assert grape.source.match_dirs is True
    assert grape.source.retention == FileTreeSource.KEEP
    assert isinstance(grape.transport, PswsDatasetSftp)
    assert grape.transport.primary_table() == "grape.dataset"
    assert grape.transport.instrument_id == "367"
    assert grape.max_records_per_pump == 64
    # base identity + per-pipeline override
    assert grape.identity.call == "AC0G/S"
    assert grape.identity.station_id == "S000418"
    assert grape.identity.ssh_key_file == "/etc/hs-uploader/keys/id_ed25519"


def test_psk_and_wsprnet_shapes(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    pipes = build_pipelines(_manifest(tmp_path), watermark=_wm())
    psk, wspr = pipes[1], pipes[2]
    assert isinstance(psk.source, SqliteSource)
    assert isinstance(psk.transport, PskReporterTcp)
    assert psk.transport.decoding_software == "psk-recorder/0.1"
    assert isinstance(wspr.transport, WsprNet)


def test_all_pipelines_share_one_watermark(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    wm = _wm()
    pipes = build_pipelines(_manifest(tmp_path), watermark=wm)
    assert all(p.watermark is wm for p in pipes)


def test_builder_entries_skipped_by_factory(tmp_path):
    m = {"pipeline": [{"name": "wsprd", "builder": "x.y:z"}]}
    assert build_pipelines(m, watermark=_wm()) == []


def test_unknown_source_type_raises(tmp_path):
    m = {"pipeline": [{"name": "bad", "source": {"type": "nope"},
                       "transport": {"type": "wsprnet"}}]}
    with pytest.raises(ValueError, match="unknown source type"):
        build_pipelines(m, watermark=_wm())


def test_unknown_transport_type_raises(tmp_path):
    m = {"pipeline": [{"name": "bad",
                       "source": {"type": "filetree", "root": str(tmp_path)},
                       "transport": {"type": "nope"}}]}
    with pytest.raises(ValueError, match="unknown transport type"):
        build_pipelines(m, watermark=_wm())


# ---- wspr_cycle source + wsprdaemon_tar transport (Stage 4) --------------


def _wspr_manifest(tmp_path):
    """Sigma's two generic wspr pipelines: cycle-aligned tar to
    wsprdaemon.org (with FTP-fallback bootstrap) + raw rows to wsprnet.org.
    """
    return {
        "identity": {
            "call": "AC0G/S", "grid": "EM38ww", "station_id": "S000418",
            "ssh_key_file": "/etc/hs-uploader/keys/id_ed25519_host",
        },
        "pipeline": [
            {
                "name": "wspr-wsprdaemon",
                "batch_limit": 10000,
                "retry": {"base": 2.0, "cap_sec": 900.0},
                "source": {
                    "type": "wspr_cycle",
                    "db_path": str(tmp_path / "sink.db"),
                    "start_at": "now", "include_psk": True,
                },
                "transport": {
                    "type": "wsprdaemon_tar",
                    "servers": ["gw1.wsprdaemon.org", "gw2.wsprdaemon.org"],
                    "version": "4.0", "receiver": "AC0G_S",
                    "compression": "bz2", "tar_root": "wsprdaemon",
                    "primary_table_name": "wspr.cycle",
                    "ftp_fallback": {
                        "servers": ["gw2.wsprdaemon.org"],
                        "ftp_user": "noisegraphs",
                        "ftp_password": "xahFie6g",
                        "remote_path": "upload",
                    },
                },
            },
            {
                "name": "wspr-wsprnet",
                "batch_limit": 900,
                "source": {
                    "type": "sqlite", "database": "wspr", "table": "spots",
                    "accepted_schema_versions": [1, 2], "start_at": "now",
                    "delete_on_commit": False,
                    "dedup_partition_by": ["time", "callsign", "band"],
                    "dedup_order_by_desc": "snr_db",
                },
                "transport": {
                    "type": "wsprnet",
                    "api_base_url": "https://wsprnet.org/api/upload/v1",
                    "version": "4.0",
                },
            },
        ],
    }


def test_wspr_cycle_source_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    wd = build_pipelines(_wspr_manifest(tmp_path), watermark=_wm())[0]
    assert isinstance(wd.source, WsprCycleSource)
    assert wd.source.include_psk is True
    assert wd.source.start_at == "now"
    assert wd.source.expected_reporters == set()  # single-rx: no merge gate


def test_wsprdaemon_tar_transport_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    wd = build_pipelines(_wspr_manifest(tmp_path), watermark=_wm())[0]
    assert isinstance(wd.transport, WsprdaemonTarSftp)
    assert wd.transport.primary_table() == "wspr.cycle"
    assert wd.transport.receiver == "AC0G_S"
    assert wd.transport.compression == "bz2"
    assert wd.transport.tar_root == "wsprdaemon"
    # FTP fallback wired for first-cycle pubkey bootstrap.
    assert isinstance(wd.transport.fallback_ftp, WsprdaemonTarFtp)
    assert wd.transport.fallback_ftp.servers == ["gw2.wsprdaemon.org"]
    assert wd.transport.fallback_ftp.ftp_user == "noisegraphs"


def test_wsprdaemon_tar_without_ftp_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    m = _wspr_manifest(tmp_path)
    del m["pipeline"][0]["transport"]["ftp_fallback"]
    wd = build_pipelines(m, watermark=_wm())[0]
    assert wd.transport.fallback_ftp is None


def test_wspr_merge_gate_when_reporters_listed(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    m = _wspr_manifest(tmp_path)
    m["pipeline"][0]["source"]["expected_reporters"] = ["AC0G/B5", "AC0G/B6"]
    m["pipeline"][0]["source"]["backstop_sec"] = 120
    wd = build_pipelines(m, watermark=_wm())[0]
    assert wd.source.expected_reporters == {"AC0G/B5", "AC0G/B6"}
    assert wd.source.backstop_sec == 120


def test_wspr_slot_keys_match_in_process_for_cursor_inheritance(tmp_path, monkeypatch):
    """The factory-built wspr pipelines must produce the SAME watermark
    slot keys (source_id, dest_id, table) as wspr-recorder's in-process
    shim, so the single-host daemon inherits the live cursor on cutover and
    does NOT re-ship the backlog.  These literals are the in-process values
    (see wspr_recorder.hs_uploader_shim): the slot must be independent of
    which SSH key authenticates."""
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    wd, wn = build_pipelines(_wspr_manifest(tmp_path), watermark=_wm())

    def slot(p):
        return (p.source_id(), p.dest_id(), p.transport.primary_table())

    assert slot(wd) == (
        "sqlite:wspr.cycle",
        "wsprdaemon-tar-sftp:gw1.wsprdaemon.org,gw2.wsprdaemon.org",
        "wspr.cycle",
    )
    assert slot(wn) == (
        "sqlite:wspr.spots",
        "wsprnet-async:https://wsprnet.org/api/upload/v1",
        "wspr.spots",
    )
