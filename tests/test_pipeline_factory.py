"""pipeline_factory + daemon composition — manifest → Pipeline objects."""

from __future__ import annotations

import pytest

from hs_uploader.core import Pipeline
from hs_uploader.pipeline_factory import build_pipelines
from hs_uploader.sources import FileTreeSource
from hs_uploader.sources.sqlite import SqliteSource
from hs_uploader.transports.pskreporter import PskReporterTcp
from hs_uploader.transports.psws_magnetometer import PswsDatasetSftp
from hs_uploader.transports.wsprnet import WsprNet
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
