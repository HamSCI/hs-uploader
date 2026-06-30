"""daemon composition: generic factory + builder entrypoints + manifest load."""

from __future__ import annotations

import textwrap

import pytest

from hs_uploader import daemon
from hs_uploader.core import Pipeline
from hs_uploader.sources import FileTreeSource, FileSpec
from hs_uploader.transports.psws_magnetometer import PswsDatasetSftp
from hs_uploader.watermark.sqlite import SqliteWatermarkStore


def _fake_builder(*, identity, watermark, config, name):
    """A builder entrypoint: returns one Pipeline from the shared identity/wm."""
    src = FileTreeSource(
        root=config["root"],
        specs=[FileSpec(pattern="OBS*", parser=None, table="x.dataset")],
        retention=FileTreeSource.KEEP, match_dirs=True,
    )
    t = PswsDatasetSftp(instrument_id=config.get("instrument_id", "1"),
                        table="x.dataset")
    return [Pipeline(name=name, source=src, transport=t,
                     watermark=watermark, identity=identity)]


def test_resolve_builder():
    fn = daemon.resolve_builder("hs_uploader.core:Uploader")
    from hs_uploader.core import Uploader
    assert fn is Uploader
    with pytest.raises(ValueError):
        daemon.resolve_builder("no_colon")


def test_build_all_pipelines_generic_plus_builder(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: _fake_builder)
    manifest = {
        "identity": {"call": "AC0G/S", "grid": "EM38ww", "station_id": "S000418"},
        "pipeline": [
            {"name": "grape-generic",
             "source": {"type": "filetree", "root": str(tmp_path),
                        "patterns": ["OBS*"], "retention": "keep",
                        "match_dirs": True, "table": "grape.dataset"},
             "transport": {"type": "psws_dataset", "instrument_id": "367",
                           "table": "grape.dataset"}},
            {"name": "via-builder", "builder": "pkg:fn",
             "config": {"root": str(tmp_path), "instrument_id": "9"}},
        ],
    }
    wm = SqliteWatermarkStore(":memory:")
    pipes = daemon.build_all_pipelines(manifest, watermark=wm)
    names = {p.name for p in pipes}
    assert names == {"grape-generic", "via-builder"}
    # builder pipeline received the shared identity + watermark
    vb = next(p for p in pipes if p.name == "via-builder")
    assert vb.identity.station_id == "S000418"
    assert vb.watermark is wm


def test_load_manifest_and_dry_run(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    manifest = tmp_path / "pipelines.toml"
    manifest.write_text(textwrap.dedent(f"""
        [identity]
        call = "AC0G/S"
        grid = "EM38ww"
        ssh_key_file = "/etc/hs-uploader/keys/id_ed25519"

        [daemon]
        pump_interval_sec = 30

        [[pipeline]]
        name = "grape-psws"
        [pipeline.source]
        type = "filetree"
        root = "{tmp_path}"
        patterns = ["OBS*"]
        retention = "keep"
        match_dirs = true
        table = "grape.dataset"
        [pipeline.transport]
        type = "psws_dataset"
        instrument_id = "367"
        table = "grape.dataset"
        [pipeline.identity]
        station_id = "S000418"
    """))
    loaded = daemon.load_manifest(manifest)
    assert loaded["identity"]["call"] == "AC0G/S"
    # dry-run builds pipelines and returns 0 without pumping
    assert daemon.run(manifest, dry_run=True) == 0


def test_run_missing_manifest_returns_error():
    assert daemon.run("/nonexistent/pipelines.toml") == 1
