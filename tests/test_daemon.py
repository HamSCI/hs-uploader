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
    # The daemon opens the watermark store even for a dry run, and
    # default_path() falls back to /var/lib/hs-uploader.  Without this the
    # test reaches for the real system path: it passes on a station, where
    # that directory exists, and fails everywhere else -- passing for a
    # reason that has nothing to do with what it is testing.
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
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


# ---- pipelines know their client; the daemon checks the store (spec 4.2, 4.7, 4.8)


def _producer_pipeline(name="psk-recorder-pskreporter", client="psk-recorder"):
    return {
        "name": name, "client": client,
        "source": {"type": "sqlite", "database": "psk", "table": "spots",
                   "accepted_schema_versions": [2], "producer": "psk-recorder",
                   "start_at": "now", "delete_on_commit": False},
        "transport": {"type": "pskreporter",
                      "decoding_software": "psk-recorder/0.1"},
    }


def _plain_pipeline(tmp_path):
    return {
        "name": "grape-generic", "client": "grape",
        "source": {"type": "filetree", "root": str(tmp_path),
                   "patterns": ["OBS*"], "retention": "keep",
                   "match_dirs": True, "table": "grape.dataset"},
        "transport": {"type": "psws_dataset", "instrument_id": "367",
                      "table": "grape.dataset"},
    }


def test_client_from_manifest_generic_and_builder(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon, "resolve_builder", lambda spec: _fake_builder)
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    manifest = {
        "pipeline": [
            _plain_pipeline(tmp_path),
            {"name": "via-builder", "builder": "pkg:fn", "client": "wsprdaemon-client",
             "config": {"root": str(tmp_path)}},
            {"name": "no-client", "builder": "pkg:fn",
             "config": {"root": str(tmp_path)}},
        ],
    }
    pipes = {p.name: p for p in daemon.build_all_pipelines(
        manifest, watermark=SqliteWatermarkStore(":memory:"))}
    assert pipes["grape-generic"].client == "grape"
    assert pipes["via-builder"].client == "wsprdaemon-client"
    assert pipes["no-client"].client == ""


def test_old_store_refuses_only_producer_pipelines(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
    store = SqliteWatermarkStore(tmp_path / "watermarks.db")
    store._conn.execute("PRAGMA user_version = 1")   # as v3.70-v3.72 left it
    store._conn.commit()
    store.close()
    manifest = {"pipeline": [_producer_pipeline(), _plain_pipeline(tmp_path)]}
    monkeypatch.setattr(daemon, "load_manifest", lambda p: manifest)
    seen = {}

    class _Spy:
        def __init__(self, pipelines, **kw):
            seen["names"] = [p.name for p in pipelines]

        def pump_until_idle(self):
            return 1

    monkeypatch.setattr(daemon, "Uploader", _Spy)
    with caplog.at_level("ERROR"):
        assert daemon.run("ignored.toml", once=True) == 0
    assert seen["names"] == ["grape-generic"]
    assert sum(r.levelname == "ERROR" for r in caplog.records) == 1

    store = SqliteWatermarkStore(tmp_path / "watermarks.db")
    rows = {r["name"]: r for r in store.pipeline_states()}
    assert rows["grape-generic"]["refused_reason"] == ""
    assert rows["grape-generic"]["client"] == "grape"
    reason = rows["psk-recorder-pskreporter"]["refused_reason"]
    assert rows["psk-recorder-pskreporter"]["client"] == "psk-recorder"
    assert "version 1" in reason and "2" in reason
    # a table, not a migration: the number is untouched
    assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_pipeline_state_reflects_current_manifest_only(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
    store = SqliteWatermarkStore(tmp_path / "watermarks.db")
    store.record_pipeline_state("psk-pskreporter", "psk-recorder")
    store.close()
    manifest = {"pipeline": [_plain_pipeline(tmp_path)]}
    monkeypatch.setattr(daemon, "load_manifest", lambda p: manifest)

    class _Spy:
        def __init__(self, pipelines, **kw):
            pass

        def pump_until_idle(self):
            return 1

    monkeypatch.setattr(daemon, "Uploader", _Spy)
    assert daemon.run("ignored.toml", once=True) == 0
    store = SqliteWatermarkStore(tmp_path / "watermarks.db")
    assert [r["name"] for r in store.pipeline_states()] == ["grape-generic"]
