"""The sink's consumers follow `sink_path()`: the SQLite source, the
`wspr_cycle` pipeline default and the wake socket."""

from __future__ import annotations

from pathlib import Path

import pytest

from hs_uploader import wake
from hs_uploader.pipeline_factory import _build_source
from hs_uploader.sink import writer as sink_writer
from hs_uploader.sources.sqlite import _ConnectionConfig


def _pin_sink(monkeypatch, value):
    monkeypatch.setattr(sink_writer, "sink_path",
                        lambda env=None, **kw: value)


def test_sqlite_source_follows_config(tmp_path, monkeypatch):
    db = tmp_path / "x" / "sink.db"
    db.parent.mkdir()
    db.touch()
    conf = tmp_path / "sink.toml"
    conf.write_text(f'path = "{db}"\n')
    # Route through the conftest-isolated resolver, naming our config.
    isolated = sink_writer.sink_path
    monkeypatch.setattr(
        sink_writer, "sink_path",
        lambda env=None, **kw: isolated(env, config_file=str(conf), **kw))
    cfg = _ConnectionConfig.from_env({})
    assert cfg is not None and cfg.path == str(db)


def test_sqlite_source_still_none_when_file_missing(tmp_path, monkeypatch):
    _pin_sink(monkeypatch, str(tmp_path / "absent" / "sink.db"))
    assert _ConnectionConfig.from_env({}) is None


def test_wspr_cycle_default_follows_sink_path(tmp_path, monkeypatch):
    _pin_sink(monkeypatch, str(tmp_path / "s.db"))
    src = _build_source({"type": "wspr_cycle"})
    assert src.db_path == tmp_path / "s.db"
    explicit = _build_source(
        {"type": "wspr_cycle", "db_path": str(tmp_path / "own.db")})
    assert explicit.db_path == tmp_path / "own.db"


def test_wake_socket_sits_beside_the_sink(tmp_path, monkeypatch):
    for var in ("HS_UPLOADER_WAKE_SOCK", "WSPR_UPLOAD_WAKE_SOCK"):
        monkeypatch.delenv(var, raising=False)
    _pin_sink(monkeypatch, "/var/lib/sigmond/sink.db")
    assert wake.wake_path() == "/var/lib/sigmond/upload-wake.sock"
    _pin_sink(monkeypatch, str(tmp_path / "hs" / "sink.db"))
    assert wake.wake_path() == str(tmp_path / "hs" / "upload-wake.sock")
    monkeypatch.setenv("HS_UPLOADER_WAKE_SOCK", "/run/x.sock")
    assert wake.wake_path() == "/run/x.sock"


def test_wake_socket_takes_an_env_argument(tmp_path, monkeypatch):
    monkeypatch.delenv("HS_UPLOADER_WAKE_SOCK", raising=False)
    monkeypatch.delenv("WSPR_UPLOAD_WAKE_SOCK", raising=False)
    assert wake.wake_path({"WSPR_UPLOAD_WAKE_SOCK": "/r/w.sock"}) == "/r/w.sock"


def test_wake_socket_without_a_sink_uses_the_state_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("HS_UPLOADER_WAKE_SOCK", raising=False)
    monkeypatch.delenv("WSPR_UPLOAD_WAKE_SOCK", raising=False)
    _pin_sink(monkeypatch, None)
    env = {"HS_UPLOADER_STATE_DIR": str(tmp_path / "st")}
    assert wake.wake_path(env) == str(tmp_path / "st" / "upload-wake.sock")
    assert wake.wake_path({}) == "/var/lib/hs-uploader/upload-wake.sock"
