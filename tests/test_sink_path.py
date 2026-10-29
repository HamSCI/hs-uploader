"""sink_path(): one rule finds the sink (env, sink.toml, legacy, state dir)."""

import logging
import os
from pathlib import Path

import pytest

from hs_uploader.sink import writer as writer_mod
from hs_uploader.sink import Writer, sink_path


@pytest.fixture(autouse=True)
def _reset_once_flags():
    writer_mod._reset_once_flags()
    yield
    writer_mod._reset_once_flags()


def _config(tmp_path, text):
    p = tmp_path / "sink.toml"
    p.write_text(text)
    return str(p)


def _legacy(tmp_path):
    p = tmp_path / "legacy" / "sink.db"
    p.parent.mkdir()
    p.write_text("")
    return str(p)


def _missing(tmp_path):
    return str(tmp_path / "absent.toml")


def test_env_override_wins(tmp_path):
    cfg = _config(tmp_path, 'path = "/y/b.db"\n')
    legacy = _legacy(tmp_path)
    kw = dict(config_file=cfg, legacy_path=legacy, state_dir=str(tmp_path))
    assert sink_path({"SIGMOND_SQLITE_PATH": "/x/a.db"}, **kw) == "/x/a.db"
    both = {"HAMSCI_SINK_PATH": "/x/h.db", "SIGMOND_SQLITE_PATH": "/x/a.db"}
    assert sink_path(both, **kw) == "/x/h.db"


def test_config_beats_a_later_legacy_file(tmp_path):
    new = str(tmp_path / "new" / "sink.db")
    cfg = _config(tmp_path, f'path = "{new}"\n')
    legacy = _legacy(tmp_path)
    assert sink_path({}, config_file=cfg, legacy_path=legacy,
                     state_dir=str(tmp_path)) == new


def test_legacy_sink_wins_without_config(tmp_path):
    legacy = _legacy(tmp_path)
    assert sink_path({}, config_file=_missing(tmp_path), legacy_path=legacy,
                     state_dir=str(tmp_path)) == legacy


def test_fresh_install_takes_the_state_dir(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    assert sink_path({}, config_file=_missing(tmp_path),
                     legacy_path=str(tmp_path / "nolegacy.db"),
                     state_dir=str(state)) == str(state / "sink.db")


def test_state_dir_comes_from_env(tmp_path):
    state = tmp_path / "envstate"
    state.mkdir()
    got = sink_path({"HS_UPLOADER_STATE_DIR": str(state)},
                    config_file=_missing(tmp_path),
                    legacy_path=str(tmp_path / "nolegacy.db"))
    assert got == str(state / "sink.db")


def test_nothing_usable_returns_none(tmp_path):
    ro = tmp_path / "ro"
    ro.mkdir()
    ro.chmod(0o500)
    try:
        if os.access(ro, os.W_OK):
            pytest.skip("running as a user who ignores directory modes")
        got = sink_path({}, config_file=_missing(tmp_path),
                        legacy_path=str(tmp_path / "nolegacy.db"),
                        state_dir=str(ro / "state"))
        assert got is None
    finally:
        ro.chmod(0o700)


def test_malformed_config_falls_through_and_logs_once(tmp_path, caplog):
    cfg = _config(tmp_path, "path = [")
    legacy = _legacy(tmp_path)
    with caplog.at_level(logging.ERROR, logger="sigmond.hamsci_sink"):
        for _ in range(2):
            assert sink_path({}, config_file=cfg, legacy_path=legacy,
                             state_dir=str(tmp_path)) == legacy
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert cfg in errors[0].getMessage()


def test_config_without_path_key_logs_and_falls_through(tmp_path, caplog):
    cfg = _config(tmp_path, 'other = "x"\n')
    legacy = _legacy(tmp_path)
    with caplog.at_level(logging.ERROR, logger="sigmond.hamsci_sink"):
        assert sink_path({}, config_file=cfg, legacy_path=legacy,
                         state_dir=str(tmp_path)) == legacy
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1


def test_writer_from_env_uses_sink_path(tmp_path):
    w = Writer.from_env(table="spots", mode="wspr",
                        env={"SIGMOND_SQLITE_PATH": str(tmp_path / "s.db")})
    assert not w.is_noop


def test_writer_from_env_noop_logs_once(monkeypatch, caplog):
    monkeypatch.setattr(writer_mod, "sink_path", lambda *a, **k: None)
    with caplog.at_level(logging.ERROR, logger="sigmond.hamsci_sink"):
        for _ in range(2):
            w = Writer.from_env(table="spots", mode="wspr", env={})
            assert w.is_noop
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "/etc/hs-uploader/sink.toml" in errors[0].getMessage()


def _readonly(path):
    path.chmod(0o500)
    if os.access(path, os.W_OK):
        path.chmod(0o700)
        pytest.skip("running as a user who ignores directory modes")


def _noop_with_paths(monkeypatch, **kw):
    """Point both resolution entry points at `kw`, then build a writer."""
    for name in ("sink_path", "_sink_choice"):
        orig = getattr(writer_mod, name)
        monkeypatch.setattr(
            writer_mod, name,
            lambda env=None, _o=orig, **k: _o(env, **{**kw, **k}))
    return Writer.from_env(table="spots", mode="wspr", env={})


def test_unwritable_legacy_dir_gives_a_noop_writer(tmp_path, monkeypatch, caplog):
    legacy = _legacy(tmp_path)
    ro = Path(legacy).parent
    _readonly(ro)
    try:
        with caplog.at_level(logging.ERROR, logger="sigmond.hamsci_sink"):
            for _ in range(2):
                w = _noop_with_paths(monkeypatch, config_file=_missing(tmp_path),
                                     legacy_path=legacy, state_dir=str(tmp_path))
                assert w.is_noop
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "not writable" in errors[0].getMessage()
        assert legacy in errors[0].getMessage()
    finally:
        ro.chmod(0o700)


def test_unwritable_state_dir_gives_a_noop_writer(tmp_path, monkeypatch, caplog):
    # The state dir's parent exists but the dir itself is read-only.
    state = tmp_path / "state"
    state.mkdir()
    _readonly(state)
    # Let the two resolutions inside from_env (sink_path, then the step
    # lookup) accept the state dir, as a writable parent would; the third
    # call, the writability check itself, tells the truth.
    real, calls = writer_mod._default_sqlite_writable, []

    def writable(path):
        calls.append(path)
        return True if len(calls) <= 2 else real(path)

    monkeypatch.setattr(writer_mod, "_default_sqlite_writable", writable)
    try:
        with caplog.at_level(logging.ERROR, logger="sigmond.hamsci_sink"):
            w = _noop_with_paths(monkeypatch, config_file=_missing(tmp_path),
                                 legacy_path=str(tmp_path / "nolegacy.db"),
                                 state_dir=str(state))
        assert w.is_noop
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "not writable" in errors[0].getMessage()
    finally:
        state.chmod(0o700)


def test_unwritable_configured_path_stays_a_real_writer(tmp_path, monkeypatch):
    ro = tmp_path / "ro"
    ro.mkdir()
    _readonly(ro)
    try:
        cfg = _config(tmp_path, f'path = "{ro}/sink.db"\n')
        w = _noop_with_paths(monkeypatch, config_file=cfg,
                             legacy_path=str(tmp_path / "nolegacy.db"),
                             state_dir=str(tmp_path))
        assert not w.is_noop
    finally:
        ro.chmod(0o700)
