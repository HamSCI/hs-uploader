"""install.sh write_sink_config: the install names the sink explicitly."""

import subprocess
from pathlib import Path

from hs_uploader.sink.writer import sink_path

INSTALL = Path(__file__).resolve().parent.parent / "install.sh"


def _run(config, legacy, state):
    # Sourcing install.sh must not run its main body.
    subprocess.run(
        ["bash", "-c",
         'source "$1"; write_sink_config "$2" "$3" "$4"',
         "_", str(INSTALL), str(config), str(legacy), str(state)],
        check=True, capture_output=True, text=True)


def test_existing_config_is_left_alone(tmp_path):
    cfg = tmp_path / "sink.toml"
    original = b'# mine\npath = "/keep"\n'
    cfg.write_bytes(original)
    _run(cfg, tmp_path / "legacy.db", tmp_path / "state")
    assert cfg.read_bytes() == original


def test_legacy_station_names_the_legacy_sink(tmp_path):
    legacy = tmp_path / "legacy.db"
    legacy.write_bytes(b"")
    cfg = tmp_path / "sink.toml"
    _run(cfg, legacy, tmp_path / "state")
    lines = cfg.read_text().splitlines()
    assert lines[0].startswith("#")
    assert lines[1] == f'path = "{legacy}"'
    assert oct(cfg.stat().st_mode & 0o777) == "0o644"


def test_fresh_install_names_the_state_dir(tmp_path):
    cfg = tmp_path / "sink.toml"
    _run(cfg, tmp_path / "absent.db", tmp_path / "state")
    assert cfg.read_text().splitlines()[1] == f'path = "{tmp_path}/state/sink.db"'
    assert list(tmp_path.glob("sink.toml.*")) == []  # no temp file left


def test_written_config_parses_and_resolves(tmp_path):
    cfg = tmp_path / "sink.toml"
    _run(cfg, tmp_path / "absent.db", tmp_path / "state")
    got = sink_path({}, config_file=str(cfg),
                    legacy_path=str(tmp_path / "absent.db"),
                    state_dir=str(tmp_path / "other"))
    assert got == str(tmp_path / "state" / "sink.db")
