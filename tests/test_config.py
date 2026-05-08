"""StationIdentity: TOML + env merge, defaults, ssh_key auto-gen."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from hs_uploader.config import StationIdentity


def test_load_pure_env(tmp_path, monkeypatch):
    monkeypatch.setenv("HS_UPLOADER_CALL", "AC0G")
    monkeypatch.setenv("HS_UPLOADER_GRID", "EM38ww")
    monkeypatch.setenv("HS_UPLOADER_STATION_ID", "S000171")
    monkeypatch.setenv("HS_UPLOADER_SSH_KEY_FILE", str(tmp_path / "id"))
    monkeypatch.setenv("HS_UPLOADER_RADIOD_ID", "bee1-hf")
    ident = StationIdentity.load()
    assert ident.call == "AC0G"
    assert ident.grid == "EM38ww"
    assert ident.station_id == "S000171"
    assert ident.ssh_key_file == str(tmp_path / "id")
    assert ident.radiod_id == "bee1-hf"


def test_load_from_toml(tmp_path):
    toml = tmp_path / "coord.toml"
    toml.write_text(
        '[hs_uploader.station]\n'
        'call = "K1ABC"\n'
        'grid = "FN42aa"\n'
        'station_id = "S000999"\n'
        'ssh_key_file = "/etc/foo/id"\n'
        'radiod_id = "rx-1"\n'
    )
    ident = StationIdentity.load(coordination_toml=toml, env={})
    assert ident.call == "K1ABC"
    assert ident.grid == "FN42aa"
    assert ident.station_id == "S000999"
    assert ident.ssh_key_file == "/etc/foo/id"
    assert ident.radiod_id == "rx-1"


def test_env_overrides_toml(tmp_path):
    toml = tmp_path / "coord.toml"
    toml.write_text(
        '[hs_uploader.station]\n'
        'call = "TOML"\n'
        'grid = "FN42aa"\n'
    )
    ident = StationIdentity.load(
        coordination_toml=toml,
        env={"HS_UPLOADER_CALL": "ENV"},
    )
    assert ident.call == "ENV"          # env wins
    assert ident.grid == "FN42aa"       # toml fills the rest


def test_load_missing_toml_uses_defaults(tmp_path):
    ident = StationIdentity.load(
        coordination_toml=tmp_path / "does-not-exist.toml",
        env={"HS_UPLOADER_CALL": "ZZ0Z"},
    )
    assert ident.call == "ZZ0Z"
    assert ident.grid == ""             # default
    assert ident.ssh_key_file.endswith("id_ed25519")  # default path


def test_ensure_ssh_key_generates(tmp_path):
    if not _ssh_keygen_available():
        import pytest
        pytest.skip("ssh-keygen not installed")
    target = tmp_path / "keys" / "id_ed25519"
    ident = StationIdentity(
        call="AC0G", grid="EM38ww", ssh_key_file=str(target),
    )
    out = ident.ensure_ssh_key()
    assert out == target
    assert target.exists()
    assert (tmp_path / "keys" / "id_ed25519.pub").exists()
    pub = ident.public_key()
    assert pub.startswith("ssh-ed25519 ")


def test_ensure_ssh_key_idempotent_when_present(tmp_path):
    target = tmp_path / "id"
    target.write_text("not really a key, but a non-empty file")
    ident = StationIdentity(ssh_key_file=str(target))
    # Must not overwrite the existing file.
    out = ident.ensure_ssh_key()
    assert out == target
    assert target.read_text() == "not really a key, but a non-empty file"


def _ssh_keygen_available() -> bool:
    try:
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-y", "-f", "/dev/null"],
            capture_output=True, timeout=2,
        )
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    except Exception:
        return True  # ssh-keygen exists, just returned non-zero on bogus input
