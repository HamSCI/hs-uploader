"""Config loader and StationIdentity for hs-uploader.

`coordination.toml` is the canonical source when present (sigmond
convention); env vars (`HS_UPLOADER_CALL`, `HS_UPLOADER_GRID`,
`HS_UPLOADER_STATION_ID`, `HS_UPLOADER_SSH_KEY_FILE`,
`HS_UPLOADER_RADIOD_ID`) fill in any missing fields.  The TOML schema
is intentionally minimal at v1 — just the station identity block.
Per-destination config (servers, ports, fallback transports) lives
inside the consuming client's own config.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # Python <3.11
    import tomli as tomllib  # type: ignore[no-redef]


_DEFAULT_KEY_FILE = "/etc/hs-uploader/keys/id_ed25519"


@dataclass
class StationIdentity:
    """Station-level identity.

    A single SSH keypair per station is shared across all clients that
    use hs-uploader (resolved decision in the design plan).  The
    ``ssh_key_file`` path is auto-generated on first use if it doesn't
    exist; see ``ensure_ssh_key`` below.
    """

    call: str = ""
    grid: str = ""
    station_id: str = ""           # PSWS-issued; optional
    ssh_key_file: str = _DEFAULT_KEY_FILE
    radiod_id: str = ""

    @classmethod
    def load(
        cls,
        coordination_toml: Optional[Path | str] = None,
        env: Optional[dict] = None,
    ) -> "StationIdentity":
        """Load identity from a TOML file (if present), then apply
        env-var overrides.  Either source is sufficient on its own;
        env wins for partial overrides.
        """
        e = env if env is not None else os.environ
        ident = cls()
        if coordination_toml is not None:
            ident = _load_from_toml(Path(coordination_toml))
        # Env-var overrides — non-empty wins.
        if v := e.get("HS_UPLOADER_CALL"):
            ident.call = v
        if v := e.get("HS_UPLOADER_GRID"):
            ident.grid = v
        if v := e.get("HS_UPLOADER_STATION_ID"):
            ident.station_id = v
        if v := e.get("HS_UPLOADER_SSH_KEY_FILE"):
            ident.ssh_key_file = v
        if v := e.get("HS_UPLOADER_RADIOD_ID"):
            ident.radiod_id = v
        return ident

    def ensure_ssh_key(self) -> Path:
        """Auto-generate an ed25519 keypair at ``ssh_key_file`` if it
        doesn't exist yet.  Returns the resolved path.

        This is the "auto-generate + share across clients on a station"
        behaviour from the design plan.  No-op when the key file is
        already present.  Will fail loudly if the parent directory
        isn't writable — clients are expected to create
        ``/etc/hs-uploader/keys/`` (mode 0700) at install time.
        """
        path = Path(self.ssh_key_file)
        if path.exists():
            return path
        path.parent.mkdir(parents=True, exist_ok=True)
        # `ssh-keygen -t ed25519 -f path -N '' -C "hs-uploader@<radiod_id>"`
        import subprocess
        comment = f"hs-uploader@{self.radiod_id or self.call or 'unknown'}"
        subprocess.run(
            [
                "ssh-keygen",
                "-q",                 # quiet
                "-t", "ed25519",
                "-f", str(path),
                "-N", "",
                "-C", comment,
            ],
            check=True,
        )
        # ssh-keygen creates path (private) and path.pub (public);
        # tighten permissions defensively.
        try:
            os.chmod(path, 0o600)
            os.chmod(str(path) + ".pub", 0o644)
        except OSError:
            pass
        return path

    def public_key(self) -> str:
        """Read the public-key file's contents (for embedding in
        ``client_upload_info.txt`` and similar pubkey-publishing
        flows).  Empty string if the key hasn't been generated yet.
        """
        pub = Path(self.ssh_key_file + ".pub")
        try:
            return pub.read_text().strip()
        except OSError:
            return ""


def _load_from_toml(path: Path) -> StationIdentity:
    if not path.exists():
        return StationIdentity()
    with open(path, "rb") as fh:
        data = tomllib.load(fh)
    block = data.get("hs_uploader", {}).get("station", {})
    return StationIdentity(
        call=block.get("call", ""),
        grid=block.get("grid", ""),
        station_id=block.get("station_id", ""),
        ssh_key_file=block.get("ssh_key_file", _DEFAULT_KEY_FILE),
        radiod_id=block.get("radiod_id", ""),
    )
