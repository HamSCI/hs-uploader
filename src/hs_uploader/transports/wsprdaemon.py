"""wsprdaemon.org transports — SFTP primary, FTP fallback.

Ported from ``wsprdaemon-client/bin/wd-upload-wsprdaemon`` so the wire
behaviour stays identical:

  * Bzip2 tar with the canonical layout
    ``wsprdaemon/{spots,noise}/RX_SITE/RECEIVER/BAND/<filename>``.
  * SFTP via ``subprocess(sftp -b -)`` — no paramiko dependency.
    `.part`-then-rename to keep the server from processing partial
    files.  Host-key rotation is handled with one auto-retry.
  * FTP fallback rebuilds the tar with ``client_upload_info.txt`` so
    the gateway can auto-provision SFTP for this reporter on the
    next cycle.

The transport reads a batch of file-shaped Records (each with
``payload_path`` set by ``FileTreeSource``) and bundles them.  The
batch's ``commit_token`` (the path list to delete on ack) flows
through the orchestrator via the deliverable; the source's
``commit()`` does the deletion.
"""

from __future__ import annotations

import io
import logging
import os
import re
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional, Sequence

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


_NOISE_TS_RE = re.compile(
    r"(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})\d{2}_noise\.txt$"
)

_HOST_KEY_ERR = re.compile(
    r"REMOTE HOST IDENTIFICATION HAS CHANGED|"
    r"Host key verification failed|"
    r"host key.*has changed",
    re.I,
)


def _build_rx_site(call: str, grid: str) -> str:
    """``AC0G/B1`` + ``EM38ww`` → ``AC0G=B1_EM38ww``  (matches wd-upload)."""
    base = call.replace("/", "=")
    return f"{base}_{grid}" if grid else base


def _config_bytes(version: str = "4.0") -> bytes:
    lines = [
        f"CLIENT_VERSION={version}",
        "UPLOADS_WSPRNET_LINE_FORMAT_VERSION=1",
        "UPLOADS_WSPRDAEMON_SPOT_LINE_FORMAT_VERSION=3",
        "SIGNAL_LEVEL_UPLOAD=yes",
    ]
    return "\n".join(lines).encode() + b"\n"


def _client_info_bytes(reporter_id: str, ssh_pubkey: str) -> bytes:
    lines = [
        f"reporter_id={reporter_id}",
        f"ssh_public_key={ssh_pubkey}",
    ]
    return "\n".join(lines).encode() + b"\n"


def _tar_add_bytes(tf: tarfile.TarFile, arcname: str, data: bytes) -> None:
    ti = tarfile.TarInfo(name=arcname)
    ti.size = len(data)
    ti.mtime = int(time.time())
    tf.addfile(ti, io.BytesIO(data))


def _arcname_for(path: Path, root: Path, rx_site: str) -> str:
    """Map a queued file's path under ``root`` to the canonical
    server-expected arcname.

    Input: ``<root>/<RECEIVER>/<BAND>/[noise/]<filename>``.
    Output:
      * spots: ``wsprdaemon/spots/RX_SITE/RECEIVER/BAND/<filename>``
      * noise: ``wsprdaemon/noise/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_noise.txt``
    """
    try:
        rel = path.relative_to(root)
    except ValueError:
        return f"wsprdaemon/{path.name}"
    parts = rel.parts
    is_noise = path.name.endswith("_noise.txt")

    if is_noise and len(parts) >= 4:
        receiver, band = parts[0], parts[1]
        m = _NOISE_TS_RE.search(path.name)
        if m:
            ts_name = (
                f"{m.group(1)[2:]}{m.group(2)}{m.group(3)}_"
                f"{m.group(4)}{m.group(5)}_noise.txt"
            )
        else:
            ts_name = path.name
        return f"wsprdaemon/noise/{rx_site}/{receiver}/{band}/{ts_name}"

    if not is_noise and len(parts) >= 3:
        receiver, band = parts[0], parts[1]
        return f"wsprdaemon/spots/{rx_site}/{receiver}/{band}/{path.name}"

    # Fallback: copy under wsprdaemon/ preserving the relative tail.
    return f"wsprdaemon/{rel.as_posix()}"


def build_wsprdaemon_tar(
    paths: Iterable[Path],
    *,
    root: Path,
    rx_site: str,
    version: str = "4.0",
    client_info: Optional[tuple[str, str]] = None,
) -> bytes:
    """Return an in-memory bzip2 tar of the canonical wsprdaemon shape.

    ``client_info`` is ``(reporter_id, ssh_pubkey)`` when included
    (FTP-fallback path so the gateway can provision SFTP for the
    reporter); ``None`` otherwise (SFTP-primary path uses the SSH key
    as identity).
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:bz2") as tf:
        _tar_add_bytes(tf, "wsprdaemon/uploads_config.txt",
                       _config_bytes(version))
        if client_info is not None:
            reporter_id, pubkey = client_info
            _tar_add_bytes(
                tf, "wsprdaemon/client_upload_info.txt",
                _client_info_bytes(reporter_id, pubkey),
            )
        for path in paths:
            arcname = _arcname_for(path, root=root, rx_site=rx_site)
            try:
                tf.add(str(path), arcname=arcname, recursive=False)
            except OSError as exc:
                logger.warning("wsprdaemon-tar: cannot add %s: %s", path, exc)
    return buf.getvalue()


# ---------- SFTP transport ----------


class WsprdaemonTarSftp:
    """Ships a bzip2 tar of WSPR spot/noise files to wsprdaemon.org via
    SFTP using ``.part``-then-rename.

    ``servers`` is a list of host-only strings (``["gw1.wsprdaemon.org",
    "gw2.wsprdaemon.org"]``); the SFTP login user is derived from
    ``StationIdentity.call`` (``AC0G/B1`` → ``AC0G_B1``) unless overridden.
    """

    ACCEPTS = {"wspr.spots": [3], "wspr.noise": [3]}

    def __init__(
        self,
        *,
        servers: Sequence[str],
        spool_root: Path | str,
        sftp_user: Optional[str] = None,
        remote_path: str = "uploads",
        connect_timeout_sec: int = 10,
        xfer_timeout_sec: int = 90,
        version: str = "4.0",
        upload_id: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.servers = list(servers)
        self.spool_root = Path(spool_root)
        self.sftp_user_override = sftp_user
        self.remote_path = remote_path
        self.connect_timeout_sec = connect_timeout_sec
        self.xfer_timeout_sec = xfer_timeout_sec
        self.version = version
        self.upload_id = upload_id
        self.name = name or f"wsprdaemon-tar-sftp:{','.join(servers)}"

    # -- Transport protocol --

    def primary_table(self) -> str:
        # Spots + noise both flow through the same pipeline; the
        # cursor key is the spots table (the noise side has its own
        # source/cursor in v1 if a caller chooses to set it up that
        # way, but the typical wsprdaemon pipeline binds one source
        # that yields both file kinds).
        return "wspr.spots"

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=10_000)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        paths = [r.payload_path for r in batch.records if r.payload_path]
        if not paths:
            return Outcome.acked()
        try:
            tar_bytes = build_wsprdaemon_tar(
                paths,
                root=self.spool_root,
                rx_site=_build_rx_site(identity.call, identity.grid),
                version=self.version,
            )
        except Exception as exc:
            return Outcome.permanent_failure(f"tar build failed: {exc}")

        tar_name = self._tar_name(identity)
        return self._upload_tar(tar_bytes, tar_name, identity)

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        # Rebuild the tar deterministically (same paths, same identity)
        # so a replay re-sends bit-identical bytes.
        paths = [r.payload_path for r in batch.records if r.payload_path]
        return build_wsprdaemon_tar(
            paths,
            root=self.spool_root,
            rx_site=_build_rx_site(identity.call, identity.grid),
            version=self.version,
        )

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        tar_name = self._tar_name(identity)
        return self._upload_tar(payload_blob, tar_name, identity)

    # -- internals --

    def _sftp_user(self, identity) -> str:
        if self.sftp_user_override:
            return self.sftp_user_override
        return identity.call.replace("/", "_") if identity.call else "wsprdaemon"

    def _tar_name(self, identity) -> str:
        ts = time.strftime("%y%m%d_%H%M_%S", time.gmtime())
        upload_id = (
            self.upload_id
            or (identity.call.replace("/", "_") if identity.call else "wsprdaemon")
        )
        return f"{upload_id}_{ts}.tbz"

    def _upload_tar(
        self, tar_bytes: bytes, tar_name: str, identity
    ) -> Outcome:
        with tempfile.NamedTemporaryFile(
            "wb", suffix=".tbz", delete=False,
        ) as fh:
            tar_path = Path(fh.name)
            fh.write(tar_bytes)
        try:
            for server in self.servers:
                ok, output = self._sftp_put(server, tar_path, tar_name, identity)
                if ok:
                    return Outcome.acked()
                logger.warning(
                    "WsprdaemonTarSftp: %s failed: %s",
                    server, output[-200:].strip(),
                )
            return Outcome.retry_later(
                f"all sftp servers failed: {self.servers}"
            )
        finally:
            try:
                tar_path.unlink()
            except OSError:
                pass

    def _sftp_put(
        self, server: str, tar_path: Path, tar_name: str, identity
    ) -> tuple[bool, str]:
        batch = self._sftp_batch_cmd(tar_path, tar_name)
        rc, out = self._run_sftp(server, batch, identity)
        if rc == 0:
            return True, out
        if _HOST_KEY_ERR.search(out):
            logger.warning(
                "host key change for %s — clearing known_hosts entry",
                server,
            )
            self._remove_host_key(server)
            rc, out = self._run_sftp(
                server, batch, identity,
                extra_opts=["StrictHostKeyChecking=accept-new"],
            )
            if rc == 0:
                return True, out
        return False, out

    def _sftp_batch_cmd(self, tar_path: Path, tar_name: str) -> bytes:
        part = f"{self.remote_path}/{tar_name}.part"
        dest = f"{self.remote_path}/{tar_name}"
        return f"put {tar_path} {part}\nrename {part} {dest}\n".encode()

    def _run_sftp(
        self,
        server: str,
        batch_cmd: bytes,
        identity,
        extra_opts: Optional[list[str]] = None,
    ) -> tuple[int, str]:
        cmd = [
            "sftp", "-b", "-",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={self.connect_timeout_sec}",
        ]
        if identity.ssh_key_file:
            cmd += ["-i", identity.ssh_key_file]
        for opt in extra_opts or []:
            cmd += ["-o", opt]
        cmd.append(f"{self._sftp_user(identity)}@{server}")
        try:
            result = subprocess.run(
                cmd,
                input=batch_cmd,
                capture_output=True,
                timeout=self.xfer_timeout_sec,
            )
            return result.returncode, (
                result.stdout + result.stderr
            ).decode(errors="replace")
        except subprocess.TimeoutExpired:
            return 1, "sftp timed out"

    @staticmethod
    def _remove_host_key(server: str) -> None:
        known_hosts = Path.home() / ".ssh" / "known_hosts"
        subprocess.run(
            ["ssh-keygen", "-f", str(known_hosts), "-R", server],
            capture_output=True,
        )


# ---------- FTP fallback transport ----------


class WsprdaemonTarFtp:
    """FTP fallback to wsprdaemon.org gateways.

    Identical tar layout to ``WsprdaemonTarSftp`` but with
    ``client_upload_info.txt`` included (the gateway uses it to
    auto-provision SFTP access for the reporter on the next cycle).
    Auth is anonymous-style user/password from a file.
    """

    ACCEPTS = {"wspr.spots": [3], "wspr.noise": [3]}

    def __init__(
        self,
        *,
        servers: Sequence[str],
        spool_root: Path | str,
        ftp_user: str = "noisegraphs",
        ftp_password_file: Optional[Path | str] = None,
        ftp_password: Optional[str] = None,
        remote_path: str = "upload",
        timeout_sec: int = 90,
        version: str = "4.0",
        upload_id: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.servers = list(servers)
        self.spool_root = Path(spool_root)
        self.ftp_user = ftp_user
        self.ftp_password_file = (
            Path(ftp_password_file) if ftp_password_file else None
        )
        self.ftp_password = ftp_password  # used when no file provided
        self.remote_path = remote_path
        self.timeout_sec = timeout_sec
        self.version = version
        self.upload_id = upload_id
        self.name = name or f"wsprdaemon-tar-ftp:{','.join(servers)}"

    def primary_table(self) -> str:
        return "wspr.spots"

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=10_000)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        paths = [r.payload_path for r in batch.records if r.payload_path]
        if not paths:
            return Outcome.acked()
        tar_bytes = build_wsprdaemon_tar(
            paths,
            root=self.spool_root,
            rx_site=_build_rx_site(identity.call, identity.grid),
            version=self.version,
            client_info=(identity.call, identity.public_key()),
        )
        return self._upload(tar_bytes, self._tar_name(identity))

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        paths = [r.payload_path for r in batch.records if r.payload_path]
        return build_wsprdaemon_tar(
            paths,
            root=self.spool_root,
            rx_site=_build_rx_site(identity.call, identity.grid),
            version=self.version,
            client_info=(identity.call, identity.public_key()),
        )

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        return self._upload(payload_blob, self._tar_name(identity))

    # -- internals --

    def _password(self) -> str:
        if self.ftp_password_file and self.ftp_password_file.exists():
            return self.ftp_password_file.read_text().strip()
        return self.ftp_password or ""

    def _tar_name(self, identity) -> str:
        ts = time.strftime("%y%m%d_%H%M_%S", time.gmtime())
        upload_id = (
            self.upload_id
            or (identity.call.replace("/", "_") if identity.call else "wsprdaemon")
        )
        return f"{upload_id}_{ts}.tbz"

    def _upload(self, tar_bytes: bytes, tar_name: str) -> Outcome:
        import ftplib
        last_err = "no servers configured"
        for server in self.servers:
            try:
                with ftplib.FTP(timeout=self.timeout_sec) as ftp:
                    ftp.connect(server)
                    ftp.login(user=self.ftp_user, passwd=self._password())
                    ftp.set_pasv(True)
                    bio = io.BytesIO(tar_bytes)
                    ftp.storbinary(
                        f"STOR {self.remote_path}/{tar_name}", bio,
                    )
                return Outcome.acked()
            except (ftplib.Error, OSError) as exc:
                last_err = f"{server}: {exc}"
                logger.warning("WsprdaemonTarFtp: %s", last_err)
        return Outcome.retry_later(f"all ftp servers failed: {last_err}")
