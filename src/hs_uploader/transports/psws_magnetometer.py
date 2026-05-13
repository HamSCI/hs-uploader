"""PSWS magnetometer SFTP transport.

Ships one ``OBS<date>T<HH:MM>.zip`` per record to
``pswsnetwork.eng.ua.edu`` and then mkdir's a Grape-style trigger
directory so the server picks the dataset up for processing.

This is the symmetric counterpart of ``hf_timestd.grape.uploader``,
factored out as a reusable hs-uploader transport so every HamSCI
client that uploads daily PSWS datasets (Grape, magnetometer, future
instruments) goes through one wire-protocol code path.

Source side: a ``FileTreeSource`` walking the daily-zip queue, each
zip path yielded as a ``Record`` with ``payload_path`` set.  Source
retention is ``delete_on_ack`` — successful upload deletes the local
zip, freeing disk.

Trigger-directory convention (matches hf-timestd/grape/uploader.py
SFTPUpload.upload()):

    trigger = f"c{dataset_name}_#{instrument_id}_#{timestamp}"

where ``dataset_name`` is the zip's stem (``OBS2026-05-12T00:00``) and
``timestamp`` is ISO compact with dashes (``2026-05-13T03-05``) since
PSWS treats the trigger dir as a filesystem entry and colons there
break some processing tools.  The data zip itself keeps colons since
that's what the user's spec calls for and the file isn't a directory.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


# Table this transport reads from.  Producers (mag-recorder's
# packager) emit Records with `table = "mag.daily_zip"` and
# `payload_path` set; the FileTreeSource convention is to leave
# `columns` empty since the data is bytes-on-disk.
TABLE = "mag.daily_zip"


class PswsMagnetometerSftp:
    """Upload daily ``OBS<date>T<HH:MM>.zip`` files to PSWS.

    Parameters
    ----------
    host
        PSWS SFTP server.  Defaults to ``pswsnetwork.eng.ua.edu``.
    instrument_id
        Per-instrument identifier registered with PSWS (e.g.
        ``"RM3100"``).  Goes into the trigger directory name.
    sftp_user
        PSWS station id (e.g. ``"S000082"``).  When unset the
        transport falls back to ``identity.station_id`` from the
        passed ``StationIdentity``.
    ssh_key_file
        Path to the private key registered on the PSWS portal.
        Defaults to ``identity.ssh_key_file`` (shared with the Grape
        upload path).
    remote_path
        Where on the server to ``put`` the zip.  PSWS uses the
        login user's home directory by default; the trigger
        directory is mkdir'd at the same level.
    bandwidth_limit_kbps
        Currently advisory — ``sftp -l`` accepts kbit/s but we
        don't pass it yet.  Wired through for future use.
    dry_run
        When set, log the sftp batch we *would* run and return
        ``Outcome.acked()`` without invoking sftp.  Use this during
        development (synthetic data, no real PSWS account) so the
        delete_on_ack source still flushes the queue without
        polluting the PSWS archive.
    """

    ACCEPTS = {TABLE: [1]}

    def __init__(
        self,
        *,
        instrument_id: str,
        host: str = "pswsnetwork.eng.ua.edu",
        sftp_user: Optional[str] = None,
        ssh_key_file: Optional[str] = None,
        remote_path: str = "",
        connect_timeout_sec: int = 10,
        xfer_timeout_sec: int = 600,
        bandwidth_limit_kbps: Optional[int] = None,
        dry_run: bool = False,
        name: Optional[str] = None,
    ):
        self.host = host
        self.instrument_id = instrument_id
        self.sftp_user_override = sftp_user
        self.ssh_key_override = ssh_key_file
        self.remote_path = remote_path.rstrip("/")
        self.connect_timeout_sec = connect_timeout_sec
        self.xfer_timeout_sec = xfer_timeout_sec
        self.bandwidth_limit_kbps = bandwidth_limit_kbps
        self.dry_run = dry_run
        self.name = name or f"psws-mag-sftp:{host}:{instrument_id}"

    # ---- Transport protocol ------------------------------------------------

    def primary_table(self) -> str:
        return TABLE

    def batch_policy(self) -> BatchPolicy:
        # One zip per ship() call.  Each upload is its own PSWS
        # dataset with its own trigger directory; batching multiple
        # would require multiple mkdir calls in one sftp session and
        # the cursor / partial-ack semantics get more complex than
        # the benefit is worth (one upload per day per station).
        return BatchPolicy(max_records=1)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        paths = [r.payload_path for r in batch.records if r.payload_path]
        if not paths:
            return Outcome.acked()  # nothing to ship; vacuously ok

        for zip_path in paths:
            outcome = self._upload_one(zip_path, identity)
            if not outcome.succeeded:
                return outcome
        return Outcome.acked()

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        # Daily zips are byte-stable on disk; serializing for retry
        # would mean inlining the zip bytes.  We instead point the
        # replay path at the same on-disk file (the FileTreeSource
        # leaves it in place until ack).  Empty blob is the signal.
        return b""

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        # Retries re-walk the queue rather than reconstruct from blob.
        # The orchestrator handles this by re-pulling a batch through
        # ship() rather than calling replay() for the empty-blob case.
        return Outcome.retry_later("psws-mag retries go through fresh ship()")

    # ---- internals ---------------------------------------------------------

    def _sftp_user(self, identity) -> str:
        if self.sftp_user_override:
            return self.sftp_user_override
        if getattr(identity, "station_id", "").strip():
            return identity.station_id.strip()
        raise ValueError(
            "PswsMagnetometerSftp: no sftp_user and identity.station_id is empty"
        )

    def _ssh_key(self, identity) -> Optional[str]:
        if self.ssh_key_override:
            return self.ssh_key_override
        return getattr(identity, "ssh_key_file", None)

    def _upload_one(self, zip_path: Path, identity) -> Outcome:
        if not zip_path.is_file():
            return Outcome.permanent_failure(
                f"zip vanished before upload: {zip_path}"
            )

        dataset_name = zip_path.stem  # OBS2026-05-12T00:00
        trigger = self._trigger_dir_name(dataset_name)

        try:
            user = self._sftp_user(identity)
        except ValueError as exc:
            return Outcome.permanent_failure(str(exc))

        remote_zip = self._remote_path(zip_path.name)
        remote_trigger = self._remote_path(trigger)
        batch_lines = [
            f'put "{zip_path}" "{remote_zip}.part"',
            f'rename "{remote_zip}.part" "{remote_zip}"',
            f'mkdir "{remote_trigger}"',
            "quit",
        ]
        batch_input = ("\n".join(batch_lines) + "\n").encode()

        if self.dry_run:
            logger.info(
                "[dry-run] PswsMagnetometerSftp would upload %s as %s@%s "
                "with trigger %s",
                zip_path, user, self.host, trigger,
            )
            logger.debug("[dry-run] sftp batch:\n%s", batch_input.decode())
            return Outcome.acked()

        rc, output = self._run_sftp(user, self._ssh_key(identity), batch_input)
        if rc == 0:
            logger.info(
                "PswsMagnetometerSftp: uploaded %s -> %s@%s (trigger=%s)",
                zip_path.name, user, self.host, trigger,
            )
            return Outcome.acked()

        # rc != 0 -- log + retry_later.  We don't try to distinguish
        # "host key changed" or "network blip" from "permanent
        # rejection" here; the watermark retry policy bounds attempts
        # before falling into dead-letter.
        logger.warning(
            "PswsMagnetometerSftp: sftp rc=%d for %s: %s",
            rc, zip_path.name, output[-300:].strip(),
        )
        return Outcome.retry_later(f"sftp rc={rc}")

    def _remote_path(self, leaf: str) -> str:
        return f"{self.remote_path}/{leaf}" if self.remote_path else leaf

    def _trigger_dir_name(self, dataset_name: str) -> str:
        # Match hf-timestd's Grape SFTPUpload.upload() exactly:
        # timestamp portion uses ISO-with-dashes, not colons, so the
        # trigger directory is filesystem-safe on the PSWS side.
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M")
        return f"c{dataset_name}_#{self.instrument_id}_#{ts}"

    def _run_sftp(
        self,
        user: str,
        ssh_key: Optional[str],
        batch_input: bytes,
    ) -> tuple[int, str]:
        cmd = [
            "sftp",
            "-b", "-",
            "-q",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={self.connect_timeout_sec}",
            "-o", "StrictHostKeyChecking=accept-new",
        ]
        if ssh_key:
            cmd += ["-i", ssh_key]
        if self.bandwidth_limit_kbps is not None:
            cmd += ["-l", str(self.bandwidth_limit_kbps)]
        cmd.append(f"{user}@{self.host}")

        try:
            result = subprocess.run(
                cmd,
                input=batch_input,
                capture_output=True,
                timeout=self.xfer_timeout_sec,
            )
            output = (result.stdout + result.stderr).decode(errors="replace")
            return result.returncode, output
        except subprocess.TimeoutExpired:
            return 1, f"sftp timed out after {self.xfer_timeout_sec}s"
        except FileNotFoundError:
            return 1, "sftp binary not found on PATH"
