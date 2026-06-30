"""PSWS dataset SFTP transport.

The single PSWS wire-protocol code path for every HamSCI client that
uploads daily PSWS datasets — magnetometer (a single ``OBS<date>.zip``),
GRAPE (a Digital RF ``OBS<date>T00-00/`` *directory* tree), and future
instruments.  Each ``Record.payload_path`` may point at **either**:

* a single file (e.g. a magnetometer ``OBS<date>T<HH:MM>.zip``) — sent
  with ``put`` + ``.part``-rename; or
* a directory tree (e.g. a GRAPE ``OBS<date>T00-00/`` dataset) — sent
  with a recursive ``mkdir``/``put`` walk (sorted, deterministic),
  mirroring the original ``hf_timestd.grape.uploader.SFTPUpload``.

After the data lands, the transport ``mkdir``'s a Grape-style trigger
directory so the server picks the dataset up for processing.

Source side: a ``FileTreeSource`` walking the dataset queue, each path
yielded as a ``Record`` with ``payload_path`` set.  For directory
datasets use ``match_dirs=True`` + ``retention="keep"`` (GRAPE keeps
datasets locally); for single-file zips ``delete_on_ack`` frees disk.

Trigger-directory convention (matches the original GRAPE uploader):

    trigger = f"c{dataset_name}_#{instrument_id}_#{timestamp}"

where ``dataset_name`` is the directory name (``OBS2026-05-12T00-00``)
or the zip's stem (``OBS2026-05-12T00:00``) and ``timestamp`` is ISO
compact with dashes (``2026-05-13T03-05``) since PSWS treats the
trigger dir as a filesystem entry and colons there break some
processing tools.

Note on verification: this transport treats a zero sftp return code as
success and does NOT re-``ls`` the trigger directory afterwards.  The
PSWS server *consumes* the trigger dir on ingest, so a post-upload
``ls -d <trigger>`` is racy and yields false "verification failed"
negatives (the bug in the original ``SFTPUpload.verify()``).
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


# Default table this transport reads from.  Producers (mag-recorder's
# packager) emit Records with `table = "mag.daily_zip"` and
# `payload_path` set; the FileTreeSource convention is to leave
# `columns` empty since the data is bytes-on-disk.  GRAPE passes
# `table="grape.dataset"` via the constructor.
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
        table: str = TABLE,
        connect_timeout_sec: int = 10,
        xfer_timeout_sec: int = 3600,
        bandwidth_limit_kbps: Optional[int] = None,
        dry_run: bool = False,
        name: Optional[str] = None,
    ):
        self.host = host
        self.instrument_id = instrument_id
        self.sftp_user_override = sftp_user
        self.ssh_key_override = ssh_key_file
        self.remote_path = remote_path.rstrip("/")
        self.table = table
        # Instance-level ACCEPTS so a GRAPE pipeline (table="grape.dataset")
        # and a magnetometer pipeline (default "mag.daily_zip") can share
        # this class with distinct watermark/source wiring.
        self.ACCEPTS = {table: [1]}
        self.connect_timeout_sec = connect_timeout_sec
        self.xfer_timeout_sec = xfer_timeout_sec
        self.bandwidth_limit_kbps = bandwidth_limit_kbps
        self.dry_run = dry_run
        self.name = name or f"psws-sftp:{host}:{instrument_id}"

    # ---- Transport protocol ------------------------------------------------

    def primary_table(self) -> str:
        return self.table

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

    def _upload_one(self, dataset_path: Path, identity) -> Outcome:
        if not dataset_path.exists():
            return Outcome.permanent_failure(
                f"dataset vanished before upload: {dataset_path}"
            )

        is_dir = dataset_path.is_dir()
        # Directory datasets (GRAPE) key the trigger off the dir name
        # (OBS2026-05-12T00-00); single-file datasets (mag zip) off the
        # file stem (OBS2026-05-12T00:00).
        dataset_name = dataset_path.name if is_dir else dataset_path.stem
        trigger = self._trigger_dir_name(dataset_name)

        try:
            user = self._sftp_user(identity)
        except ValueError as exc:
            return Outcome.permanent_failure(str(exc))

        if is_dir:
            batch_lines = self._dir_put_lines(dataset_path, dataset_name)
        else:
            remote_zip = self._remote_path(dataset_path.name)
            batch_lines = [
                f'put "{dataset_path}" "{remote_zip}.part"',
                f'rename "{remote_zip}.part" "{remote_zip}"',
            ]
        # ``-mkdir`` (leading dash) tells sftp's batch mode to ignore an
        # error for this line instead of aborting the whole batch, so a
        # re-upload / retry whose trigger dir already exists still
        # succeeds (rc=0).  put/rename stay strict so real transfer
        # failures still surface.
        batch_lines.append(f'-mkdir "{self._remote_path(trigger)}"')
        batch_lines.append("quit")
        batch_input = ("\n".join(batch_lines) + "\n").encode()

        if self.dry_run:
            logger.info(
                "[dry-run] PswsDatasetSftp would upload %s (%s) as %s@%s "
                "with trigger %s",
                dataset_path, "dir" if is_dir else "file", user, self.host,
                trigger,
            )
            logger.debug("[dry-run] sftp batch:\n%s", batch_input.decode())
            return Outcome.acked()

        rc, output = self._run_sftp(user, self._ssh_key(identity), batch_input)
        if rc == 0:
            logger.info(
                "PswsDatasetSftp: uploaded %s -> %s@%s (trigger=%s)",
                dataset_name, user, self.host, trigger,
            )
            return Outcome.acked()

        # rc != 0 -- log + retry_later.  We don't try to distinguish
        # "host key changed" or "network blip" from "permanent
        # rejection" here; the watermark retry policy bounds attempts
        # before falling into dead-letter.
        logger.warning(
            "PswsDatasetSftp: sftp rc=%d for %s: %s",
            rc, dataset_name, output[-300:].strip(),
        )
        return Outcome.retry_later(f"sftp rc={rc}")

    def _dir_put_lines(self, local_path: Path, remote_name: str) -> list[str]:
        """Recursive ``mkdir``/``put`` batch for a directory dataset.

        Deterministic (sorted) walk, mirroring the original GRAPE
        ``SFTPUpload._build_sftp_put_commands`` but with every path
        quoted so names with spaces are safe.
        """
        # ``-mkdir`` ignores "already exists" so re-uploads/retries don't
        # abort the batch; ``put`` stays strict so transfer errors surface.
        base = self._remote_path(remote_name)
        lines = [f'-mkdir "{base}"']
        for root, dirs, files in os.walk(local_path):
            dirs.sort()
            rel = os.path.relpath(root, local_path)
            remote_dir = base if rel == "." else f"{base}/{rel}"
            for d in sorted(dirs):
                lines.append(f'-mkdir "{remote_dir}/{d}"')
            for f in sorted(files):
                local_file = os.path.join(root, f)
                lines.append(f'put "{local_file}" "{remote_dir}/{f}"')
        return lines

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


# Canonical generic name — this transport handles any PSWS dataset
# (single-file or directory).  ``PswsMagnetometerSftp`` is retained as a
# back-compat alias for existing importers (mag-recorder).
PswsDatasetSftp = PswsMagnetometerSftp
