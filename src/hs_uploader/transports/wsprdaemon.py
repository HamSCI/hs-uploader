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


# ---------- SqliteSource path (records carrying columns dict) ----------
#
# When the wsprdaemon pipeline is fed by SqliteSource (sink.db
# pending_uploads / wspr.spots), records don't have on-disk
# payload_path values — they have `columns` dicts.  These two helpers
# reconstruct the wsprdaemon.org wire-format files in memory from the
# row payloads, group them per (band, mode, cycle), and pack the tar
# with the same wsprdaemon/{spots}/<rx_site>/<receiver>/<band>/
# layout the file-source path uses.
#
# Noise files (wsprdaemon/noise/...) are NOT emitted by this path yet
# — wspr-recorder Pipeline v2 doesn't persist NoiseData to sink.db.
# Adding the noise table + computing rms/fft noise in-process is a
# parallel work item; wsprdaemon.org accepts a tar with only spots.


_W_EXT_FMT = (
    "%6s %4s %5.2f %6.2f %5.2f %12.7f %-14s %-6s "
    "%2d %2d %4d %4d %4d %4d %2d %3d %3d %2d "
    "%6.1f %6.1f %4d %6s %12s "
    "%5d %6.1f %6.1f %6.1f %6.1f %6.1f %6.1f %6.1f %6.1f "
    "%4d %4d"
)
# 11-field MEPT (wsprnet) format — used for F-mode spots when shipping
# to wsprdaemon.org because there is no extended _wd_spots equivalent
# for F-modes.  W-mode short lines (matching this format) go to
# wsprnet directly via the separate WsprNet transport — wsprdaemon's
# transport does NOT emit them.
_W_SHORT_FMT = "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d"
_F_SHORT_FMT = "%-6s %4s %5.1f %3.0f %5.2f %12.7f %-14s %-6s %2d  0 %4d"

_ABSENT = -999.0       # wd-extend-spots sentinel for missing geodesy


def _loc_to_lat_lon(locator: str) -> tuple[float, float]:
    """Maidenhead 4- or 6-char locator → (lat, lon) degrees.  Port of
    wsprdaemon-client/bin/wd-extend-spots._loc_to_lat_lon."""
    d = list(locator.strip())
    lat = ((ord(d[1]) - 65) * 10) + (ord(d[3]) - 48) + 0.5 - 90.0
    lon = ((ord(d[0]) - 65) * 20) + ((ord(d[2]) - 48) * 2) + 1.0 - 180.0
    if len(locator.strip()) == 6:
        base = 96 if ord(d[4]) > 88 else 64
        lat = lat - 0.5 + (ord(d[5]) - base) / 24.0 - 1.0 / 48.0
        lon = lon - 1.0 + (ord(d[4]) - base) / 12.0 - 1.0 / 24.0
    return lat, lon


def _freq_to_band(freq_mhz: float) -> int:
    """Map MHz to wsprdaemon's integer-meters band id (e.g. 14 → 20)."""
    freq10 = int(10 * freq_mhz)
    return {
        1: 2200, 4: 630, 18: 160, 35: 80, 52: 60, 53: 60,
        70: 40, 101: 30, 140: 20, 181: 17, 210: 15, 249: 12,
        281: 10, 502: 6, 700: 4, 1444: 2, 4323: 70, 12965: 23,
    }.get(freq10, 9999)


def _derive_geo(tx_grid: str, rx_grid: str, freq_mhz: float) -> tuple:
    """Returns (band, km, rx_az, rx_lat, rx_lon, tx_az, tx_lat, tx_lon,
    v_lat, v_lon).  Port of wd-extend-spots._derive_geo."""
    import math
    band = _freq_to_band(freq_mhz)
    if not tx_grid or tx_grid.lower() == "none" or len(tx_grid.strip()) < 4:
        return (band,) + (_ABSENT,) * 9
    try:
        tx_lat, tx_lon = _loc_to_lat_lon(tx_grid)
        rx_lat, rx_lon = _loc_to_lat_lon(rx_grid)
    except Exception:
        return (band,) + (_ABSENT,) * 9
    phi_tx = math.radians(tx_lat)
    lam_tx = math.radians(tx_lon)
    phi_rx = math.radians(rx_lat)
    lam_rx = math.radians(rx_lon)
    d_phi = phi_tx - phi_rx
    d_lam = lam_tx - lam_rx
    y = math.sin(d_lam) * math.cos(phi_tx)
    x = (math.cos(phi_rx) * math.sin(phi_tx)
         - math.sin(phi_rx) * math.cos(phi_tx) * math.cos(d_lam))
    rx_az = math.degrees(math.atan2(y, x)) % 360
    y2 = math.sin(-d_lam) * math.cos(phi_rx)
    x2 = (math.cos(phi_tx) * math.sin(phi_rx)
          - math.sin(phi_tx) * math.cos(phi_rx) * math.cos(-d_lam))
    tx_az = math.degrees(math.atan2(y2, x2)) % 360
    a = (math.sin(d_phi / 2) ** 2
         + math.cos(phi_rx) * math.cos(phi_tx) * math.sin(d_lam / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(max(0.0, a)), math.sqrt(max(0.0, 1 - a)))
    km = 6371.0 * c
    try:
        if c > 1e-9:
            v_lat_rad = math.acos(
                math.cos(phi_rx) * math.cos(phi_tx)
                * abs(math.sin(d_lam / 2)) / math.sin(c / 2)
            )
        else:
            v_lat_rad = phi_rx
        v_lat = math.degrees(v_lat_rad) * (1 if tx_lat > rx_lat else -1)
        v_lon = (tx_lon + rx_lon) / 2.0
    except Exception:
        v_lat = _ABSENT
        v_lon = _ABSENT
    return band, km, rx_az, rx_lat, rx_lon, tx_az, tx_lat, tx_lon, v_lat, v_lon


def _ts_from_iso(iso: str) -> tuple[str, str, str]:
    """('2026-05-14T11:58:00Z' or '+00:00') → ('260514','1158','20260514_115800').

    Returns (date_yymmdd, time_hhmm, filename_ts).
    """
    s = iso.rstrip("Z").rstrip("+").rstrip("0").rstrip(":").rstrip("0")
    # robust: just parse via fromisoformat after stripping Z
    s = iso.replace("Z", "+00:00") if iso.endswith("Z") else iso
    from datetime import datetime
    dt = datetime.fromisoformat(s)
    return (
        dt.strftime("%y%m%d"),
        dt.strftime("%H%M"),
        dt.strftime("%Y%m%d_%H%M%S"),
    )


def _format_extended_line(cols: dict, rx_call: str, rx_grid: str) -> str:
    """Render one schema-v2 row dict as a 34-field wsprdaemon extended
    line (matches the file content wd-extend-spots wrote for v1).

    Geodesy is computed in-line; noise and overload fields are 0 until
    Phase 2 of the SQLite cutover lands.
    """
    date, hhmm, _ = _ts_from_iso(cols["time"])
    freq_mhz = cols["frequency_hz"] / 1_000_000.0
    grid = cols.get("grid") or "none"
    # `drift_hz_per_s` was converted from wsprd's Hz/minute at producer
    # time; v1 extended format wants Hz/minute as int — convert back.
    drift_int = int(round((cols.get("drift_hz_per_s") or 0.0) * 60.0))
    metric_int = int(round((cols.get("metric") or 0.0) * 1000.0))
    band, km, rx_az, rx_lat, rx_lon, tx_az, tx_lat, tx_lon, v_lat, v_lon = (
        _derive_geo(grid, rx_grid, freq_mhz)
    )
    return _W_EXT_FMT % (
        date,
        hhmm,
        cols.get("sync_quality") or 0.0,
        float(cols["snr_db"]),
        cols.get("dt") or 0.0,
        freq_mhz,
        cols["callsign"],
        grid if grid != "none" else "none",
        int(cols["pwr_dbm"]),
        drift_int,
        int(cols.get("cycles") or 0),
        int(cols.get("jitter") or 0),
        int(cols.get("blocksize") or 0),
        metric_int,
        int(cols.get("decodetype") or 0),
        int(cols.get("ipass") or 0),
        int(cols.get("nhardmin") or 0),
        int(cols.get("pkt_mode") or 2),
        0.0,                                   # rms_noise (Phase 2)
        0.0,                                   # fft_noise (Phase 2)
        band,
        rx_grid,
        rx_call,
        int(round(km if km is not None else 0)),
        rx_az,
        rx_lat,
        rx_lon,
        tx_az,
        tx_lat,
        tx_lon,
        v_lat,
        v_lon,
        0,                                     # overloads_count (Phase 2)
        0,                                     # proxy_upload_flag
    )


def _format_short_line(cols: dict, *, w_mode: bool) -> str:
    """Render one row as the 11-field MEPT short line — F-mode form
    for wsprdaemon.org (W-mode short goes to wsprnet via WsprNet
    transport, not this path)."""
    date, hhmm, _ = _ts_from_iso(cols["time"])
    freq_mhz = cols["frequency_hz"] / 1_000_000.0
    grid = cols.get("grid") or ""
    fmt = _W_SHORT_FMT if w_mode else _F_SHORT_FMT
    snr = cols["snr_db"]
    args = (
        date,
        hhmm,
        cols.get("sync_quality") or 0.0,
        int(snr) if w_mode else float(snr),
        cols.get("dt") or 0.0,
        freq_mhz,
        cols["callsign"],
        grid,
        int(cols["pwr_dbm"]),
    )
    if w_mode:
        args += (
            int(round((cols.get("drift_hz_per_s") or 0.0) * 60.0)),
            int(cols.get("pkt_mode") or 2),
        )
    else:
        # F-mode format has drift hardcoded "0" in fmt; only one arg
        # follows: ntype (pkt_mode).
        args += (int(cols.get("pkt_mode") or 3),)
    return fmt % args


def build_wsprdaemon_tar_from_records(
    records: Iterable,
    *,
    rx_call: str,
    rx_grid: str,
    receiver: str,
    rx_site: str,
    version: str = "4.0",
    client_info: Optional[tuple[str, str]] = None,
) -> bytes:
    """Build a wsprdaemon.org bzip2 tar from sink.db-style records.

    Each record's ``columns`` dict is a schema-v2 wspr.spots row
    (see wspr-recorder/wspr_recorder/spot_sink.py:spot_to_row).
    Rows are grouped by (band, mode, cycle-end-time) and rendered
    into per-group files matching v1's layout:

      wsprdaemon/spots/<RX_SITE>/<RECEIVER>/<BAND>/
        <RECEIVER>_<BAND>_W2_YYYYMMDD_HHMMSS_wd_spots.txt    (W-modes)
        <RECEIVER>_<BAND>_F2_YYYYMMDD_HHMMSS_spots.txt        (F-modes)

    The wire-format text per line is byte-identical to v1's
    wd-extend-spots / wd-decode output for the same row data, modulo
    the noise/overload fields which stay 0 until Phase 2 ships the
    in-process noise measurement path.
    """
    # Group by (band, mode, cycle_ts_filename).  Use the row's `time`
    # field (cycle end UTC) as the canonical cycle key.
    groups: dict = {}  # (band, mode, ts_filename) -> list[dict]
    for r in records:
        cols = getattr(r, "columns", None)
        if not cols:
            continue
        try:
            band = cols["band"]
            mode = cols["mode"]
            _, _, ts_filename = _ts_from_iso(cols["time"])
        except (KeyError, ValueError):
            logger.warning("wsprdaemon-tar: skipping malformed row: %r",
                           list(cols.keys()) if cols else cols)
            continue
        groups.setdefault((band, mode, ts_filename), []).append(cols)

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
        for (band, mode, ts_filename), rows in groups.items():
            is_w = mode.startswith("W")
            if is_w:
                lines = [_format_extended_line(r, rx_call, rx_grid)
                         for r in rows]
                suffix = "_wd_spots.txt"
            else:
                lines = [_format_short_line(r, w_mode=False) for r in rows]
                suffix = "_spots.txt"
            content = ("\n".join(lines) + "\n").encode()
            filename = f"{receiver}_{band}_{mode}_{ts_filename}{suffix}"
            arcname = (
                f"wsprdaemon/spots/{rx_site}/{receiver}/{band}/{filename}"
            )
            _tar_add_bytes(tf, arcname, content)
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
        spool_root: Path | str = "",
        sftp_user: Optional[str] = None,
        remote_path: str = "uploads",
        connect_timeout_sec: int = 10,
        xfer_timeout_sec: int = 90,
        version: str = "4.0",
        upload_id: Optional[str] = None,
        name: Optional[str] = None,
        receiver: Optional[str] = None,
    ):
        self.servers = list(servers)
        self.spool_root = Path(spool_root) if spool_root else None
        self.sftp_user_override = sftp_user
        self.remote_path = remote_path
        self.connect_timeout_sec = connect_timeout_sec
        self.xfer_timeout_sec = xfer_timeout_sec
        self.version = version
        self.upload_id = upload_id
        self.name = name or f"wsprdaemon-tar-sftp:{','.join(servers)}"
        # `receiver` is only needed for the SqliteSource path, where it
        # forms the per-band tar-arcname segment.  The legacy file path
        # derives it from the spool_root directory layout instead.
        self.receiver = receiver

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
        try:
            tar_bytes = self._build_tar(batch, identity)
        except Exception as exc:
            return Outcome.permanent_failure(f"tar build failed: {exc}")
        if tar_bytes is None:
            return Outcome.acked()       # nothing to ship in this batch
        tar_name = self._tar_name(identity)
        return self._upload_tar(tar_bytes, tar_name, identity)

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        # Rebuild deterministically so a replay re-sends bit-identical bytes.
        tar_bytes = self._build_tar(batch, identity)
        return tar_bytes if tar_bytes is not None else b""

    # -- tar-build dispatch (file path vs sqlite path) --

    def _build_tar(self, batch: RecordBatch, identity) -> Optional[bytes]:
        """Branch between FileTreeSource records (payload_path) and
        SqliteSource records (columns).  Returns None if there's
        nothing to ship (empty batch, or all rows malformed)."""
        rx_site = _build_rx_site(identity.call, identity.grid)
        paths = [r.payload_path for r in batch.records if getattr(r, "payload_path", None)]
        col_records = [r for r in batch.records
                       if getattr(r, "columns", None)
                       and not getattr(r, "payload_path", None)]
        if paths and self.spool_root is not None:
            return build_wsprdaemon_tar(
                paths,
                root=self.spool_root,
                rx_site=rx_site,
                version=self.version,
            )
        if col_records:
            if not self.receiver:
                raise ValueError(
                    "WsprdaemonTarSftp: SqliteSource records require "
                    "`receiver=` to be set at construction time "
                    "(it forms the tar arcname segment).  "
                    "Pass receiver from WD_RECEIVER_NAME in the shim."
                )
            return build_wsprdaemon_tar_from_records(
                col_records,
                rx_call=identity.call,
                rx_grid=identity.grid,
                receiver=self.receiver,
                rx_site=rx_site,
                version=self.version,
            )
        return None

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
        spool_root: Path | str = "",
        ftp_user: str = "noisegraphs",
        ftp_password_file: Optional[Path | str] = None,
        ftp_password: Optional[str] = None,
        remote_path: str = "upload",
        timeout_sec: int = 90,
        version: str = "4.0",
        upload_id: Optional[str] = None,
        name: Optional[str] = None,
        receiver: Optional[str] = None,
    ):
        self.servers = list(servers)
        self.spool_root = Path(spool_root) if spool_root else None
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
        # SqliteSource path needs the receiver name for the tar arcname.
        # Optional; only required when records carry `columns` rather
        # than `payload_path`.
        self.receiver = receiver

    def primary_table(self) -> str:
        return "wspr.spots"

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=10_000)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        try:
            tar_bytes = self._build_tar(batch, identity)
        except Exception as exc:
            return Outcome.permanent_failure(f"tar build failed: {exc}")
        if tar_bytes is None:
            return Outcome.acked()
        return self._upload(tar_bytes, self._tar_name(identity))

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        tar_bytes = self._build_tar(batch, identity)
        return tar_bytes if tar_bytes is not None else b""

    def _build_tar(self, batch: RecordBatch, identity) -> Optional[bytes]:
        """Same dispatch shape as WsprdaemonTarSftp._build_tar — choose
        between FileTreeSource records (payload_path) and SqliteSource
        records (columns).  FTP path always includes client_info."""
        rx_site = _build_rx_site(identity.call, identity.grid)
        client_info = (identity.call, identity.public_key())
        paths = [r.payload_path for r in batch.records if getattr(r, "payload_path", None)]
        col_records = [r for r in batch.records
                       if getattr(r, "columns", None)
                       and not getattr(r, "payload_path", None)]
        if paths and self.spool_root is not None:
            return build_wsprdaemon_tar(
                paths,
                root=self.spool_root,
                rx_site=rx_site,
                version=self.version,
                client_info=client_info,
            )
        if col_records:
            if not self.receiver:
                raise ValueError(
                    "WsprdaemonTarFtp: SqliteSource records require "
                    "`receiver=` to be set at construction time."
                )
            return build_wsprdaemon_tar_from_records(
                col_records,
                rx_call=identity.call,
                rx_grid=identity.grid,
                receiver=self.receiver,
                rx_site=rx_site,
                version=self.version,
                client_info=client_info,
            )
        return None

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
