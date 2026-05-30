"""wsprdaemon.org transports — SFTP primary, FTP fallback.

Phase 2 (PR 5, 2026-05-18) layout:

  <tar root>/
  ├── uploads_config.txt                                       (sigmond metadata)
  ├── client_upload_info.txt                                   (FTP-bootstrap only)
  ├── routing.json                                             (per-receiver forwarding flags; only when ft* present)
  ├── wspr/spots/<RX_SITE>/<RECEIVER>/<BAND>/<filename>        (WSPR spots)
  ├── wspr/noise/<RX_SITE>/<RECEIVER>/<BAND>/<filename>        (WSPR noise)
  ├── ft8/<RX_SITE>/<RECEIVER>/<BAND>/<filename>.jsonl         (FT8 spots)
  ├── ft4/<RX_SITE>/<RECEIVER>/<BAND>/<filename>.jsonl         (FT4 spots)
  └── msk144/...                                                (future)

The wsprdaemon-server (Phase 2 PR 1) accepts both this layout and the
legacy ``wsprdaemon/spots/...`` root during transition.

Compression: zstd -9 by default (chosen after a 859 MB B4-100 benchmark
that showed ~14× faster compress and ~47× faster decompress vs bzip2,
with a 25% larger wire size).  Filename suffix stays ``.tbz`` for
discovery-loop compatibility — the server sniffs leading magic bytes.

The transport reads a batch of Records (mix of FileTreeSource records
with ``payload_path`` and SqliteSource records with ``columns``) and
bundles them.  The batch's ``commit_token`` flows through the
orchestrator via the deliverable; the source's ``commit()`` does the
cleanup (no-op when delete_on_commit=False — sigmond's storage trim is
the actual cleanup).
"""

from __future__ import annotations

import datetime as _dt
import io
import json
import logging
import os
import re
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


# Phase 2 PR 5: per-tar compression. zstd-9 is the default after the
# B4-100 benchmark; bz2 stays selectable for hosts running on
# wsprdaemon-server builds pre-zstd-sniff. The server sniffs leading
# magic bytes (not the filename suffix), so a producer flipping
# `compression` mid-stream is safe as long as every active wd{10,20,30}
# is running >= Phase 2 PR 1.
COMPRESSION_ZSTD = "zstd"
COMPRESSION_BZ2  = "bz2"


# Phase 2 PR 5 also flipped the tar root from `wsprdaemon/` to `wspr/`
# (modes as peers, not WSPR-specific service nesting). The
# wsprdaemon-server PR accepts BOTH roots on the read side — but only
# AFTER the wd{10,20,30} processes have been restarted with the new
# code. Producers shipping to a wd still running the old daemon must
# stay on `wsprdaemon/` or the daemon silently drops the spots
# (its `spots_root.exists()` check returns False on `wspr/spots/...`).
#
# Knob: `WSPRDAEMON_TAR_ROOT=wspr` (new, default) or `wsprdaemon` (legacy).
TAR_ROOT_NEW    = "wspr"
TAR_ROOT_LEGACY = "wsprdaemon"


def _compress_tar_bytes(raw: bytes, *, compression: str, level: int) -> bytes:
    """Compress an in-memory uncompressed tar.

    `compression` selects the codec; `level` is the codec-specific
    quality (1..22 for zstd, 1..9 for bz2). On import failure for
    zstd we fall back to bz2 with a one-shot warning — running
    without a producer ever blocking on a missing package is more
    important than the marginal wire-size win.
    """
    if compression == COMPRESSION_ZSTD:
        try:
            import zstandard
        except ImportError:
            logger.warning(
                "wsprdaemon-tar: `zstandard` not installed, "
                "falling back to bz2 -9 for this tar",
            )
            compression = COMPRESSION_BZ2
            level = 9
        else:
            return zstandard.ZstdCompressor(level=level).compress(raw)
    # bz2 path (default 9 — only choice that was production-tested
    # before PR 5).
    import bz2
    return bz2.compress(raw, compresslevel=max(1, min(9, level)))


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


def _arcname_for(path: Path, root: Path, rx_site: str,
                 *, tar_root: str = TAR_ROOT_NEW) -> str:
    """Map a queued file's path under ``root`` to the canonical
    server-expected arcname under the Phase 2 layout.

    `tar_root` selects which top-level dir name is used:
      * "wspr"       (Phase 2 default) — modes as peers at the tar root.
      * "wsprdaemon" (legacy)          — for shipping to a wd* whose
        wsprdaemon-server process hasn't picked up the dual-root patch
        yet.  Both produce server-identical content; only the leading
        path component differs.
    """
    try:
        rel = path.relative_to(root)
    except ValueError:
        return f"{tar_root}/{path.name}"
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
        return f"{tar_root}/noise/{rx_site}/{receiver}/{band}/{ts_name}"

    if not is_noise and len(parts) >= 3:
        receiver, band = parts[0], parts[1]
        return f"{tar_root}/spots/{rx_site}/{receiver}/{band}/{path.name}"

    return f"{tar_root}/{rel.as_posix()}"


def build_wsprdaemon_tar(
    paths: Iterable[Path],
    *,
    root: Path,
    rx_site: str,
    version: str = "4.0",
    client_info: Optional[tuple[str, str]] = None,
    compression: str = COMPRESSION_ZSTD,
    compression_level: int = 9,
    tar_root: str = TAR_ROOT_NEW,
) -> bytes:
    """Return an in-memory compressed tar of the Phase 2 wsprdaemon shape.

    ``client_info`` is ``(reporter_id, ssh_pubkey)`` when included
    (FTP-fallback path so the gateway can provision SFTP for the
    reporter); ``None`` otherwise (SFTP-primary path uses the SSH key
    as identity).

    Metadata files (uploads_config.txt, client_upload_info.txt) live at
    the tar root — they describe the SHIPMENT, not a single mode.
    """
    raw_buf = io.BytesIO()
    with tarfile.open(fileobj=raw_buf, mode="w:") as tf:
        _tar_add_bytes(tf, "uploads_config.txt", _config_bytes(version))
        if client_info is not None:
            reporter_id, pubkey = client_info
            _tar_add_bytes(
                tf, "client_upload_info.txt",
                _client_info_bytes(reporter_id, pubkey),
            )
        for path in paths:
            arcname = _arcname_for(path, root=root, rx_site=rx_site,
                                    tar_root=tar_root)
            try:
                tf.add(str(path), arcname=arcname, recursive=False)
            except OSError as exc:
                logger.warning("wsprdaemon-tar: cannot add %s: %s", path, exc)
    return _compress_tar_bytes(
        raw_buf.getvalue(),
        compression=compression, level=compression_level,
    )


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

# Noise line format — v1's `_compute_noise_line` writes 15 fields:
#   pre[Pk RMS RMSpk RMStr] tx[Pk RMS RMSpk RMStr] post[Pk RMS RMSpk RMStr]
#   rms_noise fft_noise overloads
# The first 12 (sox-derived) fields are not stored in sink.db (only the
# calibrated rms/fft/overloads survive); we emit 0.00 for them so the
# wire format stays 15-field-wide and wsprdaemon-server's parser
# (which keys off fields 13-15) sees what it expects.
_NOISE_FMT = (
    "%5.2f %5.2f %5.2f %5.2f "      # pre
    "%5.2f %5.2f %5.2f %5.2f "      # tx
    "%5.2f %5.2f %5.2f %5.2f "      # post
    "%5.2f %5.2f %d"                # rms_noise fft_noise overloads
)

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


def _format_noise_line(cols: dict) -> str:
    """Render a sink.db wspr.noise row → 15-field v1 noise.txt line.

    Sink.db only carries the calibrated values (rms_noise_dbm,
    fft_noise_dbm, overload_count); the 12 sox-derived stats are
    zeroed since they weren't captured at producer time.  Server-side
    parsers historically read fields 13-15 only.
    """
    return _NOISE_FMT % (
        0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0,
        float(cols.get("rms_noise_dbm") or 0.0),
        float(cols.get("fft_noise_dbm") or 0.0),
        int(cols.get("overload_count") or 0),
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


def _psk_cycle_key(time_iso: str) -> str:
    """Floor a psk row's ISO time to the 2-min wsprdaemon cycle boundary.

    Mirrors the wspr cycle alignment so a single tar can carry every
    mode's spots from the same upload window with consistent filenames.
    Falls back to the raw timestamp if parsing fails.
    """
    try:
        dt = _dt.datetime.fromisoformat(time_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return "000000_0000"
    cycle_minute = (dt.minute // 2) * 2
    return dt.strftime("%y%m%d_%H") + f"{cycle_minute:02d}"


def _render_routing_json(receivers_forward: dict) -> bytes:
    """Emit routing.json from a {receiver_key: forward_bool} map.

    Receiver key: ``<RX_SITE>/<RECEIVER>``. When every receiver shares
    the same flag, collapse to a single ``default`` entry; otherwise
    emit ``default`` + per-receiver overrides. Producer always biases
    `default` toward True so an older server defaults to forwarding.
    """
    if not receivers_forward:
        return json.dumps(
            {"default": {"forward_to_pskreporter": True}}
        ).encode() + b"\n"
    flags = set(receivers_forward.values())
    if len(flags) == 1:
        only = flags.pop()
        return json.dumps(
            {"default": {"forward_to_pskreporter": bool(only)}}
        ).encode() + b"\n"
    # Mixed: pick the majority as default, override the minority. Keeps
    # routing.json compact even on a host with many receivers.
    n_true = sum(1 for v in receivers_forward.values() if v)
    default_flag = n_true >= (len(receivers_forward) / 2)
    obj = {"default": {"forward_to_pskreporter": bool(default_flag)}}
    for key, val in sorted(receivers_forward.items()):
        if bool(val) != default_flag:
            obj[key] = {"forward_to_pskreporter": bool(val)}
    return json.dumps(obj, sort_keys=True).encode() + b"\n"


def build_wsprdaemon_tar_from_records(
    records: Iterable,
    *,
    rx_call: str,
    rx_grid: str,
    receiver: str,
    rx_site: str,
    version: str = "4.0",
    client_info: Optional[tuple[str, str]] = None,
    compression: str = COMPRESSION_ZSTD,
    compression_level: int = 9,
    tar_root: str = TAR_ROOT_NEW,
) -> bytes:
    """Build a Phase 2 wsprdaemon tar from sink.db-style records.

    Records are split by `r.table`:

      wspr.spots → grouped by (band, mode, cycle) →
                   wspr/spots/<RX_SITE>/<RECEIVER>/<BAND>/<filename>
      wspr.noise → grouped by (band, cycle) →
                   wspr/noise/<RX_SITE>/<RECEIVER>/<BAND>/<filename>
      psk.spots  → grouped by (mode, band, cycle) →
                   <mode>/<RX_SITE>/<RECEIVER>/<BAND>/<filename>.jsonl
                   (mode = `ft8`, `ft4`, future `msk144`, …)

    Spot records without a `table` attribute default to wspr.spots for
    back-compat with the file-source path that still emits naked
    spot records. Records carrying a `psk.spots` table also produce
    a ``routing.json`` at the tar root with the per-receiver forwarding
    flags collected from each row's ``forward_to_pskreporter`` field.
    """
    spot_groups: dict = {}
    noise_groups: dict = {}
    # psk records → grouped by (mode, band, cycle); each entry is a
    # list of dicts (one JSONL row each).
    psk_groups: dict = {}
    # Per-receiver forwarding intent collected from psk rows.
    receivers_forward: dict = {}

    def _row_rx_identity(cols: dict) -> tuple:
        """Per-row receiver identity for diversity attribution.

        When several RX-888s feed one shared sink and a single
        merge-uploader ships the union, each spot/noise row must be
        filed under ITS OWN receiver's RX_SITE on wsprdaemon.org —
        NOT the uploader's identity — so per-receiver diversity stats
        stay correct.  The producer (wspr-recorder spot_sink) tags
        every row with `rx_call` / `rx_grid` / `radiod_id`; we derive
        the RX_SITE and RECEIVER arcname segments from those.

        Falls back to the uploader-level `rx_call` / `rx_grid` /
        `receiver` / `rx_site` args for rows that don't carry the
        per-row fields (the FileTreeSource path, and single-receiver
        deployments predating multi-RX tagging).  Returns
        (row_rx_call, row_rx_grid, row_receiver, row_rx_site).
        """
        row_call = (cols.get("rx_call") or rx_call or "").strip()
        row_grid = (cols.get("rx_grid") or rx_grid or "").strip()
        # RECEIVER path segment: the physical receiver, distinct from
        # the recorder host (all instances share one host).  radiod_id
        # is per-receiver (`bee1-status.local`); strip the mDNS suffix
        # and `radiod:` prefix for a clean path component.
        raw_recv = (
            cols.get("radiod_id") or cols.get("rx_source")
            or receiver or "rx"
        )
        row_recv = (
            raw_recv.replace("radiod:", "").replace("-status.local", "")
            or receiver or "rx"
        )
        row_site = _build_rx_site(row_call, row_grid) if row_call else rx_site
        return (row_call, row_grid, row_recv, row_site)

    for r in records:
        cols = getattr(r, "columns", None)
        if not cols:
            continue
        table = getattr(r, "table", "wspr.spots") or "wspr.spots"
        if table == "psk.spots":
            mode = (cols.get("mode") or "").lower()
            if mode not in ("ft8", "ft4", "msk144"):
                continue
            # psk-recorder's ChTailer doesn't set ``band`` on the row
            # (the schema field is server-side-only).  Derive it here
            # from frequency_hz so the tar arcname segment is the
            # canonical metre-band (20, 40, ...) — what
            # band_str_to_meters() on the wsprdaemon-server side
            # expects.  Falls back to 0 when frequency is missing or
            # outside the ham bands; server logs a warning + skips.
            band = cols.get("band")
            if not band:
                freq_hz = int(cols.get("frequency") or 0)
                band = _freq_to_band(freq_hz / 1_000_000.0) if freq_hz else 0
            cycle = _psk_cycle_key(cols.get("time", ""))
            psk_groups.setdefault((mode, band, cycle), []).append(cols)
            psk_receiver = cols.get("receiver") or receiver
            key = f"{rx_site}/{psk_receiver}"
            # If any single row in this receiver's batch wants
            # forwarding=False, the receiver-level flag flips False.
            # Conservative: prefer NOT-forwarding when the producer
            # explicitly opted out for any row.
            existing = receivers_forward.get(key, True)
            receivers_forward[key] = existing and bool(
                cols.get("forward_to_pskreporter", True)
            )
            continue
        try:
            band = cols["band"]
            _, _, ts_filename = _ts_from_iso(cols["time"])
        except (KeyError, ValueError):
            logger.warning("wsprdaemon-tar: skipping malformed row: %r",
                           list(cols.keys()) if cols else cols)
            continue
        # Per-row receiver identity → keys the group so two receivers'
        # same-band-same-cycle rows land in distinct RX_SITE subtrees.
        rx_id = _row_rx_identity(cols)
        if table == "wspr.noise":
            # One noise row per (receiver, band, cycle); last write
            # wins inside a single batch (shouldn't happen but be
            # defensive).
            noise_groups[(rx_id, band, ts_filename)] = cols
        else:
            try:
                mode = cols["mode"]
            except KeyError:
                logger.warning("wsprdaemon-tar: spot row missing `mode`: %r",
                               list(cols.keys()))
                continue
            spot_groups.setdefault(
                (rx_id, band, mode, ts_filename), []
            ).append(cols)

    raw_buf = io.BytesIO()
    with tarfile.open(fileobj=raw_buf, mode="w:") as tf:
        _tar_add_bytes(tf, "uploads_config.txt", _config_bytes(version))
        if client_info is not None:
            reporter_id, pubkey = client_info
            _tar_add_bytes(
                tf, "client_upload_info.txt",
                _client_info_bytes(reporter_id, pubkey),
            )
        if psk_groups:
            _tar_add_bytes(
                tf, "routing.json",
                _render_routing_json(receivers_forward),
            )
        for (rx_id, band, mode, ts_filename), rows in spot_groups.items():
            row_call, row_grid, row_recv, row_site = rx_id
            is_w = mode.startswith("W")
            if is_w:
                # Per-row identity: the spot line carries this
                # receiver's call+grid, not the uploader's.
                lines = [_format_extended_line(r, row_call, row_grid)
                         for r in rows]
                suffix = "_wd_spots.txt"
            else:
                lines = [_format_short_line(r, w_mode=False) for r in rows]
                suffix = "_spots.txt"
            content = ("\n".join(lines) + "\n").encode()
            filename = f"{row_recv}_{band}_{mode}_{ts_filename}{suffix}"
            arcname = f"{tar_root}/spots/{row_site}/{row_recv}/{band}/{filename}"
            _tar_add_bytes(tf, arcname, content)
        for (rx_id, band, ts_filename), row in noise_groups.items():
            row_call, row_grid, row_recv, row_site = rx_id
            content = (_format_noise_line(row) + "\n").encode()
            # Filename: <RECEIVER>_<BAND>_<YYYYMMDD>_<HHMMSS>_noise.txt
            filename = f"{row_recv}_{band}_{ts_filename}_noise.txt"
            arcname = f"{tar_root}/noise/{row_site}/{row_recv}/{band}/{filename}"
            _tar_add_bytes(tf, arcname, content)
        for (mode, band, cycle), rows in psk_groups.items():
            psk_receiver = (rows[0].get("receiver") or receiver)
            # One JSONL row per spot, ordered by time then frequency
            # for byte-stable retries.
            rows_sorted = sorted(
                rows,
                key=lambda c: (c.get("time", ""), c.get("frequency", 0)),
            )
            lines = [json.dumps(c, default=str, sort_keys=True) for c in rows_sorted]
            content = ("\n".join(lines) + "\n").encode()
            filename = f"{cycle}_{mode}.jsonl"
            arcname = f"{mode}/{rx_site}/{psk_receiver}/{band}/{filename}"
            _tar_add_bytes(tf, arcname, content)
    return _compress_tar_bytes(
        raw_buf.getvalue(),
        compression=compression, level=compression_level,
    )


# ---------- SFTP transport ----------


class WsprdaemonTarSftp:
    """Ships a bzip2 tar of WSPR spot/noise files to wsprdaemon.org via
    SFTP using ``.part``-then-rename.

    ``servers`` is a list of host-only strings (``["gw1.wsprdaemon.org",
    "gw2.wsprdaemon.org"]``); the SFTP login user is derived from
    ``StationIdentity.call`` (``AC0G/B1`` → ``AC0G_B1``) unless overridden.
    """

    # Schema versions actually written by producers (as of 2026-05-23):
    #   wspr.spots: SCHEMA_VERSION=2 (wspr-recorder spot_sink)
    #   wspr.noise: NOISE_SCHEMA_VERSION=1 (wspr-recorder spot_sink)
    #   psk.spots:  schema_version=2 (psk-recorder ch_tailer)
    # Pre-2026-05-23 this declared [3] for wspr.spots/noise, which
    # was aspirational — no producer ever wrote v3.  ACCEPTS is
    # currently advisory (no orchestrator gate enforces it), so the
    # mismatch wasn't causing the upload failure on its own — but the
    # inconsistency made future strict-mode enforcement a footgun.
    # Listing the actually-written versions plus [3] keeps room for
    # producers to bump without breaking deliveries.
    ACCEPTS = {
        "wspr.spots": [1, 2, 3],
        "wspr.noise": [1, 2, 3],
        "psk.spots": [2],
    }

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
        fallback_ftp: Optional["WsprdaemonTarFtp"] = None,
        primary_table_name: str = "wspr.spots",
        compression: str = COMPRESSION_ZSTD,
        compression_level: int = 9,
        tar_root: str = TAR_ROOT_NEW,
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
        self.tar_root = tar_root
        # Compression knobs. zstd-9 is the post-PR-5 default after the
        # B4-100 benchmark; bz2 stays selectable for hosts shipping to
        # an older wsprdaemon-server build.
        self.compression = compression
        self.compression_level = int(compression_level)
        # `receiver` is only needed for the SqliteSource path, where it
        # forms the per-band tar-arcname segment.  The legacy file path
        # derives it from the spool_root directory layout instead.
        self.receiver = receiver
        # The "primary" logical table this transport's watermark is keyed
        # on.  Defaults to "wspr.spots" for backward compat with the old
        # single-table SqliteSource pipeline; pass "wspr.cycle" when wired
        # behind a WsprCycleSource so the cycle-aligned watermark gets a
        # distinct key.  See sources/wspr_cycle.py for the rationale.
        self._primary_table = primary_table_name
        # Optional FTP fallback used only when every SFTP server fails
        # (typically first-time bootstrap before the gateway has the
        # reporter's pubkey).  When wired, the FTP path includes
        # ``client_upload_info.txt`` so the gateway can auto-provision
        # SFTP access for the next cycle.
        self.fallback_ftp = fallback_ftp

    # -- Transport protocol --

    def primary_table(self) -> str:
        # Caller-configurable so a cycle-aligned source (WsprCycleSource)
        # can park its watermark on "wspr.cycle" rather than colliding
        # with the table-keyed pipelines that read raw rows.
        return self._primary_table

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=10_000)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        try:
            tar_bytes = self._build_tar(batch, identity)
        except Exception as exc:
            return Outcome.permanent_failure(f"tar build failed: {exc}")
        if tar_bytes is None:
            return Outcome.acked()       # nothing to ship in this batch
        tar_name = self._tar_name(identity, batch=batch)
        outcome = self._upload_tar(tar_bytes, tar_name, identity)
        if outcome.kind == "retry_later" and self.fallback_ftp is not None:
            logger.warning(
                "WsprdaemonTarSftp: all SFTP servers failed — "
                "attempting FTP fallback (will include client_upload_info.txt "
                "so the gateway can auto-provision SFTP for the next cycle)"
            )
            return self.fallback_ftp.ship(batch, identity)
        return outcome

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
                compression=self.compression,
                compression_level=self.compression_level,
                tar_root=self.tar_root,
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
                compression=self.compression,
                compression_level=self.compression_level,
                tar_root=self.tar_root,
            )
        return None

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        # NB: replay's payload_blob is whatever ``serialize_for_retry``
        # produced when the deliverable was first queued — an SFTP-flavor
        # tar without ``client_upload_info.txt``.  We deliberately do NOT
        # chain to ``fallback_ftp.replay`` here: handing an SFTP-flavor
        # blob to FTP would bypass the gateway's auto-provisioning hook
        # (the whole point of the FTP fallback is to deliver
        # ``client_upload_info.txt``).  The FTP fallback only fires from
        # ``ship()``, where the batch is still available to rebuild a
        # proper FTP-shaped tar.
        tar_name = self._tar_name(identity)
        return self._upload_tar(payload_blob, tar_name, identity)

    # -- internals --

    def _sftp_user(self, identity) -> str:
        if self.sftp_user_override:
            return self.sftp_user_override
        return identity.call.replace("/", "_") if identity.call else "wsprdaemon"

    def _tar_name(self, identity, batch: Optional[RecordBatch] = None) -> str:
        # Cycle-aligned tar name when batch.records are available:
        # ``<upload_id>_YYMMDD_HHMM.tbz`` derived from the records'
        # WSPR cycle time.  This gives one stable name per cycle so
        # concurrent pipeline pumps can't collide on the SFTP side.
        # Falls back to upload-time only when no records are available
        # (replay path, or empty batch).
        upload_id = (
            self.upload_id
            or (identity.call.replace("/", "_") if identity.call else "wsprdaemon")
        )
        if batch is not None and batch.records:
            ts = batch.records[0].time.strftime("%y%m%d_%H%M")
        else:
            ts = time.strftime("%y%m%d_%H%M_%S", time.gmtime())
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

    # Mirrors WsprdaemonTarSftp.ACCEPTS — same producer / same payload.
    ACCEPTS = {
        "wspr.spots": [1, 2, 3],
        "wspr.noise": [1, 2, 3],
        "psk.spots": [2],
    }

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
        compression: str = COMPRESSION_ZSTD,
        compression_level: int = 9,
        tar_root: str = TAR_ROOT_NEW,
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
        # Compression carried through to build_wsprdaemon_tar*; the FTP
        # path stays in sync with SFTP so a fallback shipment is the
        # same wire format the server would have received via SFTP.
        self.compression = compression
        self.compression_level = int(compression_level)
        self.tar_root = tar_root

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
        return self._upload(tar_bytes, self._tar_name(identity, batch=batch))

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        tar_bytes = self._build_tar(batch, identity)
        return tar_bytes if tar_bytes is not None else b""

    def _build_tar(self, batch: RecordBatch, identity) -> Optional[bytes]:
        """Same dispatch shape as WsprdaemonTarSftp._build_tar — choose
        between FileTreeSource records (payload_path) and SqliteSource
        records (columns).  FTP path always includes client_info."""
        rx_site = _build_rx_site(identity.call, identity.grid)
        # reporter_id must be a valid Linux username (the gateway runs
        # ``useradd $reporter_id`` to provision SFTP).  Apply the same
        # ``/`` → ``_`` transform that WsprdaemonTarSftp._sftp_user uses
        # so the linux user the gateway creates exactly matches the
        # SFTP login user the client will later try to authenticate as.
        reporter_id = (
            identity.call.replace("/", "_") if identity.call else "wsprdaemon"
        )
        client_info = (reporter_id, identity.public_key())
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
                compression=self.compression,
                compression_level=self.compression_level,
                tar_root=self.tar_root,
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
                compression=self.compression,
                compression_level=self.compression_level,
                tar_root=self.tar_root,
            )
        return None

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        return self._upload(payload_blob, self._tar_name(identity))

    # -- internals --

    def _password(self) -> str:
        if self.ftp_password_file and self.ftp_password_file.exists():
            return self.ftp_password_file.read_text().strip()
        return self.ftp_password or ""

    def _tar_name(self, identity, batch: Optional[RecordBatch] = None) -> str:
        # Same cycle-aligned tar name policy as WsprdaemonTarSftp.
        upload_id = (
            self.upload_id
            or (identity.call.replace("/", "_") if identity.call else "wsprdaemon")
        )
        if batch is not None and batch.records:
            ts = batch.records[0].time.strftime("%y%m%d_%H%M")
        else:
            ts = time.strftime("%y%m%d_%H%M_%S", time.gmtime())
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
