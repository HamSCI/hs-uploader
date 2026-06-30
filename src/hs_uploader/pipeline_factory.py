"""Build hs-uploader ``Pipeline`` objects from a declarative manifest.

The host uploader daemon (``hs_uploader.daemon``) runs every outbound pipeline
on a host in one process.  This factory turns a manifest dict (parsed from
``/etc/hs-uploader/pipelines.toml``) into the list of ``Pipeline`` objects,
sharing ONE ``SqliteWatermarkStore`` and a base ``StationIdentity`` (with
per-pipeline identity overrides for call/grid/station_id).

Manifest shape::

    [identity]                 # base identity for every pipeline
    call = "AC0G/S"
    grid = "EM38ww"
    station_id = "S000418"
    ssh_key_file = "/etc/hs-uploader/keys/id_ed25519"

    [[pipeline]]
    name = "grape-psws"
    max_records_per_pump = 64          # optional
    [pipeline.source]   type = "filetree"  root = "..."  patterns = ["OBS*"]
                        retention = "keep" match_dirs = true table = "grape.dataset"
    [pipeline.transport] type = "psws_dataset" instrument_id = "367"
                         host = "pswsnetwork.eng.ua.edu" table = "grape.dataset"
    [pipeline.identity]  station_id = "S000418"   # optional per-pipeline override

Source types: ``sqlite``, ``filetree``.  Transport types: ``psws_dataset``,
``pskreporter``, ``wsprnet``.  (The cycle-aligned wsprdaemon tar — which needs a
runtime ``ceiling_provider`` callable + receiver/FTP-fallback objects — is built
by a code ``builder`` entrypoint instead; see ``hs_uploader.daemon``.)
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import Any, Mapping, Optional

from .config import StationIdentity
from .core import Pipeline, RetryPolicy
from .sources import FileSpec, FileTreeSource
from .sources.sqlite import SqliteSource
from .transports.pskreporter import PskReporterTcp
from .transports.psws_magnetometer import PswsDatasetSftp
from .transports.wsprnet import WsprNet
from .watermark.sqlite import SqliteWatermarkStore, default_path

logger = logging.getLogger(__name__)


def _identity(base: Mapping[str, Any], override: Optional[Mapping[str, Any]]) -> StationIdentity:
    merged = dict(base or {})
    if override:
        merged.update({k: v for k, v in override.items() if v is not None})
    ident = StationIdentity()
    for field in ("call", "grid", "station_id", "ssh_key_file", "radiod_id"):
        if merged.get(field):
            setattr(ident, field, str(merged[field]))
    return ident


# ---- source builders --------------------------------------------------------


def _build_source(spec: Mapping[str, Any]):
    stype = str(spec.get("type", "")).strip().lower()
    if stype == "filetree":
        patterns = spec.get("patterns") or [spec.get("pattern", "*")]
        table = str(spec.get("table", "files"))
        retention = (
            FileTreeSource.KEEP
            if str(spec.get("retention", "delete_on_ack")).lower() == "keep"
            else FileTreeSource.DELETE_ON_ACK
        )
        return FileTreeSource(
            root=Path(str(spec["root"])),
            specs=[FileSpec(pattern=str(p), parser=None, table=table) for p in patterns],
            retention=retention,
            match_dirs=bool(spec.get("match_dirs", False)),
            source_id=spec.get("source_id"),
        )
    if stype == "sqlite":
        extra_where = None
        if spec.get("extra_where"):
            # list of [col, op, value] → tuples
            extra_where = [tuple(c) for c in spec["extra_where"]]
        return SqliteSource.from_env(
            database=str(spec["database"]),
            table=str(spec["table"]),
            accepted_schema_versions=list(spec.get("accepted_schema_versions", [1])),
            select_columns=spec.get("select_columns"),
            extra_where=extra_where,
            start_at=spec.get("start_at"),
            delete_on_commit=bool(spec.get("delete_on_commit", True)),
            dedup_partition_by=spec.get("dedup_partition_by"),
            dedup_order_by_desc=spec.get("dedup_order_by_desc"),
        )
    raise ValueError(f"unknown source type: {stype!r}")


# ---- transport builders -----------------------------------------------------


def _build_transport(spec: Mapping[str, Any]):
    ttype = str(spec.get("type", "")).strip().lower()
    if ttype == "psws_dataset":
        kw: dict[str, Any] = dict(
            instrument_id=str(spec["instrument_id"]),
            host=str(spec.get("host", "pswsnetwork.eng.ua.edu")),
            table=str(spec.get("table", "mag.daily_zip")),
            dry_run=bool(spec.get("dry_run", False)),
        )
        if spec.get("sftp_user"):
            kw["sftp_user"] = str(spec["sftp_user"])
        if spec.get("ssh_key_file"):
            kw["ssh_key_file"] = str(spec["ssh_key_file"])
        bw = spec.get("bandwidth_limit_kbps")
        kw["bandwidth_limit_kbps"] = None if bw in (None, 0, "0", "") else int(bw)
        if spec.get("name"):
            kw["name"] = str(spec["name"])
        return PswsDatasetSftp(**kw)
    if ttype == "pskreporter":
        kw = {}
        for k in ("host", "decoding_software", "antenna", "primary_table", "name"):
            if spec.get(k) is not None:
                kw[k] = str(spec[k])
        if spec.get("port") is not None:
            kw["port"] = int(spec["port"])
        return PskReporterTcp(**kw)
    if ttype == "wsprnet":
        kw = {}
        if spec.get("api_base_url") is not None:
            kw["api_base_url"] = str(spec["api_base_url"])
        if spec.get("version") is not None:
            kw["version"] = str(spec["version"])
        if spec.get("max_spots_per_upload") is not None:
            kw["max_spots_per_upload"] = int(spec["max_spots_per_upload"])
        if spec.get("poll_interval_sec") is not None:
            kw["poll_interval_sec"] = float(spec["poll_interval_sec"])
        if spec.get("name") is not None:
            kw["name"] = str(spec["name"])
        return WsprNet(**kw)
    raise ValueError(f"unknown transport type: {ttype!r}")


def _retry(spec: Optional[Mapping[str, Any]]) -> RetryPolicy:
    if not spec:
        return RetryPolicy()
    return RetryPolicy.exponential(
        base=float(spec.get("base", 2.0)),
        cap_sec=float(spec.get("cap_sec", 300.0)),
    )


def build_pipelines(
    manifest: Mapping[str, Any],
    *,
    watermark: Optional[SqliteWatermarkStore] = None,
) -> list[Pipeline]:
    """Construct every ``Pipeline`` declared in ``manifest``.

    Pipelines with a ``source``+``transport`` block are built generically here.
    Pipelines with a ``builder = "module:func"`` are NOT handled here — the
    daemon resolves those via :func:`hs_uploader.daemon.resolve_builder`.
    """
    base_identity = manifest.get("identity", {}) or {}
    wm = watermark or SqliteWatermarkStore(default_path())
    pipelines: list[Pipeline] = []
    for entry in manifest.get("pipeline", []) or []:
        if entry.get("builder"):
            continue  # builder-entrypoint pipelines handled by the daemon
        name = str(entry.get("name") or f"pipeline-{len(pipelines)}")
        source = _build_source(entry["source"])
        transport = _build_transport(entry["transport"])
        identity = _identity(base_identity, entry.get("identity"))
        mrpp = entry.get("max_records_per_pump")
        pipelines.append(Pipeline(
            name=name,
            source=source,
            transport=transport,
            watermark=wm,
            identity=identity,
            retry=_retry(entry.get("retry")),
            batch_limit=int(entry.get("batch_limit", 1000)),
            max_records_per_pump=None if mrpp is None else int(mrpp),
        ))
        logger.info("pipeline-factory: built %s (%s → %s)",
                    name, type(source).__name__, transport.name)
    return pipelines
