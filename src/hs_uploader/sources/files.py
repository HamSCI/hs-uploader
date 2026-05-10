"""File-tree source — yields files matching one or more glob patterns.

Two retention modes:

* ``delete_on_ack`` (default) — matches the wsprdaemon spool
  convention: each iter_batches scans the directory afresh; on ack the
  source deletes the just-shipped files and prunes empty parent
  directories.  Cursor is degenerate (always ``b""``); the
  ``commit_token`` carries the path list to delete.

* ``keep`` — files stay on disk after upload; the cursor
  ``(filename, byte_offset)`` walks a deterministic ordering.  Used by
  Digital RF / PSWS (Phase 4) where datasets are byte-verified
  server-side and not deleted client-side.

Per-extension parsers can be registered to produce structured
``Record.columns``; transports that just want bytes-on-disk (the
wsprdaemon tar bundle) ignore ``columns`` and consume
``Record.payload_path`` instead.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Iterable, Iterator, Mapping, Optional, Sequence, Union

from ..core import Record, RecordBatch

logger = logging.getLogger(__name__)


# Parser callback: (path, raw_bytes) -> Mapping or Iterable[Mapping].
# A single Mapping yields one Record per file (the wsprdaemon shape).
# An Iterable yields one Record per item — used by sources whose files
# bundle N rows (e.g. psk-recorder's per-slot spots files: many decoded
# spots per file, each a separate row at PskReporter).
# If the mapping has a ``time`` key carrying a datetime, that becomes
# ``Record.time`` for that row; otherwise the file's mtime is used.
ParserFn = Callable[[Path, bytes], Union[Mapping, Iterable[Mapping]]]


@dataclass
class FileSpec:
    """One pattern + parser pair."""

    pattern: str
    parser: Optional[ParserFn] = None
    table: str = "files"


class FileTreeSource:
    """Walks ``root`` for files matching any of ``patterns``.

    Files are yielded oldest-first by mtime, in batches up to ``limit``
    records.  Each yielded ``Record`` has ``payload_path`` set; if a
    parser is registered, ``columns`` carries the parsed fields too.

    In ``delete_on_ack`` mode (default), ``commit_token`` is a
    JSON-encoded list of paths the source will delete on ack.  In
    ``keep`` mode, ``commit_token`` is empty and the cursor advances
    by mtime so subsequent polls skip already-shipped files.
    """

    DELETE_ON_ACK = "delete_on_ack"
    KEEP = "keep"

    def __init__(
        self,
        root: Path | str,
        *,
        specs: Sequence[FileSpec],
        retention: str = DELETE_ON_ACK,
        source_id: Optional[str] = None,
        prune_empty_dirs: bool = True,
    ):
        self.root = Path(root)
        self.specs = list(specs)
        if retention not in (self.DELETE_ON_ACK, self.KEEP):
            raise ValueError(
                f"retention must be 'delete_on_ack' or 'keep', got {retention!r}"
            )
        self.retention = retention
        self._source_id = source_id or f"files:{self.root}"
        self._prune_empty_dirs = prune_empty_dirs

    # -- Source protocol --

    def source_id(self) -> str:
        return self._source_id

    def health(self) -> str:
        if not self.root.exists():
            return "unreachable"
        return "ok"

    def iter_batches(self, cursor: bytes, limit: int) -> Iterator[RecordBatch]:
        if not self.root.exists():
            return
        files = self._collect_files()
        if not files:
            return
        if self.retention == self.KEEP:
            # Skip files whose mtime is at or before the cursor's
            # checkpoint.  Cursor format: ISO timestamp bytes.
            after_mtime = _decode_keep_cursor(cursor)
            files = [f for f in files if f.stat().st_mtime > after_mtime]
            if not files:
                return
        chunk = files[:limit]

        records: list[Record] = []
        for path in chunk:
            spec = self._spec_for(path)
            if spec is None:
                continue
            mtime = datetime.fromtimestamp(
                path.stat().st_mtime, tz=timezone.utc
            )
            parsed: Iterable[Mapping]
            if spec.parser is None:
                parsed = ({"path": str(path)},)
            else:
                try:
                    payload = path.read_bytes()
                    raw = spec.parser(path, payload)
                except OSError as exc:
                    logger.warning(
                        "FileTreeSource: cannot read %s: %s", path, exc
                    )
                    continue
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "FileTreeSource: parser failed on %s: %s", path, exc
                    )
                    continue
                parsed = (raw,) if isinstance(raw, Mapping) else raw

            for cols_in in parsed:
                cols = dict(cols_in)
                row_time = cols.pop("time", None)
                if not isinstance(row_time, datetime):
                    row_time = mtime
                records.append(
                    Record(
                        table=spec.table,
                        time=row_time,
                        columns=cols,
                        payload_path=path,
                    )
                )

        if not records:
            return

        if self.retention == self.DELETE_ON_ACK:
            # Dedupe — multiple records may share one source file.
            seen_paths: list[str] = []
            seen_set: set[str] = set()
            for r in records:
                p = str(r.payload_path)
                if p not in seen_set:
                    seen_set.add(p)
                    seen_paths.append(p)
            commit_token = json.dumps(seen_paths).encode("utf-8")
            cursor_after = b"<delete-on-ack>"
        else:
            commit_token = b""
            # Cursor advances to the latest mtime in the batch.
            unique_paths = {r.payload_path for r in records if r.payload_path}
            latest_mtime = max(p.stat().st_mtime for p in unique_paths)
            cursor_after = _encode_keep_cursor(latest_mtime)

        yield RecordBatch(
            records=tuple(records),
            cursor_after=cursor_after,
            commit_token=commit_token,
        )

    def commit(self, commit_token: bytes) -> None:
        if self.retention != self.DELETE_ON_ACK:
            return
        if not commit_token:
            return
        try:
            paths = json.loads(commit_token.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            logger.warning(
                "FileTreeSource.commit: bad token (%s) — skipping",
                exc,
            )
            return
        deleted_dirs: set[Path] = set()
        for p_str in paths:
            p = Path(p_str)
            try:
                p.unlink(missing_ok=True)
                deleted_dirs.add(p.parent)
            except OSError as exc:
                logger.warning("FileTreeSource: cannot delete %s: %s", p, exc)
        if self._prune_empty_dirs:
            # Bottom-up so deeper dirs get pruned before their parents.
            # Keep walking up after each rmdir so newly-emptied
            # ancestors get pruned too — but never the source root.
            to_try: list[Path] = sorted(
                deleted_dirs, key=lambda x: len(x.parts), reverse=True,
            )
            while to_try:
                d = to_try.pop(0)
                if d == self.root or self.root not in d.parents:
                    continue
                try:
                    d.rmdir()
                except OSError:
                    continue
                # Successfully removed: check the parent next.
                if d.parent != self.root:
                    to_try.append(d.parent)

    # -- internals --

    def _collect_files(self) -> list[Path]:
        seen: set[Path] = set()
        for spec in self.specs:
            for path in self.root.rglob(spec.pattern):
                if path.is_file():
                    seen.add(path)
        return sorted(seen, key=lambda p: p.stat().st_mtime)

    def _spec_for(self, path: Path) -> Optional[FileSpec]:
        # First-match wins.  Patterns with overlap (e.g. *_spots.txt
        # matches both *_spots.txt and *_wd_spots.txt) need careful
        # ordering by the caller — most specific first.
        for spec in self.specs:
            if path.match(spec.pattern):
                return spec
        return None


# ---- cursor helpers (KEEP mode) ----


def _encode_keep_cursor(mtime: float) -> bytes:
    return f"{mtime:.6f}".encode("ascii")


def _decode_keep_cursor(cursor: bytes) -> float:
    if not cursor:
        return 0.0
    try:
        return float(cursor.decode("ascii"))
    except (ValueError, UnicodeDecodeError):
        return 0.0
