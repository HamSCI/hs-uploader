"""FileTreeSource — delete-on-ack and keep retention modes.

Builds realistic wsprdaemon-style spool layouts in tmp dirs and
verifies the source emits the expected Records and cleans up correctly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hs_uploader.sources import FileSpec, FileTreeSource


def _make_wsprdaemon_spool(root: Path) -> list[Path]:
    """Build a spool tree mirroring wsprdaemon-client's queue.

    Layout: ``<root>/<RECEIVER>/<BAND>/[noise/]<filename>``.
    """
    files: list[Path] = []
    layout = [
        ("HF1/14M/210508_1200_wd_spots.txt", "spot data v3"),
        ("HF1/14M/210508_1202_wd_spots.txt", "spot data v3"),
        ("HF1/14M/noise/20210508_120000_noise.txt", "noise"),
        ("HF1/7M/210508_1200_wd_spots.txt", "7m spots"),
    ]
    for rel, body in layout:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
        files.append(p)
    return files


def test_yields_all_matching_files_oldest_first(tmp_path):
    files = _make_wsprdaemon_spool(tmp_path)
    # Touch them with increasing mtime so order is deterministic.
    import os, time
    for i, f in enumerate(files):
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))

    src = FileTreeSource(
        root=tmp_path,
        specs=[
            FileSpec("*_wd_spots.txt", table="wspr.spots"),
            FileSpec("*_noise.txt", table="wspr.noise"),
        ],
    )

    batches = list(src.iter_batches(b"", limit=100))
    assert len(batches) == 1
    records = batches[0].records
    assert len(records) == 4
    # Oldest first.
    paths = [str(r.payload_path) for r in records]
    assert paths == [str(f) for f in files]


def test_commit_deletes_acked_files(tmp_path):
    _make_wsprdaemon_spool(tmp_path)
    src = FileTreeSource(
        root=tmp_path,
        specs=[
            FileSpec("*_wd_spots.txt", table="wspr.spots"),
            FileSpec("*_noise.txt", table="wspr.noise"),
        ],
    )
    batch = next(iter(src.iter_batches(b"", limit=100)))
    paths_before = [Path(p) for p in json.loads(batch.commit_token.decode())]
    for p in paths_before:
        assert p.exists()
    src.commit(batch.commit_token)
    for p in paths_before:
        assert not p.exists()


def test_commit_prunes_empty_dirs(tmp_path):
    _make_wsprdaemon_spool(tmp_path)
    src = FileTreeSource(
        root=tmp_path,
        specs=[
            FileSpec("*_wd_spots.txt", table="wspr.spots"),
            FileSpec("*_noise.txt", table="wspr.noise"),
        ],
    )
    batch = next(iter(src.iter_batches(b"", limit=100)))
    src.commit(batch.commit_token)
    # All band/noise dirs should be gone; root remains.
    assert not (tmp_path / "HF1" / "14M" / "noise").exists()
    assert not (tmp_path / "HF1" / "14M").exists()
    assert not (tmp_path / "HF1" / "7M").exists()
    assert not (tmp_path / "HF1").exists()
    assert tmp_path.exists()


def test_keep_mode_uses_mtime_cursor(tmp_path):
    files = _make_wsprdaemon_spool(tmp_path)
    import os
    for i, f in enumerate(files):
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))

    src = FileTreeSource(
        root=tmp_path,
        specs=[FileSpec("*_wd_spots.txt", table="wspr.spots")],
        retention=FileTreeSource.KEEP,
    )
    # First poll yields the three spot files.
    batches = list(src.iter_batches(b"", limit=100))
    assert len(batches[0].records) == 3
    cursor = batches[0].cursor_after
    # Subsequent poll with that cursor: nothing new.
    assert list(src.iter_batches(cursor, limit=100)) == []

    # Add a newer file.
    new = tmp_path / "HF1" / "14M" / "210508_1300_wd_spots.txt"
    new.write_text("new spot")
    os.utime(new, (1_700_000_999, 1_700_000_999))
    batches = list(src.iter_batches(cursor, limit=100))
    assert len(batches[0].records) == 1
    assert str(batches[0].records[0].payload_path) == str(new)
    # In KEEP mode, files persist and commit is a no-op.
    src.commit(batches[0].commit_token)
    assert new.exists()


def test_health_unreachable_when_root_missing(tmp_path):
    src = FileTreeSource(
        root=tmp_path / "ghost",
        specs=[FileSpec("*.txt")],
    )
    assert src.health() == "unreachable"
    assert list(src.iter_batches(b"", limit=10)) == []


def test_invalid_retention_raises():
    with pytest.raises(ValueError):
        FileTreeSource(root="/", specs=[], retention="bogus")
