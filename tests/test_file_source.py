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


def test_multi_record_parser_fans_out_records(tmp_path):
    """A parser returning Iterable[Mapping] yields one Record per item.

    Per-slot files in psk-recorder bundle N decoded spots; each must
    arrive at PskReporterTcp as its own Record so it gets its own
    encoded spot.  One commit_token entry per file (deduped) keeps
    delete-on-ack atomic at file granularity.
    """
    from datetime import datetime, timezone

    f = tmp_path / "260510_171530.spots.txt"
    f.write_text("ignored")

    def multi_parser(path, raw):
        return [
            {
                "tx_call": "K1ABC",
                "frequency": 14074000,
                "time": datetime(2026, 5, 10, 17, 15, 30, tzinfo=timezone.utc),
            },
            {
                "tx_call": "W2DEF",
                "frequency": 14074500,
                "time": datetime(2026, 5, 10, 17, 15, 32, tzinfo=timezone.utc),
            },
            {
                "tx_call": "N3GHI",
                "frequency": 14075000,
                # No time -> falls back to file mtime.
            },
        ]

    src = FileTreeSource(
        root=tmp_path,
        specs=[FileSpec("*.spots.txt", parser=multi_parser, table="psk.spots")],
    )

    batches = list(src.iter_batches(b"", limit=100))
    assert len(batches) == 1
    records = batches[0].records
    assert len(records) == 3
    assert [r.columns["tx_call"] for r in records] == ["K1ABC", "W2DEF", "N3GHI"]
    # `time` is lifted out of columns into Record.time.
    assert "time" not in records[0].columns
    assert records[0].time == datetime(2026, 5, 10, 17, 15, 30, tzinfo=timezone.utc)
    # Third row has no parsed time -> file mtime.
    assert records[2].time != records[0].time
    # All records share the source path so delete-on-ack has one entry.
    paths = json.loads(batches[0].commit_token.decode())
    assert paths == [str(f)]

    src.commit(batches[0].commit_token)
    assert not f.exists()


def test_single_mapping_parser_back_compat(tmp_path):
    """A parser returning a single Mapping still produces one Record."""
    f = tmp_path / "wd_spot.txt"
    f.write_text("body")

    src = FileTreeSource(
        root=tmp_path,
        specs=[FileSpec("*.txt", parser=lambda p, b: {"col": "v"})],
    )
    batches = list(src.iter_batches(b"", limit=10))
    assert len(batches[0].records) == 1
    assert batches[0].records[0].columns == {"col": "v"}


def test_parser_exception_skips_file(tmp_path):
    """Parser exceptions don't kill the batch — the file is skipped."""
    bad = tmp_path / "bad.txt"
    bad.write_text("x")
    good = tmp_path / "good.txt"
    good.write_text("y")
    import os
    os.utime(bad, (1_700_000_000, 1_700_000_000))
    os.utime(good, (1_700_000_001, 1_700_000_001))

    def parser(path, raw):
        if path.name == "bad.txt":
            raise ValueError("kaboom")
        return {"ok": True}

    src = FileTreeSource(
        root=tmp_path,
        specs=[FileSpec("*.txt", parser=parser)],
    )
    batches = list(src.iter_batches(b"", limit=10))
    assert len(batches[0].records) == 1
    assert batches[0].records[0].payload_path == good
