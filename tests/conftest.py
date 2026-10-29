"""Shared test fixtures for hs-uploader.

The tests deliberately avoid network and external services: sources are
exercised against temp files / in-memory SQLite, and transports are
mocked via in-memory implementations.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# pytest puts tests/ on sys.path but not the repo root, so a test using
# `from tests.<sibling> import ...` fails collection outright -- and a
# module that never collects is worse than one that fails, because a
# green run hides it.  Put the root on the path once, here.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pytest

# Make the in-tree package importable without an editable install.
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = str(REPO_ROOT / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)


from hs_uploader.core import (  # noqa: E402  — after sys.path mutation
    BatchPolicy,
    Outcome,
    Record,
    RecordBatch,
)


# ---- in-memory transport for orchestration tests ----


class MemoryTransport:
    """Records every ship/replay call.  Default policy = ACKED.

    Configure ``self.next_outcomes`` (a list) before pumping to drive
    specific outcome sequences.  Each ``ship`` pops from the front.
    """

    name = "memory"
    ACCEPTS = {"test.spots": [1]}

    def __init__(self, accepts_table: str = "test.spots", *, accepts_versions=(1,)):
        self.ACCEPTS = {accepts_table: list(accepts_versions)}
        self._table = accepts_table
        self.shipped: list[RecordBatch] = []
        self.replayed: list[bytes] = []
        self.next_outcomes: list[Outcome] = []

    def primary_table(self) -> str:
        return self._table

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=100)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        self.shipped.append(batch)
        if self.next_outcomes:
            return self.next_outcomes.pop(0)
        return Outcome.acked()

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        # Use the cursor as a stand-in payload — easy to assert on.
        return batch.cursor_after

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        self.replayed.append(payload_blob)
        if self.next_outcomes:
            return self.next_outcomes.pop(0)
        return Outcome.acked()


# ---- tiny in-memory source ----


class MemorySource:
    """Yields a fixed list of (record, cursor) tuples once.

    Useful for orchestration tests where we just want to assert the
    Pipeline drains a known stream.
    """

    def __init__(
        self,
        table: str = "test.spots",
        records: Sequence[tuple[Record, bytes]] = (),
        source_id: str = "memory:test",
    ):
        self._records = list(records)
        self._source_id = source_id
        self._table = table

    def source_id(self) -> str:
        return self._source_id

    def health(self) -> str:
        return "ok"

    def iter_batches(self, cursor: bytes, limit: int):
        # Yield everything *after* the cursor as one batch.  Cursors
        # are simple bytes here (e.g., b"0", b"1", ...); we yield only
        # those with cursor strictly greater than the given one.
        eligible = [(r, c) for (r, c) in self._records if c > cursor]
        if not eligible:
            return
        chunk = eligible[:limit]
        yield RecordBatch(
            records=tuple(r for (r, _) in chunk),
            cursor_after=chunk[-1][1],
        )

    def commit(self, commit_token: bytes) -> None:
        # No-op for the in-memory source — Pipeline's commit hook is
        # only meaningful for sources that need external cleanup.
        return None


# ---- fixtures ----


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_defaults: run without the hermetic state-dir and sinks-dir "
        "fixture (a test that checks the library's own default paths)")


@pytest.fixture(autouse=True)
def _hermetic_switch_paths(request, tmp_path, monkeypatch):
    """Point the library's default state dir and sinks dir at scratch.

    Every default-path reader (the carry-over marker, the watermark store,
    the switch files) would otherwise read this host's real
    /var/lib/hs-uploader and /etc/hs-uploader/sinks, and a host where the
    marker stands would change what tests see (Task 8 review M1).  A test
    that patches either path itself wins, since its monkeypatch runs
    after this one.  `@pytest.mark.real_defaults` opts out.
    """
    if request.node.get_closest_marker("real_defaults"):
        return
    import hs_uploader.sinkswitch as sinkswitch
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(sinkswitch, "DEFAULT_SINKS_DIR", tmp_path / "sinks")


@pytest.fixture
def memory_transport():
    return MemoryTransport()


@pytest.fixture
def make_record():
    def _make(time_iso: str = "2026-05-08T12:00:00", **cols) -> Record:
        return Record(
            table="test.spots",
            time=datetime.fromisoformat(time_iso).replace(tzinfo=timezone.utc),
            columns=cols,
        )
    return _make


@pytest.fixture(autouse=True)
def _isolate_from_the_hosts_sink(tmp_path, monkeypatch):
    """Keep every test off the host's real sink locations.

    `sink_path` binds /etc/hs-uploader/sink.toml and /var/lib/sigmond/
    sink.db as defaults, so on a station `Writer.from_env(env={})` would
    resolve to the live sink.  Point the defaults at paths that do not
    exist under tmp_path.  Explicit arguments and the env still win, so
    tests/test_sink_path.py, which passes its own paths, is unaffected.
    """
    from hs_uploader.sink import writer as writer_mod

    real = writer_mod.sink_path
    nowhere = tmp_path / "no-such-host-sink"

    def isolated(env=None, **kw):
        e = env if env is not None else os.environ
        kw.setdefault("config_file", str(nowhere / "sink.toml"))
        kw.setdefault("legacy_path", str(nowhere / "legacy" / "sink.db"))
        if not (e.get("HS_UPLOADER_STATE_DIR") or "").strip():
            kw.setdefault("state_dir", str(nowhere / "state"))
        return real(env, **kw)

    monkeypatch.setattr(writer_mod, "sink_path", isolated)
