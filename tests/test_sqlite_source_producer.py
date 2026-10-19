"""SqliteSource scoped by `producer` and `radiod_id` (spec-key-split-v373
§4.1, §3.3).

A producer narrows every statement that touches rows: the batch
selection, the de-duplication query, the `start_at` anchor, the
stale-schema probe and `commit()`'s DELETE.  Rows stored with
`producer = ''` belong where the writer's `infer_producer` (D12) would
put them: MSK144 (trimmed, case-folded) to meteor-scatter, everything
else in `psk`, a missing mode included, to psk-recorder.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pytest

from hs_uploader.core import Pipeline, Uploader
from hs_uploader.pipeline_factory import build_pipelines
from hs_uploader.sink import PENDING_UPLOADS_DDL, infer_producer
from hs_uploader.sources.sqlite import (
    HEALTH_OK,
    HEALTH_STALE_SCHEMA,
    SqliteSource,
    _ConnectionConfig,
)
from hs_uploader.watermark.sqlite import SqliteWatermarkStore
from tests.conftest import MemoryTransport
from tests.test_core_orchestration import _ident

PSK = "psk-recorder"
MS = "meteor-scatter"

# The queue as it stood before v3.70: no `producer`, no `local`.
_OLD_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
)
"""

# (producer, payload).  producer None = a sink.db without the column.
# Every D12 case the writer's `infer_producer` handles appears with
# producer '' (and again in the no-column fixture): exact, upper-case,
# padded MSK144; FT8 upper-case; padded ft4; no mode; JSON-null mode;
# a non-string mode.
_ROWS = [
    (PSK, {"mode": "ft8", "radiod_id": "rx-a"}),
    (PSK, {"mode": "ft4", "radiod_id": "rx-b"}),
    (MS, {"mode": "msk144", "radiod_id": "rx-a"}),
    (MS, {"mode": "msk144", "radiod_id": "rx-b"}),
    ("", {"mode": "MSK144", "radiod_id": "rx-a"}),
    ("", {"mode": " msk144 ", "radiod_id": "rx-b"}),
    ("", {"mode": "msk144", "radiod_id": "rx-b"}),
    ("", {"mode": "FT8", "radiod_id": "rx-a"}),
    ("", {"mode": " ft4 ", "radiod_id": "rx-b"}),
    ("", {"radiod_id": "rx-a"}),
    ("", {"mode": None, "radiod_id": "rx-b"}),
    ("", {"mode": 144, "radiod_id": "rx-a"}),
    (PSK, {"mode": "ft8", "radiod_id": "rx-a"}),
    (MS, {"mode": "msk144", "radiod_id": "rx-b"}),
]


def _owner(producer: str, payload: dict) -> str:
    """Who owns a row, by the writer's own rule."""
    return producer or infer_producer("psk", "spots", payload)


def _build(path: Path, *, with_column: bool) -> dict[int, dict]:
    """Seed `path`; return {id: {"owner", "radiod_id"}} for every psk row."""
    conn = sqlite3.connect(path)
    conn.executescript(PENDING_UPLOADS_DDL if with_column else _OLD_DDL)
    truth: dict[int, dict] = {}
    now = datetime.now(timezone.utc).isoformat()
    for producer, payload in _ROWS:
        if with_column:
            cur = conn.execute(
                "INSERT INTO pending_uploads (target_db, target_table, "
                "schema_version, payload_json, queued_at, producer) "
                "VALUES ('psk','spots',2,?,?,?)",
                (json.dumps(payload), now, producer),
            )
            owner = _owner(producer, payload)
        else:
            cur = conn.execute(
                "INSERT INTO pending_uploads (target_db, target_table, "
                "schema_version, payload_json, queued_at) "
                "VALUES ('psk','spots',2,?,?)",
                (json.dumps(payload), now),
            )
            owner = _owner("", payload)
        truth[cur.lastrowid] = {"owner": owner, "radiod_id": payload["radiod_id"]}
    # A row of another table, which no psk source may see or delete.
    conn.execute(
        "INSERT INTO pending_uploads (target_db, target_table, schema_version, "
        "payload_json, queued_at) VALUES ('wspr','spots',2,'{\"mode\":\"ft8\"}',?)",
        (now,),
    )
    conn.commit()
    conn.close()
    return truth


def _src(path: Path, **kw) -> SqliteSource:
    kw.setdefault("accepted_schema_versions", [2])
    return SqliteSource(
        database="psk", table="spots",
        config=_ConnectionConfig(path=str(path)), **kw,
    )


def _ids(src: SqliteSource) -> set[int]:
    """Every id the source yields, following its cursor to the end."""
    out: set[int] = set()
    cursor = b""
    for _ in range(100):
        batches = list(src.iter_batches(cursor, 3))
        if not batches:
            return out
        for b in batches:
            for r in b.records:
                out.add(r.columns["_id"])
            cursor = b.cursor_after
    raise AssertionError("source never ran dry")


def _tag_ids(path: Path) -> None:
    """Copy each row's id into its payload, so a Record names its row."""
    conn = sqlite3.connect(path)
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id)")
    conn.commit()
    conn.close()


@pytest.fixture
def sink(tmp_path: Path):
    path = tmp_path / "sink.db"
    truth = _build(path, with_column=True)
    _tag_ids(path)
    return path, truth


@pytest.fixture
def old_sink(tmp_path: Path):
    path = tmp_path / "old-sink.db"
    truth = _build(path, with_column=False)
    _tag_ids(path)
    return path, truth


def _remaining(path: Path) -> set[int]:
    conn = sqlite3.connect(path)
    ids = {r[0] for r in conn.execute("SELECT id FROM pending_uploads")}
    conn.close()
    return ids


# ---- key ------------------------------------------------------------------


def test_source_id_forms(tmp_path: Path) -> None:
    assert _src(tmp_path / "x.db").source_id() == "sqlite:psk.spots"
    assert _src(tmp_path / "x.db", producer=PSK).source_id() == "sqlite:psk.spots@psk-recorder"
    assert _src(tmp_path / "x.db", producer=MS).source_id() == "sqlite:psk.spots@meteor-scatter"
    assert (
        _src(tmp_path / "x.db", producer=PSK, radiod_id="rx-a").source_id()
        == "sqlite:psk.spots@psk-recorder/rx-a"
    )
    # from_env carries both through.
    s = SqliteSource.from_env(
        database="psk", table="spots", accepted_schema_versions=[2],
        producer=MS, radiod_id="rx-b", env={"SIGMOND_SQLITE_PATH": str(tmp_path / "x.db")},
    )
    assert s.source_id() == "sqlite:psk.spots@meteor-scatter/rx-b"


def test_radiod_id_without_producer_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _src(tmp_path / "x.db", radiod_id="rx-a")


# ---- selection --------------------------------------------------------------


def _assert_partition(path: Path, truth: dict[int, dict]) -> None:
    every = _ids(_src(path))
    psk = _ids(_src(path, producer=PSK))
    ms = _ids(_src(path, producer=MS))
    assert every == set(truth)
    assert psk.isdisjoint(ms)
    assert psk | ms == every
    assert psk == {i for i, t in truth.items() if t["owner"] == PSK}
    assert ms == {i for i, t in truth.items() if t["owner"] == MS}
    # Both sides own some '' rows, so the D12 branch is exercised.
    assert len(ms) >= 3 and len(psk) >= 3


def test_producer_rows_partition(sink) -> None:
    path, truth = sink
    _assert_partition(path, truth)


def test_producer_rows_partition_with_extra_where(sink) -> None:
    path, truth = sink
    got = _ids(_src(path, producer=MS, extra_where=[("radiod_id", "=", "rx-a")]))
    assert got == {i for i, t in truth.items()
                   if t["owner"] == MS and t["radiod_id"] == "rx-a"}


def test_no_producer_column(old_sink) -> None:
    path, truth = old_sink
    _assert_partition(path, truth)
    # pump() through a real Pipeline never raises on the old file.
    for producer in (PSK, MS):
        transport = MemoryTransport()
        pipe = Pipeline(
            name=f"t-{producer}", source=_src(path, producer=producer),
            transport=transport, watermark=SqliteWatermarkStore(":memory:"),
            identity=_ident(),
        )
        uploader = Uploader([pipe])
        try:
            uploader.pump()
        finally:
            uploader.close()
        assert pipe.source.health() == HEALTH_OK
        shipped = {r.columns["_id"] for b in transport.shipped for r in b.records}
        assert shipped == {i for i, t in truth.items() if t["owner"] == producer}


def test_unknown_producer_reads_column_only(sink) -> None:
    path, _ = sink
    assert _ids(_src(path, producer="wspr-recorder")) == set()


def test_instance_rows_partition(sink) -> None:
    path, truth = sink
    for producer in (PSK, MS):
        whole = _ids(_src(path, producer=producer))
        a = _ids(_src(path, producer=producer, radiod_id="rx-a"))
        b = _ids(_src(path, producer=producer, radiod_id="rx-b"))
        assert a and b
        assert a.isdisjoint(b)
        assert a | b == whole
        assert a == {i for i, t in truth.items()
                     if t["owner"] == producer and t["radiod_id"] == "rx-a"}


def test_dedup_query_scoped_by_producer(sink) -> None:
    path, truth = sink
    # Partition on radiod_id: one winner per radiod per producer.  Without
    # the producer clause in the CTE, the other producer's rows would
    # compete for (and win) the partitions.
    for producer in (PSK, MS):
        got = _ids(_src(path, producer=producer,
                        dedup_partition_by=("radiod_id",),
                        dedup_order_by_desc="_id"))
        mine = {i for i, t in truth.items() if t["owner"] == producer}
        assert got <= mine
        assert len(got) == 2  # rx-a and rx-b


def test_dedup_query_scoped_by_radiod_id(sink) -> None:
    path, truth = sink
    got = _ids(_src(path, producer=PSK, radiod_id="rx-a",
                    dedup_partition_by=("mode",), dedup_order_by_desc="_id"))
    assert got and all(truth[i]["radiod_id"] == "rx-a"
                       and truth[i]["owner"] == PSK for i in got)


# ---- commit -------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ["sink", "old_sink"])
def test_commit_deletes_only_own_producer(fixture, request) -> None:
    path, truth = request.getfixturevalue(fixture)
    before = _remaining(path)
    src = _src(path, producer=PSK, delete_on_commit=True)
    src.commit(str(max(before)).encode())
    gone = before - _remaining(path)
    assert gone == {i for i, t in truth.items() if t["owner"] == PSK}
    # The wspr row and every meteor-scatter row stay.
    assert len(_remaining(path)) == len(before) - len(gone)


def test_commit_deletes_only_own_instance(sink) -> None:
    path, truth = sink
    before = _remaining(path)
    _src(path, producer=MS, radiod_id="rx-b").commit(str(max(before)).encode())
    gone = before - _remaining(path)
    assert gone == {i for i, t in truth.items()
                    if t["owner"] == MS and t["radiod_id"] == "rx-b"}


# ---- start_at -----------------------------------------------------------------


def _append(path: Path, producer: str, payload: dict) -> int:
    conn = sqlite3.connect(path)
    cur = conn.execute(
        "INSERT INTO pending_uploads (target_db, target_table, schema_version, "
        "payload_json, queued_at, producer) VALUES ('psk','spots',2,?,?,?)",
        (json.dumps(payload), datetime.now(timezone.utc).isoformat(), producer),
    )
    conn.execute("UPDATE pending_uploads SET payload_json = "
                 "json_set(payload_json, '$._id', id) WHERE id = ?", (cur.lastrowid,))
    conn.commit()
    conn.close()
    return cur.lastrowid


def test_start_at_now_anchors_within_producer(sink) -> None:
    path, truth = sink
    # The table's newest psk row now belongs to psk-recorder, so the
    # table's max id and meteor-scatter's max id differ.
    last_ms = max(i for i, t in truth.items() if t["owner"] == MS)
    tail = _append(path, PSK, {"mode": "ft8", "radiod_id": "rx-a"})
    src = _src(path, producer=MS, start_at="now")
    assert src._cursor_from_start_at().last_id == last_ms  # noqa: SLF001
    assert tail > last_ms
    # And it ships a meteor-scatter row stored after the anchor.
    new_ms = _append(path, MS, {"mode": "msk144", "radiod_id": "rx-a"})
    assert _ids(src) == {new_ms}


def test_start_at_now_anchors_within_instance(sink) -> None:
    path, truth = sink
    last_a = max(i for i, t in truth.items()
                 if t["owner"] == PSK and t["radiod_id"] == "rx-a")
    _append(path, PSK, {"mode": "ft8", "radiod_id": "rx-b"})
    src = _src(path, producer=PSK, radiod_id="rx-a", start_at="now")
    assert src._cursor_from_start_at().last_id == last_a  # noqa: SLF001


def test_start_at_now_old_sink(old_sink) -> None:
    path, truth = old_sink
    last_psk = max(i for i, t in truth.items() if t["owner"] == PSK)
    src = _src(path, producer=PSK, start_at="now")
    assert src._cursor_from_start_at().last_id == last_psk  # noqa: SLF001


# ---- stale-schema probe ---------------------------------------------------------


def test_stale_row_of_another_producer_does_not_halt(sink) -> None:
    path, _ = sink
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO pending_uploads (target_db, target_table, schema_version, "
        "payload_json, queued_at, producer) VALUES ('psk','spots',9,"
        "'{\"mode\":\"msk144\",\"radiod_id\":\"rx-a\"}',?,?)",
        (datetime.now(timezone.utc).isoformat(), MS),
    )
    conn.commit()
    conn.close()
    psk = _src(path, producer=PSK)
    _ids(psk)
    assert psk.health() == HEALTH_OK
    other_instance = _src(path, producer=MS, radiod_id="rx-b")
    _ids(other_instance)
    assert other_instance.health() == HEALTH_OK
    ms = _src(path, producer=MS)
    _ids(ms)
    assert ms.health() == HEALTH_STALE_SCHEMA


# ---- factory --------------------------------------------------------------------


def test_factory_passes_producer_and_radiod_id(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))

    def pipe(name, **extra):
        return {
            "name": name,
            "source": {"type": "sqlite", "database": "psk", "table": "spots",
                       "accepted_schema_versions": [2], **extra},
            "transport": {"type": "pskreporter", "decoding_software": "x/1"},
        }

    manifest = {
        "identity": {"call": "AC0G/S", "grid": "EM38ww"},
        "pipeline": [
            pipe("plain"),
            pipe("by-producer", producer=MS),
            pipe("by-instance", producer=PSK, radiod_id="rx-a"),
        ],
    }
    pipes = build_pipelines(manifest, watermark=SqliteWatermarkStore(":memory:"))
    got = {p.name: p.source for p in pipes}
    assert got["plain"].source_id() == "sqlite:psk.spots"
    assert got["plain"].producer is None and got["plain"].radiod_id is None
    assert got["by-producer"].source_id() == "sqlite:psk.spots@meteor-scatter"
    assert got["by-instance"].source_id() == "sqlite:psk.spots@psk-recorder/rx-a"
