"""The key-split proof script passes on a consistent sink, and fails when
the producer scoping is switched off (spec-key-split-v373 §8 test 10)."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from hs_uploader.sink.writer import PENDING_UPLOADS_DDL
from hs_uploader.sources.sqlite import SqliteSource

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "prove_key_split.py"


def _load():
    spec = importlib.util.spec_from_file_location("prove_key_split", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# (producer, mode, tx_call, forward)
_ROWS = [
    ("psk-recorder", "ft8", "A1AA", 0),
    ("psk-recorder", "ft4", "A2AA", 0),
    ("", "ft8", "A3AA", 0),
    ("", "FT8", "A4AA", 0),        # not in the old mode list: nobody ships it
    ("", None, "A5AA", 0),         # no mode: nobody ships it
    ("meteor-scatter", "msk144", "B1BB", 0),
    ("", "msk144", "B2BB", 0),
    ("", "msk144", "", 0),         # no tx_call: nobody ships it
    ("", "ft8", "A6AA", 1),        # forwarded by wd30: nobody ships it
]


def _build(path: Path, with_column: bool = True) -> None:
    conn = sqlite3.connect(path)
    if with_column:
        conn.executescript(PENDING_UPLOADS_DDL)
    else:
        conn.execute(
            "CREATE TABLE pending_uploads (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "target_db TEXT NOT NULL, target_table TEXT NOT NULL, "
            "schema_version INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL, "
            "queued_at TEXT NOT NULL)"
        )
    for producer, mode, tx, fwd in _ROWS:
        payload = {"time": "2026-10-08T06:55:00+00:00", "mode": mode, "tx_call": tx,
                   "forward_to_pskreporter": bool(fwd)}
        if with_column:
            conn.execute(
                "INSERT INTO pending_uploads (target_db,target_table,schema_version,"
                "payload_json,queued_at,producer) VALUES ('psk','spots',2,?,?,?)",
                (json.dumps(payload), "2026-10-08T06:55:00+00:00", producer))
        else:
            conn.execute(
                "INSERT INTO pending_uploads (target_db,target_table,schema_version,"
                "payload_json,queued_at) VALUES ('psk','spots',2,?,?)",
                (json.dumps(payload), "2026-10-08T06:55:00+00:00"))
    conn.execute(
        "INSERT INTO pending_uploads (target_db,target_table,schema_version,"
        "payload_json,queued_at) VALUES ('wspr','spots',2,'{\"mode\":\"ft8\"}','x')")
    conn.commit()
    conn.close()


@pytest.fixture
def files(tmp_path):
    sink = tmp_path / "sink.db"
    store = tmp_path / "wm.db"
    _build(sink)
    sqlite3.connect(store).close()
    return sink, store


def test_proof_passes_and_leaves_inputs_alone(files, capsys):
    sink, store = files
    before = sink.read_bytes()
    assert _load().main(["--sink", str(sink), "--store", str(store), "--add-msk144", "6"]) == 0
    assert "PROVED" in capsys.readouterr().out
    assert sink.read_bytes() == before


def test_proof_passes_without_producer_column(tmp_path, capsys):
    sink, store = tmp_path / "old.db", tmp_path / "wm.db"
    _build(sink, with_column=False)
    sqlite3.connect(store).close()
    assert _load().main(["--sink", str(sink), "--store", str(store), "--add-msk144", "4"]) == 0


def test_proof_fails_with_producer_scoping_off(files, monkeypatch, capsys):
    sink, store = files
    monkeypatch.setattr(SqliteSource, "_scope_clauses", lambda self, conn: ([], []))
    assert _load().main(["--sink", str(sink), "--store", str(store), "--add-msk144", "6"]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out and "NOT PROVED" in out
