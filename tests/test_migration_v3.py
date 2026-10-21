"""Migration 3: the send log names its pipeline and client, and
``last_send`` holds each pair's latest outcome (spec-sink-2b-v377 §4)."""

from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path

from hs_uploader.cli import main
from hs_uploader.watermark import SqliteWatermarkStore
from hs_uploader.watermark import schema
from tests.test_watermark_schema import (
    SRC, _store_v2_missing_meteor_key, _strip_to_v2_shape, _user_version,
    _v0_store, _set_version)

_UNTOUCHED = ("watermarks", "deliverables", "dead_letter",
              "superseded_keys", "pipeline_state")


def _cols(db: Path, table: str = "attempts") -> list[str]:
    conn = sqlite3.connect(db)
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    finally:
        conn.close()


def _digest(db: Path, table: str) -> str:
    conn = sqlite3.connect(db)
    try:
        h = hashlib.sha256()
        for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3"):
            h.update(repr(row).encode())
        return h.hexdigest()
    finally:
        conn.close()


def _v2_store_with_rows(directory: Path) -> Path:
    """Rows in every table, v3.76's table shapes, user_version 2, with both
    producer keys present so the fill has nothing to copy."""
    db = _v0_store(directory)
    schema.migrate(str(db))
    _strip_to_v2_shape(db)
    _set_version(db, 2)
    return db


def test_v2_store_reaches_3(tmp_path):
    db = _v2_store_with_rows(tmp_path)
    before = {t: _digest(db, t) for t in _UNTOUCHED}
    old_attempts = sqlite3.connect(db).execute(
        "SELECT rowid, ts, source_id, dest_id, table_name, outcome, records,"
        " bytes, error FROM attempts ORDER BY rowid").fetchall()
    assert "client" not in _cols(db)

    rep = schema.migrate(str(db))

    assert rep.applied == [schema.MIGRATIONS[-1].name]
    assert _user_version(db) == 3
    assert "pipeline" in _cols(db) and "client" in _cols(db)
    assert _cols(db, "last_send") == ["client", "pipeline", "outcome", "ts",
                                      "records"]
    assert {t: _digest(db, t) for t in _UNTOUCHED} == before
    conn = sqlite3.connect(db)
    new = conn.execute(
        "SELECT rowid, ts, source_id, dest_id, table_name, outcome, records,"
        " bytes, error, pipeline, client FROM attempts ORDER BY rowid"
    ).fetchall()
    assert [r[:9] for r in new] == old_attempts and old_attempts
    assert all(r[9] == "" and r[10] == "" for r in new)


def test_v0_store_runs_1_2_3_in_order(tmp_path, capsys):
    db = _v0_store(tmp_path)
    assert main(["migrate", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    pos = [out.index(f"applied {n}:") for n in (1, 2, 3)]
    assert pos == sorted(pos)
    assert _user_version(db) == 3


def test_migration_3_adds_only_missing_columns(tmp_path):
    db = _v2_store_with_rows(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("ALTER TABLE attempts ADD COLUMN pipeline TEXT "
                 "NOT NULL DEFAULT ''")
    conn.commit()
    conn.close()
    rep = schema.migrate(str(db))
    assert _cols(db).count("pipeline") == 1 and _cols(db).count("client") == 1
    text = "\n".join(rep.lines)
    assert "attempts.client" in text and "attempts.pipeline" not in text


def test_fill_runs_at_version_3(tmp_path):
    db = _store_v2_missing_meteor_key(tmp_path)
    _set_version(db, 3)
    SqliteWatermarkStore(db).close()      # opening adds nothing the fill needs
    rep = schema.migrate(str(db))
    assert any("copied" in ln and "meteor-scatter" in ln for ln in rep.lines)
    rep = schema.migrate(str(db), check=True)
    assert not rep.would_write


def test_check_previews_migration_3_lines(tmp_path):
    db = _v2_store_with_rows(tmp_path)
    before = db.read_bytes()
    rep = schema.migrate(str(db), check=True)
    text = "\n".join(rep.lines)
    assert "attempts.pipeline" in text and "attempts.client" in text
    assert "last_send" in text
    assert rep.would_write
    assert db.read_bytes() == before
    assert _user_version(db) == 2


def test_old_writer_inserts_on_v3_store(tmp_path):
    db = _v2_store_with_rows(tmp_path)
    schema.migrate(str(db))
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO attempts(ts, source_id, dest_id, table_name, outcome, "
        "records, bytes, error) VALUES ('t','s','d','tb','acked',1,2,NULL)")
    conn.commit()
    row = conn.execute("SELECT pipeline, client FROM attempts "
                       "ORDER BY rowid DESC LIMIT 1").fetchone()
    assert row == ("", "")


def test_new_store_stamped_3_with_new_schema(tmp_path):
    db = tmp_path / "w.db"
    SqliteWatermarkStore(db).close()
    assert _user_version(db) == 3
    assert "pipeline" in _cols(db) and "client" in _cols(db)
    assert _cols(db, "last_send") == ["client", "pipeline", "outcome", "ts",
                                      "records"]


def test_capabilities_bare_prints_3():
    out = subprocess.run(
        [sys.executable, "-m", "hs_uploader.cli", "capabilities"],
        capture_output=True, text=True, env={"PYTHONPATH": str(SRC)})
    assert out.stdout == "3\n"
    assert int(out.stdout.strip()) == 3
