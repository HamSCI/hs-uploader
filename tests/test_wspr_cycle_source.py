"""WsprCycleSource: behaviour before any producer has created the sink schema.

A station that has never recorded a spot has no ``pending_uploads`` table --
sqlite3.connect() happily creates an empty database file, so the table is
absent rather than the file being missing.  The uploader is started by
bring-up before the recorders exist, so it polls that empty database for
minutes.  It must treat "producer hasn't flushed yet" as "no work", not as
an error.

Regression test for the greenfield traceback seen on AC0G-B4's first boot
(appliance v3.21, 2026-08-08): 17 x
``sqlite3.OperationalError: no such table: pending_uploads``.
"""
import sqlite3

import pytest

from hs_uploader.sources.wspr_cycle import WsprCycleSource


def test_empty_database_yields_nothing(tmp_path):
    """A database file with no tables at all must not raise."""
    db = tmp_path / "sink.db"
    sqlite3.connect(str(db)).close()          # exists, but schema-less
    assert db.exists()

    src = WsprCycleSource(db_path=db)
    assert list(src.iter_batches(cursor=b"", limit=100)) == []


def test_missing_database_file_yields_nothing(tmp_path):
    """Not even the file exists yet -- connect() will create it empty."""
    src = WsprCycleSource(db_path=tmp_path / "not-created-yet.db")
    assert list(src.iter_batches(cursor=b"", limit=100)) == []


def test_other_tables_but_no_queue_table(tmp_path):
    """Some other producer made the db, but pending_uploads isn't there."""
    db = tmp_path / "sink.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    src = WsprCycleSource(db_path=db)
    assert list(src.iter_batches(cursor=b"", limit=100)) == []


def test_real_sqlite_errors_still_raise(tmp_path):
    """The guard must not swallow genuine database failures.

    A directory where the database should be is not a "producer hasn't
    started" condition -- it is broken configuration and must surface.
    """
    db = tmp_path / "sink.db"
    db.mkdir()

    src = WsprCycleSource(db_path=db)
    with pytest.raises(sqlite3.Error):
        list(src.iter_batches(cursor=b"", limit=100))
