"""tools/neutrality_check.py: the v3.70 proof that two hs-uploader trees
send the same thing (sigmond tasks/plan-sink-control.md §10.4).

Every test runs the tool for real: two worker processes, each importing
the tree it names.  A synthetic station stands in for ND or B4.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
from pathlib import Path

import pytest

from hs_uploader.watermark.sqlite import SqliteWatermarkStore

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
_spec = importlib.util.spec_from_file_location(
    "neutrality_check", ROOT / "tools" / "neutrality_check.py")
nc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nc)

NOW = "2026-10-07T12:07:30Z"
PSK_KEY = ("sqlite:psk.spots", "pskreporter-tcp:report.pskreporter.info:4739",
           "psk.spots")
WSPRNET_KEY = ("sqlite:wspr.spots",
               "wsprnet-async:https://wsprnet.org/api/upload/v1", "wspr.spots")
CYCLE_KEY = ("sqlite:wspr.cycle",
             "wsprdaemon-tar-sftp:gw1.wsprdaemon.org,gw2.wsprdaemon.org",
             "wspr.cycle")

# pending_uploads as a v3.69 station holds it: sigmond's writer at 64cf83f,
# _QUEUE_DDL and both indexes, without Task 1's producer and local columns.
SINK_DDL = """
CREATE TABLE pending_uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_db       TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    queued_at       TEXT NOT NULL
);
CREATE INDEX idx_pending_uploads_target
    ON pending_uploads (target_db, target_table, id);
CREATE INDEX idx_pending_uploads_cycle_time
    ON pending_uploads (target_db, target_table,
                        json_extract(payload_json, '$.time'));
"""

# id: (target_db, target_table, payload)
ROWS = [
    ("psk", "spots", {"time": "2026-10-07T12:00:15Z", "mode": "ft8", "tx_call": "K1ABC", "forward_to_pskreporter": 0}),
    ("psk", "spots", {"time": "2026-10-07T12:00:30Z", "mode": "ft8", "tx_call": "K2BCD", "forward_to_pskreporter": 0}),
    ("psk", "spots", {"time": "2026-10-07T12:01:00Z", "mode": "msk144", "tx_call": "K3CDE", "forward_to_pskreporter": 0}),
    ("psk", "spots", {"time": "2026-10-07T12:02:15Z", "mode": "ft4", "tx_call": "K4DEF", "forward_to_pskreporter": 0}),
    ("psk", "spots", {"time": "2026-10-07T12:02:30Z", "mode": "ft8", "tx_call": "K5EFG", "forward_to_pskreporter": 1}),
    ("wspr", "spots", {"time": "2026-10-07T12:00:00Z", "callsign": "W1AW", "band": 20, "snr_db": -10}),
    ("wspr", "noise", {"time": "2026-10-07T12:00:00Z", "rms_level": -120.0}),
    ("wspr", "spots", {"time": "2026-10-07T12:02:00Z", "callsign": "W1AW", "band": 20, "snr_db": -12}),
    ("wspr", "spots", {"time": "2026-10-07T12:02:00Z", "callsign": "W1AW", "band": 20, "snr_db": -8}),
    ("wspr", "noise", {"time": "2026-10-07T12:02:00Z", "rms_level": -119.0}),
    ("wspr", "spots", {"time": "2026-10-07T12:06:00Z", "callsign": "W1AW", "band": 20, "snr_db": -9}),
]

MANIFEST = """\
[identity]
call = "AC0G/T"
grid = "EM38ww"

[[pipeline]]
name = "psk-pskreporter"
batch_limit = 500
[pipeline.source]
type = "sqlite"
database = "psk"
table = "spots"
accepted_schema_versions = [2]
start_at = "now"
delete_on_commit = false
extra_where = [["tx_call", "!=", ""], ["mode", "IN", ["ft8", "ft4", "msk144"]], ["forward_to_pskreporter", "=", 0]]
[pipeline.transport]
type = "pskreporter"
decoding_software = "psk-recorder/0.1"

[[pipeline]]
name = "wspr-wsprdaemon"
batch_limit = 10000
max_records_per_pump = 20000
[pipeline.source]
type = "wspr_cycle"
db_path = "/var/lib/sigmond/sink.db"
start_at = "now"
include_psk = true
[pipeline.transport]
type = "wsprdaemon_tar"
servers = ["gw1.wsprdaemon.org", "gw2.wsprdaemon.org"]
receiver = "AC0G_T"
primary_table_name = "wspr.cycle"

[[pipeline]]
name = "wspr-wsprnet"
batch_limit = 900
[pipeline.source]
type = "sqlite"
database = "wspr"
table = "spots"
accepted_schema_versions = [1, 2]
start_at = "now"
delete_on_commit = false
dedup_partition_by = ["time", "callsign", "band"]
dedup_order_by_desc = "snr_db"
[pipeline.transport]
type = "wsprnet"
api_base_url = "https://wsprnet.org/api/upload/v1"

[[pipeline]]
name = "heartbeat"
[pipeline.source]
type = "filetree"
root = "{heartbeat}"
patterns = ["*.json"]
table = "station.heartbeat"
retention = "delete_on_ack"
[pipeline.transport]
type = "heartbeat_sftp"
host = "hb.example.org"
"""


def _station(tmp_path: Path, manifest_text: str = MANIFEST):
    """A synthetic station: pipelines.toml, sink.db, watermarks.db and one
    heartbeat file, each as the snapshot procedure leaves it (read-only)."""
    snap = tmp_path / "snap"
    snap.mkdir()
    heartbeat = tmp_path / "heartbeat"
    heartbeat.mkdir()
    beat = heartbeat / "beat-1.json"
    beat.write_text('{"station": "AC0G-T"}')

    sink = snap / "sink.db"
    conn = sqlite3.connect(sink)
    conn.executescript(SINK_DDL)
    for db, table, payload in ROWS:
        conn.execute(
            "INSERT INTO pending_uploads(target_db, target_table, "
            "schema_version, payload_json, queued_at) VALUES (?,?,?,?,?)",
            (db, table, 2, json.dumps(payload), payload["time"]))
    conn.commit()
    conn.close()

    watermarks = snap / "watermarks.db"
    store = SqliteWatermarkStore(watermarks)
    store.advance_cursor(*PSK_KEY, cursor=b"1", last_ack="2026-10-07T12:01:00+00:00")
    store.advance_cursor(*WSPRNET_KEY, cursor=b"6", last_ack="2026-10-07T12:01:00+00:00")
    store.advance_cursor(*CYCLE_KEY, cursor=b"2026-10-07T12:00:00Z",
                         last_ack="2026-10-07T12:02:10+00:00")
    store.enqueue_deliverable(
        pipeline="wspr-wsprdaemon", payload_blob=b"tar-bytes",
        enqueued_at="2026-10-07T12:02:10+00:00",
        next_attempt_at="2026-10-07T12:02:12+00:00",
        source_id=CYCLE_KEY[0], dest_id=CYCLE_KEY[1], table=CYCLE_KEY[2],
        cursor_after=b"2026-10-07T12:00:00Z")
    store.close()

    manifest = snap / "pipelines.toml"
    manifest.write_text(manifest_text.format(heartbeat=heartbeat))
    for f in (sink, watermarks, manifest):
        f.chmod(0o444)
    return manifest, sink, watermarks, beat


def _args(manifest, sink, watermarks, old=SRC, new=SRC, now=NOW):
    return ["--manifest", str(manifest), "--sink", str(sink),
            "--watermarks", str(watermarks), "--old", str(old),
            "--new", str(new), "--now", now]


def _section(out: str, name: str) -> str:
    """The five lines the tool prints for one pipeline."""
    lines = out.splitlines()
    start = lines.index(next(l for l in lines if l.startswith(f"{name}: ")))
    return "\n".join(lines[start:start + 5])


def _changed_tree(tmp_path: Path, module: str, old: str, new: str,
                  name: str = "new-src") -> Path:
    """A copy of this tree with one line of one module changed."""
    tree = tmp_path / name
    shutil.copytree(SRC, tree, ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
    path = tree / "hs_uploader" / module
    text = path.read_text()
    assert text.count(old) == 1
    path.write_text(text.replace(old, new))
    return tree


def test_one_tree_against_itself_agrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    assert nc.main(_args(manifest, sink, watermarks)) == 0
    out = capsys.readouterr().out

    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: AGREE")
    assert "  NEW key  (" + ", ".join(PSK_KEY) + ")" in psk
    # Rows 2-4 pass the filter; row 5 carries forward_to_pskreporter = 1.
    assert "3 records, rows 2..4, cursor 1 -> 4" in psk

    wsprnet = _section(out, "wspr-wsprnet")
    assert wsprnet.startswith("wspr-wsprnet: AGREE")
    # Row 9 beats row 8 on SNR; row 6's partition shipped before the cursor.
    assert "2 records, rows 9..11, cursor 6 -> 11" in wsprnet

    cycle = _section(out, "wspr-wsprdaemon")
    assert cycle.startswith("wspr-wsprdaemon: AGREE")
    # The 12:02 cycle: two spots, one noise row and two psk rows.
    assert ("5 records, cursor 2026-10-07T12:00:00Z -> 2026-10-07T12:02:00Z"
            in cycle)
    assert "; 1 queued" in cycle

    beat = _section(out, "heartbeat")
    assert beat.startswith("heartbeat: AGREE")
    assert "1 record, cursor (empty) -> <delete-on-ack>" in beat

    assert "4 pipelines: 4 agree, 0 disagree" in out
    assert out.rstrip().endswith("RESULT: AGREE")


def test_it_writes_nothing_it_was_given(tmp_path, capsys):
    manifest, sink, watermarks, beat = _station(tmp_path)
    given = (manifest, sink, watermarks, beat)
    before = {p: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
              for p in given}
    assert nc.main(_args(manifest, sink, watermarks)) == 0
    assert {p: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in given} == before
    # The heartbeat source deletes on ack.  The recorder stops the pump at
    # the send, so no commit runs and the file stays.
    assert beat.exists()
    # No sidecar appeared beside the snapshot either.
    assert sorted(p.name for p in sink.parent.iterdir()) == [
        "pipelines.toml", "sink.db", "watermarks.db"]


def test_a_changed_source_id_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # SqliteSource names a producer in its key: the change v3.71 makes on
    # purpose and v3.70 must not.
    new = _changed_tree(
        tmp_path, "sources/sqlite.py",
        'return f"sqlite:{self.database}.{self.table}"',
        'return f"sqlite:{self.database}.{self.table}@psk-recorder"')
    assert nc.main(_args(manifest, sink, watermarks, new=new)) == 1
    out = capsys.readouterr().out

    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    assert "  OLD key  (sqlite:psk.spots, " in psk
    assert "  NEW key  (sqlite:psk.spots@psk-recorder, " in psk
    # Under its new key NEW finds no send record, and start_at = "now"
    # puts it past every stored row.
    assert "  NEW next no batch; stored cursor (empty); 0 queued" in psk
    assert _section(out, "wspr-wsprnet").startswith("wspr-wsprnet: DISAGREE")
    # WsprCycleSource and the heartbeat form their keys elsewhere.
    assert _section(out, "wspr-wsprdaemon").startswith("wspr-wsprdaemon: AGREE")
    assert _section(out, "heartbeat").startswith("heartbeat: AGREE")
    assert "4 pipelines: 2 agree, 2 disagree" in out
    assert out.rstrip().endswith("RESULT: DISAGREE")


def test_a_changed_row_selection_disagrees_under_the_same_key(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    new = _changed_tree(tmp_path, "sources/sqlite.py",
                        '"id > ?",', '"id >= ?",')
    assert nc.main(_args(manifest, sink, watermarks, new=new)) == 1
    psk = _section(capsys.readouterr().out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    key = "(" + ", ".join(PSK_KEY) + ")"
    assert f"  OLD key  {key}" in psk and f"  NEW key  {key}" in psk
    # NEW re-sends row 1, which the stored cursor already covers.
    assert "  NEW next 4 records, rows 1..4, cursor 1 -> 4" in psk


def test_a_changed_key_alone_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # The heartbeat stores no cursor, so only its key can tell the trees apart.
    new = _changed_tree(tmp_path, "sources/files.py",
                        'source_id or f"files:{self.root}"',
                        'source_id or f"file:{self.root}"')
    assert nc.main(_args(manifest, sink, watermarks, new=new)) == 1
    beat = _section(capsys.readouterr().out, "heartbeat").splitlines()
    assert beat[0] == "heartbeat: DISAGREE"
    assert beat[3].replace("OLD", "NEW") == beat[4]


@pytest.mark.parametrize("which", ["manifest", "sink", "watermarks"])
def test_a_given_file_that_changes_fails_the_check(tmp_path, capsys, monkeypatch, which):
    manifest, sink, watermarks, _ = _station(tmp_path)
    victim = {"manifest": manifest, "sink": sink, "watermarks": watermarks}[which]
    real_run_tree = nc._run_tree

    def run_tree_then_write(label, *args, **kwargs):
        result = real_run_tree(label, *args, **kwargs)
        if label == "NEW":    # a live writer touches the file mid-check
            victim.chmod(0o644)
            with open(victim, "ab") as fh:
                fh.write(b"\n")
        return result

    monkeypatch.setattr(nc, "_run_tree", run_tree_then_write)
    assert nc.main(_args(manifest, sink, watermarks)) == 1
    assert "ERROR: a given file changed while the check ran" in capsys.readouterr().out


def test_a_wal_that_grows_during_the_run_fails_the_check(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)
    sink.chmod(0o644)
    conn = sqlite3.connect(sink)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute(
        "INSERT INTO pending_uploads(target_db, target_table, "
        "schema_version, payload_json, queued_at) VALUES (?,?,?,?,?)",
        ("psk", "spots", 2, json.dumps({
            "time": "2026-10-07T12:04:15Z", "mode": "ft8",
            "tx_call": "K6FGH", "forward_to_pskreporter": 0}),
         "2026-10-07T12:04:15Z"))
    conn.commit()
    real_run_tree = nc._run_tree

    def run_tree_then_commit(label, *args, **kwargs):
        result = real_run_tree(label, *args, **kwargs)
        if label == "NEW":
            # A recorder commits a row.  Only the -wal changes; sink.db keeps
            # its bytes until a checkpoint.
            conn.execute(
                "INSERT INTO pending_uploads(target_db, target_table, "
                "schema_version, payload_json, queued_at) VALUES (?,?,?,?,?)",
                ("psk", "spots", 2, "{}", "2026-10-07T12:09:00Z"))
            conn.commit()
        return result

    monkeypatch.setattr(nc, "_run_tree", run_tree_then_commit)
    try:
        assert Path(f"{sink}-wal").exists()
        main_before = hashlib.sha256(sink.read_bytes()).hexdigest()
        assert nc.main(_args(manifest, sink, watermarks)) == 1
        assert hashlib.sha256(sink.read_bytes()).hexdigest() == main_before
    finally:
        conn.close()
    assert "ERROR: a given file changed while the check ran" in capsys.readouterr().out


def test_a_wal_that_appears_during_the_run_fails_the_check(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)
    real_run_tree = nc._run_tree

    def run_tree_then_create_wal(label, *args, **kwargs):
        result = real_run_tree(label, *args, **kwargs)
        if label == "NEW":    # a recorder opens the live database
            Path(f"{watermarks}-wal").write_bytes(b"frames")
        return result

    monkeypatch.setattr(nc, "_run_tree", run_tree_then_create_wal)
    assert nc.main(_args(manifest, sink, watermarks)) == 1
    assert "ERROR: a given file changed while the check ran" in capsys.readouterr().out


def test_a_text_cursor_shows_the_same_error_in_both_trees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    watermarks.chmod(0o644)
    conn = sqlite3.connect(watermarks)
    conn.execute("UPDATE watermarks SET cursor = CAST(cursor AS TEXT) "
                 "WHERE source_id = ?", (PSK_KEY[0],))
    conn.commit()
    conn.close()
    # Both trees fail alike.  The pipeline line says they agree, and the
    # summary says the check saw nothing: INCONCLUSIVE, exit 1.
    assert nc.main(_args(manifest, sink, watermarks)) == 1
    out = capsys.readouterr().out
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: AGREE")
    assert "  OLD next error TypeError: " in psk
    assert ("An error stopped these pipelines, so the check shows nothing of "
            "what they send: psk-pskreporter") in out
    assert out.rstrip().endswith(
        "RESULT: INCONCLUSIVE (1 raised an error; see above)")


def test_both_trees_read_the_frozen_clock(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # At 12:03:30 the 12:02 cycle has not closed, so nothing ships.  A tree
    # that read the real clock would ship it.  (This run also spells the
    # manifest flag --pipelines, as Task 10 does; both spellings work.)
    args = _args(manifest, sink, watermarks, now="2026-10-07T12:03:30Z")
    args[args.index("--manifest")] = "--pipelines"
    assert nc.main(args) == 0
    cycle = _section(capsys.readouterr().out, "wspr-wsprdaemon")
    assert cycle.startswith("wspr-wsprdaemon: AGREE")
    for tree in ("OLD", "NEW"):
        assert (f"  {tree} next no batch; stored cursor 2026-10-07T12:00:00Z; "
                "1 queued") in cycle


def test_rows_still_in_the_wal_reach_the_copy(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    sink.chmod(0o644)
    conn = sqlite3.connect(sink)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute(
            "INSERT INTO pending_uploads(target_db, target_table, "
            "schema_version, payload_json, queued_at) VALUES (?,?,?,?,?)",
            ("psk", "spots", 2, json.dumps({
                "time": "2026-10-07T12:04:15Z", "mode": "ft8",
                "tx_call": "K6FGH", "forward_to_pskreporter": 0}),
             "2026-10-07T12:04:15Z"))
        conn.commit()
        assert Path(f"{sink}-wal").exists()
        assert nc.main(_args(manifest, sink, watermarks)) == 0
    finally:
        conn.close()
    psk = _section(capsys.readouterr().out, "psk-pskreporter")
    assert "4 records, rows 2..12, cursor 1 -> 12" in psk


def test_a_directory_without_hs_uploader_is_a_usage_error(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    with pytest.raises(SystemExit) as exc:
        nc.main(_args(manifest, sink, watermarks, old=tmp_path))
    assert exc.value.code == 2
    assert "holds no hs_uploader package" in capsys.readouterr().err


# ---- fix round 1: every comparison watched failing ---------------------------


def _run(capsys, manifest, sink, watermarks, **kwargs):
    code = nc.main(_args(manifest, sink, watermarks, **kwargs))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _no_digest(line: str) -> str:
    return re.sub(r"digest \w+", "digest -", line)


def _differs_only_in_the_digest(section: str) -> None:
    """OLD and NEW show the same key, row ids, cursor and queue; only the
    digest of the batch tells them apart."""
    lines = section.splitlines()
    old_key, new_key, old_next, new_next = lines[1:5]
    assert old_key.replace("OLD", "NEW") == new_key
    assert old_next.replace("OLD", "NEW") != new_next
    assert _no_digest(old_next).replace("OLD", "NEW") == _no_digest(new_next)


def _add_sink_row(sink: Path, db: str, table: str, payload: dict, queued_at: str):
    sink.chmod(0o644)
    conn = sqlite3.connect(sink)
    conn.execute(
        "INSERT INTO pending_uploads(target_db, target_table, schema_version, "
        "payload_json, queued_at) VALUES (?,?,?,?,?)",
        (db, table, 2, json.dumps(payload), queued_at))
    conn.commit()
    conn.close()
    sink.chmod(0o444)


# One line of sources/sqlite.py changes; the rows, ids and cursors stay.
# Only what the batch carries differs.
SAME_ROWS_OTHER_BATCH = [
    pytest.param(
        'columns = self._project_columns(payload)',
        'columns = {**self._project_columns(payload), "extra": 1}',
        id="payload"),
    pytest.param(
        'records=tuple(records),',
        'records=tuple(reversed(records)),',
        id="record-order"),
    pytest.param(
        'time_value = self._extract_record_time(payload, queued_at)',
        'time_value = self._extract_record_time(payload, queued_at).replace(microsecond=7)',
        id="record-time"),
    pytest.param(
        'columns=columns,',
        'columns=columns, dedup_key=b"k",',
        id="dedup-key"),
    pytest.param(
        'commit_token=new_cursor,',
        'commit_token=b"",',
        id="commit-token"),
]


@pytest.mark.parametrize("old,new", SAME_ROWS_OTHER_BATCH)
def test_a_batch_that_differs_alone_disagrees(tmp_path, capsys, old, new):
    manifest, sink, watermarks, _ = _station(tmp_path)
    tree = _changed_tree(tmp_path, "sources/sqlite.py", old, new)
    code, out, err = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    _differs_only_in_the_digest(psk)
    assert "3 records, rows 2..4, cursor 1 -> 4" in psk
    assert _section(out, "wspr-wsprnet").startswith("wspr-wsprnet: DISAGREE")
    # The cycle source and the heartbeat build their batches elsewhere.
    assert _section(out, "wspr-wsprdaemon").startswith("wspr-wsprdaemon: AGREE")
    assert _section(out, "heartbeat").startswith("heartbeat: AGREE")
    assert out.rstrip().endswith("RESULT: DISAGREE")
    assert "psk-pskreporter: OLD and NEW differ in: batch" in err


def test_a_changed_commit_token_on_a_filetree_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # The token names the files the source deletes after an ack.  A tree that
    # names one more would delete a file that never shipped.
    tree = _changed_tree(
        tmp_path, "sources/files.py",
        'commit_token = json.dumps(seen_paths).encode("utf-8")',
        'commit_token = json.dumps(seen_paths + ["/etc/hostname"]).encode("utf-8")')
    code, out, _ = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    beat = _section(out, "heartbeat")
    assert beat.startswith("heartbeat: DISAGREE")
    _differs_only_in_the_digest(beat)
    assert "1 record, cursor (empty) -> <delete-on-ack>" in beat


@pytest.mark.parametrize("old,new", [
    pytest.param("records=tuple(records),", "records=tuple(reversed(records)),",
                 id="record-order"),
    pytest.param("commit_token=b\"\",  # commit() is a no-op",
                 "commit_token=b\"x\",  # commit() is a no-op", id="commit-token"),
])
def test_a_changed_cycle_bundle_disagrees(tmp_path, capsys, old, new):
    manifest, sink, watermarks, _ = _station(tmp_path)
    tree = _changed_tree(tmp_path, "sources/wspr_cycle.py", old, new)
    code, out, _ = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    cycle = _section(out, "wspr-wsprdaemon")
    assert cycle.startswith("wspr-wsprdaemon: DISAGREE")
    _differs_only_in_the_digest(cycle)
    assert "5 records, cursor 2026-10-07T12:00:00Z -> 2026-10-07T12:02:00Z" in cycle
    assert _section(out, "psk-pskreporter").startswith("psk-pskreporter: AGREE")


def test_a_changed_cycle_judgement_at_now_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # NEW counts a cycle closed two minutes early.  At 12:03:30 the 12:02
    # cycle is still open, so OLD ships nothing and NEW ships the cycle.
    tree = _changed_tree(
        tmp_path, "sources/wspr_cycle.py",
        "floor -= timedelta(seconds=self.ship_buffer_sec)",
        "floor += timedelta(minutes=2)")
    code, out, _ = _run(capsys, manifest, sink, watermarks, new=tree,
                        now="2026-10-07T12:03:30Z")
    assert code == 1
    cycle = _section(out, "wspr-wsprdaemon")
    assert cycle.startswith("wspr-wsprdaemon: DISAGREE")
    assert ("  OLD next no batch; stored cursor 2026-10-07T12:00:00Z; "
            "1 queued") in cycle
    assert ("  NEW next 5 records, cursor 2026-10-07T12:00:00Z -> "
            "2026-10-07T12:02:00Z") in cycle
    assert _section(out, "psk-pskreporter").startswith("psk-pskreporter: AGREE")


def test_a_changed_stored_send_record_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # NEW reads b"01" where the file holds b"1".  SqliteSource parses both as
    # row 1, so the rows and the digest stay the same.  Only the send record
    # the store hands back differs.
    tree = _changed_tree(
        tmp_path, "watermark/sqlite.py",
        'return bytes(row["cursor"]) if row else b""',
        'return b"0" + bytes(row["cursor"]) if row else b""')
    code, out, err = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    assert "  OLD next 3 records, rows 2..4, cursor 1 -> 4, digest " in psk
    assert "  NEW next 3 records, rows 2..4, cursor 01 -> 4, digest " in psk
    digests = re.findall(r"digest (\w+)", psk)
    assert len(digests) == 2 and digests[0] == digests[1]
    assert "psk-pskreporter: OLD and NEW differ in: stored" in err


@pytest.mark.parametrize("old,new,psk_queued,cycle_queued", [
    pytest.param('"SELECT COUNT(*) AS n FROM deliverables WHERE pipeline=?",',
                 '"SELECT COUNT(*) + 1 AS n FROM deliverables WHERE pipeline=?",',
                 ("0", "1"), ("1", "2"), id="count"),
    pytest.param('"SELECT COUNT(*) AS n FROM deliverables WHERE pipeline=?",',
                 '"SELECT COUNT(*) AS n FROM deliverables WHERE pipeline!=?",',
                 ("0", "1"), ("1", "0"), id="set"),
])
def test_changed_queued_retries_disagree(tmp_path, capsys, old, new,
                                         psk_queued, cycle_queued):
    manifest, sink, watermarks, _ = _station(tmp_path)
    tree = _changed_tree(tmp_path, "watermark/sqlite.py", old, new)
    code, out, err = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    cycle = _section(out, "wspr-wsprdaemon")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    assert cycle.startswith("wspr-wsprdaemon: DISAGREE")
    assert "  OLD next 3 records, rows 2..4, cursor 1 -> 4, digest " in psk
    assert psk.splitlines()[3].endswith(f"; {psk_queued[0]} queued")
    assert psk.splitlines()[4].endswith(f"; {psk_queued[1]} queued")
    assert cycle.splitlines()[3].endswith(f"; {cycle_queued[0]} queued")
    assert cycle.splitlines()[4].endswith(f"; {cycle_queued[1]} queued")
    assert "wspr-wsprdaemon: OLD and NEW differ in: queued" in err


def test_an_error_in_one_tree_only_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    tree = _changed_tree(
        tmp_path, "sources/sqlite.py",
        'columns = self._project_columns(payload)',
        'columns = self._project_columns(payload); raise RuntimeError("boom")')
    code, out, _ = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    assert "  OLD next 3 records, rows 2..4" in psk
    assert "  NEW next error RuntimeError: boom" in psk
    assert out.rstrip().endswith("RESULT: DISAGREE (2 raised an error; see above)")


def test_a_changed_batch_cap_disagrees_though_the_backlog_is_small(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # Three rows wait.  A cap of 4 still lets them all through, so the batch
    # looks the same; only the cap the core applies differs.
    tree = _changed_tree(
        tmp_path, "transports/pskreporter.py",
        "return BatchPolicy(max_records=500)",
        "return BatchPolicy(max_records=4)")
    code, out, err = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: DISAGREE")
    assert psk.splitlines()[3].replace("OLD", "NEW") == psk.splitlines()[4]
    assert "psk-pskreporter: OLD and NEW differ in: limits" in err


# ---- fix round 1: results that prove nothing --------------------------------

UNBUILT = MANIFEST + '''
[[pipeline]]
name = "client-built"
builder = "no_such_client.mod:build"
'''


def test_a_manifest_without_pipelines_is_inconclusive(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(
        tmp_path, '[identity]\ncall = "AC0G/T"\n')
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "0 pipelines: 0 agree, 0 disagree" in out
    assert out.rstrip().endswith("RESULT: INCONCLUSIVE (no pipeline was compared)")


def test_a_pipeline_neither_tree_built_is_inconclusive(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path, UNBUILT)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "OLD built no pipeline for: client-built" in out
    assert "NEW built no pipeline for: client-built" in out
    assert "4 pipelines: 4 agree, 0 disagree" in out
    assert out.rstrip().endswith("RESULT: INCONCLUSIVE (1 not built; see above)")


def test_a_tree_that_builds_one_pipeline_fewer_disagrees(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # NEW's daemon drops the heartbeat pipeline after building it.
    tree = _changed_tree(
        tmp_path, "daemon.py", "base_identity = _base_identity(manifest)",
        'pipelines = [p for p in pipelines if p.name != "heartbeat"]\n'
        "    base_identity = _base_identity(manifest)")
    code, out, _ = _run(capsys, manifest, sink, watermarks, new=tree)
    assert code == 1
    assert _section(out, "heartbeat").startswith("heartbeat: DISAGREE")
    assert "  NEW key  (not built)" in _section(out, "heartbeat")
    assert out.rstrip().endswith("RESULT: DISAGREE")


def test_two_pipelines_with_one_name_both_appear(tmp_path, capsys):
    twin = MANIFEST + '''
[[pipeline]]
name = "heartbeat"
[pipeline.source]
type = "filetree"
root = "{heartbeat}"
patterns = ["*.json"]
table = "station.heartbeat"
retention = "delete_on_ack"
[pipeline.transport]
type = "heartbeat_sftp"
host = "hb2.example.org"
'''
    manifest, sink, watermarks, _ = _station(tmp_path, twin)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 0
    assert _section(out, "heartbeat#2").startswith("heartbeat#2: AGREE")
    assert "heartbeat-sftp:hb2.example.org" in _section(out, "heartbeat#2")
    assert "5 pipelines: 5 agree, 0 disagree" in out


def test_a_manifest_that_builds_nowhere_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(
        tmp_path, MANIFEST.replace('type = "wsprnet"', 'type = "nosuch"'))
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    for label in ("OLD", "NEW"):
        assert (f"ERROR: the {label} tree could not build the pipelines: "
                "ValueError: unknown transport type: 'nosuch'") in out
    assert "RESULT" not in out


def test_a_worker_that_crashes_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    broken = tmp_path / "broken" / "hs_uploader"
    broken.mkdir(parents=True)
    (broken / "__init__.py").write_text('raise RuntimeError("no import today")\n')
    code, out, err = _run(capsys, manifest, sink, watermarks, old=broken.parent)
    assert code == 1
    assert "ERROR: the OLD tree's worker exited 1" in out
    assert "RESULT" not in out
    assert "[OLD] RuntimeError: no import today" in err


def test_a_clock_that_cannot_freeze_is_an_error_not_a_skip(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # wspr_completion wraps datetime in a subclass: the freezer cannot swap
    # it, and a silent skip would let the tree judge cycles on the real clock.
    tree = _changed_tree(
        tmp_path, "sources/wspr_completion.py",
        "from datetime import datetime, timedelta, timezone",
        "from datetime import datetime as _real_datetime, timedelta, timezone\n\n\n"
        "class datetime(_real_datetime):\n    pass")
    code, out, _ = _run(capsys, manifest, sink, watermarks, old=tree, new=tree)
    assert code == 1
    for tree_label in ("OLD", "NEW"):
        assert (f"  {tree_label} next error RuntimeError: cannot freeze the "
                "clock of hs_uploader.sources.wspr_completion") in out
    assert out.rstrip().endswith(
        "RESULT: INCONCLUSIVE (4 raised an error; see above)")


# ---- fix round 1: the frozen clock reaches every module that reads it -------


def test_the_completion_gate_reads_the_frozen_clock(tmp_path, capsys):
    gated = MANIFEST.replace(
        "include_psk = true\n", 'include_psk = true\nexpected_reporters = ["AC0G"]\n')
    manifest, sink, watermarks, _ = _station(tmp_path, gated)
    # No noise row names AC0G, so the 12:02 cycle waits for the 90 s backstop.
    # At 12:05:00 it has not passed, and nothing ships.  On today's real clock
    # it has, and both trees would ship it.
    code, out, _ = _run(capsys, manifest, sink, watermarks, now="2026-10-07T12:05:00Z")
    assert code == 0
    cycle = _section(out, "wspr-wsprdaemon")
    for label in ("OLD", "NEW"):
        assert (f"  {label} next no batch; stored cursor 2026-10-07T12:00:00Z; "
                "1 queued") in cycle


def test_the_sqlite_source_reads_the_frozen_clock(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # A row with no usable time takes the time the source reads.  Two trees on
    # the real clock would read two instants and disagree.
    _add_sink_row(sink, "psk", "spots",
                  {"mode": "ft8", "tx_call": "K7XYZ", "forward_to_pskreporter": 0},
                  "not-a-time")
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 0
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: AGREE")
    assert "4 records, rows 2..12, cursor 1 -> 12" in psk


# ---- fix round 1: each tree works on its own copy ---------------------------


def test_each_tree_opens_its_own_copies_and_nothing_given(tmp_path):
    manifest, sink, watermarks, _ = _station(tmp_path)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    args = argparse.Namespace(manifest=manifest, sink=sink, watermarks=watermarks)
    old = nc._run_tree("OLD", SRC, args, NOW, scratch)
    new = nc._run_tree("NEW", SRC, args, NOW, scratch)
    given = {os.path.realpath(p) for p in (sink, watermarks)}
    for label, result in (("old", old), ("new", new)):
        opened = set(result["opened"])
        assert opened and not opened & given
        assert all(p.startswith(os.path.realpath(scratch / label) + os.sep)
                   for p in opened)
        assert os.path.realpath(scratch / label / "sink.db") in opened
        assert os.path.realpath(scratch / label / "watermarks.db") in opened
    assert not set(old["opened"]) & set(new["opened"])


def test_a_tree_that_writes_its_copy_leaves_the_other_tree_and_the_given_files(
        tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    # OLD deletes row 3 from the sink copy it reads.  NEW still sees all
    # three psk rows: it reads a copy of its own.  The given sink.db is
    # read-only, so a tree that wrote to it would fail outright.
    tree = _changed_tree(
        tmp_path, "daemon.py", "base_identity = _base_identity(manifest)",
        "base_identity = _base_identity(manifest)\n"
        "    import os as _os, sqlite3 as _sq\n"
        '    _c = _sq.connect(_os.environ["SIGMOND_SQLITE_PATH"])\n'
        '    _c.execute("DELETE FROM pending_uploads WHERE id = 3")\n'
        "    _c.commit()\n"
        "    _c.close()")
    before = hashlib.sha256(sink.read_bytes()).hexdigest()
    code, out, _ = _run(capsys, manifest, sink, watermarks, old=tree)
    assert code == 1
    psk = _section(out, "psk-pskreporter")
    assert "  OLD next 2 records, rows 2..4, cursor 1 -> 4" in psk
    assert "  NEW next 3 records, rows 2..4, cursor 1 -> 4" in psk
    assert hashlib.sha256(sink.read_bytes()).hexdigest() == before
    conn = sqlite3.connect(f"file:{sink}?mode=ro", uri=True)
    assert conn.execute("SELECT COUNT(*) FROM pending_uploads").fetchone() == (11,)
    conn.close()


def test_a_tree_that_reads_a_given_file_fails_the_check(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)
    real_copy = nc._copy_db

    def link_the_sink(src, dst):
        if dst.name == "sink.db":
            dst.symlink_to(src)     # the tree reads the given file, not a copy
        else:
            real_copy(src, dst)

    monkeypatch.setattr(nc, "_copy_db", link_the_sink)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert f"ERROR: the OLD tree opened {os.path.realpath(sink)}, a file you gave" in out


def test_two_trees_that_share_one_copy_fail_the_check(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)
    shared = tmp_path / "shared"
    shared.mkdir()

    def share_one_copy(src, dst):
        target = shared / dst.name
        if not target.exists():
            shutil.copyfile(src, target)
        dst.symlink_to(target)

    monkeypatch.setattr(nc, "_copy_db", share_one_copy)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: both trees opened " in out
    assert str(shared) in out


# ---- fix round 1: copies that give the check nothing to read -----------------


def test_a_sink_that_is_no_database_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    sink.chmod(0o644)
    sink.write_bytes(b"this is not a database\n" * 400)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: the copy of sink.db cannot be read as a database" in out
    assert "RESULT" not in out


def test_a_torn_sink_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    sink.chmod(0o644)
    sink.write_bytes(sink.read_bytes()[:5000])
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: the copy of sink.db " in out
    assert "RESULT" not in out


def test_the_wrong_file_as_sink_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    code, out, _ = _run(capsys, manifest, watermarks, watermarks)
    assert code == 1
    assert "ERROR: sink.db holds no pending_uploads table; is it the right file?" in out


def test_an_empty_watermarks_file_stops_the_check(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    watermarks.chmod(0o644)
    watermarks.write_bytes(b"")
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: watermarks.db holds no watermarks table" in out


# ---- fix round 1: failures in main() and bad arguments -----------------------


def test_an_unreadable_given_file_prints_an_error(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)

    def refuse(paths):
        raise PermissionError(errno.EACCES, "Permission denied", "sink.db")

    monkeypatch.setattr(nc, "_fingerprint", refuse)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: [Errno 13] Permission denied: 'sink.db'" in out


def test_a_full_temp_directory_names_tmpdir(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)

    def no_room(src, dst):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(nc, "_copy_db", no_room)
    code, out, _ = _run(capsys, manifest, sink, watermarks)
    assert code == 1
    assert "ERROR: [Errno 28] No space left on device" in out
    assert "Set TMPDIR" in out


@pytest.mark.parametrize("flag,value,message", [
    ("--now", "garbage", "Invalid isoformat string"),
    ("--now", "2026-10-07T12:07:30", "--now needs a UTC offset"),
    ("--sink", "/nonexistent/sink.db", "is not a file"),
    ("--manifest", "/nonexistent/pipelines.toml", "is not a file"),
])
def test_bad_arguments_are_usage_errors(tmp_path, capsys, flag, value, message):
    manifest, sink, watermarks, _ = _station(tmp_path)
    args = _args(manifest, sink, watermarks)
    args[args.index(flag) + 1] = value
    with pytest.raises(SystemExit) as exc:
        nc.main(args)
    assert exc.value.code == 2
    assert message in capsys.readouterr().err


def test_a_missing_tree_is_a_usage_error(tmp_path, capsys):
    manifest, sink, watermarks, _ = _station(tmp_path)
    args = _args(manifest, sink, watermarks)
    del args[args.index("--new"):args.index("--new") + 2]
    with pytest.raises(SystemExit) as exc:
        nc.main(args)
    assert exc.value.code == 2
    assert "--new is required" in capsys.readouterr().err


# ---- fix round 1: the report and the comparison, on hand-built results ------


def _entry(**over):
    entry = {"key": ["s", "d", "t"], "stored": "1", "queued": 0,
             "limits": [500, 500, None], "batch": None, "error": None}
    entry.update(over)
    return entry


def _result(pipelines, unbuilt=()):
    return {"pipelines": pipelines, "unbuilt": list(unbuilt)}


@pytest.mark.parametrize("field,other", [
    ("key", ["s2", "d", "t"]),
    ("stored", "2"),
    ("queued", 1),
    ("limits", [500, 4, None]),
    ("batch", {"records": 1, "digest": "x"}),
    ("error", "TypeError: boom"),
])
def test_each_compared_field_alone_breaks_agreement(field, other):
    assert set(nc._FIELDS) == {"key", "stored", "queued", "limits", "batch", "error"}
    assert nc._agree(_entry(), _entry())
    assert not nc._agree(_entry(), _entry(**{field: other}))
    assert not nc._agree(_entry(**{field: other}), _entry())


def test_a_pipeline_missing_from_one_side_never_agrees():
    assert not nc._agree(None, _entry())
    assert not nc._agree(_entry(), None)
    assert not nc._agree(None, None)


def test_the_report_names_a_pipeline_that_only_new_built(capsys):
    old = _result({"a": _entry()})
    new = _result({"a": _entry(), "b": _entry()})
    assert nc._report(old, new) == 1
    out = capsys.readouterr().out
    assert "b: DISAGREE" in out
    assert "  OLD key  (not built)" in out
    assert out.rstrip().endswith("RESULT: DISAGREE")


def test_the_report_counts_an_error_in_one_tree_only(capsys):
    old = _result({"a": _entry()})
    new = _result({"a": _entry(error="KeyError: 'x'")})
    assert nc._report(old, new) == 1
    out = capsys.readouterr().out
    assert "a: DISAGREE" in out
    assert out.rstrip().endswith("RESULT: DISAGREE (1 raised an error; see above)")


def test_the_report_calls_different_unbuilt_lists_a_disagreement(capsys):
    old = _result({"a": _entry()}, unbuilt=["x"])
    new = _result({"a": _entry()})
    assert nc._report(old, new) == 1
    out = capsys.readouterr().out
    assert "OLD built no pipeline for: x" in out
    assert out.rstrip().endswith("RESULT: DISAGREE")


def test_the_report_gives_every_reason_for_an_inconclusive_result(capsys):
    old = _result({"a": _entry(error="E: x")}, unbuilt=["x"])
    new = _result({"a": _entry(error="E: x")}, unbuilt=["x"])
    assert nc._report(old, new) == 1
    out = capsys.readouterr().out
    assert out.rstrip().endswith(
        "RESULT: INCONCLUSIVE (1 not built; see above; 1 raised an error; see above)")


def test_the_report_agrees_only_when_nothing_is_in_doubt(capsys):
    both = {"a": _entry(), "b": _entry()}
    assert nc._report(_result(both), _result(dict(both))) == 0
    assert capsys.readouterr().out.rstrip().endswith("RESULT: AGREE")
