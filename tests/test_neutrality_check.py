"""tools/neutrality_check.py: the v3.70 proof that two hs-uploader trees
send the same thing (sigmond tasks/plan-sink-control.md §10.4).

Every test runs the tool for real: two worker processes, each importing
the tree it names.  A synthetic station stands in for ND or B4.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
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


def _station(tmp_path: Path):
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
    manifest.write_text(MANIFEST.format(heartbeat=heartbeat))
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


def _changed_tree(tmp_path: Path, module: str, old: str, new: str) -> Path:
    """A copy of this tree with one line of one module changed."""
    tree = tmp_path / "new-src"
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


def test_a_given_file_that_changes_fails_the_check(tmp_path, capsys, monkeypatch):
    manifest, sink, watermarks, _ = _station(tmp_path)
    real_run_tree = nc._run_tree

    def run_tree_then_write(label, *args, **kwargs):
        result = real_run_tree(label, *args, **kwargs)
        if label == "NEW":    # a live writer touches the file mid-check
            watermarks.chmod(0o644)
            with open(watermarks, "ab") as fh:
                fh.write(b"\0")
        return result

    monkeypatch.setattr(nc, "_run_tree", run_tree_then_write)
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
    # Both trees fail alike, so they agree, and the summary says so aloud.
    assert nc.main(_args(manifest, sink, watermarks)) == 0
    out = capsys.readouterr().out
    psk = _section(out, "psk-pskreporter")
    assert psk.startswith("psk-pskreporter: AGREE")
    assert "  OLD next error TypeError: " in psk
    assert ("An error stopped these pipelines, so the check shows nothing of "
            "what they send: psk-pskreporter") in out
    assert out.rstrip().endswith("RESULT: AGREE (1 raised an error; see above)")


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
