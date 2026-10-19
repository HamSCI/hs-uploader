#!/usr/bin/env python3
"""Prove the key split on a station snapshot (spec-key-split-v373 §8 test 10).

The old shared pipeline `psk-pskreporter` selects every pskreporter row
for both clients.  The split gives each client its own pipeline:
`psk-recorder-pskreporter` (modes ft8, ft4) and
`meteor-scatter-pskreporter` (mode msk144).  This script runs all three
sources over a COPY of a station's sink.db, from an empty cursor with
`start_at` removed, and compares the sets of row ids.

It exits 0 when the two new sets are disjoint and their union equals the
old set.  Otherwise it prints the differing ids and exits 1.

Two checks run.

1. As rendered: each new source carries `producer` and its own mode
   list, beside the old block's other `extra_where` terms.
2. Scope alone: the same, with the mode clause removed from all three
   sources.  Check 1 cannot see a broken producer clause on consistent
   data, because the disjoint mode lists already partition the rows.
   Check 2 leaves the producer clause as the only partition.

The given files stay untouched.  The script copies the sink (with its
-wal and -shm sidecars) and the store into a temporary directory.

Usage:
    prove_key_split.py --sink SINK.db --store WM.db [--add-msk144 N]
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from hs_uploader.pipeline_factory import _build_source  # noqa: E402

# The old block, verbatim from psk-recorder/deploy.toml
# ([[hs_uploader.pipeline]] name = "psk-pskreporter"), source part only.
# start_at is left out: the proof reads from the beginning.
OLD_SOURCE = {
    "type": "sqlite",
    "database": "psk",
    "table": "spots",
    "accepted_schema_versions": [2],
    "delete_on_commit": False,
    "select_columns": [
        "time", "frequency", "mode", "snr_db", "tx_call", "grid",
        "score", "message", "rx_source", "frequency_bucket_hz",
    ],
    "extra_where": [
        ["tx_call", "!=", ""],
        ["mode", "IN", ["ft8", "ft4", "msk144"]],
        ["forward_to_pskreporter", "=", 0],
    ],
}

# pipeline name -> (producer, modes)
NEW_PIPELINES = {
    "psk-recorder-pskreporter": ("psk-recorder", ["ft8", "ft4"]),
    "meteor-scatter-pskreporter": ("meteor-scatter", ["msk144"]),
}


def _is_mode_clause(term) -> bool:
    return term[0] == "mode"


def old_spec(with_mode: bool = True) -> dict:
    spec = copy.deepcopy(OLD_SOURCE)
    if not with_mode:
        spec["extra_where"] = [t for t in spec["extra_where"] if not _is_mode_clause(t)]
    return spec


def new_spec(producer: str, modes: list, with_mode: bool = True) -> dict:
    spec = old_spec(with_mode=False)
    if with_mode:
        # The mode clause keeps the old term's place in the list.
        spec["extra_where"].insert(1, ["mode", "IN", list(modes)])
    spec["producer"] = producer
    return spec


def drain_ids(source) -> set:
    """Every id the source ships from an empty cursor, to the end.

    The projection (select_columns) drops the id, so the source runs one
    row per batch and the cursor after each batch names that row.
    """
    ids: set = set()
    cursor = b""
    while True:
        batches = list(source.iter_batches(cursor, 1))
        if not batches:
            return ids
        for batch in batches:
            if batch.records:
                ids.add(int(batch.cursor_after.decode("ascii")))
            cursor = batch.cursor_after


def copy_with_sidecars(src: Path, dst: Path) -> None:
    shutil.copy2(src, dst)
    for suffix in ("-wal", "-shm"):
        side = Path(str(src) + suffix)
        if side.exists():
            shutil.copy2(side, Path(str(dst) + suffix))


def add_msk144(db: Path, n: int) -> int:
    """Insert n synthetic MSK144 rows into the copy; return how many.

    Half carry producer='meteor-scatter', half producer=''.  A copy
    without the producer column gets every row without one.
    """
    conn = sqlite3.connect(db)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(pending_uploads)")}
        row = conn.execute(
            "SELECT payload_json, queued_at FROM pending_uploads "
            "WHERE target_db='psk' AND target_table='spots' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        payload = json.loads(row[0]) if row else {}
        queued_at = row[1] if row else "2026-10-08T00:00:00+00:00"
        payload.update({"mode": "msk144", "tx_call": "K1ABC", "forward_to_pskreporter": False})
        for i in range(n):
            p = dict(payload, message=f"CQ K1ABC FN42 #{i}")
            if "producer" in cols:
                conn.execute(
                    "INSERT INTO pending_uploads (target_db, target_table, "
                    "schema_version, payload_json, queued_at, producer) "
                    "VALUES ('psk','spots',2,?,?,?)",
                    (json.dumps(p), queued_at, "meteor-scatter" if i % 2 == 0 else ""),
                )
            else:
                conn.execute(
                    "INSERT INTO pending_uploads (target_db, target_table, "
                    "schema_version, payload_json, queued_at) "
                    "VALUES ('psk','spots',2,?,?)",
                    (json.dumps(p), queued_at),
                )
        conn.commit()
    finally:
        conn.close()
    return n


def check(label: str, old_ids: set, new_ids: dict, out) -> bool:
    names = list(new_ids)
    a, b = new_ids[names[0]], new_ids[names[1]]
    overlap = a & b
    union = a | b
    missing = old_ids - union
    extra = union - old_ids
    ok = not (overlap or missing or extra)
    print(f"[{label}] old={len(old_ids)}  " + "  ".join(f"{n}={len(s)}" for n, s in new_ids.items())
          + f"  overlap={len(overlap)} missing={len(missing)} extra={len(extra)}  "
          + ("PASS" if ok else "FAIL"), file=out)
    for what, ids in (("in both new pipelines", overlap),
                      ("in old, in neither new", missing),
                      ("in a new pipeline, not in old", extra)):
        if ids:
            print(f"  ids {what}: {sorted(ids)[:50]}{' ...' if len(ids) > 50 else ''}", file=out)
    return ok


def run(sink: Path, store: Optional[Path], add: int, out=sys.stdout) -> int:
    with tempfile.TemporaryDirectory(prefix="prove-key-split-") as tmp:
        tmpd = Path(tmp)
        sink_copy = tmpd / "sink.db"
        copy_with_sidecars(sink, sink_copy)
        if store is not None:
            copy_with_sidecars(store, tmpd / "store.db")
        if add:
            add_msk144(sink_copy, add)
        conn = sqlite3.connect(sink_copy)
        try:
            has_col = "producer" in {r[1] for r in conn.execute("PRAGMA table_info(pending_uploads)")}
            n_psk = conn.execute(
                "SELECT count(*) FROM pending_uploads WHERE target_db='psk' AND target_table='spots'"
            ).fetchone()[0]
        finally:
            conn.close()
        print(f"sink {sink}: producer column {'present' if has_col else 'absent'}, "
              f"{n_psk} psk.spots rows (incl. {add} synthetic msk144)", file=out)

        saved = os.environ.get("SIGMOND_SQLITE_PATH")
        os.environ["SIGMOND_SQLITE_PATH"] = str(sink_copy)
        try:
            ok = True
            for label, with_mode in (("as rendered", True), ("scope alone", False)):
                old_ids = drain_ids(_build_source(old_spec(with_mode)))
                new_ids = {
                    name: drain_ids(_build_source(new_spec(prod, modes, with_mode)))
                    for name, (prod, modes) in NEW_PIPELINES.items()
                }
                ok = check(label, old_ids, new_ids, out) and ok
        finally:
            if saved is None:
                os.environ.pop("SIGMOND_SQLITE_PATH", None)
            else:
                os.environ["SIGMOND_SQLITE_PATH"] = saved
    print("PROVED" if ok else "NOT PROVED", file=out)
    return 0 if ok else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--sink", required=True, type=Path)
    ap.add_argument("--store", required=True, type=Path)
    ap.add_argument("--add-msk144", type=int, default=0, metavar="N")
    args = ap.parse_args(argv)
    return run(args.sink, args.store, args.add_msk144)


if __name__ == "__main__":
    sys.exit(main())
