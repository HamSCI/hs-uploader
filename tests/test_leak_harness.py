"""The leak harness (spec sink-2b-part2 test 1 and the ship half of test 11;
design plan-sink-control §11; plan Task 10).

What runs is the daemon itself: ``daemon.run`` reads a scratch legacy
manifest and ``pipelines.d/``, builds its own ``SinkGate`` and its own
``Uploader``, and pumps in its real loop.  Real ``Writer``s fill a scratch
``sink.db``; the daemon's ``SqliteSource``, ``WsprCycleSource`` and
``FileTreeSource`` read it.  Only the transports are stand-ins: each wraps the
real transport the factory builds (same name, same table, same batch policy)
and records every byte that would leave the host, instead of sending it.

The pipelines:

* ``sigmond.toml``: the heartbeat, ``governed = false`` (gate rule 1);
* ``psk-recorder.toml``: FT8 rows from the sink to pskreporter, one batch a pump;
* ``wspr-recorder.toml``: WSPR cycles to wsprdaemon, in drain mode (several
  batches in one pump);
* ``meteor-scatter.toml``: MSK144 rows to pskreporter, a client file that
  says ``governed = false``.  Only ``sigmond.toml`` may say that (plan
  ruling 2, ruling X10), so its switches judge it; it stays at ``fill``.

The walk: first without the marker (part 1 behaviour: everything stores and
ships), then with it, through every pair of (site state, client state) and
every transition between two pairs, 72 of them.  Each step writes rows, drops
a heartbeat file and pumps until idle.  Then a switch falls from inside a
transport callback twice: between two drain-mode batches, and between two
replays.
Last, the impostor rises to ``upload`` and its backlog ships (its positive
control).

Each assertion carries a tag, so a failure names what leaked:

* ``LEAK``: something left for a client whose switches read off or fill;
* ``OFF ROW SHIPPED``: a row written while its producer read off shipped;
* ``BACKLOG``: what shipped is not every stored row, once each, oldest first;
* ``HEARTBEAT``: a step's heartbeat did not ship;
* ``HELD WORK``: a pipeline held at the start of a pump touched its store
  (spec §3.3: it returns without work).  The check at the top of
  ``_pump_one`` has two backers, the checks before each replay and before
  each batch, so its removal leaks nothing; this assertion is what sees it;
* ``FLIP BATCH``: a drain-mode batch left after its switch fell mid-pump;
* ``FLIP REPLAY``: a replay left after its switch fell mid-pump;
* ``CONTROL``: with every switch at upload the daemon's bytes differ from a
  part 1 Uploader's (no gate) fed the same rows.

The writer uses the library's default paths.  The test points them at the
scratch tree itself (``HS_UPLOADER_STATE_DIR``, ``DEFAULT_SINKS_DIR``) and
checks they equal the daemon's: the writer must judge by the same switches
and marker the gate reads.  That keeps the harness runnable on a commit
whose writer takes no path arguments (plan Task 10 step 2).

Deterministic: the Uploader's clock is injected, the pump interval is 10 ms,
and every row time is fixed, in 2020, so the WSPR source's real-time ceiling
never withholds a cycle.  Only the gate's own clock is real; it times fault
reminders, and no fault occurs here.
"""
from __future__ import annotations

import json
import os
import signal
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hs_uploader import daemon, pipeline_factory, sinkswitch
from hs_uploader.core import Outcome, Uploader
from hs_uploader.pipelines_d import load_layout, sinks_dir_for
from hs_uploader.sink.writer import SqliteConfig, Writer
from hs_uploader.sinkswitch import marker_path
from hs_uploader.watermark.sqlite import SqliteWatermarkStore

STATES = ("off", "fill", "upload")
PSK, WSPR, MSK = "psk-recorder", "wspr-recorder", "meteor-scatter"
GOVERNED = (PSK, WSPR, MSK)
HEARTBEAT = "heartbeat-sftp"
DEST = {PSK: "pskreporter-psk", WSPR: "wsprdaemon-tar", MSK: "pskreporter-msk"}
ROW_PREFIX = {PSK: "psk-", WSPR: "wspr-", MSK: "msk-"}
T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
_REAL_BUILD = pipeline_factory._build_transport

_MANIFEST = """\
[identity]
call = "AC0G/S"
grid = "EM38ww"

[daemon]
pump_interval_sec = 0.01
wake = false
"""

_FILES = {
    "sigmond.toml": """\
[[pipeline]]
name = "heartbeat"
governed = false
[pipeline.source]
type = "filetree"
root = "{hb}"
patterns = ["*.json"]
table = "station.heartbeat"
retention = "delete_on_ack"
[pipeline.transport]
type = "heartbeat_sftp"
host = "hb.example"
name = "heartbeat-sftp"
""",
    "psk-recorder.toml": """\
[[pipeline]]
name = "psk-pskr"
[pipeline.source]
type = "sqlite"
database = "psk"
table = "spots"
producer = "psk-recorder"
[pipeline.transport]
type = "pskreporter"
name = "pskreporter-psk"
""",
    "wspr-recorder.toml": """\
[[pipeline]]
name = "wspr-wd"
max_records_per_pump = 1000
[pipeline.source]
type = "wspr_cycle"
db_path = "{sink_db}"
start_at = "beginning"
[pipeline.transport]
type = "wsprdaemon_tar"
servers = ["gw1.example"]
name = "wsprdaemon-tar"
""",
    # An impostor: a client file may not exempt itself (plan ruling 2, X10).
    "meteor-scatter.toml": """\
[[pipeline]]
name = "msk-pskr"
governed = false
[pipeline.source]
type = "sqlite"
database = "psk"
table = "spots"
producer = "meteor-scatter"
[pipeline.transport]
type = "pskreporter"
name = "pskreporter-msk"
""",
}


# ---- the stand-in destination -------------------------------------------------


class Recorder:
    """One destination.  Keeps, in order, every byte that would leave.

    It wraps the transport the factory built, so its name (the send record's
    destination), table and batch policy are the real ones.  A batch's bytes
    are its records, fully: table, time, columns, and a heartbeat file's
    name and content.  Paths under the tree's root read ``<root>``, so two
    trees compare.  ``on_ship`` and ``on_replay`` run after the bytes are
    kept, on the pipeline's own thread, inside the pump.
    """

    def __init__(self, real, root: Path):
        self.name = real.name
        self._table = real.primary_table()
        self._policy = real.batch_policy()
        self._root = str(root)
        self._lock = threading.Lock()
        self.sent: list[bytes] = []
        self.on_ship = None
        self.on_replay = None

    def primary_table(self) -> str:
        return self._table

    def batch_policy(self):
        return self._policy

    def encode(self, batch) -> bytes:
        out = []
        for r in batch.records:
            d = {"table": r.table, "time": r.time.isoformat(),
                 "columns": dict(r.columns)}
            if r.payload_path is not None:
                p = Path(r.payload_path)
                d["file"], d["content"] = p.name, p.read_text()
            out.append(d)
        text = json.dumps(out, sort_keys=True, default=str)
        return text.replace(self._root, "<root>").encode()

    def _keep(self, blob: bytes) -> None:
        with self._lock:
            self.sent.append(bytes(blob))

    def ship(self, batch, identity) -> Outcome:
        self._keep(self.encode(batch))
        if self.on_ship is not None:
            self.on_ship(batch)
        return Outcome.acked()

    def serialize_for_retry(self, batch, identity) -> bytes:
        return self.encode(batch)

    def replay(self, blob, identity) -> Outcome:
        self._keep(blob)
        if self.on_replay is not None:
            self.on_replay(blob)
        return Outcome.acked()


def _records(rec: Recorder) -> list[dict]:
    out = []
    for blob in rec.sent:
        out.extend(json.loads(blob))
    return out


def shipped_rows(rec: Recorder, prefix: str) -> list[str]:
    """The sink rows ``rec`` sent, by their ``row`` id, in the order sent."""
    return [d["columns"]["row"] for d in _records(rec)
            if str(d.get("columns", {}).get("row", "")).startswith(prefix)]


def carried(rec: Recorder) -> list[str]:
    return [d["columns"]["carried"] for d in _records(rec)
            if "carried" in d.get("columns", {})]


def heartbeats(rec: Recorder) -> list[str]:
    return [d["file"] for d in _records(rec) if "file" in d]


# ---- a scratch station --------------------------------------------------------


class Tree:
    """One scratch station: manifest, pipelines.d/, sinks/, sink.db, writers."""

    def __init__(self, root: Path):
        self.root = root
        self.manifest = root / "pipelines.toml"
        self.pdir = root / "pipelines.d"
        self.sinks = root / "sinks"
        self.hb = root / "heartbeat"
        self.sink_db = root / "sink.db"
        for d in (root, self.pdir, self.sinks, self.hb, root / "state"):
            d.mkdir(parents=True, exist_ok=True)
        self.manifest.write_text(_MANIFEST)
        for name, text in _FILES.items():
            (self.pdir / name).write_text(
                text.format(hb=self.hb, sink_db=self.sink_db))
        cfg = SqliteConfig(path=str(self.sink_db))
        # One writer per queue table, as the recorders hold them.  The psk
        # writer names no producer, so each row's comes from its mode (D12):
        # MSK144 rows belong to meteor-scatter.  Defaults decide the switch
        # and marker paths (see the module docstring).
        self.writers = {
            "psk": Writer("psk", "spots", schema_version=1, config=cfg,
                          auto_flush_seconds=0),
            "wspr.spots": Writer("wspr", "spots", schema_version=1,
                                 config=cfg, auto_flush_seconds=0),
            "wspr.noise": Writer("wspr", "noise", schema_version=1,
                                 config=cfg, auto_flush_seconds=0),
        }
        self.recorders: dict[str, Recorder] = {}
        self.k = 0
        self.beats = 0

    def write_rows(self) -> dict[str, list[str]]:
        """One step's rows: two FT8 spots, one MSK144 spot, and one WSPR
        cycle (a spot and its noise row).  Returns each producer's row ids."""
        k, self.k = self.k, self.k + 1
        ts = (T0 + timedelta(minutes=2 * k)).strftime("%Y-%m-%dT%H:%M:%SZ")
        psk = [{"time": ts, "mode": "FT8", "row": f"psk-{k}-{i}"}
               for i in range(2)]
        msk = [{"time": ts, "mode": "MSK144", "row": f"msk-{k}"}]
        self.writers["psk"].insert(psk + msk)
        self.writers["wspr.spots"].insert([{"time": ts, "row": f"wspr-{k}"}])
        self.writers["wspr.noise"].insert([{"time": ts, "row": f"noise-{k}"}])
        for w in self.writers.values():
            w.flush()
        return {PSK: [r["row"] for r in psk], MSK: [r["row"] for r in msk],
                WSPR: [f"wspr-{k}"]}

    def beat(self) -> str:
        """Drop one heartbeat file, with a fixed time."""
        k, self.beats = self.beats, self.beats + 1
        p = self.hb / f"hb-{k:04d}.json"
        p.write_text(json.dumps({"beat": k}))
        os.utime(p, (1_600_000_000 + k, 1_600_000_000 + k))
        return p.name

    def close(self) -> None:
        for w in self.writers.values():
            w.close()


# ---- the expectation ----------------------------------------------------------


class Model:
    """What the switches say, and so what must store and what may ship.

    Kept by the test alone, from design §3.4 and spec §3.1, §4.1: the
    effective state is the stricter of the site's and the client's; without
    the marker everything stores and sends; the writer drops a row only
    while its producer reads off.
    """

    def __init__(self):
        self.enforced = False
        self.site = "off"
        self.client = {PSK: "off", WSPR: "off", MSK: "fill"}
        self.stored = {c: [] for c in GOVERNED}
        self.dropped = {c: set() for c in GOVERNED}
        self.released = {c: 0 for c in GOVERNED}

    def eff(self, c: str) -> str:
        if not self.enforced:
            return "upload"
        return STATES[min(STATES.index(self.site),
                          STATES.index(self.client[c]))]

    def stores(self, c: str) -> bool:
        return self.eff(c) != "off"

    def expected(self, c: str) -> list[str]:
        return self.stored[c][: self.released[c]]


def euler_walk() -> list[tuple[str, str]]:
    """Every (site, client) pair, and every transition between two pairs,
    once each: an Euler circuit of the complete digraph on the nine pairs,
    from (off, off) back to it."""
    nodes = [(s, c) for s in STATES for c in STATES]
    out = {n: [m for m in nodes if m != n] for n in nodes}
    stack, path = [nodes[0]], []
    while stack:
        v = stack[-1]
        if out[v]:
            stack.append(out[v].pop())
        else:
            path.append(stack.pop())
    path.reverse()
    return path[1:]


# ---- the daemon, driven -------------------------------------------------------


@dataclass
class Pump:
    held: set            # clients the model held when the pump began
    calls: list          # store calls the pump made
    did_work: bool


class SpyStore(SqliteWatermarkStore):
    """The daemon's store, noting each call that names a pipeline during a
    pump: by name (``deliverable_count``, ``pop_due_deliverable``) or by
    send-record key (``get_cursor``)."""

    current = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.active = False
        self.calls: list = []
        SpyStore.current = self

    def _note(self, what) -> None:
        if self.active:
            self.calls.append(what)

    def deliverable_count(self, pipeline=None):
        self._note(("name", pipeline))
        return super().deliverable_count(pipeline)

    def pop_due_deliverable(self, pipeline, *, now):
        self._note(("name", pipeline))
        return super().pop_due_deliverable(pipeline, now=now)

    def get_cursor(self, source_id, dest_id, table):
        self._note(("key", (source_id, dest_id, table)))
        return super().get_cursor(source_id, dest_id, table)


class Harness:
    """Runs ``daemon.run`` on a tree; a scenario generator runs after each
    pump, and each ``yield`` in it lets the daemon pump once more."""

    def __init__(self, tree: Tree, monkeypatch):
        self.tree = tree
        self.model = Model()
        self.t = 1_700_000_000.0
        self.pumps: list[Pump] = []
        self.uploader = None
        self.gen = None
        self.error = None
        self.done = False
        self._mp = monkeypatch

    def clock(self) -> float:
        return self.t

    # -- switches --

    def set_site(self, state: str) -> None:
        if self.model.site != state or not (self.tree.sinks / "site.toml").exists():
            sinkswitch.write_switch(self.tree.sinks / "site.toml", {"state": state})
        self.model.site = state

    def set_client(self, client: str, state: str) -> None:
        path = self.tree.sinks / f"{client}.toml"
        if self.model.client[client] != state or not path.exists():
            sinkswitch.write_switch(path, {"state": state})
        self.model.client[client] = state

    def rec(self, client: str) -> Recorder:
        return self.tree.recorders[DEST[client]]

    def pipe(self, client: str):
        return next(p for p in self.uploader.pipelines if p.client == client)

    # -- the run --

    def run(self, scenario) -> None:
        h = self
        self.gen = scenario(self)

        class Scripted(Uploader):
            def __init__(self, *a, **kw):
                kw["now_fn"] = h.clock
                super().__init__(*a, **kw)
                h.uploader = self

            def pump(self):
                held = {c for c in GOVERNED if h.model.eff(c) != "upload"}
                store = SpyStore.current
                store.calls, store.active = [], True
                try:
                    out = super().pump()
                finally:
                    store.active = False
                h.pumps.append(Pump(held, list(store.calls), out))
                h._advance()
                return out

        self._mp.setattr(daemon, "Uploader", Scripted)
        self._mp.setattr(daemon, "SqliteWatermarkStore", SpyStore)
        rc = daemon.run(self.tree.manifest)
        if self.error is not None:
            raise self.error
        assert rc == 0 and self.done, "the scenario did not run to its end"

    def _advance(self) -> None:
        if self.done:
            return
        try:
            next(self.gen)
            return
        except StopIteration:
            pass
        except BaseException as e:  # noqa: BLE001 -- re-raised after run()
            self.error = e
        self.done = True
        os.kill(os.getpid(), signal.SIGTERM)

    def settle(self, limit: int = 8):
        """Pump until a pump finds no work."""
        for _ in range(limit):
            self.t += 600.0
            yield
            if not self.pumps[-1].did_work:
                return
        raise AssertionError(f"the daemon did not settle in {limit} pumps")

    # -- one step --

    def step(self, label: str):
        """Write a step's rows and a heartbeat, pump until idle, check."""
        m = self.model
        before = {c: len(self.rec(c).sent) for c in GOVERNED}
        rows = self.tree.write_rows()
        for c in GOVERNED:
            if m.stores(c):
                m.stored[c].extend(rows[c])
            else:
                m.dropped[c].update(rows[c])
        beat = self.tree.beat()
        first = len(self.pumps)
        yield from self.settle()
        for c in GOVERNED:
            if m.eff(c) == "upload":
                m.released[c] = len(m.stored[c])
        self.check(label, before, beat, self.pumps[first:])

    def check(self, label, before, beat, pumps) -> None:
        m = self.model
        state = f"[{label}] site={m.site} " + " ".join(
            f"{c}={m.client[c]}" for c in GOVERNED)
        assert beat in heartbeats(self.tree.recorders[HEARTBEAT]), (
            f"HEARTBEAT: {beat} did not ship {state}")
        for c in GOVERNED:
            if m.eff(c) != "upload":
                assert len(self.rec(c).sent) == before[c], (
                    f"LEAK: {c} reads {m.eff(c)} yet sent "
                    f"{self.rec(c).sent[before[c]:]} {state}")
        for c in GOVERNED:
            sent = shipped_rows(self.rec(c), ROW_PREFIX[c])
            leaked = [r for r in sent if r in m.dropped[c]]
            assert not leaked, (
                f"OFF ROW SHIPPED: {c} sent {leaked}, written while it "
                f"read off {state}")
            assert sent == m.expected(c), (
                f"BACKLOG: {c} sent {sent}, expected every stored row once, "
                f"oldest first: {m.expected(c)} {state}")
        self.check_held_work(label, pumps)

    def check_held_work(self, label, pumps) -> None:
        for n, p in enumerate(pumps):
            for c in p.held:
                pipe = self.pipe(c)
                mine = {("name", pipe.name), ("key", daemon.pipeline_key(pipe))}
                touched = [call for call in p.calls if call in mine]
                assert not touched, (
                    f"HELD WORK: [{label}] pump {n}: {c} was held at the "
                    f"start of the pump yet made {touched}")


# ---- fixtures -----------------------------------------------------------------


@pytest.fixture(autouse=True)
def fresh_switch_state():
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()
    yield
    sinkswitch._cache.clear()
    sinkswitch._logged.clear()


@pytest.fixture
def signals():
    saved = {s: signal.getsignal(s)
             for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    yield
    for s, h in saved.items():
        signal.signal(s, h)


@pytest.fixture
def station(tmp_path, monkeypatch):
    """The daemon's tree, with the library's default paths pointed at it."""
    tree = Tree(tmp_path / "daemon")
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tree.root / "state"))
    monkeypatch.setattr(sinkswitch, "DEFAULT_SINKS_DIR", tree.sinks)
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tree.sink_db))
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    # The writer and the gate must read one set of switches and one marker.
    assert sinks_dir_for(tree.manifest) == sinkswitch.DEFAULT_SINKS_DIR
    assert marker_path() == tree.root / "state" / "carry-over-v1.done"
    _stand_in_transports(monkeypatch, tree)
    yield tree
    tree.close()


def _stand_in_transports(monkeypatch, tree: Tree) -> None:
    """Every transport the factory builds becomes a Recorder in ``tree``."""

    def build(spec):
        rec = Recorder(_REAL_BUILD(spec), tree.root)
        tree.recorders[rec.name] = rec
        return rec

    monkeypatch.setattr(pipeline_factory, "_build_transport", build)


# ---- the walk -----------------------------------------------------------------


def test_every_switch_state_and_transition_through_the_daemon(
        station, monkeypatch, signals):
    walk = euler_walk()
    assert len(walk) == 72 and len(set(zip([("off", "off")] + walk, walk))) == 72

    def scenario(h: Harness):
        m = h.model
        h.set_site("off")
        h.set_client(PSK, "off")
        h.set_client(WSPR, "off")
        h.set_client(MSK, "fill")
        # No marker: part 1 behaviour.  Every row stores and ships whatever
        # the switches say (gate rule 3; the writer drops nothing).  This is
        # the walk's own positive control: the recorders do see sends.
        yield from h.step("no marker")
        assert shipped_rows(h.rec(PSK), "psk") and shipped_rows(h.rec(MSK), "msk")
        # The carry-over finishes: the marker appears between two pumps.
        marker_path().write_text("carried over\n")
        m.enforced = True
        yield from h.step("marker appears")
        for i, (site, client) in enumerate(walk):
            h.set_site(site)
            h.set_client(PSK, client)
            h.set_client(WSPR, client)
            yield from h.step(f"walk {i}")

        # -- a switch falls between two drain-mode batches ---------------------
        h.set_site("upload")
        h.set_client(PSK, "upload")
        h.set_client(WSPR, "upload")
        yield from h.step("drain before the flips")
        h.set_client(WSPR, "fill")
        for i in range(3):
            yield from h.step(f"wspr backlog {i}")
        backlog = m.stored[WSPR][m.released[WSPR]:]
        assert len(backlog) == 3
        rec = h.rec(WSPR)

        def fall(_batch):
            rec.on_ship = None
            h.set_client(WSPR, "fill")

        rec.on_ship = fall
        h.set_client(WSPR, "upload")
        before = shipped_rows(rec, "wspr")
        h.t += 600.0
        yield
        assert shipped_rows(rec, "wspr") == before + backlog[:1], (
            f"FLIP BATCH: wspr-recorder fell to fill inside its first batch's "
            f"ship, yet sent {shipped_rows(rec, 'wspr')[len(before):]} "
            f"of {backlog}")
        m.released[WSPR] += 1
        yield from h.settle()
        assert shipped_rows(rec, "wspr") == before + backlog[:1]
        h.check_held_work("after the batch flip", h.pumps[-1:])
        h.set_client(WSPR, "upload")
        yield from h.step("wspr raised after the batch flip")

        # -- a switch falls between two replays ---------------------------------
        pipe = h.pipe(PSK)
        store = SpyStore.current
        key = daemon.pipeline_key(pipe)
        for n in (1, 2):
            store.enqueue_deliverable(
                pipeline=pipe.name,
                payload_blob=json.dumps(
                    [{"columns": {"carried": f"retry-{n}"}}]).encode(),
                enqueued_at="2020-01-01T00:00:00+00:00",
                next_attempt_at=f"2020-01-01T00:00:0{n}+00:00",
                source_id=key[0], dest_id=key[1], table=key[2])
        rec = h.rec(PSK)

        def fall_replay(_blob):
            rec.on_replay = None
            h.set_client(PSK, "fill")

        rec.on_replay = fall_replay
        h.t += 600.0
        yield
        assert carried(rec) == ["retry-1"], (
            f"FLIP REPLAY: psk-recorder fell to fill inside its first "
            f"replay, yet replayed {carried(rec)}")
        assert store.deliverable_count(pipe.name) == 1
        yield from h.settle()
        assert carried(rec) == ["retry-1"]
        assert store.deliverable_count(pipe.name) == 1
        h.set_client(PSK, "upload")
        yield from h.step("psk raised after the replay flip")
        assert carried(rec) == ["retry-1", "retry-2"]
        assert store.deliverable_count(pipe.name) == 0

        # -- the impostor rises: its stored backlog ships, once -----------------
        assert m.stored[MSK][m.released[MSK]:], "the impostor holds no backlog"
        h.set_client(MSK, "upload")
        yield from h.step("meteor-scatter raised")
        assert m.released[MSK] == len(m.stored[MSK])

    Harness(station, monkeypatch).run(scenario)


# ---- the positive control -----------------------------------------------------


def test_all_upload_ships_the_bytes_of_a_part1_uploader_without_a_gate(
        station, tmp_path, monkeypatch, signals):
    """With every switch at upload and the marker standing, the daemon sends
    exactly the bytes a gateless Uploader sends, fed the same rows the same
    way (spec test 1's control; test 11's ship half: the heartbeat)."""
    steps = 4

    def scenario(h: Harness):
        for c in GOVERNED:
            h.set_client(c, "upload")
        h.set_site("upload")
        marker_path().write_text("carried over\n")
        h.model.enforced = True
        for i in range(steps):
            yield from h.step(f"upload {i}")

    Harness(station, monkeypatch).run(scenario)

    # The same rows through part 1's Uploader: no gate, no switches read.
    part1 = Tree(tmp_path / "part1")
    _stand_in_transports(monkeypatch, part1)
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(part1.sink_db))
    store = SqliteWatermarkStore(part1.root / "state" / "watermarks.db")
    pipes, refused = daemon.build_layout(load_layout(part1.manifest),
                                         watermark=store)
    assert not refused
    up = Uploader(pipes, inline_single=False, now_fn=lambda: 1_700_000_000.0)
    try:
        for _ in range(steps):
            part1.write_rows()
            part1.beat()
            for _ in range(8):
                if not up.pump():
                    break
    finally:
        up.close()
        part1.close()

    assert sorted(station.recorders) == sorted(part1.recorders)
    for name in sorted(part1.recorders):
        assert part1.recorders[name].sent, f"CONTROL: {name} sent nothing"
        assert station.recorders[name].sent == part1.recorders[name].sent, (
            f"CONTROL: {name}: the daemon's bytes differ from part 1's")
