"""hs-uploader sink set / mark-carried / status (spec sink-2b-part2 §9.1, §9.2;
tests 12 and 13, status half).

Every test runs hermetic: conftest points HS_UPLOADER_STATE_DIR and
sinkswitch.DEFAULT_SINKS_DIR at tmp_path, so the "real directory" the root
check guards is tmp_path/sinks here, and the marker's default path sits in
tmp_path.
"""
from __future__ import annotations

import fcntl
import io
import json
import os
import shutil
import stat
import tomllib
from pathlib import Path

import pytest

from hs_uploader import cli, sinkswitch
from hs_uploader.cli import main
from hs_uploader.watermark import SqliteWatermarkStore

FIXTURES = Path(__file__).parent / "fixtures" / "sink_status"


@pytest.fixture
def root(monkeypatch):
    """Run as root for the root check (the injectable euid)."""
    monkeypatch.setattr(cli, "_euid", lambda: 0)


@pytest.fixture
def nonroot(monkeypatch):
    monkeypatch.setattr(cli, "_euid", lambda: 1000)


def _read(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def _all_files(d: Path) -> set[str]:
    return {str(p.relative_to(d)) for p in d.rglob("*")}


# ---- sink set ---------------------------------------------------------------


def test_set_goes_through_write_switch(tmp_path, monkeypatch, capsys, nonroot):
    sinks = tmp_path / "s"
    calls = []
    real = sinkswitch.write_switch

    def spy(path, change):
        calls.append((Path(path), dict(change)))
        return real(path, change)

    monkeypatch.setattr(sinkswitch, "write_switch", spy)
    assert main(["sink", "set", "c1", "fill", "--reason", "bench",
                 "--by", "mjh", "--sinks", str(sinks)]) == 0
    assert calls == [(sinks / "c1.toml",
                      {"state": "fill", "reason": "bench", "set_by": "mjh"})]
    out = capsys.readouterr().out
    assert out == f"sink set: c1 missing -> fill ({sinks / 'c1.toml'})\n"
    assert main(["sink", "set", "c1", "upload", "--reason", "r",
                 "--by", "mjh", "--sinks", str(sinks)]) == 0
    assert capsys.readouterr().out == (
        f"sink set: c1 fill -> upload ({sinks / 'c1.toml'})\n")
    d = _read(sinks / "c1.toml")
    assert d["state"] == "upload" and d["client"] == "c1"
    assert [h["to"] for h in d["history"]] == ["fill", "upload"]


def test_set_reports_a_state_outside_the_three_as_invalid(
        tmp_path, capsys, nonroot):
    sinks = tmp_path / "s"
    sinks.mkdir()
    (sinks / "c1.toml").write_text('client = "c1"\nstate = "bogus"\n')
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--sinks", str(sinks)]) == 0
    assert capsys.readouterr().out == (
        f"sink set: c1 invalid -> fill ({sinks / 'c1.toml'})\n")
    assert _read(sinks / "c1.toml")["history"][-1]["from"] == "off"


def test_set_site(tmp_path, capsys, nonroot):
    sinks = tmp_path / "s"
    assert main(["sink", "set", "site", "off", "--reason", "why",
                 "--by", "smd sink (mjh)", "--sinks", str(sinks)]) == 0
    d = _read(sinks / "site.toml")
    assert d["state"] == "off" and d["set_by"] == "smd sink (mjh)"
    assert "client" not in d


def test_set_takes_the_lock(tmp_path, monkeypatch, capsys, nonroot):
    """While another holder keeps sinks/.lock, `sink set` writes nothing."""
    sinks = tmp_path / "s"
    sinks.mkdir()
    monkeypatch.setattr(sinkswitch, "LOCK_TIMEOUT_S", 0.2)
    fd = os.open(sinks / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        assert main(["sink", "set", "c1", "fill", "--reason", "r",
                     "--by", "b", "--sinks", str(sinks)]) == 1
        err = capsys.readouterr().err
        assert "lock" in err and "nothing written" in err
        assert not (sinks / "c1.toml").exists()
    finally:
        os.close(fd)
    # Positive control: the lock released, the same call writes.
    assert main(["sink", "set", "c1", "fill", "--reason", "r",
                 "--by", "b", "--sinks", str(sinks)]) == 0
    assert _read(sinks / "c1.toml")["state"] == "fill"


@pytest.mark.parametrize("argv, word", [
    (["site", "bogus"], "bogus"),
    (["c1", "Upload"], "Upload"),
    (["../x", "fill"], "../x"),
    (["A", "fill"], "'A'"),
    (["c1/../site", "fill"], "c1/../site"),
])
def test_set_refuses_bad_state_or_name(tmp_path, capsys, nonroot, argv, word):
    sinks = tmp_path / "s"
    sinks.mkdir()
    before = _all_files(tmp_path)
    assert main(["sink", "set", *argv, "--reason", "r", "--by", "b",
                 "--sinks", str(sinks)]) == 1
    err = capsys.readouterr().err
    assert word in err
    assert _all_files(tmp_path) == before


def test_set_needs_reason_and_by(tmp_path, nonroot):
    with pytest.raises(SystemExit) as e:
        main(["sink", "set", "c1", "fill", "--by", "b",
              "--sinks", str(tmp_path)])
    assert e.value.code == 2
    with pytest.raises(SystemExit):
        main(["sink", "set", "c1", "fill", "--reason", "r",
              "--sinks", str(tmp_path)])


def test_set_refuses_a_non_root_caller_on_the_real_directory(
        tmp_path, monkeypatch, capsys):
    real = sinkswitch.DEFAULT_SINKS_DIR          # tmp_path/sinks (hermetic)
    monkeypatch.setattr(cli, "_euid", lambda: 1000)
    for extra in ([], ["--sinks", str(real)],
                  ["--sinks", str(real) + "/../sinks"]):
        assert main(["sink", "set", "c1", "fill", "--reason", "r",
                     "--by", "b", *extra]) == 1
        assert "root" in capsys.readouterr().err
    assert not real.exists()
    # The dir under the real manifest counts too (by its constant).
    monkeypatch.setattr(cli, "_REAL_MANIFEST", str(tmp_path / "rm" / "p.toml"))
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--manifest", str(tmp_path / "rm" / "p.toml")]) == 1
    assert "root" in capsys.readouterr().err
    assert not (tmp_path / "rm").exists()
    # Positive control: as root, the default directory takes the write.
    monkeypatch.setattr(cli, "_euid", lambda: 0)
    assert main(["sink", "set", "c1", "fill", "--reason", "r",
                 "--by", "b"]) == 0
    assert _read(real / "c1.toml")["state"] == "fill"


def test_set_manifest_resolves_sinks_beside_it(tmp_path, capsys, nonroot):
    m = tmp_path / "etc" / "pipelines.toml"
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--manifest", str(m)]) == 0
    assert _read(tmp_path / "etc" / "sinks" / "c1.toml")["state"] == "upload"


def test_set_sinks_and_manifest_exclusive(tmp_path, nonroot):
    with pytest.raises(SystemExit):
        main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
              "--sinks", str(tmp_path), "--manifest", str(tmp_path / "m")])


def test_set_identity_json_from_stdin(tmp_path, monkeypatch, nonroot):
    sinks = tmp_path / "s"
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(
        {"B4-100": {"call": "AC0G/B4", "grid": "EM38ww"},
         "bee1": {"call": "AC0G", "grid": "EM38"}})))
    assert main(["sink", "set", "wspr-recorder", "upload", "--reason",
                 "carried over", "--by", "carry-over", "--identity-json", "-",
                 "--sinks", str(sinks)]) == 0
    h = _read(sinks / "wspr-recorder.toml")["history"][-1]
    assert h["identity"] == {"B4-100": {"call": "AC0G/B4", "grid": "EM38ww"},
                             "bee1": {"call": "AC0G", "grid": "EM38"}}
    assert h["by"] == "carry-over"


def test_set_identity_json_from_a_file(tmp_path, nonroot):
    sinks = tmp_path / "s"
    f = tmp_path / "id.json"
    f.write_text(json.dumps({"B4-100": {"call": "AC0G/B4"}}))
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--identity-json", str(f), "--sinks", str(sinks)]) == 0
    assert _read(sinks / "c1.toml")["history"][-1]["identity"] == {
        "B4-100": {"call": "AC0G/B4"}}


@pytest.mark.parametrize("text", ["{not json", "[1, 2]", '{"B4": 3}', ""])
def test_set_identity_json_refuses_what_is_not_tables(
        tmp_path, monkeypatch, capsys, nonroot, text):
    sinks = tmp_path / "s"
    monkeypatch.setattr("sys.stdin", io.StringIO(text))
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--identity-json", "-", "--sinks", str(sinks)]) == 1
    assert "identity" in capsys.readouterr().err
    assert not (sinks / "c1.toml").exists()


def test_set_emitter_error_names_the_file(tmp_path, capsys, nonroot):
    sinks = tmp_path / "s"
    sinks.mkdir()
    f = sinks / "c1.toml"
    f.write_text('client = "c1"\nstate = "upload"\n'
                 "[[history]]\nat = 2026-10-01T00:00:00Z\n")  # hand-typed datetime
    before = f.read_bytes()
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--sinks", str(sinks)]) == 1
    err = capsys.readouterr().err
    assert str(f) in err and "cannot re-emit" in err
    assert f.read_bytes() == before


def test_set_broken_file_exits_1_naming_it(tmp_path, capsys, nonroot):
    sinks = tmp_path / "s"
    sinks.mkdir()
    (sinks / "c1.toml").write_text("state = [bad\n")
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--sinks", str(sinks)]) == 1
    assert str(sinks / "c1.toml") in capsys.readouterr().err


# ---- sink mark-carried --------------------------------------------------------


def test_mark_carried_writes_the_marker(tmp_path, capsys):
    m = sinkswitch.marker_path()                 # tmp_path (hermetic)
    assert main(["sink", "mark-carried", "--note", "site upload; 4 clients"]) == 0
    assert capsys.readouterr().out == f"sink mark-carried: {m}\n"
    lines = m.read_text().splitlines()
    assert lines[0].startswith("carried over at ")
    assert lines[1:] == ["site upload; 4 clients"]
    st = os.lstat(m)
    assert stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o644


def test_mark_carried_marker_flag_and_no_note(tmp_path, capsys):
    m = tmp_path / "x" / "marker"
    m.parent.mkdir()
    assert main(["sink", "mark-carried", "--marker", str(m)]) == 0
    assert len(m.read_text().splitlines()) == 1


def test_mark_carried_goes_through_write_marker(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(sinkswitch, "write_marker",
                        lambda p, lines: calls.append((Path(p), list(lines))))
    assert main(["sink", "mark-carried", "--note", "n",
                 "--marker", str(tmp_path / "m")]) == 0
    assert calls == [(tmp_path / "m", ["n"])]


def test_mark_carried_replaces_a_planted_link_never_follows_it(tmp_path):
    """Task 2's symlink case, through the CLI: the link's target stays."""
    victim = tmp_path / "victim"
    victim.write_text("precious\n")
    m = tmp_path / "state" / "carry-over-v1.done"
    m.parent.mkdir()
    os.symlink(victim, m)
    assert main(["sink", "mark-carried", "--note", "n", "--marker", str(m)]) == 0
    assert victim.read_text() == "precious\n"
    st = os.lstat(m)
    assert stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o644
    assert m.read_text().splitlines()[1:] == ["n"]


def test_mark_carried_failure_exits_1(tmp_path, capsys):
    m = tmp_path / "missing-dir" / "marker"
    assert main(["sink", "mark-carried", "--marker", str(m)]) == 1
    assert str(m) in capsys.readouterr().err


# ---- sink status ---------------------------------------------------------------

HB = ('[[pipeline]]\nname = "hb"\nclient = "sigmond"\n'
      '[pipeline.source]\ntype = "sqlite"\n'
      '[pipeline.transport]\ntype = "heartbeat_sftp"\nhost = "h"\n')


def _pipe(name, *, client=None, transport='type = "pskreporter"', extra=""):
    c = f'client = "{client}"\n' if client else ""
    return (f'[[pipeline]]\nname = "{name}"\n{c}{extra}'
            f'[pipeline.source]\ntype = "sqlite"\n'
            f'[pipeline.transport]\n{transport}\n')


class Station:
    def __init__(self, tmp_path: Path):
        self.root = tmp_path / "st"
        self.etc = self.root / "etc"
        self.pd = self.etc / "pipelines.d"
        self.sinks = self.etc / "sinks"
        self.state = self.root / "state"
        for d in (self.pd, self.state):
            d.mkdir(parents=True)
        self.manifest = self.etc / "pipelines.toml"
        self.manifest.write_text(HB)
        self.marker = self.state / "carry-over-v1.done"
        self.db = self.state / "watermarks.db"

    def file(self, client, *pipes):
        (self.pd / f"{client}.toml").write_text("".join(pipes))

    def switch(self, name, state):
        sinkswitch.write_switch(self.sinks / f"{name}.toml",
                                {"state": state, "reason": f"r-{name}",
                                 "set_by": "test", "set_at": "2026-10-01T00:00:00Z"})

    def mark(self):
        sinkswitch.write_marker(self.marker, [])

    def status(self, *extra) -> list[str]:
        return ["--state", str(self.db), "sink", "status",
                "--manifest", str(self.manifest), "--marker", str(self.marker),
                *extra]


def _json_status(st: Station, capsys, rc=0) -> dict:
    assert main(st.status("--json")) == rc
    return json.loads(capsys.readouterr().out)


@pytest.fixture
def st(tmp_path):
    s = Station(tmp_path)
    s.file("psk-recorder", _pipe("psk-pskr"))
    s.file("wspr-recorder", _pipe("wspr-wsprnet", transport='type = "wsprnet"'),
           _pipe("wspr-tar", transport='type = "wsprdaemon_tar"'))
    s.manifest.write_text(HB + _pipe("legacy-noclient"))
    s.switch("site", "upload")
    s.switch("psk-recorder", "fill")
    s.switch("wspr-recorder", "upload")
    return s


TOP = {"schema", "enforced", "marker", "sinks_dir", "site", "clients",
       "held_pipelines", "refused_files", "no_client_pipelines",
       "builder_pipelines", "errors"}


def test_status_json_schema_key_for_key(st, capsys):
    st.mark()
    d = _json_status(st, capsys)
    assert set(d) == TOP
    assert d["schema"] == 1
    assert set(d["site"]) == {"state", "error", "set_at", "set_by", "reason"}
    assert d["clients"], "fixture has clients"
    for c in d["clients"]:
        assert set(c) == {"client", "file_state", "effective", "held_by",
                          "error", "pipelines"}
    assert d["held_pipelines"]
    for h in d["held_pipelines"]:
        assert set(h) == {"pipeline", "client", "held_by", "reason"}
    (st.pd / "bad.toml").write_text(_pipe("b", extra='governed = "yes"\n'))
    d = _json_status(st, capsys)
    assert d["refused_files"]
    for r in d["refused_files"]:
        assert set(r) == {"file", "pipeline", "client", "reason"}


def test_status_enforced_lists_held_pipelines(st, capsys):
    st.mark()
    d = _json_status(st, capsys)
    assert d["enforced"] is True
    assert d["marker"] == str(st.marker) and d["sinks_dir"] == str(st.sinks)
    assert d["site"] == {"state": "upload", "error": "", "reason": "r-site",
                         "set_by": "test", "set_at": "2026-10-01T00:00:00Z"}
    assert d["held_pipelines"] == [
        {"pipeline": "psk-pskr", "client": "psk-recorder",
         "held_by": "client", "reason": "sink filling (client)"}]
    assert d["errors"] == [] and d["builder_pipelines"] == []
    by = {c["client"]: c for c in d["clients"]}
    assert set(by) == {"psk-recorder", "wspr-recorder"}   # no sigmond row
    assert by["psk-recorder"] == {
        "client": "psk-recorder", "file_state": "fill", "effective": "fill",
        "held_by": "client", "error": "", "pipelines": ["psk-pskr"]}
    assert by["wspr-recorder"]["held_by"] is None
    assert by["wspr-recorder"]["pipelines"] == ["wspr-wsprnet", "wspr-tar"]
    assert d["no_client_pipelines"] == ["legacy-noclient"]
    assert main(st.status()) == 0
    out = capsys.readouterr().out
    assert f"  enforced: the carry-over marker {st.marker} stands" in out
    assert "not enforced" not in out
    assert "  held: psk-pskr (psk-recorder): sink filling (client)" in out


def test_status_site_fill_holds_every_governed_pipeline(st, capsys):
    st.mark()
    st.switch("site", "fill")
    d = _json_status(st, capsys)
    held = {h["pipeline"]: h for h in d["held_pipelines"]}
    assert set(held) == {"psk-pskr", "wspr-wsprnet", "wspr-tar"}
    assert held["wspr-tar"]["reason"] == "sink filling (site)"
    by = {c["client"]: c for c in d["clients"]}
    assert by["wspr-recorder"]["held_by"] == "site"
    # The legacy heartbeat (rule 2) and the client-less pipeline (rule 4) send.
    assert "hb" not in held and "legacy-noclient" not in held


def test_status_held_by_fault(st, capsys):
    st.mark()
    (st.sinks / "wspr-recorder.toml").write_text("state = [bad\n")
    st.file("mag-recorder", _pipe("mag-psws"))       # no switch file
    d = _json_status(st, capsys)
    by = {c["client"]: c for c in d["clients"]}
    assert by["wspr-recorder"]["held_by"] == "fault"
    assert by["wspr-recorder"]["file_state"] == "error"
    assert by["wspr-recorder"]["error"]
    assert by["mag-recorder"]["held_by"] == "fault"
    assert by["mag-recorder"]["file_state"] == "missing"
    held = {h["pipeline"]: h["reason"] for h in d["held_pipelines"]}
    assert held["mag-psws"] == "no switch file (client)"
    assert held["wspr-tar"] == "switch unreadable (client)"


def test_status_without_marker_says_not_enforced_but_reports_effective(
        st, capsys):
    """X5: effective and held_by do not depend on the marker; only
    `enforced` and `held_pipelines` do (the gate sends by rule 3)."""
    d = _json_status(st, capsys)
    assert d["enforced"] is False
    assert d["held_pipelines"] == []
    by = {c["client"]: c for c in d["clients"]}
    assert by["psk-recorder"]["effective"] == "fill"
    assert by["psk-recorder"]["held_by"] == "client"
    assert main(st.status()) == 0
    out = capsys.readouterr().out
    assert (f"  not enforced: no carry-over marker at {st.marker}; "
            "every pipeline sends as its manifest says") in out
    assert "held:" not in out


def test_status_held_pipelines_come_from_the_gate(st, capsys, monkeypatch):
    """One decision code: a gate that holds everything holds everything here."""
    from hs_uploader import gate
    st.mark()
    monkeypatch.setattr(gate.SinkGate, "decide",
                        lambda self, pipe: gate.Decision(False, 5, "stub says"))
    d = _json_status(st, capsys)
    assert {h["pipeline"] for h in d["held_pipelines"]} == {
        "hb", "legacy-noclient", "psk-pskr", "wspr-wsprnet", "wspr-tar"}
    assert {h["reason"] for h in d["held_pipelines"]} == {"stub says"}


def test_status_governed_false_only_from_sigmond_file(st, capsys):
    st.mark()
    st.switch("site", "off")
    st.file("sigmond", _pipe("sig-hb", extra="governed = false\n",
                              transport='type = "heartbeat_sftp"\nhost = "h"'))
    st.file("psk-recorder", _pipe("psk-pskr", extra="governed = false\n"))
    d = _json_status(st, capsys)
    held = {h["pipeline"] for h in d["held_pipelines"]}
    assert "sig-hb" not in held
    assert "psk-pskr" in held            # governed=false outside sigmond.toml
    assert "sigmond" not in {c["client"] for c in d["clients"]}


def test_status_lists_a_client_whose_file_the_loader_refuses(st, capsys):
    st.mark()
    st.file("meteor-scatter", _pipe("ms", extra='governed = "yes"\n'))
    d = _json_status(st, capsys)
    assert d["refused_files"] == [{
        "file": "meteor-scatter.toml", "pipeline": None,
        "client": "meteor-scatter",
        "reason": "pipeline 'ms': governed must be true or false"}]
    assert main(st.status()) == 0
    out = capsys.readouterr().out
    assert ("  refused: pipelines.d/meteor-scatter.toml (meteor-scatter): "
            "pipeline 'ms': governed must be true or false") in out


def test_status_lists_a_file_the_daemon_refused_at_build(st, capsys):
    """A refusal only the daemon's build saw (it is in pipeline_state)."""
    st.mark()
    store = SqliteWatermarkStore(st.db)
    store.record_pipeline_states([
        ("psk-pskr", "psk-recorder", ""),
        ("store-held", "x", "store schema too old"),   # not a file: not listed
        ("pipelines.d/wspr-recorder.toml", "wspr-recorder",
         "pipeline 'wspr-tar' failed to build: KeyError('servers')")])
    store.close()
    os.chmod(st.db, 0o444)                           # status only reads it
    d = _json_status(st, capsys)
    assert d["refused_files"] == [
        {"file": None, "pipeline": "store-held", "client": "x",
         "reason": "store schema too old"},
        {"file": "wspr-recorder.toml", "pipeline": None,
         "client": "wspr-recorder",
         "reason": "pipeline 'wspr-tar' failed to build: KeyError('servers')"}]
    assert d["errors"] == []
    assert main(st.status()) == 0
    assert ("  refused: pipeline store-held (x): store schema too old"
            in capsys.readouterr().out)


def test_status_survives_non_text_values_in_a_replaced_store(st, capsys):
    """A store someone replaced may hold BLOBs or numbers where text
    belongs.  Status decodes them, says so in errors, and still prints
    schema 1 (Task 18 LOW)."""
    import sqlite3
    st.mark()
    with sqlite3.connect(st.db) as c:             # a replaced store: no types
        c.execute("CREATE TABLE pipeline_state (name, client, refused_reason)")
        c.execute("INSERT INTO pipeline_state VALUES ('psk-pskr', 'psk', '')")
        c.execute("INSERT INTO pipeline_state (name, client, refused_reason) "
                  "VALUES (?, ?, ?)",
                  (b"pipelines.d/blob.toml", b"blob\xff", b"why"))
        c.execute("INSERT INTO pipeline_state (name, client, refused_reason) "
                  "VALUES (?, ?, ?)", (7, None, 3.5))
    d = _json_status(st, capsys)
    assert d["schema"] == 1
    assert d["refused_files"] == [
        {"file": None, "pipeline": "7", "client": "", "reason": "3.5"},
        {"file": "blob.toml", "pipeline": None, "client": "blob\ufffd",
         "reason": "why"}]
    assert len(d["errors"]) == 1 and "non-text" in d["errors"][0]
    assert main(st.status()) == 0
    assert "  refused: pipelines.d/blob.toml" in capsys.readouterr().out
    # Control: a store of text alone reports no such error.
    with sqlite3.connect(st.db) as c:
        c.execute("DELETE FROM pipeline_state WHERE typeof(name) != 'text' "
                  "OR typeof(client) != 'text' "
                  "OR typeof(refused_reason) != 'text'")
    assert _json_status(st, capsys)["errors"] == []


def test_status_never_creates_the_store(st, capsys):
    assert not st.db.exists()
    _json_status(st, capsys)
    assert not st.db.exists()


def test_status_without_sinks_dir(st, capsys):
    shutil.rmtree(st.sinks)
    d = _json_status(st, capsys)
    assert d["site"]["state"] == "missing"
    assert {c["file_state"] for c in d["clients"]} == {"missing"}
    assert main(st.status()) == 0
    assert "no switch files" in capsys.readouterr().out


def test_status_manifest_read_failure_exits_2(tmp_path, capsys):
    m = tmp_path / "missing.toml"
    assert main(["sink", "status", "--json", "--manifest", str(m),
                 "--marker", str(tmp_path / "mk")]) == 2
    cap = capsys.readouterr()
    d = json.loads(cap.out)
    assert set(d) == TOP
    assert len(d["errors"]) == 1 and d["errors"][0].startswith(
        f"cannot read {m}")
    assert f"cannot read {m}" in cap.err


def test_status_marker_defaults_to_marker_path(st, capsys):
    sinkswitch.write_marker(sinkswitch.marker_path(), [])
    assert main(["--state", str(st.db), "sink", "status", "--json",
                 "--manifest", str(st.manifest)]) == 0
    d = json.loads(capsys.readouterr().out)
    assert d["enforced"] is True and d["marker"] == str(sinkswitch.marker_path())


# ---- the fixture trees sigmond captures (Task 15) ----------------------------


def _fixture_argv(tree: Path, *extra) -> list[str]:
    return ["--state", str(tree / "state" / "watermarks.db"), "sink", "status",
            "--manifest", str(tree / "etc" / "pipelines.toml"),
            "--marker", str(tree / "state" / "carry-over-v1.done"), *extra]


@pytest.mark.parametrize("name, enforced, held", [
    ("enforced", True, {"psk-recorder-pskreporter"}),
    ("not-enforced", False, set()),
    ("site-off", True, {"psk-recorder-pskreporter", "wspr-recorder-wsprnet",
                        "mag-recorder-psws"}),
    ("refused", True, set()),
])
def test_fixture_trees_read_as_named(name, enforced, held, capsys):
    tree = FIXTURES / name
    d = json.loads(_run(tree, capsys, "--json"))
    assert d["enforced"] is enforced
    assert {h["pipeline"] for h in d["held_pipelines"]} == held
    if name == "refused":
        assert [r["client"] for r in d["refused_files"]] == ["meteor-scatter"]
    _run(tree, capsys)                                  # the text form runs
    assert not (tree / "state" / "watermarks.db").exists()  # status writes nothing


def _run(tree, capsys, *extra) -> str:
    assert main(_fixture_argv(tree, *extra)) == 0
    return capsys.readouterr().out


# ---- fix round 1 (Task 9 review) ---------------------------------------------

def test_status_reports_an_unreadable_store_in_errors(st, capsys):
    st.db.write_bytes(b"this is not a database at all" * 40)
    d = _json_status(st, capsys)
    assert len(d["errors"]) == 1 and str(st.db) in d["errors"][0]
    assert d["refused_files"] == []
    assert main(st.status()) == 0
    assert f"  error: cannot read the store {st.db}" in capsys.readouterr().out


def test_status_never_forms_a_path_from_a_bad_client_name(st, capsys):
    """client = "../secret" must not read etc/secret.toml (sinks/../secret)."""
    (st.etc / "secret.toml").write_text('state = "upload"\n')
    st.manifest.write_text(HB + _pipe("sneaky", client="../secret"))
    st.mark()
    d = _json_status(st, capsys)
    by = {c["client"]: c for c in d["clients"]}
    assert by["../secret"]["file_state"] == "error"
    assert by["../secret"]["effective"] == "off"
    assert by["../secret"]["held_by"] == "fault"
    held = {h["pipeline"]: h for h in d["held_pipelines"]}
    assert held["sneaky"]["held_by"] == "fault"
    assert held["sneaky"]["reason"] == "bad client name"


def test_status_held_by_fault_for_a_broken_site_file(st, capsys):
    st.mark()
    (st.sinks / "site.toml").write_text("state = [bad\n")
    d = _json_status(st, capsys)
    assert d["site"]["state"] == "error" and d["site"]["error"]
    by = {c["client"]: c for c in d["clients"]}
    assert by["wspr-recorder"]["held_by"] == "fault"   # its own file: upload
    assert by["wspr-recorder"]["effective"] == "off"
    held = {h["pipeline"]: h for h in d["held_pipelines"]}
    assert held["wspr-tar"] == {"pipeline": "wspr-tar",
                                "client": "wspr-recorder", "held_by": "fault",
                                "reason": "switch unreadable (site)"}


def test_status_tie_goes_to_the_site(st, capsys):
    st.mark()
    st.switch("site", "fill")                   # psk-recorder reads fill too
    d = _json_status(st, capsys)
    by = {c["client"]: c for c in d["clients"]}
    assert by["psk-recorder"]["held_by"] == "site"
    held = {h["pipeline"]: h for h in d["held_pipelines"]}
    assert held["psk-pskr"]["held_by"] == "site"
    assert held["psk-pskr"]["reason"] == "sink filling (site)"


def test_status_keeps_stderr_clean_and_reports_in_the_json(st, monkeypatch):
    """The gate and the switch reader log faults; status shows them in its
    own report, not as stray lines on stderr.  A real process: under pytest
    the root logger has a handler, so Python's last-resort stderr handler
    would never fire in-process.  The child gets a minimal environment: a
    PYTHONWARNINGS (or any other setting) in the caller's must not reach
    it, so the test plants one (an invalid filter writes to stderr)."""
    import subprocess
    import sys
    monkeypatch.setenv("PYTHONWARNINGS", "bogus::NoSuchWarning")
    st.mark()
    (st.sinks / "wspr-recorder.toml").write_text("state = [bad\n")
    st.file("mag-recorder", _pipe("mag-psws",
                                   extra='governed = false\n'))  # WARNING
    env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
           "PYTHONPATH": str(Path(cli.__file__).parents[1]),
           "HS_UPLOADER_STATE_DIR": str(st.state)}
    out = subprocess.run(
        [sys.executable, "-P", "-m", "hs_uploader.cli", *st.status("--json")],
        cwd="/", env=env, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stderr == ""
    by = {c["client"]: c for c in json.loads(out.stdout)["clients"]}
    assert "parse error" in by["wspr-recorder"]["error"]  # positive control
    held = {h["pipeline"] for h in json.loads(out.stdout)["held_pipelines"]}
    assert "mag-psws" in held                     # governed=false ignored


def test_status_removes_its_quiet_handler(st, capsys):
    import logging
    _json_status(st, capsys)
    assert not any(isinstance(h, logging.NullHandler)
                   for h in logging.getLogger("hs_uploader").handlers)


def test_status_names_builder_entries(st, capsys):
    st.manifest.write_text(HB + '[[pipeline]]\nname = "b"\nbuilder = "x:y"\n'
                           'client = "c9"\n')
    d = _json_status(st, capsys)
    assert d["builder_pipelines"] == ["b"]
    assert main(st.status()) == 0
    assert "status cannot see inside: b" in capsys.readouterr().out


@pytest.mark.parametrize("text, words", [
    ('{"B4-100": {"call": null}}', ["'B4-100'", "key call", "null"]),
    ('{"B4-100": {"psws": {"ids": [1, 2]}}}',
     ["'B4-100'", "key psws.ids", "array of values"]),
    ('{"B4-100": {"": "x"}}', ["'B4-100'", "empty key"]),
    ('{"": {"call": "x"}}', ["empty instance name"]),
])
def test_set_identity_json_refuses_what_toml_cannot_hold(
        tmp_path, monkeypatch, capsys, nonroot, text, words):
    sinks = tmp_path / "s"
    sinks.mkdir()
    sinkswitch.write_switch(sinks / "c1.toml", {"state": "fill"})
    before = (sinks / "c1.toml").read_bytes()
    monkeypatch.setattr("sys.stdin", io.StringIO(text))
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--identity-json", "-", "--sinks", str(sinks)]) == 1
    err = capsys.readouterr().err
    for w in words:
        assert w in err
    assert "move the file aside" not in err
    assert (sinks / "c1.toml").read_bytes() == before


def test_set_identity_json_nested_tables_and_arrays_of_tables_pass(
        tmp_path, monkeypatch, nonroot):
    sinks = tmp_path / "s"
    ident = {"B4-100": {"call": "AC0G", "n": 3, "ok": True, "f": 1.5,
                        "psws": {"station_id": "S1"}, "empty": [],
                        "ids": [{"id": "a"}, {"id": "b"}]}}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(ident)))
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--identity-json", "-", "--sinks", str(sinks)]) == 0
    assert _read(sinks / "c1.toml")["history"][-1]["identity"] == ident


def test_set_identity_json_is_capped(tmp_path, monkeypatch, capsys, nonroot):
    sinks = tmp_path / "s"
    pad = " " * (cli.IDENTITY_JSON_CAP - 30)
    small = '{"B4": {"call": "x"}}' + pad
    assert len(small) <= cli.IDENTITY_JSON_CAP
    monkeypatch.setattr("sys.stdin", io.StringIO(small))
    assert main(["sink", "set", "c1", "upload", "--reason", "r", "--by", "b",
                 "--identity-json", "-", "--sinks", str(sinks)]) == 0
    big = '{"B4": {"call": "x"}}' + " " * cli.IDENTITY_JSON_CAP
    monkeypatch.setattr("sys.stdin", io.StringIO(big))
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--identity-json", "-", "--sinks", str(sinks)]) == 1
    assert "larger than" in capsys.readouterr().err
    f = tmp_path / "big.json"
    f.write_text(big)
    assert main(["sink", "set", "c1", "fill", "--reason", "r", "--by", "b",
                 "--identity-json", str(f), "--sinks", str(sinks)]) == 1
    assert _read(sinks / "c1.toml")["state"] == "upload"


# -- pipe_views parity with the daemon's own build (review I1) --

def _tname(t) -> tuple:
    return (type(t).__name__, type(getattr(t, "inner", None)).__name__)


def _parity(tmp_path, manifest_text: str, files: dict) -> None:
    from hs_uploader import daemon
    from hs_uploader.gate import exempt_rule
    from hs_uploader.pipelines_d import load_layout
    etc = tmp_path / "par"
    (etc / "pipelines.d").mkdir(parents=True)
    m = etc / "pipelines.toml"
    m.write_text(manifest_text)
    for name, text in files.items():
        (etc / "pipelines.d" / name).write_text(text)
    layout = load_layout(m)
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    try:
        built, refused = daemon.build_layout(
            layout, watermark=store,
            paths=daemon.SinkPaths(etc / "sinks", tmp_path / "marker"))
    finally:
        store.close()
    assert refused == []
    views = [v for v in daemon.pipe_views(layout) if not v.builder]
    want = [(p.name, p.client, p.governed, p.origin, _tname(p.transport),
             exempt_rule(p)) for p in built]
    got = [(v.name, v.client, v.governed, v.origin, _tname(v.transport),
            exempt_rule(v)) for v in views]
    assert sorted(got) == sorted(want)
    assert len(want) >= 3


def _src(table):
    return (f'[pipeline.source]\ntype = "sqlite"\ndatabase = "sigmond"\n'
            f'table = "{table}"\n')


def test_pipe_views_match_the_daemon_build_legacy(tmp_path):
    """Unnamed legacy entries around a builder, a discard-wrapped heartbeat,
    and a heartbeat-transport pipeline whose client is not sigmond."""
    text = (
        '[[pipeline]]\nclient = "c1"\n' + _src("t.a")
        + '[pipeline.transport]\ntype = "pskreporter"\n'
        + '[[pipeline]]\nname = "bld"\nbuilder = "no_such_module:f"\n'
        + '[[pipeline]]\nclient = "c1"\n' + _src("t.b")
        + '[pipeline.transport]\ntype = "wsprnet"\n'
        + '[[pipeline]]\nname = "hb"\nclient = "sigmond"\ndiscard = true\n'
        + _src("t.hb") + '[pipeline.transport]\ntype = "heartbeat_sftp"\n'
        'host = "h"\n'
        + '[[pipeline]]\nname = "hb-other"\nclient = "other"\n' + _src("t.ho")
        + '[pipeline.transport]\ntype = "heartbeat_sftp"\nhost = "h"\n')
    _parity(tmp_path, text, {})


def test_pipe_views_match_the_daemon_build_files(tmp_path):
    entry = ('[[pipeline]]\n{extra}' + '{src}'
             + '[pipeline.transport]\ntype = "pskreporter"\n')
    files = {
        "c2.toml": "".join(entry.format(extra=e, src=_src(t)) for e, t in [
            ("", "f.a"), ("", "f.b"), ('discard = true\n', "f.c"),
            ('governed = false\n', "f.d")]),
        "sigmond.toml": entry.format(extra='name = "sig"\ngoverned = false\n',
                                     src=_src("f.s")),
    }
    _parity(tmp_path, '[[pipeline]]\nname = "named"\n' + _src("l.a")
            + '[pipeline.transport]\ntype = "wsprnet"\n', files)


@pytest.mark.parametrize("name", ["enforced", "not-enforced", "site-off",
                                  "refused"])
def test_fixture_trees_build_through_the_daemon(name, tmp_path):
    """The fixture trees are real station trees: the daemon builds them."""
    from hs_uploader import daemon
    from hs_uploader.pipelines_d import load_layout
    tree = FIXTURES / name
    layout = load_layout(tree / "etc" / "pipelines.toml")
    store = SqliteWatermarkStore(tmp_path / "wm.db")
    try:
        built, refused = daemon.build_layout(
            layout, watermark=store,
            paths=daemon.SinkPaths(tree / "etc" / "sinks",
                                   tree / "state" / "carry-over-v1.done"))
    finally:
        store.close()
    assert {p.name for p in built} == {
        v.name for v in daemon.pipe_views(layout)}
    assert len(built) == 4
    assert [r.client for r in refused] == (
        ["meteor-scatter"] if name == "refused" else [])
