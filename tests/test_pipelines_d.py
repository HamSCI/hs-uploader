"""The pipelines.d/ loader (spec sink-2b §3.1-§3.3, §3.6; tests 1-5, 8 start).

Each pipeline file ``pipelines.d/<client>.toml`` sits beside the legacy
manifest.  A file that exists silences that client's legacy entries; a file
that breaks a rule, fails to build, or collides is refused whole and never
stops the daemon.
"""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path

import pytest

from hs_uploader import daemon, pipelines_d
from hs_uploader.watermark.sqlite import SqliteWatermarkStore


# ---- fixtures -----------------------------------------------------------------


def _ft(name, root, *, client=None, instrument="367", table="grape.dataset",
        extra=""):
    """One filetree -> psws_dataset entry as TOML text (cheap to build)."""
    client_line = f'client = "{client}"\n' if client is not None else ""
    return textwrap.dedent(f"""
        [[pipeline]]
        name = "{name}"
        {client_line}{extra}
        [pipeline.source]
        type = "filetree"
        root = "{root}"
        patterns = ["OBS*"]
        retention = "keep"
        match_dirs = true
        table = "{table}"
        [pipeline.transport]
        type = "psws_dataset"
        instrument_id = "{instrument}"
        table = "{table}"
    """)


def _wspr(name, db, *, client=None, start_at=None, server="gw1.wsprdaemon.org"):
    client_line = f'client = "{client}"\n' if client is not None else ""
    start_line = f'start_at = "{start_at}"\n' if start_at is not None else ""
    return textwrap.dedent(f"""
        [[pipeline]]
        name = "{name}"
        {client_line}
        [pipeline.source]
        type = "wspr_cycle"
        db_path = "{db}"
        {start_line}
        [pipeline.transport]
        type = "wsprdaemon_tar"
        servers = ["{server}"]
        receiver = "AC0G_S"
    """)


_IDENTITY = textwrap.dedent("""
    [identity]
    call = "AC0G/S"
    grid = "EM38ww"
    station_id = "S000418"
    ssh_key_file = "/etc/hs-uploader/keys/id_ed25519"
""")


@pytest.fixture
def station(tmp_path, monkeypatch):
    """A station directory: the legacy manifest, plus its pipelines.d/."""
    monkeypatch.setenv("SIGMOND_SQLITE_PATH", str(tmp_path / "sink.db"))
    monkeypatch.setenv("HS_UPLOADER_STATE_DIR", str(tmp_path))
    etc = tmp_path / "etc"
    etc.mkdir()
    data = tmp_path / "data"
    for d in ("grape", "psk1", "psk2", "nd", "new", "x", "y"):
        (data / d).mkdir(parents=True)

    class S:
        manifest = etc / "pipelines.toml"
        pdir = etc / "pipelines.d"
        root = data

        def legacy(self, *entries, head=_IDENTITY):
            self.manifest.write_text(head + "".join(entries))

        def file(self, name, text):
            self.pdir.mkdir(exist_ok=True)
            (self.pdir / name).write_text(text)

        def build(self):
            layout = pipelines_d.load_layout(self.manifest)
            pipes, refused = daemon.build_layout(
                layout, watermark=SqliteWatermarkStore(":memory:"))
            return layout, pipes, refused

    return S()


def _legacy_std(s):
    """A B4-shaped legacy manifest: grape, two psk-recorder entries, and an
    ND-shaped entry with no ``client``."""
    s.legacy(
        _ft("grape-psws", s.root / "grape", client="grape"),
        _ft("psk-a", s.root / "psk1", client="psk-recorder", instrument="1"),
        _ft("psk-b", s.root / "psk2", client="psk-recorder", instrument="2"),
        _ft("nd-psk", s.root / "nd", instrument="3"),
    )


# ---- test 1: no pipelines.d ---------------------------------------------------


def test_no_dir_entries_equal_load_manifest(station):
    _legacy_std(station)
    layout, pipes, refused = station.build()
    want = daemon.load_manifest(station.manifest)["pipeline"]
    assert [e for e, _ in layout.entries] == want
    assert [o for _, o in layout.entries] == ["legacy"] * 4
    assert [e.get("client") for e, _ in layout.entries] == [
        "grape", "psk-recorder", "psk-recorder", None]
    assert layout.refused == [] and refused == []
    assert [p.name for p in pipes] == ["grape-psws", "psk-a", "psk-b", "nd-psk"]
    assert [p.client for p in pipes] == ["grape", "psk-recorder",
                                         "psk-recorder", ""]
    assert all(p.origin == "legacy" and p.governed for p in pipes)


def test_legacy_manifest_is_required(tmp_path):
    with pytest.raises(FileNotFoundError):
        pipelines_d.load_layout(tmp_path / "pipelines.toml")


# ---- test 2: precedence -------------------------------------------------------


def test_file_silences_its_clients_legacy_entries(station):
    _legacy_std(station)
    station.file("psk-recorder.toml", _ft("psk-new", station.root / "new",
                                          instrument="9"))
    layout, pipes, refused = station.build()
    assert refused == []
    assert sorted(p.name for p in pipes) == ["grape-psws", "nd-psk", "psk-new"]
    new = next(p for p in pipes if p.name == "psk-new")
    assert new.client == "psk-recorder"
    assert new.origin == "pipelines.d/psk-recorder.toml"


@pytest.mark.parametrize("broken", [
    "[[pipeline]\nname = ",                       # does not parse
    '[daemon]\npump_interval_sec = 5\n',          # breaks a rule
    "",                                           # empty: exists, declares none
])
def test_broken_file_still_silences_its_client(station, broken):
    _legacy_std(station)
    station.file("psk-recorder.toml", broken)
    _, pipes, _ = station.build()
    assert sorted(p.name for p in pipes) == ["grape-psws", "nd-psk"]


def test_entry_without_client_never_matches_a_file(station):
    # ND's legacy entries carry no client; a file named for any client,
    # even "nd", never matches them.
    _legacy_std(station)
    station.file("nd.toml", _ft("nd-new", station.root / "new", instrument="8"))
    _, pipes, _ = station.build()
    assert "nd-psk" in {p.name for p in pipes}


# ---- test 3: the directory comes from the manifest's path ---------------------


def test_dir_derives_from_manifest_parent():
    m = Path("/tmp/x/pipelines.toml")
    assert pipelines_d.pipelines_dir_for(m) == Path("/tmp/x/pipelines.d")
    assert pipelines_d.sinks_dir_for(m) == Path("/tmp/x/sinks")


def test_loader_reads_the_dir_beside_the_manifest(station):
    station.legacy(_ft("grape-psws", station.root / "grape", client="grape"))
    station.file("psk-recorder.toml", _ft("psk-new", station.root / "new",
                                          instrument="9"))
    layout = pipelines_d.load_layout(station.manifest)
    assert [o for _, o in layout.entries] == [
        "legacy", "pipelines.d/psk-recorder.toml"]


# ---- test 4: file rules ---------------------------------------------------------


def _refusal(refused, file):
    hits = [r for r in refused if r.file == file]
    assert len(hits) == 1, refused
    return hits[0]


def test_client_comes_from_the_file_name(station):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape"))
    _, pipes, refused = station.build()
    assert refused == []
    assert [(p.name, p.client) for p in pipes] == [("g", "grape")]


def test_matching_client_accepted_mismatched_refused(station):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape", client="grape"))
    station.file("psk.toml", _ft("p", station.root / "psk1", client="other",
                                 instrument="2"))
    _, pipes, refused = station.build()
    assert [p.name for p in pipes] == ["g"]
    r = _refusal(refused, "psk.toml")
    assert r.client == "psk" and "other" in r.reason


@pytest.mark.parametrize("fname", ["a@b.toml", "A.toml", "x.y.toml",
                                   "site.toml", "-x.toml"])
def test_bad_client_names_refused(station, fname):
    station.legacy()
    station.file(fname, _ft("p", station.root / "x"))
    _, pipes, refused = station.build()
    assert pipes == []
    assert "client name" in _refusal(refused, fname).reason


def test_path_like_name_is_not_a_client():
    assert pipelines_d.file_client("../x.toml") is None
    assert pipelines_d.file_client("a@b.toml") is None
    assert pipelines_d.file_client("psk-recorder.toml") == "psk-recorder"


def test_start_at_now_refused_in_a_file(station):
    station.legacy()
    station.file("wspr.toml", _wspr("w", station.root / "sink.db",
                                    start_at="now"))
    _, pipes, refused = station.build()
    assert pipes == []
    assert "start_at" in _refusal(refused, "wspr.toml").reason


def test_wspr_cycle_defaults_beginning_in_file_now_in_legacy(station):
    station.legacy(_wspr("legacy-w", station.root / "sink.db", client="a"))
    station.file("wspr.toml", _wspr("file-w", station.root / "sink2.db",
                                    server="gw2.wsprdaemon.org"))
    _, pipes, refused = station.build()
    assert refused == []
    got = {p.name: p.source.start_at for p in pipes}
    assert got == {"legacy-w": "now", "file-w": "beginning"}


@pytest.mark.parametrize("head", [
    "[daemon]\npump_interval_sec = 5\n",
    '[identity]\nssh_key_file = "/root/.ssh/id"\n',
])
def test_host_wide_settings_refuse_the_file(station, head):
    station.legacy()
    station.file("grape.toml", head + _ft("g", station.root / "grape"))
    _, pipes, refused = station.build()
    assert pipes == []
    _refusal(refused, "grape.toml")


def test_identity_instance_tables_load_with_one_info(station, caplog):
    station.legacy()
    station.file("grape.toml",
                 '[identity.rx1]\nreceiver = "AC0G_S1"\n'
                 '[identity.rx2]\nreceiver = "AC0G_S2"\n'
                 + _ft("g", station.root / "grape"))
    with caplog.at_level(logging.INFO, logger="hs_uploader.pipelines_d"):
        _, pipes, refused = station.build()
    assert refused == [] and [p.name for p in pipes] == ["g"]
    notes = [r for r in caplog.records if r.name == "hs_uploader.pipelines_d"
             and "identity" in r.getMessage()]
    assert len(notes) == 1 and notes[0].levelno == logging.INFO
    assert "grape.toml" in notes[0].getMessage()


@pytest.mark.parametrize("fname", ["psk-recorder.toml.new",
                                   ".psk-recorder.toml", "psk-recorder.toml~"])
def test_side_files_never_load(station, fname):
    _legacy_std(station)
    station.file(fname, _ft("psk-new", station.root / "new", instrument="9"))
    layout, pipes, refused = station.build()
    # Not loaded, not refused, and not "present": legacy psk stays.
    assert refused == []
    assert sorted(p.name for p in pipes) == [
        "grape-psws", "nd-psk", "psk-a", "psk-b"]


def test_governed_lands_on_the_pipeline(station):
    station.legacy()
    station.file("grape.toml",
                 _ft("g1", station.root / "grape", extra="governed = false\n")
                 + _ft("g2", station.root / "x", instrument="2"))
    _, pipes, _ = station.build()
    assert {p.name: p.governed for p in pipes} == {"g1": False, "g2": True}


def test_non_boolean_governed_refused(station):
    station.legacy()
    station.file("grape.toml",
                 _ft("g1", station.root / "grape", extra='governed = "no"\n'))
    _, pipes, refused = station.build()
    assert pipes == [] and "governed" in _refusal(refused, "grape.toml").reason


def test_file_takes_base_identity_plus_its_overrides(station):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape")
                 + '[pipeline.identity]\nstation_id = "S000999"\n')
    _, pipes, _ = station.build()
    ident = pipes[0].identity
    assert ident.call == "AC0G/S" and ident.station_id == "S000999"
    assert ident.ssh_key_file == "/etc/hs-uploader/keys/id_ed25519"


def test_second_entry_failing_to_build_sends_nothing(station):
    station.legacy(_ft("grape-psws", station.root / "grape", client="grape"))
    station.file("psk.toml",
                 _ft("p1", station.root / "psk1", instrument="1")
                 + _ft("p2", station.root / "psk2", instrument="2")
                 .replace('type = "psws_dataset"', 'type = "no-such"'))
    _, pipes, refused = station.build()
    assert [p.name for p in pipes] == ["grape-psws"]
    assert "p2" in _refusal(refused, "psk.toml").reason


# ---- test 5: collisions ---------------------------------------------------------


def test_file_repeating_a_legacy_key_is_refused_naming_both(station):
    _legacy_std(station)
    # same source + transport as grape-psws: the same key, another name
    station.file("other.toml", _ft("grape-copy", station.root / "grape"))
    _, pipes, refused = station.build()
    assert "grape-copy" not in {p.name for p in pipes}
    r = _refusal(refused, "other.toml")
    assert "grape-copy" in r.reason and "grape-psws" in r.reason
    assert "key" in r.reason


def test_file_repeating_a_legacy_name_is_refused_naming_both(station):
    _legacy_std(station)
    station.file("other.toml", _ft("grape-psws", station.root / "new",
                                   instrument="9")
                 + _ft("fine", station.root / "x", instrument="10"))
    _, pipes, refused = station.build()
    assert "fine" not in {p.name for p in pipes}       # refused whole
    r = _refusal(refused, "other.toml")
    assert r.reason.count("grape-psws") == 2
    assert "legacy" in r.reason and "pipelines.d/other.toml" in r.reason


def test_collision_inside_one_file_refuses_it(station):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape")
                 + _ft("g", station.root / "x", instrument="2"))
    _, pipes, refused = station.build()
    assert pipes == []
    _refusal(refused, "grape.toml")


def test_later_file_colliding_with_an_earlier_file_is_refused(station):
    station.legacy()
    station.file("aaa.toml", _ft("g", station.root / "grape"))
    station.file("bbb.toml", _ft("g", station.root / "x", instrument="2"))
    _, pipes, refused = station.build()
    assert [p.origin for p in pipes] == ["pipelines.d/aaa.toml"]
    _refusal(refused, "bbb.toml")


def test_legacy_collision_alone_logs_error_and_runs_both(station, caplog):
    station.legacy(
        _ft("grape-psws", station.root / "grape", client="grape"),
        _ft("grape-twin", station.root / "grape", client="grape"),
    )
    with caplog.at_level(logging.ERROR):
        _, pipes, refused = station.build()
    assert [p.name for p in pipes] == ["grape-psws", "grape-twin"]
    assert refused == []
    errs = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errs) == 1
    assert "grape-psws" in errs[0] and "grape-twin" in errs[0]


# ---- test 8 (start half): the daemon starts; pipeline_state records refusals -----


class _Spy:
    seen: list = []

    def __init__(self, pipelines, **kw):
        _Spy.seen = [p.name for p in pipelines]

    def pump_until_idle(self):
        return 1


def test_broken_file_at_start_silences_only_its_client(station, monkeypatch,
                                                       caplog):
    _legacy_std(station)
    station.file("psk-recorder.toml", "[[pipeline]\nname = ")
    station.file("other.toml", _ft("grape-copy", station.root / "grape"))
    monkeypatch.setattr(daemon, "Uploader", _Spy)
    with caplog.at_level(logging.INFO):
        assert daemon.run(station.manifest, once=True) == 0
    assert sorted(_Spy.seen) == ["grape-psws", "nd-psk"]

    rows = {r["name"]: r for r in
            SqliteWatermarkStore(station.root.parent / "watermarks.db")
            .pipeline_states()}
    assert set(rows) == {"grape-psws", "nd-psk", "pipelines.d/psk-recorder.toml",
                         "pipelines.d/other.toml"}
    broken = rows["pipelines.d/psk-recorder.toml"]
    assert broken["client"] == "psk-recorder" and broken["refused_reason"]
    collided = rows["pipelines.d/other.toml"]
    assert collided["client"] == "other"
    assert "grape-psws" in collided["refused_reason"]
    assert rows["grape-psws"]["refused_reason"] == ""

    # §3.6: one INFO line per pipeline, naming name, client and origin.
    info = [r.getMessage() for r in caplog.records
            if r.levelno == logging.INFO and r.name == "hs_uploader.daemon"
            and "origin" in r.getMessage()]
    assert len(info) == 2
    assert any("grape-psws" in m and "grape" in m and "legacy" in m
               for m in info)


def test_daemon_logs_origin_of_a_file_pipeline(station, monkeypatch, caplog):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape"))
    monkeypatch.setattr(daemon, "Uploader", _Spy)
    with caplog.at_level(logging.INFO):
        assert daemon.run(station.manifest, once=True) == 0
    assert _Spy.seen == ["g"]
    assert any("g" in r.getMessage() and "pipelines.d/grape.toml"
               in r.getMessage() and "client grape" in r.getMessage()
               for r in caplog.records)


# ---- the signature (ruling X5) ----------------------------------------------------


def test_signature_missing_dir_and_changes(station):
    sig0 = pipelines_d.pipelines_signature(station.pdir)
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape"))
    sig1 = pipelines_d.pipelines_signature(station.pdir)
    assert sig1 != sig0
    assert pipelines_d.pipelines_signature(station.pdir) == sig1
    station.file("grape.toml", _ft("g", station.root / "grape") + "\n")
    assert pipelines_d.pipelines_signature(station.pdir) != sig1


def test_load_layout_takes_signature_before_reading(station, monkeypatch):
    station.legacy()
    station.file("grape.toml", _ft("g", station.root / "grape"))
    order = []
    real_sig = pipelines_d.pipelines_signature
    real_read = pipelines_d._read_toml
    monkeypatch.setattr(pipelines_d, "pipelines_signature",
                        lambda d: order.append("sig") or real_sig(d))
    monkeypatch.setattr(pipelines_d, "_read_toml",
                        lambda p: order.append("read") or real_read(p))
    layout = pipelines_d.load_layout(station.manifest)
    assert order[0] == "sig" and "read" in order
    assert layout.signature == real_sig(station.pdir)


# ---- fix round 1 (rulings C15, FIFO, unhashable client, wspr_cycle start_at) ------


def test_broken_file_of_the_sole_client_runs_idle_and_records_why(
        station, monkeypatch, caplog):
    # C15: the only client's file breaks, so nothing builds.  The daemon must
    # neither exit (systemd would loop on it) nor skip the refusal row.
    station.legacy(_ft("psk-a", station.root / "psk1", client="psk-recorder"))
    station.file("psk-recorder.toml", "[[pipeline]\nname = ")
    monkeypatch.setattr(daemon, "Uploader", _Spy)
    _Spy.seen = None
    with caplog.at_level(logging.ERROR):
        assert daemon.run(station.manifest, once=True) == 0
    assert _Spy.seen == []
    rows = {r["name"]: r for r in
            SqliteWatermarkStore(station.root.parent / "watermarks.db")
            .pipeline_states()}
    assert set(rows) == {"pipelines.d/psk-recorder.toml"}
    assert rows["pipelines.d/psk-recorder.toml"]["refused_reason"]
    assert any("running idle" in r.getMessage() for r in caplog.records)


def test_empty_build_without_refusals_still_exits_1(station, monkeypatch):
    station.legacy()
    monkeypatch.setattr(daemon, "Uploader", _Spy)
    assert daemon.run(station.manifest, once=True) == 1


def test_fifo_named_toml_is_refused_not_opened(station):
    import os
    import threading
    station.legacy(_ft("grape-psws", station.root / "grape", client="grape"))
    station.pdir.mkdir()
    fifo = station.pdir / "psk.toml"
    os.mkfifo(fifo)
    out = {}
    t = threading.Thread(
        target=lambda: out.setdefault(
            "layout", pipelines_d.load_layout(station.manifest)),
        daemon=True)
    t.start()
    t.join(5)
    blocked = t.is_alive()
    if blocked:   # unblock the reader so the test run can end
        fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        os.close(fd)
        t.join(5)
    assert not blocked, "load_layout blocked on a FIFO"
    r = _refusal(out["layout"].refused, "psk.toml")
    assert "regular file" in r.reason


def test_unhashable_legacy_client_does_not_raise(station):
    station.legacy(_ft("odd", station.root / "grape",
                       extra='client = ["a", "b"]\n'))
    station.file("grape.toml", _ft("g", station.root / "x", instrument="2"))
    layout = pipelines_d.load_layout(station.manifest)
    assert [e["name"] for e, _ in layout.entries] == ["odd", "g"]


@pytest.mark.parametrize("value", ['"NOW"', '""', '"later"',
                                   "1979-05-27T07:32:00Z"])
def test_wspr_cycle_in_a_file_takes_only_beginning(station, value):
    station.legacy()
    station.file("wspr.toml",
                 _wspr("w", station.root / "sink.db")
                 .replace("[pipeline.source]",
                          f"[pipeline.source]\nstart_at = {value}"))
    _, pipes, refused = station.build()
    assert pipes == []
    assert "start_at" in _refusal(refused, "wspr.toml").reason


def test_wspr_cycle_beginning_accepted_in_a_file(station):
    station.legacy()
    station.file("wspr.toml", _wspr("w", station.root / "sink.db",
                                    start_at="beginning"))
    _, pipes, refused = station.build()
    assert refused == [] and pipes[0].source.start_at == "beginning"


def test_legacy_wspr_cycle_start_at_unchanged(station):
    station.legacy(_wspr("w", station.root / "sink.db", client="wspr-recorder",
                         start_at="later"))
    _, pipes, refused = station.build()
    assert refused == [] and pipes[0].source.start_at == "later"
