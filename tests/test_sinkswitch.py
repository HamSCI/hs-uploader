"""Switch-file library: reading (spec §6.1, test 17) and writing (test 18)."""
import itertools
import logging
import os
import stat
import tomllib

import pytest

from hs_uploader import sinkswitch as ss


def put(d, name, text):
    p = d / f"{name}.toml"
    p.write_text(text)
    return p


def st(state):
    return f'state = "{state}"\n'


def test_constants():
    assert ss.STATES == ("off", "fill", "upload")
    assert str(ss.DEFAULT_SINKS_DIR) == "/etc/hs-uploader/sinks"


def test_missing_client_reads_off(tmp_path):
    e = ss.effective_state("wspr-recorder", sinks_dir=tmp_path)
    assert e.state == "off" and e.client_state is None and e.errors == []


def test_missing_site_sets_no_limit(tmp_path):
    put(tmp_path, "c", st("upload"))
    e = ss.effective_state("c", sinks_dir=tmp_path)
    assert e.state == "upload" and e.site_state is None


def test_parse_error_reads_off_and_reports(tmp_path):
    put(tmp_path, "c", "state = [\n")
    e = ss.effective_state("c", sinks_dir=tmp_path)
    assert e.state == "off" and len(e.errors) == 1
    assert "c.toml" in e.errors[0]
    r = ss.read_switch(tmp_path / "c.toml")
    assert not r.missing and r.error and r.state is None and r.data == {}


def test_parse_error_is_not_missing(tmp_path):
    # a broken client file under a permissive site file must still read off
    put(tmp_path, "site", st("upload"))
    put(tmp_path, "c", "state = [\n")
    assert ss.effective_state("c", sinks_dir=tmp_path).state == "off"
    assert not ss.read_switch(tmp_path / "c.toml").missing
    assert ss.read_switch(tmp_path / "nope.toml").missing


def test_unknown_state_reads_off(tmp_path):
    put(tmp_path, "c", st("burst"))
    e = ss.effective_state("c", sinks_dir=tmp_path)
    assert e.state == "off" and e.errors


def test_bad_site_file_reads_off(tmp_path):
    put(tmp_path, "c", st("upload"))
    put(tmp_path, "site", st("bogus"))
    e = ss.effective_state("c", sinks_dir=tmp_path)
    assert e.state == "off" and e.errors


@pytest.mark.parametrize("c,s", list(itertools.product(ss.STATES, ss.STATES)))
def test_stricter_of_client_and_site(tmp_path, c, s):
    put(tmp_path, "c", st(c))
    put(tmp_path, "site", st(s))
    e = ss.effective_state("c", sinks_dir=tmp_path)
    expect = ss.STATES[min(ss.STATES.index(c), ss.STATES.index(s))]
    assert e.state == expect
    assert (e.client_state, e.site_state) == (c, s)


def test_rewrite_shows_at_next_call(tmp_path):
    p = put(tmp_path, "c", st("off"))
    assert ss.effective_state("c", sinks_dir=tmp_path).state == "off"
    # same size, same mtime: only the inode differs
    old = p.stat()
    q = tmp_path / "new"
    q.write_text(st("fill"))
    os.utime(q, ns=(old.st_atime_ns, old.st_mtime_ns))
    os.replace(q, p)
    assert ss.effective_state("c", sinks_dir=tmp_path).state == "fill"
    # new size
    p.write_text('state = "upload"\n')
    assert ss.effective_state("c", sinks_dir=tmp_path).state == "upload"
    # new mtime only
    st0 = p.stat()
    p.write_text(st("upload"))
    os.utime(p, ns=(st0.st_atime_ns, st0.st_mtime_ns + 5_000_000_000))
    assert ss.effective_state("c", sinks_dir=tmp_path).state == "upload"


def test_error_logs_once_per_signature(tmp_path, caplog):
    p = put(tmp_path, "c", "state = [\n")
    with caplog.at_level(logging.ERROR, logger="hs_uploader.sinkswitch"):
        for _ in range(5):
            ss.effective_state("c", sinks_dir=tmp_path)
        assert len([r for r in caplog.records if r.levelno >= logging.ERROR]) == 1
        p.write_text("state = [[\n")
        ss.effective_state("c", sinks_dir=tmp_path)
        ss.effective_state("c", sinks_dir=tmp_path)
        assert len([r for r in caplog.records if r.levelno >= logging.ERROR]) == 2


@pytest.mark.parametrize("n", ["../x", "a@b", "A", "a.b", "", "-a", "site"])
def test_invalid_client_names(n):
    assert not ss.valid_client_name(n)


@pytest.mark.parametrize("n", ["a", "wspr-recorder", "0a", "a-1"])
def test_valid_client_names(n):
    assert ss.valid_client_name(n)


def test_effective_state_bad_client_name_fails_closed(tmp_path):
    (tmp_path.parent / "x.toml").write_text(st("upload"))
    e = ss.effective_state("../x", sinks_dir=tmp_path)
    assert e.state == "off" and len(e.errors) == 1 and "../x" in e.errors[0]
    assert ss.effective_state("site", sinks_dir=tmp_path).state == "off"


# ---- writing (spec §6.1, test 18) ------------------------------------------

import multiprocessing
import time


def _writer(path, reason):
    ss.write_switch(
        path,
        {"state": "fill", "reason": reason, "set_by": reason, "set_at": "2026-10-06T21:30:00Z"},
    )


def test_race_keeps_both_history_entries(tmp_path):
    """Hold the lock, start both writers, release: forces the interleaving
    in which a writer that read before locking would lose the other's entry."""
    import fcntl

    path = tmp_path / "c.toml"
    lockf = open(tmp_path / ".lock", "a+")
    fcntl.flock(lockf, fcntl.LOCK_EX)
    ctx = multiprocessing.get_context("fork")
    procs = [ctx.Process(target=_writer, args=(path, r)) for r in ("one", "two")]
    for p in procs:
        p.start()
    time.sleep(0.7)  # both writers now sit at (or before) the lock
    fcntl.flock(lockf, fcntl.LOCK_UN)
    for p in procs:
        p.join(10)
        assert p.exitcode == 0
    hist = tomllib.loads(path.read_text())["history"]
    assert sorted(h["reason"] for h in hist) == ["one", "two"]


def test_reader_never_sees_half_a_file(tmp_path):
    path = tmp_path / "c.toml"
    ss.write_switch(path, {"state": "off", "reason": "x", "set_by": "t", "set_at": "t"})
    ctx = multiprocessing.get_context("fork")

    def burst():
        for i in range(150):
            ss.write_switch(
                path, {"state": ("fill", "upload")[i % 2], "reason": "r" * 200, "set_by": "t", "set_at": "t"}
            )

    p = ctx.Process(target=burst)
    p.start()
    reads = 0
    while p.is_alive():
        tomllib.loads(path.read_text())  # raises on a torn file
        reads += 1
    p.join()
    assert p.exitcode == 0 and reads > 0


def test_pinned_false_round_trips_as_bool(tmp_path):
    path = tmp_path / "c.toml"
    ss.write_switch(
        path,
        {"state": "fill", "reason": "r", "set_by": "t", "set_at": "t",
         "grant": {"working_bytes": 0, "pinned": False, "granted_by": "sigmond"}},
    )
    g = tomllib.loads(path.read_text())["grant"]
    assert g["pinned"] is False and g["working_bytes"] == 0


@pytest.mark.parametrize("bad", ["../site", "A.toml", "site.toml.bak", "x/../c.toml", "a.b.toml"])
def test_bad_path_raises(tmp_path, bad):
    with pytest.raises(ValueError):
        ss.write_switch(tmp_path / bad, {"state": "off"})
    assert not list(tmp_path.glob("*.toml"))


def test_site_file_is_writable_without_identity(tmp_path):
    ss.write_switch(tmp_path / "site.toml", {"state": "fill", "reason": "r", "set_by": "t", "set_at": "t"})
    assert ss.effective_state("c", sinks_dir=tmp_path).site_state == "fill"


def test_new_file_mode_is_0644(tmp_path):
    old = os.umask(0o077)
    try:
        ss.write_switch(tmp_path / "c.toml", {"state": "off"})
    finally:
        os.umask(old)
    assert stat.S_IMODE((tmp_path / "c.toml").stat().st_mode) == 0o644


def test_history_appends_with_from_to_and_identity(tmp_path):
    path = tmp_path / "c.toml"
    ident = {"AC0G=S": {"callsign": "AC0G/S", "lat": 38.92, "psws": {"station": "S1", "instrument": "1"}}}
    ss.write_switch(path, {"state": "fill", "reason": "a", "set_by": "x", "set_at": "t1", "identity": ident})
    ss.write_switch(path, {"state": "upload", "reason": "b", "set_by": "y", "set_at": "t2"})
    d = tomllib.loads(path.read_text())
    assert d["client"] == "c" and d["state"] == "upload"
    h = d["history"]
    assert [(e["from"], e["to"], e["by"]) for e in h] == [("off", "fill", "x"), ("fill", "upload", "y")]
    assert h[0]["identity"] == ident and "identity" not in h[1]
    assert ss.effective_state("c", sinks_dir=tmp_path).client_state == "upload"


def test_write_refuses_broken_file_and_writes_nothing(tmp_path):
    path = put(tmp_path, "c", "state = [\n")
    before = path.read_text()
    with pytest.raises(ValueError):
        ss.write_switch(path, {"state": "fill"})
    assert path.read_text() == before
    assert sorted(x.name for x in tmp_path.iterdir()) == ["c.toml", ".lock"] or \
        sorted(x.name for x in tmp_path.iterdir()) == [".lock", "c.toml"]


def test_write_rejects_unknown_state(tmp_path):
    with pytest.raises(ValueError):
        ss.write_switch(tmp_path / "c.toml", {"state": "burst"})


def test_dumps_toml_round_trips_an_empty_array():
    data = {"state": "off", "hold": [], "t": {"xs": []}}
    assert tomllib.loads(ss.dumps_toml(data)) == data


def test_dumps_toml_round_trips():
    data = {
        "s": 'a "q" \\ b\nc\té', "b": True, "n": -3, "f": 1.5,
        "t": {"k": "v", "odd key": 2},
        "arr": [{"x": 1, "sub": {"y": "z"}}, {"x": 2, "ident": {"A=B": {"lat": 0.25}}}],
    }
    assert tomllib.loads(ss.dumps_toml(data)) == data


@pytest.mark.parametrize("bad", [{"a": None}, {"a": [1, 2]}, {"a": (1,)}, {"a": b"x"}, {1: 2}])
def test_dumps_toml_rejects_other_types(bad):
    with pytest.raises(TypeError):
        ss.dumps_toml(bad)


# ---- fix round 1 -------------------------------------------------------------

@pytest.mark.parametrize("text", ["", "# only a comment\n"])
def test_empty_or_comment_only_file_can_be_rewritten(tmp_path, text):
    path = put(tmp_path, "c", text)
    ss.write_switch(path, {"state": "fill", "reason": "r", "set_by": "t", "set_at": "t"})
    assert ss.read_switch(path).state == "fill"


def test_refusal_says_to_move_file_aside(tmp_path):
    path = put(tmp_path, "c", "state = [\n")
    with pytest.raises(ValueError, match="move the file aside"):
        ss.write_switch(path, {"state": "fill"})


def test_lock_holder_that_never_releases_times_out(tmp_path, monkeypatch):
    import fcntl

    monkeypatch.setattr(ss, "LOCK_TIMEOUT_S", 0.3)
    lockf = open(tmp_path / ".lock", "a+")
    fcntl.flock(lockf, fcntl.LOCK_EX)
    t0 = time.monotonic()
    with pytest.raises(TimeoutError, match=r"\.lock"):
        ss.write_switch(tmp_path / "c.toml", {"state": "off"})
    assert time.monotonic() - t0 < 3
    assert not (tmp_path / "c.toml").exists()
    lockf.close()


def test_lock_file_mode_is_0600(tmp_path):
    old = os.umask(0o000)
    try:
        ss.write_switch(tmp_path / "c.toml", {"state": "off"})
    finally:
        os.umask(old)
    assert stat.S_IMODE((tmp_path / ".lock").stat().st_mode) == 0o600


def test_new_sinks_dir_is_0755_under_umask_077(tmp_path):
    old = os.umask(0o077)
    try:
        ss.write_switch(tmp_path / "sinks" / "c.toml", {"state": "off"})
    finally:
        os.umask(old)
    assert stat.S_IMODE((tmp_path / "sinks").stat().st_mode) == 0o755
