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


def test_effective_state_refuses_bad_client_name(tmp_path):
    with pytest.raises(ValueError):
        ss.effective_state("../x", sinks_dir=tmp_path)
