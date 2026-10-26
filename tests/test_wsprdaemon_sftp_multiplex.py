"""The wsprdaemon tar transport holds ONE ssh connection up per gateway.

Every sftp this transport runs is otherwise a full TCP connect + key exchange
+ auth + teardown.  A station shipping a WSPR cycle every 2 minutes pays about
1,296 of those a day; across 92 gateway accounts that is ~119,000 handshakes a
day arriving at gw1, which is exactly what `MaxStartups 10:30:100` throttles.
ControlMaster takes it to ~10 reconnects per station per day.

⚠ A COST WIN, NOT A LATENCY WIN.  The gateway reflector holds every arriving
file for `min_age_seconds: 10` on a `scan_interval: 5` loop; that floor is
untouched here.

Three properties are worth a test, and each has cost a real outage somewhere:

  * The ControlPath MUST vary with the server.  Without `%h` the master
    opened to gw1 is reused when the transport fails over to gw2, and every
    tar is then delivered to the gateway that was already failing -- while
    reporting success, because the put succeeds.
  * A host-key change must force a FRESH connection.  Reusing a master that
    was authenticated against the key we just decided is stale carries the
    tar over the very connection whose identity is in question.
  * An unusable control directory must NOT stop the upload.  The multiplexing
    is an optimisation; a transport that refused to ship because it could not
    open a socket would trade a cost win for an outage.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest

from hs_uploader import StationIdentity
from hs_uploader.transports import wsprdaemon as wd


def _ident(key="/etc/hs-uploader/keys/id_ed25519"):
    return StationIdentity(call="AC0G/B1", grid="EM38ww", ssh_key_file=key)


def _transport(tmp_path, **kw):
    kw.setdefault("ssh_control_dir", tmp_path)
    return wd.WsprdaemonTarSftp(servers=["gw1.example.org", "gw2.example.org"],
                                sftp_user="AC0G_B4", **kw)


def _opts(cmd):
    """The -o values of an sftp argv, as a dict."""
    out = {}
    for flag, val in zip(cmd, cmd[1:]):
        if flag == "-o" and "=" in val:
            k, v = val.split("=", 1)
            out[k] = v
    return out


class TestControlPath:

    def test_the_control_path_varies_per_server(self, tmp_path):
        """⛔ The regression this guards: a ControlPath without %h makes the
        gw1 master serve gw2's transfers, so failover silently keeps
        delivering to the gateway that was already failing."""
        t = _transport(tmp_path)
        assert t._ssh_control_path is not None
        assert "%h" in t._ssh_control_path, "the path must vary with the host"
        assert "%r" in t._ssh_control_path, "and with the remote user"
        assert "%p" in t._ssh_control_path, "and with the port"

    def test_a_path_that_would_overrun_the_unix_socket_cap_is_declined(self, tmp_path):
        """A unix socket path is capped near 108 bytes and ssh refuses a
        longer one.  Declining is correct; silently emitting a path ssh will
        reject would break every upload on that host."""
        deep = tmp_path / ("x" * 90)
        assert wd._ssh_control_path(deep, ["gw1.example.org"], "AC0G_B4") is None

    def test_no_control_dir_means_no_control_path(self):
        assert wd._ssh_control_path(None, ["gw1.example.org"], "u") is None

    def test_the_expansion_is_measured_against_the_REAL_hosts(self, tmp_path):
        """A path that fits a short gateway name and not a long one must be
        declined for the long one.  Guessing a host length either declines
        paths that would have worked or emits one ssh will reject."""
        base = tmp_path / "d"
        short = wd._ssh_control_path(base, ["gw1"], "u")
        long = wd._ssh_control_path(base, ["g" * 200], "u")
        assert short is not None and long is None


class TestTheArgv:

    def _argv(self, tmp_path, **kw):
        t = _transport(tmp_path, **kw)
        with mock.patch.object(wd.subprocess, "run") as run:
            run.return_value = mock.Mock(returncode=0, stdout=b"", stderr=b"")
            t._run_sftp("gw1.example.org", b"put x\n", _ident())
        return run.call_args.args[0]

    def test_multiplexing_options_are_on_the_sftp_command(self, tmp_path):
        opts = _opts(self._argv(tmp_path))
        assert opts["ControlMaster"] == "auto"
        assert opts["ControlPath"].startswith(str(tmp_path))
        assert opts["ControlPersist"] == wd.SSH_CONTROL_PERSIST

    def test_the_EMITTED_control_path_still_carries_the_tokens(self, tmp_path):
        """⛔ Checking the attribute is not enough: what ssh sees is the -o
        value on the argv.  Without %h there, the gw1 master serves gw2's
        transfers and failover delivers to the failing gateway while
        reporting success."""
        opts = _opts(self._argv(tmp_path))
        assert "%h" in opts["ControlPath"]
        assert "%r" in opts["ControlPath"]
        assert "%p" in opts["ControlPath"]

    def test_keepalives_bound_a_HUNG_master(self, tmp_path):
        """ControlMaster=auto already falls back when the socket is DEAD.
        The case worth guarding is a master whose peer went away silently:
        without keepalives every transfer queues behind it until the transfer
        timeout."""
        opts = _opts(self._argv(tmp_path))
        assert opts["ServerAliveInterval"] == wd.SSH_SERVER_ALIVE_INTERVAL
        assert opts["ServerAliveCountMax"] == wd.SSH_SERVER_ALIVE_COUNT_MAX

    def test_the_identity_and_known_hosts_options_still_go_through(self, tmp_path):
        """Mutation guard: the options this change inserts must not displace
        the ones that were already load-bearing."""
        cmd = self._argv(tmp_path)
        opts = _opts(cmd)
        assert opts["BatchMode"] == "yes"
        assert opts["StrictHostKeyChecking"] == "accept-new"
        assert "-i" in cmd and _ident().ssh_key_file in cmd
        assert cmd[-1] == "AC0G_B4@gw1.example.org"

    def test_opting_out_removes_every_multiplexing_option(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HS_UPLOADER_SSH_MULTIPLEX", "0")
        t = wd.WsprdaemonTarSftp(servers=["gw1.example.org"], sftp_user="AC0G_B4")
        assert t._ssh_control_path is None
        with mock.patch.object(wd.subprocess, "run") as run:
            run.return_value = mock.Mock(returncode=0, stdout=b"", stderr=b"")
            t._run_sftp("gw1.example.org", b"put x\n", _ident())
        opts = _opts(run.call_args.args[0])
        assert "ControlMaster" not in opts
        assert "ControlPath" not in opts


class TestControlDirResolution:

    def test_a_control_dir_that_cannot_be_CREATED_disables_multiplexing(
            self, tmp_path, monkeypatch):
        """⛔ The upload must survive a directory it cannot use.  Returning
        None here is what keeps a missing tmpfiles.d entry from becoming a
        fleet-wide upload outage."""
        if os.geteuid() == 0:
            pytest.skip("root ignores the mode bits")
        blocked = tmp_path / "blocked"
        blocked.mkdir(mode=0o500)
        monkeypatch.setenv("HS_UPLOADER_SSH_CONTROL_DIR", str(blocked / "sub"))
        assert wd._ssh_control_dir() is None

    def test_an_EXISTING_but_unwritable_control_dir_is_declined_too(
            self, tmp_path, monkeypatch):
        """The distinct failure: the directory is there, so mkdir succeeds
        (exist_ok), but this process cannot create a socket in it -- the
        ownership-drift case the tmpfiles.d `z` line exists to repair.  ssh
        would fail per transfer; declining gives plain connections instead."""
        if os.geteuid() == 0:
            pytest.skip("root ignores the mode bits")
        there = tmp_path / "there"
        there.mkdir(mode=0o555)
        monkeypatch.setenv("HS_UPLOADER_SSH_CONTROL_DIR", str(there))
        assert wd._ssh_control_dir() is None

    def test_a_writable_control_dir_is_used(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HS_UPLOADER_SSH_MULTIPLEX", raising=False)
        monkeypatch.setenv("HS_UPLOADER_SSH_CONTROL_DIR", str(tmp_path / "cm"))
        assert wd._ssh_control_dir() == tmp_path / "cm"

    def test_the_env_opt_out_is_honoured_before_any_mkdir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HS_UPLOADER_SSH_MULTIPLEX", "0")
        monkeypatch.setenv("HS_UPLOADER_SSH_CONTROL_DIR", str(tmp_path / "never"))
        assert wd._ssh_control_dir() is None
        assert not (tmp_path / "never").exists()


class TestHostKeyRecovery:

    def test_a_changed_host_key_retries_WITHOUT_the_master(self, tmp_path):
        """⛔ The master was authenticated against the key we have just
        decided is stale.  Carrying the tar over it would report success on
        the very connection whose identity is in question -- and would make
        clearing known_hosts mean nothing."""
        t = _transport(tmp_path)
        calls = []

        def fake_run(server, batch, identity, extra_opts=None, multiplex=True):
            calls.append(multiplex)
            if len(calls) == 1:
                return 255, "REMOTE HOST IDENTIFICATION HAS CHANGED!"
            return 0, "ok"

        with mock.patch.object(t, "_run_sftp", side_effect=fake_run), \
             mock.patch.object(t, "_remove_host_key") as rm, \
             mock.patch.object(t, "_close_control_master") as close:
            ok, _ = t._sftp_put("gw1.example.org", Path("/tmp/x.tbz"),
                                "x.tbz", _ident())
        assert ok
        assert calls == [True, False], "the retry must not reuse the master"
        rm.assert_called_once()
        close.assert_called_once()

    def test_closing_the_master_is_a_no_op_when_nothing_is_multiplexed(self, tmp_path):
        t = _transport(tmp_path)
        t._ssh_control_path = None
        with mock.patch.object(wd.subprocess, "run") as run:
            t._close_control_master("gw1.example.org", _ident())
        run.assert_not_called()

    def test_closing_the_master_names_the_same_path_and_target(self, tmp_path):
        t = _transport(tmp_path)
        with mock.patch.object(wd.subprocess, "run") as run:
            t._close_control_master("gw2.example.org", _ident())
        cmd = run.call_args.args[0]
        assert cmd[:3] == ["ssh", "-O", "exit"]
        assert f"ControlPath={t._ssh_control_path}" in cmd
        assert cmd[-1] == "AC0G_B4@gw2.example.org"
