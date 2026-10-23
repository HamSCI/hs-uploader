"""SinkGate: may a pipeline send?  (spec sink-2b-part2 §3.1)

The gate answers in five rules, in order; the first that applies decides.

1. ``governed = false`` from ``pipelines.d/sigmond.toml``: send.  Anywhere
   else the gate ignores the flag, logs one WARNING per pipeline, and lets
   rules 2-5 judge it (plan ruling 2).
2. The legacy heartbeat (origin ``legacy`` only, ruling 3): client
   ``sigmond`` or a ``heartbeat_sftp`` transport: send.
3. Nothing stands at the marker path: send (part 1 behaviour).
4. The pipeline names no client: send; one INFO line per pipeline.
5. Send when ``effective_state(client)`` reads ``upload``; hold otherwise.
   ``effective_state`` holds the read rules: a missing client file, a broken
   file, an unknown state and a missing ``sinks/`` all read ``off``.

The gate keeps no cache of the marker (ruling 4): each decision asks
``marker_stands`` afresh.

What it logs (spec §9.2, plan ruling 6):

* An order logs once, at its transition: "pipeline X held: sink filling
  (site)" when a pipeline stops sending, "pipeline X sending: sink
  uploading" when it starts again.  A pipeline held at its first decision
  logs its held line then, so the daemon's start names each held pipeline.
  A pipeline held for a week logs nothing more.
* A fault logs at ERROR once, then a reminder at most every
  ``REMIND_S`` seconds while it lasts.  Faults: an exception inside a
  decision (keyed by pipeline and exception type, never by its text;
  a clean decision marks it cleared, and a recurrence logs as a first
  sight only once 6 h have passed since its last line; a gate fault logs
  no transition line), a
  broken or unreadable switch file (``sinkswitch`` logs its first sight,
  once per file signature; the gate adds the reminders), and ``sinks/``
  missing while the marker stands.  A pipeline with a bad client name
  judges only its own fault: ``effective_state`` reads neither
  ``site.toml`` nor ``sinks/`` for it.

The daemon keeps ONE gate for its life, across rebuilds, so a pipeline the
rebuild kept keeps its last decision and the rule 4 line appears once per
pipeline per daemon.  All the gate's state (what it has logged, each
pipeline's last decision, each fault's last line) sits behind one lock,
since the pump runs pipelines on threads, and every table is capped at
``SAID_CAP`` keys.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Hashable, Optional

from .pipelines_d import LEGACY
from .sinkswitch import (STATES, effective_state, marker_stands,
                         valid_client_name)
from .transports.discard import DiscardTransport
from .transports.heartbeat_sftp import HeartbeatSftp

log = logging.getLogger(__name__)

SINK_GATE_ORIGIN = "pipelines.d/sigmond.toml"
HEARTBEAT_CLIENT = "sigmond"

SAID_CAP = 1024  # bound on the gate's set of logged keys
REMIND_S = 6 * 3600.0  # a fault that lasts logs a reminder this often

_VERB = {"off": "off", "fill": "filling", "upload": "uploading"}


@dataclass(frozen=True)
class Decision:
    send: bool
    rule: int  # 1-5; 0 for a fault inside the gate
    reason: str


def _is_heartbeat_transport(transport) -> bool:
    while isinstance(transport, DiscardTransport):
        transport = transport.inner
    return isinstance(transport, HeartbeatSftp)


class SinkGate:
    """The daemon's ``gate`` callable: ``may_send(pipe) -> bool``."""

    def __init__(self, sinks_dir: Path, marker: Path, *,
                 clock: Optional[Callable[[], float]] = None):
        self.sinks_dir = Path(sinks_dir)
        self.marker = Path(marker)
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._said: set[Hashable] = set()
        self._capped = False
        # Each pipeline's last decision (send or not), for transition lines.
        self._last: dict[tuple, bool] = {}
        # When each fault in ``_said`` last logged.  A key enters here only
        # once ``_said`` admitted it, so the cap on ``_said`` bounds it too.
        self._fault_at: dict[Hashable, float] = {}
        # Each pipeline's gate-exception fault keys, so a clean decision
        # clears them and a new outbreak logs as a first sight.  Holds only
        # keys ``_said`` admitted.
        self._exc_keys: dict[tuple, set] = {}
        # Exception fault keys a clean decision cleared.  Each keeps its
        # time in ``_fault_at``: the 6 h clock governs a recurrence.
        self._cleared: set[Hashable] = set()
        self._last_capped = False

    def may_send(self, pipe) -> bool:
        return self.decide(pipe).send

    def decide(self, pipe) -> Decision:
        try:
            d = self._decide(pipe)
            self._clear_exceptions(pipe)
        except Exception as e:  # fail closed (ruling 6)
            msg = f"{type(e).__name__}: {e}"
            # Key by type, never by free text: a message that changes each
            # call must not log each call (ruling 6).
            fkey = ("fault", _key(pipe), type(e).__name__)
            say = self._recur(fkey)
            if say is None:
                say = self._fault(fkey, first_logs=True)
            with self._lock:
                if fkey in self._fault_at:
                    self._exc_keys.setdefault(_key(pipe), set()).add(fkey)
            if say:
                log.error("pipeline %s held: the sink gate failed (%s)%s",
                          getattr(pipe, "name", "?"), msg,
                          "" if say == "first" else " (reminder: it persists)")
            d = Decision(False, 0, f"gate fault: {msg}")
        self._transition(pipe, d)
        return d

    def _transition(self, pipe, d: Decision) -> None:
        """Log a pipeline's change between sending and held, once each.

        Its first decision logs only when it holds.  Any failure here
        leaves the decision alone (a log line never changes a send).
        """
        if d.rule == 0:
            # A gate fault: its ERROR line and reminders report it.  It
            # neither logs a transition nor counts as the last decision,
            # so the next one is judged against the last real decision.
            return
        try:
            k = _key(pipe)
            warn = False
            with self._lock:
                prev = self._last.get(k)
                if prev is None and len(self._last) >= SAID_CAP:
                    # past the cap: decide, but log no transition
                    warn, self._last_capped = not self._last_capped, True
                else:
                    self._last[k] = d.send
            if warn:
                log.warning("sink gate: %d pipelines tracked; further "
                            "pipelines will log no transition lines",
                            SAID_CAP)
            if warn or (prev is None and k not in self._last):
                return
            if prev == d.send or (prev is None and d.send):
                return
            log.info("pipeline %s %s: %s", getattr(pipe, "name", "?"),
                     "sending" if d.send else "held", d.reason)
        except Exception:  # noqa: BLE001
            pass

    def _clear_exceptions(self, pipe) -> None:
        """A clean decision marks the pipeline's exception faults cleared.

        Each keeps its last-logged time: a recurrence within REMIND_S of
        that line stays silent; a later one logs as a first sight.
        """
        with self._lock:
            keys = self._exc_keys.pop(_key(pipe), None)
            self._cleared.update(keys or ())

    def _recur(self, fkey: Hashable) -> Optional[str]:
        """For a cleared exception fault that recurs: "first" when REMIND_S
        has passed since its last line, else "" (silent).  None when the key
        is not a cleared fault."""
        now = self._clock()
        with self._lock:
            if fkey not in self._cleared:
                return None
            self._cleared.discard(fkey)
            if now - self._fault_at.get(fkey, now - REMIND_S) < REMIND_S:
                return ""
            self._fault_at[fkey] = now
            return "first"

    def forget(self, pipes) -> None:
        """Drop the per-pipeline state of every pipeline not in ``pipes``.

        The daemon calls it after a rebuild changed the running set, so
        removed pipelines leave ``_last`` and their exception faults.  The
        once-per-daemon lines (rule 4, ruling 2) stay said.
        """
        keep = {_key(p) for p in pipes}
        with self._lock:
            for k in [k for k in self._last if k not in keep]:
                del self._last[k]
            for k in [k for k in self._exc_keys if k not in keep]:
                for fkey in self._exc_keys.pop(k):
                    self._fault_at.pop(fkey, None)
                    self._said.discard(fkey)
            for fkey in [f for f in self._cleared if f[1] not in keep]:
                self._cleared.discard(fkey)
                self._fault_at.pop(fkey, None)
                self._said.discard(fkey)

    # -- the rules ---------------------------------------------------------

    def _decide(self, pipe) -> Decision:
        origin = pipe.origin
        exempt = exempt_rule(pipe)
        if exempt == 1:
            return Decision(True, 1, "ungoverned")
        if not pipe.governed and self._first(("ungoverned", _key(pipe))):
            log.warning(
                "pipeline %r (%s) says governed = false; only %s may "
                "say that, so its switches judge it",
                pipe.name, origin, SINK_GATE_ORIGIN)
        if exempt == 2:
            return Decision(True, 2, "legacy heartbeat")
        if not marker_stands(self.marker):
            return Decision(True, 3, "no carry-over marker")
        if not pipe.client:
            if self._first(("no-client", _key(pipe))):
                log.info("pipeline %s names no client; its manifest's "
                         "policy governs it", pipe.name)
            return Decision(True, 4, "no client")
        eff = effective_state(pipe.client, sinks_dir=self.sinks_dir)
        self._file_faults(pipe.client, eff)
        if eff.state == "upload":
            return Decision(True, 5, "sink uploading")
        return Decision(False, 5, self._why(pipe.client, eff))

    @staticmethod
    def _why(client: str, eff) -> str:
        """Word the hold from the same `Effective` that decided it; no second read.

        A missing ``sinks/`` reads as a missing client file.
        """
        if not valid_client_name(client):
            return "bad client name"
        if eff.client_missing:
            return "no switch file (client)"
        if eff.client_error:
            return "switch unreadable (client)"
        if eff.site_error:
            return "switch unreadable (site)"
        return f"sink {_VERB.get(eff.state, eff.state)} ({_order_by(eff)})"

    def _file_faults(self, client: str, eff) -> None:
        """Remind of a switch-file fault every REMIND_S while it lasts.

        ``sinkswitch`` logs each broken file's first sight, once per file
        signature, so the gate's first sight stays silent and only the
        reminders come from here.  A fault that clears forgets its clock,
        so its return logs as a first sight again (from ``sinkswitch``).
        ``sinks/`` missing while the marker stands logs here, first sight
        included: nothing else reports it.
        """
        faults = [(("fault", "client", client), eff.client_error, False)]
        # A bad client name returns from effective_state before it reads
        # site.toml or looks for sinks/: it says nothing about either, so
        # it must neither raise nor clear their shared faults.
        if valid_client_name(client):
            faults += [
                (("fault", "site"), eff.site_error, False),
                (("fault", "no-sinks"),
                 eff.client_missing and not self.sinks_dir.is_dir(), True),
            ]
        for key, err, first_logs in faults:
            if not err:
                self._clear(key)
                continue
            say = self._fault(key, first_logs=first_logs)
            if not say:
                continue
            tail = "" if say == "first" else " (reminder: it persists)"
            if key[1] == "no-sinks":
                log.error("sink switches: %s is missing while the carry-over "
                          "marker stands; every governed pipeline holds%s",
                          self.sinks_dir, tail)
            else:
                log.error("sink switch: %s; the pipelines it governs hold%s",
                          err, tail)

    def _fault(self, key: Hashable, *, first_logs: bool) -> str:
        """"first", "remind" or "" (say nothing).  Thread-safe.

        The first sight of ``key`` enters the capped ``_said`` set; it
        returns "first" when ``first_logs``, else "" (another module logged
        it).  Later sightings return "remind" once ``REMIND_S`` has passed
        since the last line, else "".
        """
        now = self._clock()
        with self._lock:
            if key in self._fault_at:
                if now - self._fault_at[key] < REMIND_S:
                    return ""
                self._fault_at[key] = now
                return "remind"
        if not self._first(key):
            return ""
        with self._lock:
            self._fault_at[key] = now
        return "first" if first_logs else ""

    def _clear(self, key: Hashable) -> None:
        with self._lock:
            if key in self._fault_at:
                del self._fault_at[key]
                self._said.discard(key)

    def _first(self, key: Hashable) -> bool:
        """True the first time `key` is seen.  Thread-safe."""
        with self._lock:
            if key in self._said:
                return False
            if len(self._said) >= SAID_CAP:
                if not self._capped:
                    self._capped = True
                    log.warning("sink gate: %d lines already logged once; "
                                "further first-time lines will not be logged",
                                SAID_CAP)
                return False
            self._said.add(key)
            return True


def _order_by(eff) -> str:
    """Which switch's order sets ``eff``: "site" when the site file reads as
    strict as the client's or stricter, else "client"."""
    if eff.site_state is not None and eff.client_state is not None and (
            STATES.index(eff.site_state) <= STATES.index(eff.client_state)):
        return "site"
    return "client"


def held_by(client: str, eff) -> Optional[str]:
    """Which switch holds ``client``: None while ``eff`` reads upload,
    "fault" for a bad name, a missing or unreadable file (the reads that
    fail closed), else "site" or "client", as the gate words its hold
    (``sink status``, spec §7.5, §9.2)."""
    if eff.state == "upload":
        return None
    if (not valid_client_name(client) or eff.client_missing
            or eff.client_error or eff.site_error):
        return "fault"
    return _order_by(eff)


def exempt_rule(pipe) -> int:
    """1 or 2 when rule 1 or rule 2 lets ``pipe`` send whatever the switches
    say; 0 when the switches govern it.  Logs nothing.  The starting-record
    pass (spec §3.5) asks this, so it and the gate agree on "governed"."""
    if not pipe.governed and pipe.origin == SINK_GATE_ORIGIN:
        return 1
    if pipe.origin == LEGACY and (pipe.client == HEARTBEAT_CLIENT
                                  or _is_heartbeat_transport(pipe.transport)):
        return 2
    return 0


def _key(pipe) -> tuple:
    return (getattr(pipe, "origin", "?"), getattr(pipe, "name", "?"))
