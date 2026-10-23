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
  decision (keyed by pipeline and exception type, never by its text), a
  broken or unreadable switch file (``sinkswitch`` logs its first sight,
  once per file signature; the gate adds the reminders), and ``sinks/``
  missing while the marker stands.

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

    def may_send(self, pipe) -> bool:
        return self.decide(pipe).send

    def decide(self, pipe) -> Decision:
        try:
            d = self._decide(pipe)
        except Exception as e:  # fail closed (ruling 6)
            msg = f"{type(e).__name__}: {e}"
            # Key by type, never by free text: a message that changes each
            # call must not log each call (ruling 6).
            say = self._fault(("fault", _key(pipe), type(e).__name__),
                              first_logs=True)
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
        try:
            k = _key(pipe)
            with self._lock:
                prev = self._last.get(k)
                if prev is None and len(self._last) >= SAID_CAP:
                    return  # past the cap: decide, but log no transition
                self._last[k] = d.send
            if prev == d.send or (prev is None and d.send):
                return
            log.info("pipeline %s %s: %s", getattr(pipe, "name", "?"),
                     "sending" if d.send else "held", d.reason)
        except Exception:  # noqa: BLE001
            pass

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
        who = "client"
        if eff.site_state is not None and eff.client_state is not None and (
                STATES.index(eff.site_state) <= STATES.index(eff.client_state)):
            who = "site"
        return f"sink {_VERB.get(eff.state, eff.state)} ({who})"

    def _file_faults(self, client: str, eff) -> None:
        """Remind of a switch-file fault every REMIND_S while it lasts.

        ``sinkswitch`` logs each broken file's first sight, once per file
        signature, so the gate's first sight stays silent and only the
        reminders come from here.  A fault that clears forgets its clock,
        so its return logs as a first sight again (from ``sinkswitch``).
        ``sinks/`` missing while the marker stands logs here, first sight
        included: nothing else reports it.
        """
        faults = (
            (("fault", "site"), eff.site_error, False),
            (("fault", "client", client), eff.client_error, False),
            (("fault", "no-sinks"),
             eff.client_missing and not self.sinks_dir.is_dir(), True),
        )
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
