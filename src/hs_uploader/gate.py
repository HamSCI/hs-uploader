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
``marker_stands`` afresh.  An exception inside a decision holds the
pipeline and logs at ERROR once per pipeline and error (ruling 6).  The
only state the gate keeps, the record of what it has already logged, sits
behind one lock, since the pump runs pipelines on threads.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Hashable

from .pipelines_d import LEGACY
from .sinkswitch import (SITE, STATES, effective_state, marker_stands,
                         read_switch, valid_client_name)
from .transports.discard import DiscardTransport
from .transports.heartbeat_sftp import HeartbeatSftp

log = logging.getLogger(__name__)

SINK_GATE_ORIGIN = "pipelines.d/sigmond.toml"
HEARTBEAT_CLIENT = "sigmond"

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

    def __init__(self, sinks_dir: Path, marker: Path):
        self.sinks_dir = Path(sinks_dir)
        self.marker = Path(marker)
        self._lock = threading.Lock()
        self._said: set[Hashable] = set()

    def may_send(self, pipe) -> bool:
        return self.decide(pipe).send

    def decide(self, pipe) -> Decision:
        try:
            return self._decide(pipe)
        except Exception as e:  # fail closed (ruling 6)
            msg = f"{type(e).__name__}: {e}"
            if self._first(("fault", _key(pipe), msg)):
                log.error("pipeline %r held: the sink gate failed (%s)",
                          getattr(pipe, "name", "?"), msg)
            return Decision(False, 0, f"gate fault: {msg}")

    # -- the rules ---------------------------------------------------------

    def _decide(self, pipe) -> Decision:
        origin = pipe.origin
        if not pipe.governed:
            if origin == SINK_GATE_ORIGIN:
                return Decision(True, 1, "ungoverned")
            if self._first(("ungoverned", _key(pipe))):
                log.warning(
                    "pipeline %r (%s) says governed = false; only %s may "
                    "say that, so its switches judge it",
                    pipe.name, origin, SINK_GATE_ORIGIN)
        if origin == LEGACY and (pipe.client == HEARTBEAT_CLIENT
                                 or _is_heartbeat_transport(pipe.transport)):
            return Decision(True, 2, "legacy heartbeat")
        if not marker_stands(self.marker):
            return Decision(True, 3, "no carry-over marker")
        if not pipe.client:
            if self._first(("no-client", _key(pipe))):
                log.info("pipeline %r names no client; its manifest's "
                         "policy governs it", pipe.name)
            return Decision(True, 4, "no client")
        eff = effective_state(pipe.client, sinks_dir=self.sinks_dir)
        if eff.state == "upload":
            return Decision(True, 5, "sink uploading")
        return Decision(False, 5, self._why(pipe.client, eff))

    def _why(self, client: str, eff) -> str:
        """Name the switch that holds.  The decision is already made; this
        only words it, so a file that changes in between changes no answer."""
        if not valid_client_name(client):
            return "bad client name"
        if not self.sinks_dir.is_dir():
            return "no switch files (sinks/ missing)"
        c = read_switch(self.sinks_dir / f"{client}.toml")
        s = read_switch(self.sinks_dir / f"{SITE}.toml")
        if c.missing:
            return "no switch file (client)"
        if c.error:
            return "switch unreadable (client)"
        if s.error:
            return "switch unreadable (site)"
        who = "client"
        if s.state is not None and c.state is not None and (
                STATES.index(s.state) <= STATES.index(c.state)):
            who = "site"
        return f"sink {_VERB.get(eff.state, eff.state)} ({who})"

    def _first(self, key: Hashable) -> bool:
        """True the first time `key` is seen.  Thread-safe."""
        with self._lock:
            if key in self._said:
                return False
            self._said.add(key)
            return True


def _key(pipe) -> tuple:
    return (getattr(pipe, "origin", "?"), getattr(pipe, "name", "?"))
