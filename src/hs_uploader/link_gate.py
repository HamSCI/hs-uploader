"""LinkGate: hold every network pipeline while a scheduled link is down.

A station on a constrained link (the South Pole reaches the world only during
satellite passes) knows from a published schedule when it cannot talk.  Under
that schedule there is no point opening a connection: it times out, backs
off, logs, and spends a retry.  So outside every window the pipeline holds,
exactly as a sink ``hold`` does: its data stays, its cursor stays, and the
first pump after the window opens ships the backlog.

The schedule is not ours to fetch.  ``sigmond-link`` (sigmond) fetches it
during a window and writes it to ``DEFAULT_PATH``::

    {"version": 1,
     "fetched_at": "2026-11-12T01:08:00+00:00",
     "horizon": ["2026-11-05T00:00:00+00:00", "2026-11-20T00:00:00+00:00"],
     "windows": [{"start": "...", "end": "...", "sats": ["TDRS-6"]}, ...]}

Trust the schedule's "no", verify its "yes" (plan-constrained-link-delivery):

* inside a window, send, and let the transport's own outcome handling judge
  the link;
* outside every window but inside the horizon, hold;
* everything else sends: no file (the feature is off), a file that does not
  parse, ``now`` outside the horizon, or a schedule fetched longer ago than
  ``max_age_s``.  A stale file must never silence a station.

The gate never raises: a fault reads as "send".  It logs one line per
transition of the link (not per pipeline) and one WARNING per distinct
reason it fails open.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from .transports.discard import DiscardTransport

log = logging.getLogger(__name__)

DEFAULT_PATH = Path("/var/lib/sigmond/link-schedule.json")
DEFAULT_MAX_AGE_S = 8 * 86400.0   # a weekly schedule plus a day of slack


def _ts(s: str) -> dt.datetime:
    t = dt.datetime.fromisoformat(s)
    if t.tzinfo is None:
        raise ValueError(f"timestamp without a timezone: {s!r}")
    return t


class LinkState:
    """What the schedule says about one instant."""

    __slots__ = ("up", "scheduled", "reason", "until")

    def __init__(self, up: bool, scheduled: bool, reason: str,
                 until: Optional[dt.datetime] = None):
        self.up = up                # may we send?
        self.scheduled = scheduled  # did a valid schedule decide it?
        self.reason = reason
        self.until = until          # when the answer next changes, if known

    def __repr__(self) -> str:
        return f"LinkState(up={self.up}, {self.reason!r}, until={self.until})"


def evaluate(doc: dict, now: dt.datetime, *,
             max_age_s: float = DEFAULT_MAX_AGE_S) -> LinkState:
    """Decide from a parsed schedule document.  Raises on a malformed one."""
    fetched = _ts(doc["fetched_at"])
    if (now - fetched).total_seconds() > max_age_s:
        return LinkState(True, False, f"schedule stale (fetched {fetched:%Y-%m-%d %H:%MZ})")
    h0, h1 = (_ts(x) for x in doc["horizon"])
    if not (h0 <= now < h1):
        return LinkState(True, False, "no schedule covers now")
    wins = sorted((_ts(w["start"]), _ts(w["end"])) for w in doc["windows"])
    for s, e in wins:
        if s <= now < e:
            return LinkState(True, True, "in a scheduled window", e)
        if s > now:
            return LinkState(False, True, "between scheduled windows", s)
    return LinkState(False, True, "after the last scheduled window", h1)


def _network_free(transport) -> bool:
    """A transport that never touches the network needs no link."""
    return isinstance(transport, DiscardTransport)


class LinkGate:
    """``may_send(pipe) -> bool``; the daemon hangs it on ``SinkGate.link``."""

    def __init__(self, path: Path = DEFAULT_PATH, *,
                 max_age_s: float = DEFAULT_MAX_AGE_S,
                 now_fn: Optional[Callable[[], dt.datetime]] = None,
                 recheck_s: float = 10.0):
        self.path = Path(path)
        self.max_age_s = max_age_s
        self._now = now_fn or (lambda: dt.datetime.now(dt.timezone.utc))
        self._recheck_s = recheck_s
        self._lock = threading.Lock()
        self._sig: Optional[tuple] = None
        self._doc: Optional[dict] = None
        self._doc_err: Optional[str] = None
        self._checked = -1e18
        self._last_up: Optional[bool] = None
        self._said: set[str] = set()

    # -- the file ------------------------------------------------------------

    def _load(self) -> None:
        """Reread the file when its signature changes (at most every recheck_s)."""
        mono = time.monotonic()
        if mono - self._checked < self._recheck_s:
            return
        self._checked = mono
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            self._sig, self._doc, self._doc_err = None, None, None
            return
        sig = (st.st_mtime_ns, st.st_size, st.st_ino)
        if sig == self._sig:
            return
        self._sig = sig
        try:
            with open(self.path) as f:
                doc = json.load(f)
            evaluate(doc, self._now(), max_age_s=self.max_age_s)  # validates
            self._doc, self._doc_err = doc, None
        except Exception as e:  # noqa: BLE001 — any bad file fails open
            self._doc, self._doc_err = None, f"{type(e).__name__}: {e}"

    # -- the decision --------------------------------------------------------

    def state(self) -> Optional[LinkState]:
        """None when no schedule applies (no file): the feature is off."""
        with self._lock:
            try:
                self._load()
                if self._doc_err:
                    self._once(f"bad:{self._doc_err}",
                               "link schedule %s unreadable (%s); sending anyway",
                               self.path, self._doc_err)
                    return LinkState(True, False, "schedule unreadable")
                if self._doc is None:
                    return None
                st = evaluate(self._doc, self._now(), max_age_s=self.max_age_s)
            except Exception as e:  # noqa: BLE001 — never hold on a fault
                self._once(f"fault:{type(e).__name__}",
                           "link gate fault (%s: %s); sending anyway",
                           type(e).__name__, e)
                return LinkState(True, False, "gate fault")
            if not st.scheduled:
                self._once(f"open:{st.reason}", "link schedule: %s; sending anyway",
                           st.reason)
            self._transition(st)
            return st

    def may_send(self, pipe) -> bool:
        if _network_free(getattr(pipe, "transport", None)):
            return True
        st = self.state()
        return True if st is None else st.up

    def _transition(self, st: LinkState) -> None:
        if st.up == self._last_up:
            return
        first = self._last_up is None
        self._last_up = st.up
        until = f" until {st.until:%Y-%m-%d %H:%MZ}" if st.until else ""
        if not st.up:
            log.info("link DOWN by schedule (%s)%s: network pipelines hold, "
                     "no connection attempts", st.reason, until)
        elif not first:
            log.info("link UP by schedule (%s)%s: pipelines send", st.reason, until)

    def _once(self, key: str, msg: str, *args) -> None:
        if key in self._said or len(self._said) > 256:
            return
        self._said.add(key)
        log.warning(msg, *args)

