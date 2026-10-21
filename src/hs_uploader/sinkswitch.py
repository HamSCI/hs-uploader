"""Sink switch files: reading and (below) writing.  Stdlib only.

A client's venv and the daemon load this same module.  The file shape and
the rules follow design §3.4 (tasks/plan-sink-control.md) and spec §6.1.
Nothing here enforces a switch; it is a library.
"""
from __future__ import annotations

import logging
import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional

log = logging.getLogger(__name__)

DEFAULT_SINKS_DIR = Path("/etc/hs-uploader/sinks")
STATES = ("off", "fill", "upload")  # strictness order, loosest last
CLIENT_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
SITE = "site"  # sinks/site.toml is the site switch, never a client


def valid_client_name(name: str) -> bool:
    """True for a legal client name.  `site` is reserved: it names the site file."""
    return (
        isinstance(name, str)
        and CLIENT_NAME_RE.fullmatch(name) is not None
        and name != SITE
    )


@dataclass
class SwitchRead:
    state: Optional[str]
    missing: bool
    error: str
    data: Mapping = field(default_factory=dict)  # parsed file; {} if missing or broken


@dataclass
class Effective:
    state: str
    client_state: Optional[str]
    site_state: Optional[str]
    errors: list[str]


# path -> (signature, SwitchRead); signatures already logged
_cache: dict[str, tuple[tuple, SwitchRead]] = {}
_logged: set[tuple] = set()


def _signature(st: os.stat_result) -> tuple:
    return (st.st_mtime_ns, st.st_size, st.st_ino)


def _parse(path: Path) -> SwitchRead:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return SwitchRead(None, True, "", {})
    except OSError as e:
        return SwitchRead(None, False, f"{path}: unreadable: {e}", {})
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
        return SwitchRead(None, False, f"{path}: parse error: {e}", {})
    state = data.get("state")
    if state not in STATES:
        return SwitchRead(
            None, False, f"{path}: state {state!r} is not one of {'/'.join(STATES)}", data
        )
    return SwitchRead(state, False, "", data)


def read_switch(path: Path, *, cache: bool = True) -> SwitchRead:
    """Parse one switch file.  A cached answer holds while (mtime, size, inode) hold.

    An error is logged once per file signature.  `cache=False` always re-parses
    (the writer uses it under the lock).
    """
    path = Path(path)
    key = str(path)
    try:
        sig = _signature(path.stat())
    except FileNotFoundError:
        _cache.pop(key, None)
        return SwitchRead(None, True, "", {})
    except OSError as e:
        return SwitchRead(None, False, f"{path}: unreadable: {e}", {})
    if cache:
        hit = _cache.get(key)
        if hit and hit[0] == sig:
            return hit[1]
    res = _parse(path)
    _cache[key] = (sig, res)
    if res.error and (key, sig) not in _logged:
        _logged.add((key, sig))
        log.error("sink switch: %s (reading as off)", res.error)
    return res


def effective_state(client: str, *, sinks_dir: Path = DEFAULT_SINKS_DIR) -> Effective:
    """The stricter of the client's file and site.toml (design §3.4)."""
    if not valid_client_name(client):
        raise ValueError(f"bad client name: {client!r}")
    sinks_dir = Path(sinks_dir)
    c = read_switch(sinks_dir / f"{client}.toml")
    s = read_switch(sinks_dir / f"{SITE}.toml")
    errors = [r.error for r in (c, s) if r.error]
    # A missing client file fails closed; a missing site file sets no limit;
    # any other failure on either file reads off.
    if c.missing or c.error or s.error:
        state = "off"
    elif s.missing:
        state = c.state
    else:
        state = STATES[min(STATES.index(c.state), STATES.index(s.state))]
    return Effective(state, c.state, s.state, errors)
