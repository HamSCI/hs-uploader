"""Sink switch files: reading and (below) writing.  Stdlib only.

A client's venv and the daemon load this same module.  The file shape and
the rules follow design §3.4 (tasks/plan-sink-control.md) and spec §6.1.
Nothing here enforces a switch; it is a library.
"""
from __future__ import annotations

import fcntl
import logging
import math
import os
import time
import re
import tomllib
import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional

log = logging.getLogger(__name__)

DEFAULT_SINKS_DIR = Path("/etc/hs-uploader/sinks")
STATES = ("off", "fill", "upload")  # strictness order, loosest last
CLIENT_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
LOCK_TIMEOUT_S = 10.0  # bound on waiting for sinks/.lock
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
    failed: bool = False  # True only for a read or parse failure (not a bad state)


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
        return SwitchRead(None, False, f"{path}: unreadable: {e}", {}, True)
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
        return SwitchRead(None, False, f"{path}: parse error: {e}", {}, True)
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
        return SwitchRead(None, False, f"{path}: unreadable: {e}", {}, True)
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
        # Fail closed.  Never form a path from the bad name.
        msg = f"bad client name {client!r}: reading as off"
        if ("name", repr(client)) not in _logged:
            _logged.add(("name", repr(client)))
            log.error("sink switch: %s", msg)
        return Effective("off", None, None, [msg])
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


# ---- writing ---------------------------------------------------------------

_BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")
_ESC = {"\\": "\\\\", '"': '\\"', "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}


def _key(k) -> str:
    if not isinstance(k, str) or not k:
        raise TypeError(f"TOML key must be a non-empty string, not {k!r}")
    return k if _BARE_KEY.match(k) else _str(k)


def _str(v: str) -> str:
    out = []
    for ch in v:
        if ch in _ESC:
            out.append(_ESC[ch])
        elif ord(ch) < 0x20 or ord(ch) == 0x7F:
            out.append("\\u%04x" % ord(ch))
        else:
            out.append(ch)
    return '"' + "".join(out) + '"'


def _scalar(v) -> str:
    if isinstance(v, bool):  # before int: bool is an int
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "nan"
        if math.isinf(v):
            return "inf" if v > 0 else "-inf"
        return repr(v)
    if isinstance(v, str):
        return _str(v)
    raise TypeError(f"cannot write {type(v).__name__} as a TOML value")


def _is_table_array(v) -> bool:
    return isinstance(v, list) and len(v) > 0 and all(isinstance(i, Mapping) for i in v)


def _emit(table: Mapping, path: list[str], out: list[str]) -> None:
    subs = []
    for k, v in table.items():
        if isinstance(v, list) and not v:
            out.append(f"{_key(k)} = []")
        elif isinstance(v, Mapping) or isinstance(v, list):
            subs.append((k, v))
        else:
            out.append(f"{_key(k)} = {_scalar(v)}")
    for k, v in subs:
        here = path + [_key(k)]
        name = ".".join(here)
        if isinstance(v, Mapping):
            out.append("")
            out.append(f"[{name}]")
            _emit(v, here, out)
        elif _is_table_array(v):
            for item in v:
                out.append("")
                out.append(f"[[{name}]]")
                _emit(item, here, out)
        else:
            raise TypeError(f"{k!r}: only arrays of tables are supported")


def dumps_toml(data: Mapping) -> str:
    """Emit strings, booleans, integers, floats, tables, empty arrays and arrays of tables.

    Anything else raises TypeError.  (sigmond's `_set_toml_field` quotes every
    value, so `pinned = false` would read back as a string.)
    """
    out: list[str] = []
    _emit(data, [], out)
    return "\n".join(out).lstrip("\n") + "\n"


def _check_path(path: Path) -> str:
    """Return the file's name (`site` or a client) or raise ValueError."""
    path = Path(path)
    if ".." in path.parts or path.suffix != ".toml":
        raise ValueError(f"not a switch file path: {path}")
    name = path.name[: -len(".toml")]
    if name != SITE and not valid_client_name(name):
        raise ValueError(f"not a switch file path: {path}")
    return name


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fsync_dir(d: Path) -> None:
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_switch(path: Path, change: Mapping) -> None:
    """Change one switch file under an exclusive lock, atomically (design §3.4).

    `change`: `state` (required), and optionally `reason`, `set_by`, `set_at`,
    `grant` (merged into the file's grant table), `identity` (tables written
    into this history entry).  Appends one `[[history]]` entry.  A file that
    fails to parse raises ValueError and nothing is written.
    """
    path = Path(path)
    name = _check_path(path)
    state = change.get("state")
    if state not in STATES:
        raise ValueError(f"state {state!r} is not one of {'/'.join(STATES)}")
    d = path.parent
    if not d.is_dir():
        d.mkdir(parents=True, exist_ok=True, mode=0o755)
        os.chmod(d, 0o755)  # the umask must not narrow it (design §3.4)
    lockpath = d / ".lock"
    lockfd = os.open(lockpath, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(lockfd, 0o600)  # the umask must not widen it
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        while True:
            try:
                fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"could not take the switch lock {lockpath} within "
                        f"{LOCK_TIMEOUT_S} s; nothing written"
                    ) from None
                time.sleep(0.05)
        cur = read_switch(path, cache=False)
        if cur.failed:
            raise ValueError(
                f"refusing to rewrite a broken switch file: {cur.error}; "
                "move the file aside to recreate it"
            )
        data = dict(cur.data)
        old = cur.state or "off"
        at = change.get("set_at") or _now()
        by = change.get("set_by", "")
        reason = change.get("reason", "")
        if name != SITE:
            data["client"] = name
        data["state"] = state
        data["reason"] = reason
        data["set_by"] = by
        data["set_at"] = at
        if change.get("grant"):
            data["grant"] = {**data.get("grant", {}), **change["grant"]}
        entry = {"at": at, "from": old, "to": state, "by": by, "reason": reason}
        if name != SITE and change.get("identity"):
            entry["identity"] = dict(change["identity"])
        data["history"] = list(data.get("history", [])) + [entry]
        text = dumps_toml(data)
        tmp = d / f".{path.name}.{os.getpid()}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.fchmod(fd, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        _fsync_dir(d)
    finally:
        os.close(lockfd)  # closing drops the flock
