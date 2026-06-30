"""Cross-process wake for the host uploader daemon.

Producers (recorders) commit rows/datasets into the shared sink and fire a
one-byte Unix *datagram* to a well-known socket; the uploader daemon binds that
socket and a listener thread sets the pump's wake Event for each datagram.  It
is strictly best-effort — if the daemon isn't up the send is dropped and the
daemon's polling backstop still catches the work, so producers never block.

This is the generic home for what wspr-recorder pioneered in
``wspr_recorder.upload_wake``; the daemon and every producer share ONE socket
(default ``/var/lib/sigmond/upload-wake.sock``, beside the sink), so any
producer datagram wakes the whole daemon regardless of which pipeline it feeds.

Path resolution (first match wins):
``HS_UPLOADER_WAKE_SOCK`` / ``WSPR_UPLOAD_WAKE_SOCK`` env override, else
``<sink dir>/upload-wake.sock`` from ``HAMSCI_SINK_PATH`` /
``SIGMOND_SQLITE_PATH``, else ``/var/lib/sigmond/upload-wake.sock``.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_DEFAULT_SOCK = "/var/lib/sigmond/upload-wake.sock"

_DEBUG = os.environ.get("HS_UPLOADER_WAKE_DEBUG", os.environ.get(
    "WSPR_WAKE_DEBUG", "")).strip().lower() in ("1", "true", "yes", "on")


def wake_path() -> str:
    """Resolve the shared wake-socket path (same dir as the sink)."""
    for var in ("HS_UPLOADER_WAKE_SOCK", "WSPR_UPLOAD_WAKE_SOCK"):
        v = os.environ.get(var, "").strip()
        if v:
            return v
    for var in ("HAMSCI_SINK_PATH", "SIGMOND_SQLITE_PATH"):
        sink = os.environ.get(var, "").strip()
        if sink:
            return str(Path(sink).parent / "upload-wake.sock")
    return _DEFAULT_SOCK


def notify(path: Optional[str] = None) -> None:
    """Best-effort: ask the uploader daemon to pump now.

    Safe to call from any producer process; a missing/closed listener is
    silently ignored (the pump's polling backstop covers that case).
    """
    p = path or wake_path()
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            s.sendto(b"w", p)
            if _DEBUG:
                logger.info("upload-wake: notify sent -> %s", p)
        finally:
            s.close()
    except OSError as exc:
        if _DEBUG:
            logger.info("upload-wake: notify to %s failed: %s", p, exc)


class WakeListener:
    """Daemon-side receiver: bind the datagram socket and call ``on_wake`` for
    every datagram on a daemon thread.

    ``start`` removes any stale socket and binds fresh, group-writable (0o660)
    so peer producer processes in the ``sigmond`` group can send.  ``stop``
    closes the socket, joins the thread, and unlinks the path.
    """

    def __init__(self, on_wake: Callable[[], None], path: Optional[str] = None):
        self._on_wake = on_wake
        self._path = path or wake_path()
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    @property
    def path(self) -> str:
        return self._path

    def start(self) -> bool:
        try:
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass
        except OSError:
            pass
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.bind(self._path)
            sock.settimeout(1.0)
        except OSError as exc:
            logger.warning("upload-wake: cannot bind %s: %s — cross-process "
                           "wake disabled (polling backstop only)",
                           self._path, exc)
            return False
        try:
            os.chmod(self._path, 0o660)
        except OSError:
            pass
        self._sock = sock
        self._thread = threading.Thread(
            target=self._run, name="hs-uploader-wake", daemon=True,
        )
        self._thread.start()
        logger.info("upload-wake: listening on %s", self._path)
        return True

    def _run(self) -> None:
        while not self._stop:
            try:
                self._sock.recvfrom(16)
            except socket.timeout:
                continue
            except OSError:
                if self._stop:
                    break
                continue
            if _DEBUG:
                logger.info("upload-wake: wake received")
            try:
                self._on_wake()
            except Exception:
                logger.debug("upload-wake: on_wake raised", exc_info=True)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop = True
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        try:
            os.unlink(self._path)
        except OSError:
            pass
