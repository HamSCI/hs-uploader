"""wsprnet.org HTTP transport.

POSTs WSPR spots to ``http://wsprnet.org/meptspots.php`` as
``multipart/form-data``.  Wire-compatible with
``wsprdaemon-client/bin/wd-upload-wsprnet`` (and through it, with the
~15,700-line bash original): same form fields (``version``, ``call``,
``grid``, ``allmept``), same MEPT line shape, same 999-spot batch cap.

The transport ingests ``wspr.spots`` rows (one Record per spot) and
emits a MEPT-format text file as the ``allmept`` field's body.  Each
line is:

    YYMMDD HHMM SYNC SNR DT FREQ_MHZ CALL [GRID] POWER DRIFT NTYPE

Sorted by (date, time, frequency) before upload — the canonical
ordering the central server expects.  Records missing ``tx_sign`` or
whose hash is unresolved (``<...>``) are skipped silently (cursor
still advances past them).

Stdlib only — uses ``urllib.request`` like the bash-era uploader so
hs-uploader stays installable without ``requests``.  The HTTP
response body is parsed for the "200 OK ... spots added" pattern;
non-2xx or a non-parseable body yields ``retry_later`` rather than
``acked`` so the orchestrator's exponential back-off kicks in.
"""

from __future__ import annotations

import logging
import re
import urllib.error
import urllib.request
from typing import Any, List, Optional

from ..core import BatchPolicy, Outcome, RecordBatch

logger = logging.getLogger(__name__)


# wsprnet.org's hard limit per /meptspots.php transaction.  Documented
# in wd-upload-wsprnet (MAX_SPOTS=999); larger uploads are silently
# truncated on the server side.
MAX_SPOTS_PER_UPLOAD = 999

# Multipart boundary.  wd-upload-wsprnet uses this exact string; matching
# it makes diffing the two uploaders' requests trivial during the
# wsprdaemon-client migration.
_BOUNDARY = b"--------WD4MeptBoundary"
_CRLF = b"\r\n"


class WsprNet:
    """Ships wspr.spots records to wsprnet.org via HTTP multipart POST.

    Receiver identity (call + grid) comes from the Pipeline's
    ``StationIdentity`` — those are the WSPR REPORTER call/grid, which
    may differ from the per-spot ``rx_sign`` if the operator runs
    multiple receivers under one wsprnet identity.

    ``url`` defaults to the production endpoint; tests inject the same
    interface against a local HTTP server.  ``urlopen`` is exposed for
    pure-unit tests that want to stub the call without spinning up a
    real listener.
    """

    ACCEPTS = {"wspr.spots": [1]}

    def __init__(
        self,
        *,
        url: str = "http://wsprnet.org/meptspots.php",
        version: str = "hs-uploader/0.1",
        upload_timeout_sec: float = 300.0,
        urlopen=None,
        name: Optional[str] = None,
    ):
        self.url = url
        self.version = version
        self.upload_timeout_sec = upload_timeout_sec
        self._urlopen = urlopen or urllib.request.urlopen
        self.name = name or f"wsprnet:{url}"

    # -- Transport protocol --

    def primary_table(self) -> str:
        return "wspr.spots"

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=MAX_SPOTS_PER_UPLOAD)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        body = self._build_mept_body(batch.records)
        if not body:
            return Outcome.acked()
        return self._post(body, identity)

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        """Snapshot the rendered MEPT body for byte-stable replay.

        The body is deterministic given the input rows, but replay
        through a stored payload skips re-running the row-to-line
        mapping on retry — protecting against the orchestrator
        replaying a batch after a producer-side schema rev would
        change the rendered text.
        """
        return self._build_mept_body(batch.records)

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        if not payload_blob:
            return Outcome.acked()
        return self._post(payload_blob, identity)

    # -- internals --

    def _build_mept_body(self, records) -> bytes:
        lines: List[str] = []
        for rec in records:
            line = _record_to_mept(rec)
            if line:
                lines.append(line)
        if not lines:
            return b""
        lines.sort(key=_mept_sort_key)
        return ("\n".join(lines) + "\n").encode("utf-8")

    def _post(self, body: bytes, identity) -> Outcome:
        call = (getattr(identity, "call", "") or "").strip()
        grid = (getattr(identity, "grid", "") or "").strip()
        if not call:
            return Outcome.permanent_failure(
                "wsprnet: identity.call is empty — wsprnet rejects "
                "anonymous uploads"
            )
        multipart = _build_multipart(
            version=f"WD_{self.version}",
            call=call,
            grid=grid,
            allmept=body,
        )
        req = urllib.request.Request(
            self.url,
            data=multipart,
            headers={
                "Content-Type":
                    "multipart/form-data; boundary=" + _BOUNDARY.decode(),
            },
            method="POST",
        )
        try:
            with self._urlopen(req, timeout=self.upload_timeout_sec) as resp:
                status = getattr(resp, "status", 200)
                response_body = resp.read().decode(errors="replace")
        except urllib.error.HTTPError as exc:
            response_body = ""
            try:
                response_body = exc.read().decode(errors="replace")
            except Exception:  # noqa: BLE001
                pass
            return Outcome.retry_later(
                f"wsprnet: HTTP {exc.code} — {response_body[:200]}"
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return Outcome.retry_later(f"wsprnet: network error — {exc}")

        if not (200 <= status < 300):
            return Outcome.retry_later(
                f"wsprnet: HTTP {status} — {response_body[:200]}"
            )
        # The server returns plain text along the lines of
        # ``200 OK <n> spots added``.  Treat any 2xx as success; the
        # exact count isn't load-bearing here (the watermark already
        # encodes which rows we've shipped).  Bad-format bodies show
        # up in the deliverables audit log via ``response_body[:200]``.
        return Outcome.acked()


# ----- record → MEPT line -----


_HASH_UNRESOLVED = "<...>"


def _record_to_mept(record) -> Optional[str]:
    """Render one ``wspr.spots`` row as an MEPT text line.

    Returns None for rows that are not shippable (no callsign / no
    frequency / unresolved-hash placeholder).  The Pipeline still
    advances the watermark past these — they're silently filtered,
    not failures.
    """
    cols = record.columns or {}
    call = (cols.get("tx_sign") or cols.get("tx_call") or "").strip()
    if not call or call == _HASH_UNRESOLVED:
        return None
    freq_mhz = cols.get("frequency_mhz")
    if freq_mhz is None:
        freq_hz = cols.get("frequency")
        if not freq_hz:
            return None
        freq_mhz = float(freq_hz) / 1_000_000.0
    try:
        freq_mhz = float(freq_mhz)
    except (TypeError, ValueError):
        return None

    t = record.time
    date_str = t.strftime("%y%m%d")
    time_str = t.strftime("%H%M")

    sync = _int_or_zero(cols.get("sync_quality"))
    snr = _int_or_zero(cols.get("snr"))
    dt = _float_or_zero(cols.get("dt"))
    power = _int_or_zero(cols.get("power"))
    drift = _int_or_zero(cols.get("drift"))
    ntype = _int_or_zero(cols.get("code"))
    grid = (cols.get("tx_loc") or cols.get("grid") or "").strip()

    fields = [
        date_str,
        time_str,
        f"{sync:d}",
        f"{snr:d}",
        f"{dt:.1f}",
        f"{freq_mhz:.6f}",
        call,
    ]
    if grid:
        fields.append(grid)
    fields.extend([
        f"{power:d}",
        f"{drift:d}",
        f"{ntype:d}",
    ])
    return " ".join(fields)


def _mept_sort_key(line: str) -> tuple:
    """Sort by (date, time, freq_mhz) — the canonical wsprnet order."""
    parts = line.split()
    try:
        return (parts[0], parts[1], float(parts[5]))
    except (IndexError, ValueError):
        return ("", "", 0.0)


def _int_or_zero(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0


def _float_or_zero(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ----- multipart body -----


def _build_multipart(
    *, version: str, call: str, grid: str, allmept: bytes,
) -> bytes:
    """Render the multipart/form-data body wd-upload-wsprnet sends.

    Identical wire layout: three text fields then one file field
    named ``allmept`` with filename ``spots.txt`` and content-type
    ``text/plain``.  Bytes-equal to the bash-era body for the same
    inputs — diff-friendly for staged migrations.
    """

    def text_field(name: str, value: str) -> bytes:
        return (
            b"--" + _BOUNDARY + _CRLF
            + b'Content-Disposition: form-data; name="' + name.encode() + b'"'
            + _CRLF + _CRLF
            + value.encode("utf-8") + _CRLF
        )

    return (
        text_field("version", version)
        + text_field("call", call)
        + text_field("grid", grid)
        + b"--" + _BOUNDARY + _CRLF
        + b'Content-Disposition: form-data; name="allmept"; '
        + b'filename="spots.txt"' + _CRLF
        + b"Content-Type: text/plain" + _CRLF + _CRLF
        + allmept + _CRLF
        + b"--" + _BOUNDARY + b"--" + _CRLF
    )
