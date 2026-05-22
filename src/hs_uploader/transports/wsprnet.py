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
import time
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
        max_spots_per_upload: int = MAX_SPOTS_PER_UPLOAD,
    ):
        self.url = url
        self.version = version
        self.upload_timeout_sec = upload_timeout_sec
        self._urlopen = urlopen or urllib.request.urlopen
        self.name = name or f"wsprnet:{url}"
        # Caller-controlled batch ceiling so an operator can opt into
        # per-spot POSTs (``max_spots_per_upload=1``) for diagnostic
        # tracking — wsprnet's "M out of N added" response then
        # answers per-spot rather than per-batch.  Default 999 matches
        # the legacy ``wd-upload-wsprnet`` behaviour.
        if max_spots_per_upload < 1:
            max_spots_per_upload = 1
        if max_spots_per_upload > MAX_SPOTS_PER_UPLOAD:
            max_spots_per_upload = MAX_SPOTS_PER_UPLOAD
        self._max_spots_per_upload = max_spots_per_upload

    # -- Transport protocol --

    def primary_table(self) -> str:
        return "wspr.spots"

    def batch_policy(self) -> BatchPolicy:
        return BatchPolicy(max_records=self._max_spots_per_upload)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        # Dedup once here so we can compute cycle attribution from
        # the EXACT records that go into the body (downstream
        # ``_build_mept_body`` redoes dedup on the same input, idempotent).
        deduped = dedup_records_for_wsprnet(batch.records)
        body = self._build_mept_body(batch.records)
        if not body:
            return Outcome.acked()
        # Per-cycle attribution: count records per cycle UTC so the
        # log line shows operators which cycles' spots are in THIS
        # POST.  Example: "cycle 01:08=68, 01:10=2" — multi-cycle
        # POSTs happen during backlog drain.
        cycle_counts: dict = {}
        for rec in deduped:
            try:
                key = rec.time.strftime('%H:%M')
            except Exception:
                continue
            cycle_counts[key] = cycle_counts.get(key, 0) + 1
        cycle_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(cycle_counts.items())
        ) or "(none)"
        return self._post(body, identity, cycle_summary)

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
        # Replay path doesn't have the original record objects, so
        # we can't compute cycle attribution.  Use a placeholder
        # the operator can still match against.
        return self._post(payload_blob, identity, "(replay)")

    # -- internals --

    def _build_mept_body(self, records) -> bytes:
        # Collapse duplicate spots first — multi-RX888 plan, phase 5.
        # When wspr-recorder runs against more than one radiod, the
        # same WSPR transmission lands in pending_uploads N times
        # (once per receiver) with the same (time, callsign, freq_hz)
        # key but typically different snr_db.  wsprnet has no
        # diversity tier; sending N copies wastes its budget and
        # produces "0/N added" rejections.  Keep the max-SNR record
        # per spot-key; the others are dropped silently here.  The
        # wsprdaemon-tar transport runs its own untouched _build path
        # and still ships all receivers' rows.
        records = dedup_records_for_wsprnet(records)
        lines: List[str] = []
        for rec in records:
            line = _record_to_mept(rec)
            if line:
                lines.append(line)
        if not lines:
            return b""
        lines.sort(key=_mept_sort_key)
        return ("\n".join(lines) + "\n").encode("utf-8")

    def _post(
        self,
        body: bytes,
        identity,
        cycle_summary: str = "(unknown)",
    ) -> Outcome:
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
        # Count the MEPT lines we're about to send.  Each line in
        # ``body`` is one spot; trailing newlines don't count.  This
        # lets the log show both client-side count (we sent N) and
        # server-side count (it accepted M of N).
        client_spots = body.count(b"\n")
        body_kb = round(len(body) / 1024.0, 1)
        logger.info(
            "wsprnet POST start: %d spots (cycles: %s, %.1f KB)",
            client_spots, cycle_summary, body_kb,
        )
        post_t0 = time.monotonic()
        try:
            with self._urlopen(req, timeout=self.upload_timeout_sec) as resp:
                status = getattr(resp, "status", 200)
                response_body = resp.read().decode(errors="replace")
        except urllib.error.HTTPError as exc:
            # HTTP 4xx/5xx: server received our request and chose to
            # reject.  Per policy (operator @rrobinett 2026-05-14):
            # `hs_uploader should only retry upload spots if there is
            # a curl error.  whatever wsprnet reports in a successful
            # curl transfer, hs-uploader should never retry them` — so
            # ack the batch (advance watermark, no retry).  Retrying
            # would just resend identical payload to the same server
            # for the same rejection; better to move on to fresh spots.
            elapsed = time.monotonic() - post_t0
            response_body = ""
            try:
                response_body = exc.read().decode(errors="replace")
            except Exception:  # noqa: BLE001
                pass
            logger.warning(
                "wsprnet POST failed in %.2fs: HTTP %d — %s",
                elapsed, exc.code, response_body[:160],
            )
            return Outcome(
                kind="acked",
                reason=f"HTTP {exc.code} (no retry): {response_body[:200]}",
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # Network-level failure (no HTTP response received) — retry.
            elapsed = time.monotonic() - post_t0
            logger.warning(
                "wsprnet POST network error in %.2fs: %s", elapsed, exc,
            )
            return Outcome.retry_later(f"wsprnet: network error — {exc}")

        if not (200 <= status < 300):
            # Defensive — _urlopen normally raises HTTPError for non-2xx,
            # but if a custom opener delivers the response object instead,
            # apply the same no-retry policy as the HTTPError branch.
            return Outcome(
                kind="acked",
                reason=f"HTTP {status} (no retry): {response_body[:200]}",
            )
        # The server returns plain text along the lines of
        # ``N out of M spot(s) added``.  We still treat any 2xx as
        # success (the watermark advances regardless — duplicates and
        # silently-dropped rows aren't worth retrying), but stash the
        # "N out of M" count in Outcome.reason so the on_batch_outcome
        # callback can log the true acceptance rate.  Without this the
        # operator sees `shipped wsprnet=900` in the journal while
        # wsprnet may have actually added only ~38 of them (the rest
        # dropped as duplicates or hitting MAX_SPOTS_PER_UPLOAD
        # truncation).  Observed on B4-100 2026-05-14 — production was
        # POSTing 900-spot batches but only ~38% were added per body.
        import re
        m = re.search(r"(\d+)\s+out\s+of\s+(\d+)\s+spot", response_body)
        reason = f"{m.group(1)}/{m.group(2)} added" if m else ""
        elapsed = time.monotonic() - post_t0
        rate = (client_spots / elapsed) if elapsed > 0 else 0
        logger.info(
            "wsprnet POST done: %d spots (cycles: %s) in %.2fs "
            "(%.1f spots/s); server: %s",
            client_spots, cycle_summary, elapsed, rate,
            reason or "no count",
        )
        return Outcome(kind="acked", reason=reason)


# ----- record → MEPT line -----


_HASH_UNRESOLVED = "<...>"


def dedup_records_for_wsprnet(records):
    """Collapse multi-receiver duplicates of the same WSPR transmission.

    Returns a list where each ``(date, time, callsign, freq_mhz_q)``
    tuple appears at most once, keeping the record with the largest
    ``snr_db`` (or ``snr``).  ``freq_mhz_q`` is the frequency rounded
    to 0.0001 MHz (≈ 100 Hz) — finer than WSPR's natural bandwidth so
    spots that genuinely differ in carrier frequency remain distinct,
    coarser than wsprd's float precision so identical spots seen by
    two receivers with negligible drift collapse.

    Ordering is stable: the first record encountered for each key
    sets that key's position in the output, and ties on snr_db keep
    the earlier record (matches the wsprnet_audit table's INSERT OR
    IGNORE first-write-wins semantics, so transport and audit converge
    on the same identity).

    Records that don't match the wsprnet record shape (missing call /
    freq / time) are passed through unchanged — ``_record_to_mept``
    will drop them at format time.

    Exported for use by the producer-side audit / verifier — both
    paths must dedup on the same key so their counts line up.

    Multi-RX888 plan, phase 5.
    """
    best: dict = {}                 # key -> (position, record, snr)
    pass_through: list = []         # records that don't dedup cleanly
    for pos, rec in enumerate(records):
        cols = rec.columns or {}
        call = (
            cols.get("tx_sign") or cols.get("tx_call")
            or cols.get("callsign") or ""
        ).strip()
        if not call:
            pass_through.append((pos, rec))
            continue
        # Quantize frequency the same way for every record-shape
        # variant the transport accepts.
        freq_mhz = cols.get("frequency_mhz")
        if freq_mhz is None:
            freq_hz = cols.get("frequency") or cols.get("frequency_hz")
            if not freq_hz:
                pass_through.append((pos, rec))
                continue
            try:
                freq_mhz = float(freq_hz) / 1_000_000.0
            except (TypeError, ValueError):
                pass_through.append((pos, rec))
                continue
        try:
            freq_q = round(float(freq_mhz), 4)
        except (TypeError, ValueError):
            pass_through.append((pos, rec))
            continue
        t = rec.time
        if t is None:
            pass_through.append((pos, rec))
            continue
        key = (t.strftime("%y%m%d"), t.strftime("%H%M"), call, freq_q)
        raw_snr = cols.get("snr") if cols.get("snr") is not None else cols.get("snr_db")
        snr = _float_or_zero(raw_snr)
        prev = best.get(key)
        if prev is None or snr > prev[2]:
            best[key] = (pos, rec, snr)
    # Re-interleave deduped winners with pass-throughs by original
    # position so the output keeps caller-supplied ordering wherever
    # possible.  (Stable order makes the MEPT body diff cleanly
    # under tests + makes byte-stable replay deterministic.)
    ordered = sorted(
        list(best.values()) + [(pos, rec, 0.0) for pos, rec in pass_through],
        key=lambda t: t[0],
    )
    return [t[1] for t in ordered]


def _record_to_mept(record) -> Optional[str]:
    """Render one ``wspr.spots`` row as a wsprnet.org MEPT line.

    Wire-compliant with v1 wsprdaemon-client/bin/wd-upload-wsprnet:
    byte-identical lines for the same source data.  Format strings
    match wd-decode's awk-printf at line 345 (W-mode) and 466 (F-mode)
    exactly:

      W2 mode (wsprd):
        "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d"
        date  time sync  snr   dt    freq        call         grid  pwr drift ntype

      F2/F5/F15/F30 modes (jt9):
        "%-6s %4s %5.1f %3.0f %5.2f %12.7f %-14s %-6s %2d  0 %4d"
        (sync is float-but-integer-valued; drift hardcoded "0";
         snr is %3.0f because jt9 outputs float-valued integers)

    Accepts BOTH schema shapes:
      * legacy / file-source row: tx_sign, frequency_mhz, snr,
        power, drift, code, tx_loc
      * v2 sink.db / wspr-recorder ``spot_to_row`` row: callsign,
        frequency_hz, snr_db, pwr_dbm, drift_hz_per_s, pkt_mode, grid

    Filter rules (matching v1 wd-upload-wsprnet._has_hash_callsign):
      * "<...>" literal unresolved-hash placeholder is dropped.
      * "<CALLSIGN>" angle-bracketed RESOLVED callsigns PASS THROUGH —
        wsprnet.org accepts them; the server strips brackets.

    Bug fix 2026-05-14: prior versions only knew the legacy field
    names AND used loose `%d`/`%.1f` formatting.  v2 sink.db records
    produced empty bodies (silent acked) AND for legacy records the
    on-the-wire format diverged from v1's bytes.  Both fixed here.
    """
    cols = record.columns or {}
    call = (cols.get("tx_sign") or cols.get("tx_call")
            or cols.get("callsign") or "").strip()
    if not call or call == _HASH_UNRESOLVED:
        return None
    freq_mhz = cols.get("frequency_mhz")
    if freq_mhz is None:
        freq_hz = cols.get("frequency") or cols.get("frequency_hz")
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

    sync = _float_or_zero(cols.get("sync_quality"))    # NOT int — v1 prints %.Nf
    # snr: take it as a float so the F-mode %3.0f path also works.
    raw_snr = cols.get("snr") if cols.get("snr") is not None else cols.get("snr_db")
    snr = _float_or_zero(raw_snr)
    dt = _float_or_zero(cols.get("dt"))
    power = _int_or_zero(cols.get("power") if cols.get("power") is not None
                         else cols.get("pwr_dbm"))
    # drift: legacy is integer Hz/min; v2 sink.db carries Hz/s float.
    # MEPT wants integer Hz/min — multiply v2's Hz/s by 60.
    if cols.get("drift") is not None:
        drift = _int_or_zero(cols.get("drift"))
    elif cols.get("drift_hz_per_s") is not None:
        drift = int(round(float(cols.get("drift_hz_per_s") or 0.0) * 60.0))
    else:
        drift = 0
    ntype = _int_or_zero(cols.get("code") if cols.get("code") is not None
                         else cols.get("pkt_mode"))
    grid = (cols.get("tx_loc") or cols.get("grid") or "").strip()

    # Mode-aware format — W2 (pkt_mode=2) vs F-modes (pkt_mode>=3).
    # `mode` column ("W2", "F2", "F5", ...) takes precedence; fall
    # back to ntype/pkt_mode (2 is W2, anything else F-family).
    mode_token = (cols.get("mode") or "").strip().upper()
    is_w_mode = (mode_token.startswith("W") if mode_token
                 else (ntype == 2))
    if is_w_mode:
        # v1 wd-decode line 345 (W-mode short format):
        return ("%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d"
                % (date_str, time_str, sync, int(snr), dt, freq_mhz,
                   call, grid, power, drift, ntype))
    # v1 wd-decode line 466 (F-mode short format — drift hardcoded 0):
    return ("%-6s %4s %5.1f %3.0f %5.2f %12.7f %-14s %-6s %2d  0 %4d"
            % (date_str, time_str, sync, snr, dt, freq_mhz,
               call, grid, power, ntype))


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
