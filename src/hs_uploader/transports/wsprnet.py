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
response body is parsed for the "N out of M spot(s) added" pattern.

Delivery policy (Rob, 2026-11-13, replacing the 2026-05 "never retry"):
  * A timeout, network error, HTTP 5xx or 2xx reply with no spot count
    is ``retry_later``.  The pipeline's retry delay never drops below
    ``RETRY_MIN_DELAY_SEC`` (120 s), because wsprnet.org often goes on
    recording an upload after the client gave up; re-sending sooner put
    both copies on wsprnet.  After the wait, the spots it did record come
    back as duplicates, which it refuses.
  * An HTTP 4xx is acked: wsprnet read the request and refused it, and
    the same bytes would be refused again.
  * "Upload limit of 1000 spots or 100 dupes reached" means wsprnet
    stopped after the first <read> lines and never looked at the rest.
    Those lines are posted again at once — they were never processed, so
    there is nothing to race — until every line has been read.
Measured 2026-11-13 with the old policy: the eight Sigmond stations that
post directly lost 7-15 % of their spots on wsprnet (wsprdaemon.rx vs
wspr.rx, 24 h); the WD30 forwarder, which retries, lost 0.2-0.8 %.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.error
import urllib.request
from typing import Any, List, Optional

from ..core import BatchPolicy, Outcome, RecordBatch
from .. import wspr_modes

logger = logging.getLogger(__name__)


# wsprnet.org's hard limit per /meptspots.php transaction.  Documented
# in wd-upload-wsprnet (MAX_SPOTS=999); larger uploads are silently
# truncated on the server side.
MAX_SPOTS_PER_UPLOAD = 999

# What we post per transaction by default.  Under load wsprnet takes ~2 s
# per spot (K9TRV-5, 2026-11-13: 255 s for 123 spots), so 999 could never
# finish inside any sane timeout; 250 finishes inside DEFAULT_UPLOAD_TIMEOUT_SEC.
# Same figure as the WD client (wsprdaemon 8a48999) and the WD30 forwarder.
DEFAULT_SPOTS_PER_UPLOAD = 250
DEFAULT_UPLOAD_TIMEOUT_SEC = 600.0

_ADDED_RE = re.compile(r"(\d+)\s+out\s+of\s+(\d+)\s+spot", re.I)
_LIMIT_RE = re.compile(r"Upload limit of[^<.]*reached", re.I)

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

    # wspr-recorder's spot_sink writes SCHEMA_VERSION=2; psk-recorder's
    # ch_tailer writes 2.  Pre-2026-05-23 this declared only [1] but
    # ACCEPTS is advisory (no orchestrator gate), so wsprnet shipped
    # v2 rows fine — listing both versions keeps the declaration
    # truthful and future-proof.
    ACCEPTS = {"wspr.spots": [1, 2, 3]}

    # The pipeline factory floors this transport's retry delay here (see
    # RetryPolicy.min_delay_sec and the module docstring).
    RETRY_MIN_DELAY_SEC = 120.0

    def __init__(
        self,
        *,
        url: str = "http://wsprnet.org/meptspots.php",
        version: str = "hs-uploader/0.1",
        upload_timeout_sec: float = DEFAULT_UPLOAD_TIMEOUT_SEC,
        urlopen=None,
        name: Optional[str] = None,
        max_spots_per_upload: int = DEFAULT_SPOTS_PER_UPLOAD,
        api_base_url: Optional[str] = None,
        # 5s (not 1s) to ease status-poll load on wsprnet.org: server
        # processing is ~3-6s, so the 1st-2nd poll still catches "done"
        # with no practical latency cost, well within poll_max_sec.
        poll_interval_sec: float = 5.0,
        poll_max_sec: float = 60.0,
    ):
        self.url = url
        self.version = version
        self.upload_timeout_sec = upload_timeout_sec
        self._urlopen = urlopen or urllib.request.urlopen
        self.name = name or f"wsprnet:{url}"
        # Optional async upload API (W1GJM, 2026-05).  When set (e.g.
        # ``https://wsprnet.org/api/upload/v1``) the same multipart body
        # is POSTed to ``<base>/upload``, which returns immediately with
        # ``{"nonce": ..., "queued": true}``; we then poll
        # ``<base>/status/<nonce>`` until ``done``/``failed`` for the
        # accepted/submitted counts.  Unset → the legacy blocking
        # ``meptspots.php`` POST (default).
        self._api_base_url = (api_base_url or "").strip().rstrip("/") or None
        self._poll_interval_sec = max(0.05, float(poll_interval_sec))
        self._poll_max_sec = max(1.0, float(poll_max_sec))
        if self._api_base_url:
            self.name = name or f"wsprnet-async:{self._api_base_url}"
        # Caller-controlled batch ceiling so an operator can opt into
        # per-spot POSTs (``max_spots_per_upload=1``) for diagnostic
        # tracking — wsprnet's "M out of N added" response then
        # answers per-spot rather than per-batch.  Default 250
        # (DEFAULT_SPOTS_PER_UPLOAD); MAX_SPOTS_PER_UPLOAD (999) is the cap.
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
        # Count the MEPT lines we're about to send.  Each line in
        # ``body`` is one spot; trailing newlines don't count.  This
        # lets the log show both client-side count (we sent N) and
        # server-side count (it accepted M of N).
        client_spots = body.count(b"\n")
        body_kb = round(len(body) / 1024.0, 1)
        logger.info(
            "wsprnet POST start: %d spots (cycles: %s, %.1f KB)%s",
            client_spots, cycle_summary, body_kb,
            " [async API]" if self._api_base_url else "",
        )
        if self._api_base_url:
            return self._post_async(multipart, client_spots, cycle_summary)
        lines = body.splitlines(keepends=True)
        added_total = 0
        while True:
            verdict, detail, added, read = self._post_once(
                b"".join(lines), call, grid, cycle_summary, len(lines))
            if verdict == "retry":
                return Outcome.retry_later(detail)
            if verdict == "refused":
                return Outcome(kind="acked", reason=detail)
            added_total += added
            if verdict == "done" or read >= len(lines):
                break
            # verdict == "cut": wsprnet stopped at its duplicate cutoff
            # after reading `read` lines.  The rest were never looked at,
            # so post them now; there is no earlier copy to race.
            if read <= 0:
                return Outcome.retry_later(
                    "wsprnet hit its upload limit without reading a spot")
            logger.warning(
                "wsprnet stopped after reading %d of %d spot(s) (duplicate "
                "cutoff); posting the %d it never read",
                read, len(lines), len(lines) - read,
            )
            lines = lines[read:]
        return Outcome(kind="acked",
                       reason=f"{added_total}/{client_spots} added")

    def _post_once(
        self, body: bytes, call: str, grid: str, cycle_summary: str,
        n_lines: int,
    ):
        """One blocking POST to meptspots.php.

        Returns ``(verdict, detail, added, read)``, verdict one of:
          * ``"done"``    — wsprnet read every line;
          * ``"cut"``     — it stopped at its duplicate cutoff after ``read``;
          * ``"retry"``   — no usable answer (timeout, network, 5xx, no count);
          * ``"refused"`` — HTTP 4xx, which re-sending cannot change.
        """
        multipart = _build_multipart(
            version=f"WD_{self.version}", call=call, grid=grid, allmept=body,
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
        post_t0 = time.monotonic()
        try:
            with self._urlopen(req, timeout=self.upload_timeout_sec) as resp:
                status = getattr(resp, "status", 200)
                response_body = resp.read().decode(errors="replace")
        except urllib.error.HTTPError as exc:
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
            status = exc.code
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # No answer at all.  wsprnet may still be recording this body,
            # so it is retried only after the pipeline's retry floor
            # (RETRY_MIN_DELAY_SEC), by which time any spot it took comes
            # back as a refused duplicate.
            elapsed = time.monotonic() - post_t0
            logger.warning(
                "wsprnet POST timeout/network error in %.2fs (retrying "
                "after the hold-off): %s", elapsed, exc,
            )
            return ("retry",
                    f"timeout/network error after {elapsed:.0f}s: {exc}", 0, 0)

        if not (200 <= status < 300):
            if 500 <= status < 600 or status == 429:
                return ("retry", f"HTTP {status}: {response_body[:200]}", 0, 0)
            return ("refused",
                    f"HTTP {status} (refused, not retried): "
                    f"{response_body[:200]}", 0, 0)
        # wsprnet answers in blocks, one "N out of M spot(s) added" each:
        # sum them all, or a 250-spot upload reads back as "0 of 100".
        pairs = _ADDED_RE.findall(response_body)
        elapsed = time.monotonic() - post_t0
        if not pairs:
            logger.warning(
                "wsprnet POST in %.2fs returned no spot count (retrying "
                "after the hold-off): %s", elapsed,
                " ".join(response_body.split())[:160],
            )
            return ("retry", "wsprnet reply had no spot count", 0, 0)
        added = sum(int(a) for a, _ in pairs)
        read = sum(int(b) for _, b in pairs)
        cut = bool(_LIMIT_RE.search(response_body)) and read < n_lines
        rate = (n_lines / elapsed) if elapsed > 0 else 0
        logger.info(
            "wsprnet POST done: %d spots (cycles: %s) in %.2fs "
            "(%.1f spots/s); server: %s",
            n_lines, cycle_summary, elapsed, rate,
            f"{added}/{read} added" + (" (duplicate cutoff)" if cut else ""),
        )
        return ("cut" if cut else "done", "", added, read)

    def _post_async(
        self,
        multipart: bytes,
        client_spots: int,
        cycle_summary: str,
    ) -> Outcome:
        """Async upload API: submit → nonce → poll status.

        A submit that never got a job id (HTTP 5xx, timeout, network error,
        unreadable reply, no nonce) and a job whose status is ``failed`` are
        ``retry_later``: nothing tells us wsprnet holds those spots.  On
        2026-11-12/13 every one of them was a whole WSPR cycle missing from
        wsprnet (AB4EJ: 28 HTTP 500 + 52 submit timeouts, 119 cycles lost
        in 24 h).  An HTTP 4xx is still acked: it was read and refused.

        A job still pending when polling ends is acked: the server holds it
        and its result for 24 h, and on a backed-up day that is ~90 % of
        jobs, so re-submitting them would double wsprnet's queue.
        """
        import json
        upload_url = f"{self._api_base_url}/upload"
        post_t0 = time.monotonic()
        # 1. Submit — returns {"nonce": ..., "queued": true} immediately.
        req = urllib.request.Request(
            upload_url,
            data=multipart,
            headers={
                "Content-Type":
                    "multipart/form-data; boundary=" + _BOUNDARY.decode(),
            },
            method="POST",
        )
        try:
            with self._urlopen(req, timeout=self.upload_timeout_sec) as resp:
                submit = json.loads(resp.read().decode(errors="replace") or "{}")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode(errors="replace")
            except Exception:  # noqa: BLE001
                pass
            logger.warning(
                "wsprnet async submit failed: HTTP %d — %s",
                exc.code, body[:160],
            )
            if 500 <= exc.code < 600 or exc.code == 429:
                return Outcome.retry_later(
                    f"async submit HTTP {exc.code}: {body[:200]}")
            return Outcome(kind="acked",
                           reason=f"HTTP {exc.code} (refused, not retried): "
                                  f"{body[:200]}")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            elapsed = time.monotonic() - post_t0
            logger.warning(
                "wsprnet async submit timeout/network error in %.2fs "
                "(retrying after the hold-off): %s", elapsed, exc,
            )
            return Outcome.retry_later(
                f"async submit timeout/network error after {elapsed:.0f}s: "
                f"{exc}")
        except ValueError as exc:
            logger.warning("wsprnet async: submit response not JSON: %s", exc)
            return Outcome.retry_later("async submit response not JSON")

        nonce = (submit or {}).get("nonce")
        if not nonce:
            logger.warning("wsprnet async: no nonce in submit response: %r",
                           submit)
            return Outcome.retry_later("async submit returned no nonce")

        # 2. Poll <base>/status/<nonce> until done/failed or timeout.
        status_url = f"{self._api_base_url}/status/{nonce}"
        deadline = time.monotonic() + self._poll_max_sec
        result = None
        while time.monotonic() < deadline:
            time.sleep(self._poll_interval_sec)
            try:
                sreq = urllib.request.Request(status_url, method="GET")
                with self._urlopen(sreq, timeout=self.upload_timeout_sec) as r:
                    result = json.loads(r.read().decode(errors="replace") or "{}")
            except (urllib.error.HTTPError, urllib.error.URLError,
                    TimeoutError, OSError, ValueError) as exc:
                logger.debug("wsprnet async: status poll error (retrying): %s",
                             exc)
                continue
            if (result or {}).get("status") in ("done", "failed"):
                break

        elapsed = time.monotonic() - post_t0
        st = (result or {}).get("status")
        if st not in ("done", "failed"):
            logger.warning(
                "wsprnet async: status still %r after %.1fs polling "
                "(nonce %s) — acked, results kept 24h server-side",
                st or "unknown", elapsed, nonce,
            )
            return Outcome(kind="acked",
                           reason=f"async {st or 'pending'} (nonce {nonce})")

        accepted = result.get("accepted")
        submitted = result.get("submitted")
        rejected = result.get("rejected")
        rejections = result.get("rejections") or []
        if accepted is not None and submitted is not None:
            reason = f"{accepted}/{submitted} added"
            if rejected:
                reason += f", {rejected} rejected"
        else:
            reason = st
        # Keep the legacy ``wsprnet POST done: … in Ts (R spots/s);
        # server: …`` shape so `smd watch wspr`'s parser renders the line
        # exactly as it did on the synchronous path; the async-specific
        # detail (status / processing_ms / nonce) rides in the server
        # tail (its capture group is greedy).
        rate = (client_spots / elapsed) if elapsed > 0 else 0.0
        logger.info(
            "wsprnet POST done: %d spots (cycles: %s) in %.2fs "
            "(%.1f spots/s); server: %s [async status=%s, %sms, nonce %s]",
            client_spots, cycle_summary, elapsed, rate, reason, st,
            result.get("processing_ms"), nonce,
        )
        # Itemise what the server bounced (e.g. no_grid for type-2 spots)
        # so operators can see it in the journal / `smd watch wspr`.
        # rejections[] shape is server-defined — render whatever it sends.
        if rejections:
            logger.info(
                "wsprnet POST rejections (%d, nonce %s): %s",
                len(rejections), nonce,
                "; ".join(str(r) for r in rejections[:25]),
            )
        if st == "failed":
            # The job ran and failed: nothing says wsprnet kept these spots.
            # Re-submitting after the hold-off is safe; any it did keep come
            # back as refused duplicates.
            return Outcome.retry_later(f"async job failed: {reason}")
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
    # wsprnet's mode column takes wsprnet's code, never the stored
    # wsprdaemon number (FST4W-300/900/1800 showed as "unknown").  A row
    # naming no known mode passes its stored number through.
    mode = wspr_modes.resolve(cols)
    if mode is not None:
        ntype = mode.wsprnet_code
        is_w_mode = mode.name.startswith("W")
    else:
        ntype = _int_or_zero(cols.get("code") if cols.get("code") is not None
                             else cols.get("pkt_mode"))
        is_w_mode = ntype == 2
    grid = (cols.get("tx_loc") or cols.get("grid") or "").strip()

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
