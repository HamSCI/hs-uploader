"""PSKReporter TCP transport.

Owns a TCP socket to ``report.pskreporter.info:4739`` and ships
reception-report packets in the IPFIX-style wire format documented in
``hs_uploader.payload.psk_pskr``.

Why TCP and not UDP?  Per the user's correction (Phase 5 of the design
plan): UDP defaults are popular but bring no delivery guarantees.  The
psk-recorder spots flow is high-value enough to warrant a connected
socket with retries.  ftlib-pskreporter's TCP path uses
``setsockopt(SO_KEEPALIVE)`` and re-tries up to 5 times per packet on
send failure; we mirror that lifetime model but slot it into hs-uploader's
per-batch ack-or-retry-later abstraction so the orchestrator owns the
back-off cadence.

Why we own the socket (not wrap pskreporter-sender)?  Wrapping the
subprocess gives us the wire format and reconnect logic for free, but
loses every other piece of hs-uploader machinery: the watermark, the
deliverable persistence across restarts, the structured outcome enum.
A 200-line own-the-socket implementation is a fair trade.

The transport stays connected across multiple ``ship()`` calls.  The
session id (4 random bytes) is regenerated on every reconnect so a
post-disconnect server doesn't see a sequence-number gap from a stale
id.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional

from ..core import BatchPolicy, Outcome, RecordBatch
from ..payload.psk_pskr import (
    MAX_PACKET_LEN_TCP,
    ReceiverInfo,
    Spot,
    build_packet,
    chunk_spots,
    encode_spot,
    session_id,
)

logger = logging.getLogger(__name__)


# Mode-name normalization: psk.spots stores 'ft8' / 'ft4' (lowercase);
# pskreporter expects upper-cased.
_MODE_MAP = {
    "ft8": "FT8",
    "ft4": "FT4",
    "msk144": "MSK144",
    "fst4": "FST4",
}


def _mode_for_pskr(mode: str) -> str:
    return _MODE_MAP.get(mode.lower(), mode.upper())


@dataclass
class _Conn:
    """One live TCP connection + its session metadata.

    Created lazily on first ``ship()`` call; rebuilt on send failure.
    """

    sock: socket.socket
    session_id: bytes
    sequence: int


class PskReporterTcp:
    """Ships psk.spots / wspr.spots / etc. records to PSKReporter.

    Default destination is the production endpoint
    ``report.pskreporter.info:4739``.  Override via the constructor
    for staging environments or local mock listeners (the test suite
    does this).

    The transport is **stateful across batches**: it holds a TCP
    socket open across multiple ``ship()`` invocations and only
    reconnects on send failure.  This is what makes the running
    ``sequence`` field meaningful — the server uses it for
    deduplication.

    Decode software / antenna info come from the client config (passed
    in via the constructor) so different consuming clients
    (psk-recorder, wsprdaemon-client) can label themselves
    distinctively in the receiver-info header.
    """

    ACCEPTS = {
        "psk.spots": [2],
        "wspr.spots": [1],
    }

    def __init__(
        self,
        *,
        host: str = "report.pskreporter.info",
        port: int = 4739,
        decoding_software: str = "hs-uploader/0.1",
        antenna: str = "",
        connect_timeout_sec: float = 10.0,
        send_timeout_sec: float = 30.0,
        max_send_attempts_per_packet: int = 5,
        max_packet_len: int = MAX_PACKET_LEN_TCP,
        primary_table: str = "psk.spots",
        socket_factory=None,
        name: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.decoding_software = decoding_software
        self.antenna = antenna
        self.connect_timeout_sec = connect_timeout_sec
        self.send_timeout_sec = send_timeout_sec
        self.max_send_attempts_per_packet = max_send_attempts_per_packet
        self.max_packet_len = max_packet_len
        self._primary_table = primary_table
        self._socket_factory = socket_factory or _default_socket_factory
        self.name = name or f"pskreporter-tcp:{host}:{port}"

        self._conn: Optional[_Conn] = None
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    # -- Transport protocol --

    def primary_table(self) -> str:
        return self._primary_table

    def batch_policy(self) -> BatchPolicy:
        # Conservative cap so even a large batch fits in 1-2 packets.
        return BatchPolicy(max_records=500)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        spots = [self._record_to_spot(r) for r in batch.records if r is not None]
        spots = [s for s in spots if s is not None]
        if not spots:
            return Outcome.acked()
        receiver = self._receiver_info(identity)
        return self._send_spots(spots, receiver)

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        """Serialize for retry as receiver-info + length-prefixed
        encoded-spot blobs.  We do NOT pre-build the packet because
        the session id and sequence change between attempts; replay
        rebuilds the packet fresh from the spot bytes.
        """
        receiver = self._receiver_info(identity)
        rx_blob = (
            _u16(len(receiver.callsign.encode())) + receiver.callsign.encode()
            + _u16(len(receiver.locator.encode())) + receiver.locator.encode()
            + _u16(len(receiver.decoding_software.encode()))
            + receiver.decoding_software.encode()
            + _u16(len(receiver.antenna.encode())) + receiver.antenna.encode()
        )

        spots = [self._record_to_spot(r) for r in batch.records]
        spots = [s for s in spots if s is not None]
        encoded = [encode_spot(s) for s in spots]

        out = bytearray()
        out += _u16(len(rx_blob)) + rx_blob
        out += len(encoded).to_bytes(4, "big")
        for blob in encoded:
            out += _u16(len(blob)) + blob
        return bytes(out)

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        try:
            offset = 0
            rx_len = int.from_bytes(payload_blob[offset:offset + 2], "big")
            offset += 2
            rx_blob = payload_blob[offset:offset + rx_len]
            offset += rx_len
            receiver = _decode_receiver_blob(rx_blob)

            n = int.from_bytes(payload_blob[offset:offset + 4], "big")
            offset += 4
            encoded: List[bytes] = []
            for _ in range(n):
                length = int.from_bytes(payload_blob[offset:offset + 2], "big")
                offset += 2
                encoded.append(payload_blob[offset:offset + length])
                offset += length
        except Exception as exc:
            return Outcome.permanent_failure(
                f"could not decode retry blob: {exc}"
            )
        if not encoded:
            return Outcome.acked()
        return self._send_encoded_chunks(encoded, receiver)

    # -- internals --

    def _record_to_spot(self, record) -> Optional[Spot]:
        """Map a Record from psk.spots / wspr.spots into a Spot.

        Returns None if the record is missing fields PSKReporter
        requires (callsign + frequency + timestamp).  Out-of-spec
        rows are silently skipped — the cursor still advances past
        them.
        """
        cols = record.columns or {}
        call = cols.get("tx_call") or cols.get("callsign") or ""
        freq = cols.get("frequency") or cols.get("frequency_hz") or 0
        if not call or not freq:
            return None
        # Prefer calibrated SNR (psk.spots v2 jt9 path); fall back to
        # decoder score (decode_ft8 path) if no snr_db.
        snr = (
            cols.get("snr_db")
            if cols.get("snr_db") is not None
            else cols.get("score", -128)
        )
        try:
            snr_int = int(snr) if snr is not None else -128
        except (TypeError, ValueError):
            snr_int = -128
        mode = _mode_for_pskr(cols.get("mode") or "FT8")
        locator = cols.get("grid") or cols.get("locator") or ""
        ts = int(record.time.timestamp())
        msg_bytes = cols.get("message_bytes") or b""
        if isinstance(msg_bytes, str):
            try:
                msg_bytes = bytes.fromhex(msg_bytes)
            except ValueError:
                msg_bytes = b""
        return Spot(
            callsign=str(call),
            frequency_hz=int(freq),
            snr_db=snr_int,
            mode=str(mode),
            locator=str(locator),
            timestamp=ts,
            message_bytes=msg_bytes,
        )

    def _receiver_info(self, identity) -> ReceiverInfo:
        return ReceiverInfo(
            callsign=identity.call or "",
            locator=identity.grid or "",
            decoding_software=self.decoding_software,
            antenna=self.antenna,
        )

    def _send_spots(self, spots, receiver: ReceiverInfo) -> Outcome:
        encoded = [encode_spot(s) for s in spots]
        return self._send_encoded_chunks(encoded, receiver)

    def _send_encoded_chunks(
        self, encoded: List[bytes], receiver: ReceiverInfo
    ) -> Outcome:
        with self._lock:
            for chunk in chunk_spots(
                encoded,
                max_packet_len=self.max_packet_len,
                receiver=receiver,
            ):
                if not chunk:
                    continue
                ok, err = self._send_one_packet_locked(chunk, receiver)
                if not ok:
                    return Outcome.retry_later(err)
        return Outcome.acked()

    def _send_one_packet_locked(
        self, chunk: List[bytes], receiver: ReceiverInfo
    ) -> tuple[bool, str]:
        last_err = "no attempts made"
        for attempt in range(self.max_send_attempts_per_packet):
            try:
                self._connect_if_needed_locked()
                conn = self._conn
                assert conn is not None
                packet = build_packet(
                    receiver=receiver,
                    encoded_spots=chunk,
                    timestamp=int(time.time()),
                    sequence=conn.sequence,
                    session_id=conn.session_id,
                )
                conn.sock.sendall(packet)
                conn.sequence += len(chunk)
                return True, ""
            except (OSError, ConnectionError) as exc:
                last_err = f"send attempt {attempt + 1} failed: {exc}"
                logger.warning("PskReporterTcp: %s — reconnecting", last_err)
                self._close_locked()
        return False, last_err

    def _connect_if_needed_locked(self) -> None:
        if self._conn is not None:
            return
        sock = self._socket_factory(
            self.host, self.port, self.connect_timeout_sec
        )
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.settimeout(self.send_timeout_sec)
        except OSError:
            pass
        self._conn = _Conn(sock=sock, session_id=session_id(), sequence=0)

    def _close_locked(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.sock.close()
        except OSError:
            pass
        self._conn = None


def _default_socket_factory(host: str, port: int, timeout: float) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    return sock


def _u16(n: int) -> bytes:
    return int(n).to_bytes(2, "big")


def _decode_receiver_blob(blob: bytes) -> ReceiverInfo:
    offset = 0
    fields = []
    while offset < len(blob) and len(fields) < 4:
        length = int.from_bytes(blob[offset:offset + 2], "big")
        offset += 2
        fields.append(blob[offset:offset + length].decode("utf-8"))
        offset += length
    while len(fields) < 4:
        fields.append("")
    return ReceiverInfo(
        callsign=fields[0],
        locator=fields[1],
        decoding_software=fields[2],
        antenna=fields[3],
    )
