"""PSKReporter wire-format encoder.

PSKReporter ingest at ``report.pskreporter.info:4739`` (TCP) accepts
IPFIX-style binary records.  Format reverse-engineered (and faithfully
mirrored) from ``ftlib-pskreporter`` (pjsg's reference implementation
at ``/usr/local/bin/pskreporter.py``).

Packet structure
================

::

  [Header: 16 bytes]
    0x00 0x0A                  protocol version (always 10)
    length                     uint16 BE — total packet length
    timestamp                  uint32 BE — unix epoch seconds
    sequence                   uint32 BE — running spot count from this id
    id                         4 random bytes — uploader session identity
  [ReceiverInfoHeader]         fixed 0x2C bytes — declares 4 string fields
  [SenderInfoHeader]           fixed 0x44 bytes — declares 8 sender fields
  [ReceiverInfo block]         encoded receiver string fields, padded to 4
  [SenderInfo block]           concatenated encoded spot records, padded to 4

Per-spot encoding (concatenated into the SenderInfo block)
==========================================================

::

  callsign        length-prefixed UTF-8
  frequency       uint32 BE — Hz
  snr             int8   — signed dB
  mode            length-prefixed UTF-8
  locator         length-prefixed UTF-8
  informationSource  uint8 — 0x01 = "automatically extracted"
  timestamp       uint32 BE — unix epoch seconds
  message_bytes   length-prefixed bytes — FT8/FT4 message bytes (may be empty)

Constants
=========

* ``MAX_PACKET_LEN_TCP``  25000 (ftlib's TCP cap)
* ``MAX_PACKET_LEN_UDP``  1400  (kept for completeness; v1 transports use TCP)

The encoder is pure / stateless except for ``id`` (random 4 bytes per
session) and ``sequence`` (running spot count) — both managed by the
``PskReporterTcp`` transport, not here.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, List, Tuple


MAX_PACKET_LEN_TCP = 25_000
MAX_PACKET_LEN_UDP = 1_400

PROTOCOL_VERSION = b"\x00\x0a"

_RECEIVER_DELIM = bytes([0x99, 0x92])
_SENDER_DELIM = bytes([0x99, 0x94])

# Frozen template descriptors lifted verbatim from
# ftlib-pskreporter.Uploader.getReceiverInformationHeader /
# getSenderInformationHeader.  These declare the IPFIX templates the
# server reads next.
_RECEIVER_INFO_HEADER = bytes(
    [0x00, 0x03, 0x00, 0x2C]
    + list(_RECEIVER_DELIM)
    + [0x00, 0x04, 0x00, 0x01]
    + [0x80, 0x02, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # receiverCallsign
    + [0x80, 0x04, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # receiverLocator
    + [0x80, 0x08, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # decodingSoftware
    + [0x80, 0x09, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # antennaInformation
    + [0x00, 0x00]                                       # padding
)

_SENDER_INFO_HEADER = bytes(
    [0x00, 0x02, 0x00, 0x44]
    + list(_SENDER_DELIM)
    + [0x00, 0x08]                                       # 8 fields
    + [0x80, 0x01, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # senderCallsign
    + [0x80, 0x05, 0x00, 0x04, 0x00, 0x00, 0x76, 0x8F]   # frequency
    + [0x80, 0x06, 0x00, 0x01, 0x00, 0x00, 0x76, 0x8F]   # snr
    + [0x80, 0x0A, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # mode
    + [0x80, 0x03, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # senderLocator
    + [0x80, 0x0B, 0x00, 0x01, 0x00, 0x00, 0x76, 0x8F]   # informationSource
    + [0x00, 0x96, 0x00, 0x04]                           # flowStartSeconds
    + [0x80, 0x0E, 0xFF, 0xFF, 0x00, 0x00, 0x76, 0x8F]   # messageBits
)


@dataclass(frozen=True)
class Spot:
    """One PSKReporter spot row.

    ``message_bytes`` is the raw FT8/FT4 message bytes (the WSJT-X
    "msgbits" if available, otherwise an empty byte string is fine —
    the field is required by the wire format but the server tolerates
    empty values).
    """

    callsign: str          # sender callsign
    frequency_hz: int      # >= 0; transmitted as uint32 BE
    snr_db: int            # signed; clamped to -128..127 in encoding
    mode: str              # 'FT8' / 'FT4' / ...
    locator: str           # sender Maidenhead grid (may be empty)
    timestamp: int         # unix epoch seconds
    message_bytes: bytes = b""


@dataclass(frozen=True)
class ReceiverInfo:
    """Reception-report header — the receiver describing itself."""

    callsign: str
    locator: str
    decoding_software: str = "hs-uploader/0.1"
    antenna: str = ""


def encode_spot(spot: Spot) -> bytes:
    return (
        _enc_str(spot.callsign)
        + int(spot.frequency_hz).to_bytes(4, "big")
        + _i8(spot.snr_db)
        + _enc_str(spot.mode)
        + _enc_str(spot.locator)
        + bytes([0x01])                                  # informationSource = automatic
        + int(spot.timestamp).to_bytes(4, "big", signed=False)
        + _enc_bytes(spot.message_bytes)
    )


def encode_receiver_info(rx: ReceiverInfo) -> bytes:
    body = (
        _enc_str(rx.callsign)
        + _enc_str(rx.locator)
        + _enc_str(rx.decoding_software)
        + _enc_str(rx.antenna)
    )
    body = _pad(body, 4)
    return _RECEIVER_DELIM + (len(body) + 4).to_bytes(2, "big") + body


def build_packet(
    *,
    receiver: ReceiverInfo,
    encoded_spots: List[bytes],
    timestamp: int,
    sequence: int,
    session_id: bytes,
) -> bytes:
    """Build one IPFIX packet containing the given encoded spots.

    Caller is responsible for chunking spots so the resulting packet
    stays under the transport's max length (see ``chunk_spots``).
    """
    if len(session_id) != 4:
        raise ValueError(f"session_id must be 4 bytes, got {len(session_id)}")

    rx_info = encode_receiver_info(receiver)
    sender_info = _pad(b"".join(encoded_spots), 4)
    sender_block = _SENDER_DELIM + (len(sender_info) + 4).to_bytes(2, "big") + sender_info

    body = _RECEIVER_INFO_HEADER + _SENDER_INFO_HEADER + rx_info + sender_block
    total_length = 16 + len(body)

    header = (
        PROTOCOL_VERSION
        + total_length.to_bytes(2, "big")
        + int(timestamp).to_bytes(4, "big")
        + int(sequence).to_bytes(4, "big")
        + session_id
    )
    return header + body


def chunk_spots(
    encoded_spots: Iterable[bytes],
    max_packet_len: int = MAX_PACKET_LEN_TCP,
    *,
    receiver: ReceiverInfo,
) -> Iterable[List[bytes]]:
    """Yield sub-lists of ``encoded_spots`` such that each, when wrapped
    by ``build_packet`` with the given ``receiver``, stays under
    ``max_packet_len`` bytes.

    Mirrors ftlib's chunking logic: include the rx-info / template
    overhead in the budget so the worst case still fits.
    """
    rx_info = encode_receiver_info(receiver)
    overhead = (
        16
        + len(_RECEIVER_INFO_HEADER)
        + len(_SENDER_INFO_HEADER)
        + len(rx_info)
        + 4    # sender block delim + length field
    )
    budget = max_packet_len - overhead
    if budget <= 0:
        raise ValueError(
            f"max_packet_len {max_packet_len} too small for header overhead {overhead}"
        )

    chunk: List[bytes] = []
    chunk_size = 0
    for sp in encoded_spots:
        if chunk and chunk_size + len(sp) > budget:
            yield chunk
            chunk, chunk_size = [], 0
        chunk.append(sp)
        chunk_size += len(sp)
    if chunk:
        yield chunk


def session_id() -> bytes:
    """4 random bytes for one uploader session.  Stable across the
    lifetime of a single TCP connection, regenerated on reconnect.
    """
    import os
    return os.urandom(4)


# ---- internal encoders ----


def _enc_str(s: str) -> bytes:
    if s is None:
        s = ""
    raw = s.encode("utf-8")
    if len(raw) > 255:
        raw = raw[:255]
    return bytes([len(raw)]) + raw


def _enc_bytes(b: bytes) -> bytes:
    if b is None:
        b = b""
    if len(b) > 255:
        b = b[:255]
    return bytes([len(b)]) + b


def _i8(n: int) -> bytes:
    if n is None:
        n = -128
    n = max(-128, min(127, int(n)))
    return n.to_bytes(1, "big", signed=True)


def _pad(b: bytes, alignment: int) -> bytes:
    rem = (-len(b)) % alignment
    return b + b"\x00" * rem
