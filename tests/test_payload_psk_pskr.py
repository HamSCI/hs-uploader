"""PSKReporter wire-format encoder unit tests.

These tests cross-check our encoder against constants from
ftlib-pskreporter (``/usr/local/bin/pskreporter.py``) so wire
compatibility is asserted at the byte level: protocol version,
template descriptor headers, length fields, padding, per-spot field
order.
"""

from __future__ import annotations

from hs_uploader.payload.psk_pskr import (
    MAX_PACKET_LEN_TCP,
    PROTOCOL_VERSION,
    ReceiverInfo,
    Spot,
    build_packet,
    chunk_spots,
    encode_receiver_info,
    encode_spot,
    session_id,
)


_SPOT = Spot(
    callsign="K1ABC",
    frequency_hz=14_074_000,
    snr_db=-12,
    mode="FT8",
    locator="FN42aa",
    timestamp=1_700_000_000,
    message_bytes=bytes.fromhex("aabbccdd"),
)


def test_protocol_version():
    assert PROTOCOL_VERSION == b"\x00\x0a"


def test_session_id_length():
    assert len(session_id()) == 4
    # Two consecutive calls should differ (vanishing probability of clash).
    assert session_id() != session_id()


def test_encode_spot_field_order():
    enc = encode_spot(_SPOT)
    # callsign: 1-byte length + "K1ABC"
    assert enc[:6] == b"\x05K1ABC"
    # frequency: 4 bytes BE.
    assert enc[6:10] == (14_074_000).to_bytes(4, "big")
    # snr: 1 signed byte.
    assert enc[10] == (256 + (-12)) % 256
    # mode: 1-byte length + "FT8"
    assert enc[11:15] == b"\x03FT8"
    # locator: 1-byte length + "FN42aa"
    assert enc[15:22] == b"\x06FN42aa"
    # informationSource = 0x01
    assert enc[22] == 0x01
    # timestamp: 4 bytes BE.
    assert enc[23:27] == (1_700_000_000).to_bytes(4, "big")
    # message_bytes: 1-byte length + 4 bytes
    assert enc[27] == 0x04
    assert enc[28:32] == bytes.fromhex("aabbccdd")
    # No trailing bytes.
    assert len(enc) == 32


def test_encode_spot_clamps_snr():
    spot = Spot(
        callsign="K1", frequency_hz=1, snr_db=10_000, mode="FT8",
        locator="", timestamp=0,
    )
    enc = encode_spot(spot)
    # snr byte (after callsign + freq) clamped to 127.
    assert enc[2 + 1 + 4] == 127

    spot = Spot(
        callsign="K1", frequency_hz=1, snr_db=-200, mode="FT8",
        locator="", timestamp=0,
    )
    enc = encode_spot(spot)
    # -128 in two's complement = 0x80
    assert enc[2 + 1 + 4] == 0x80


def test_encode_spot_truncates_long_strings():
    long = "X" * 500
    spot = Spot(
        callsign=long, frequency_hz=1, snr_db=0, mode="FT8",
        locator="", timestamp=0,
    )
    enc = encode_spot(spot)
    assert enc[0] == 255  # length byte capped
    assert enc[1:256] == b"X" * 255


def test_receiver_info_padded_to_4():
    rx = ReceiverInfo(
        callsign="A",   # 1+1 = 2 bytes
        locator="",     # 1+0 = 1 byte
        decoding_software="",  # 1+0 = 1 byte
        antenna="",     # 1+0 = 1 byte
    )
    blob = encode_receiver_info(rx)
    # Body is 5 bytes, padded to 8; plus delim+length = 4 → total 12.
    assert len(blob) == 12
    # Length field reflects body+4.
    assert blob[2:4] == (12).to_bytes(2, "big")
    # First two bytes are the receiver delimiter.
    assert blob[:2] == bytes([0x99, 0x92])


def test_build_packet_minimal():
    rx = ReceiverInfo(
        callsign="K1ABC", locator="FN42aa",
        decoding_software="hs-uploader/0.1",
    )
    enc = [encode_spot(_SPOT)]
    pkt = build_packet(
        receiver=rx,
        encoded_spots=enc,
        timestamp=1_700_000_000,
        sequence=0,
        session_id=b"\x01\x02\x03\x04",
    )
    # Protocol version first.
    assert pkt[0:2] == PROTOCOL_VERSION
    # Length field equals total length.
    assert int.from_bytes(pkt[2:4], "big") == len(pkt)
    # Timestamp matches.
    assert pkt[4:8] == (1_700_000_000).to_bytes(4, "big")
    # Session id matches.
    assert pkt[12:16] == b"\x01\x02\x03\x04"


def test_build_packet_rejects_bad_session_id():
    rx = ReceiverInfo(callsign="K", locator="")
    import pytest
    with pytest.raises(ValueError):
        build_packet(
            receiver=rx, encoded_spots=[], timestamp=0, sequence=0,
            session_id=b"\x01\x02\x03",   # 3 bytes
        )


def test_chunk_spots_respects_packet_cap():
    rx = ReceiverInfo(callsign="K1ABC", locator="FN42aa")
    # 100 small spots, packet cap that forces 2-3 chunks.
    encoded = [encode_spot(_SPOT) for _ in range(100)]
    chunks = list(chunk_spots(encoded, max_packet_len=600, receiver=rx))
    assert len(chunks) >= 2
    # Every chunk should pack into a packet under the cap.
    for chunk in chunks:
        pkt = build_packet(
            receiver=rx,
            encoded_spots=chunk,
            timestamp=0,
            sequence=0,
            session_id=b"\x00" * 4,
        )
        assert len(pkt) <= 600

    # All input spots accounted for.
    assert sum(len(c) for c in chunks) == 100
