"""PskReporterTcp tests — verify wire bytes against a fake socket.

We inject a ``socket_factory`` that returns a ``FakeSocket`` whose
``sendall`` records the bytes; tests then parse those bytes back to
assert on protocol version, packet length, and encoded-spot field
order.  No real network I/O.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from hs_uploader import StationIdentity
from hs_uploader.core import Outcome, Record, RecordBatch
from hs_uploader.payload.psk_pskr import PROTOCOL_VERSION
from hs_uploader.transports import PskReporterTcp


class FakeSocket:
    def __init__(self):
        self.sent: bytes = b""
        self.closed = False
        self.options: List[tuple] = []

    def setsockopt(self, level, opt, val):
        self.options.append((level, opt, val))

    def settimeout(self, t):
        pass

    def sendall(self, data):
        if self.closed:
            raise OSError("socket closed")
        self.sent += data

    def close(self):
        self.closed = True


class FlakySocket(FakeSocket):
    """First N sendall() calls raise OSError; subsequent calls succeed."""

    def __init__(self, fail_first: int = 1):
        super().__init__()
        self._remaining_failures = fail_first

    def sendall(self, data):
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise OSError("connection reset (fake)")
        return super().sendall(data)


def _ident(call="K1ABC", grid="FN42aa") -> StationIdentity:
    return StationIdentity(call=call, grid=grid)


def _record(call="W1AW", **cols) -> Record:
    payload = {
        "tx_call": call,
        "frequency": 14_074_000,
        "snr_db": -10,
        "mode": "ft8",
        "grid": "FN31pr",
    }
    payload.update(cols)
    return Record(
        table="psk.spots",
        time=datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc),
        columns=payload,
    )


def test_ship_emits_one_packet_with_protocol_version():
    sock = FakeSocket()
    transport = PskReporterTcp(
        host="fake.local", port=4739,
        socket_factory=lambda h, p, t: sock,
    )
    batch = RecordBatch(records=(_record(),), cursor_after=b"x")
    outcome = transport.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert sock.sent[:2] == PROTOCOL_VERSION
    # Length field equals on-wire length.
    assert int.from_bytes(sock.sent[2:4], "big") == len(sock.sent)


def test_ship_skips_records_missing_callsign():
    sock = FakeSocket()
    transport = PskReporterTcp(
        socket_factory=lambda h, p, t: sock,
    )
    # Record with no tx_call — should be silently dropped.
    bad = Record(
        table="psk.spots",
        time=datetime(2026, 5, 8, tzinfo=timezone.utc),
        columns={"frequency": 14_074_000},
    )
    batch = RecordBatch(records=(bad,), cursor_after=b"x")
    outcome = transport.ship(batch, _ident())
    assert outcome.kind == "acked"
    assert sock.sent == b""    # nothing sent, but still acked


def test_ship_normalizes_mode_to_uppercase():
    sock = FakeSocket()
    transport = PskReporterTcp(socket_factory=lambda h, p, t: sock)
    batch = RecordBatch(
        records=(_record(mode="ft4"),),
        cursor_after=b"x",
    )
    transport.ship(batch, _ident())
    # FT4 should appear in the sent bytes as plain "FT4".
    assert b"FT4" in sock.sent


def test_ship_uses_snr_db_when_present_falls_back_to_score():
    sock1 = FakeSocket()
    PskReporterTcp(socket_factory=lambda h, p, t: sock1).ship(
        RecordBatch(records=(_record(snr_db=-15),), cursor_after=b"x"),
        _ident(),
    )
    # SNR byte position: header(16) + receiver-info-header(0x2C=44) +
    # sender-info-header(0x44=68) + receiver-info-block(variable) +
    # sender-block-delim(2) + sender-length(2) + first-spot bytes:
    # callsign(1+4) + freq(4) + snr(1) → snr is at the right offset
    # from the start of the encoded spot.  Easier: look for the
    # encoded SNR byte in the buffer.
    assert bytes([(256 + (-15)) % 256]) in sock1.sent

    # When snr_db is missing, fall back to score.
    sock2 = FakeSocket()
    PskReporterTcp(socket_factory=lambda h, p, t: sock2).ship(
        RecordBatch(
            records=(_record(snr_db=None, score=-5),),
            cursor_after=b"x",
        ),
        _ident(),
    )
    assert bytes([(256 + (-5)) % 256]) in sock2.sent


def test_socket_keepalive_is_set():
    import socket as _socket
    sock = FakeSocket()
    transport = PskReporterTcp(socket_factory=lambda h, p, t: sock)
    transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    keepalive = (_socket.SOL_SOCKET, _socket.SO_KEEPALIVE, 1)
    assert keepalive in sock.options


def test_send_failure_returns_retry_later():
    """All max_send_attempts_per_packet attempts fail → retry_later."""
    def factory(h, p, t):
        # Always fail.
        return FlakySocket(fail_first=99)
    transport = PskReporterTcp(
        socket_factory=factory,
        max_send_attempts_per_packet=3,
    )
    outcome = transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    assert outcome.kind == "retry_later"


def test_send_recovers_after_one_failure():
    """First send fails, transport reconnects, second send acks.

    Each ``socket_factory`` call returns a fresh FakeSocket so the
    "reconnect" produces a clean socket.  The test asserts the packet
    eventually lands.
    """
    sockets: List[FakeSocket] = []
    def factory(h, p, t):
        # First socket fails on its first sendall, second is fine.
        s = FlakySocket(fail_first=1) if not sockets else FakeSocket()
        sockets.append(s)
        return s

    transport = PskReporterTcp(
        socket_factory=factory,
        max_send_attempts_per_packet=3,
    )
    outcome = transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    assert outcome.kind == "acked"
    # The first socket got rebuilt; its sent buffer is empty (the
    # failed attempt didn't write anything).  The second socket has
    # the actual packet.
    assert sockets[0].closed
    assert sockets[1].sent[:2] == PROTOCOL_VERSION


def test_session_id_changes_after_reconnect():
    sockets: List[FakeSocket] = []
    def factory(h, p, t):
        s = FakeSocket()
        sockets.append(s)
        return s
    transport = PskReporterTcp(socket_factory=factory)

    # First ship.
    transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    sid1 = sockets[0].sent[12:16]

    # Force a disconnect and ship again.
    transport.close()
    transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    sid2 = sockets[1].sent[12:16]

    assert sid1 != sid2  # vanishingly small clash probability


def test_sequence_counter_increments_within_session():
    sock = FakeSocket()
    transport = PskReporterTcp(
        socket_factory=lambda h, p, t: sock,
        max_packet_len=10_000,
    )
    # First ship: sequence=0
    transport.ship(
        RecordBatch(records=(_record(),), cursor_after=b"x"),
        _ident(),
    )
    seq1 = int.from_bytes(sock.sent[8:12], "big")

    # Second ship on the SAME socket: sequence has advanced.
    transport.ship(
        RecordBatch(records=(_record(call="K1ABC"), _record(call="W2ABC")),
                    cursor_after=b"y"),
        _ident(),
    )
    # The second packet starts after the first's bytes.
    second_packet_start = len(sock.sent) - 0  # we'll find it differently:
    # Find offset of the SECOND header.  Easier: just decode the second
    # packet's sequence by reading from a known offset — we know the
    # first packet's length is in bytes 2:4.
    first_len = int.from_bytes(sock.sent[2:4], "big")
    seq2 = int.from_bytes(sock.sent[first_len + 8:first_len + 12], "big")
    assert seq2 > seq1


def test_replay_round_trip():
    """``serialize_for_retry`` → ``replay`` is wire-equivalent to a
    fresh ship."""
    fresh = FakeSocket()
    fresh_transport = PskReporterTcp(
        socket_factory=lambda h, p, t: fresh,
    )
    batch = RecordBatch(
        records=(_record(call="K1ABC"), _record(call="W1AW")),
        cursor_after=b"x",
    )
    blob = fresh_transport.serialize_for_retry(batch, _ident())

    replay = FakeSocket()
    replay_transport = PskReporterTcp(
        socket_factory=lambda h, p, t: replay,
    )
    outcome = replay_transport.replay(blob, _ident())
    assert outcome.kind == "acked"
    assert replay.sent[:2] == PROTOCOL_VERSION
    # Both callsigns should appear.
    assert b"K1ABC" in replay.sent
    assert b"W1AW" in replay.sent
