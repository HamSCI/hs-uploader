"""The shared WSPR/FST4W mode table and its fallbacks."""

import pytest

from hs_uploader import wspr_modes
from hs_uploader.wspr_modes import resolve


@pytest.mark.parametrize("name,wsprnet,wsprdaemon,period", [
    ("W2", 2, 2, 120), ("F2", 3, 3, 120), ("F5", 5, 6, 300),
    ("F15", 15, 16, 900), ("F30", 30, 31, 1800)])
def test_each_mode_carries_both_wire_codes(name, wsprnet, wsprdaemon, period):
    m = wspr_modes.BY_NAME[name]
    assert (m.wsprnet_code, m.wsprdaemon_code, m.period_s) == (
        wsprnet, wsprdaemon, period)


def test_wsprdaemon_codes_are_unique():
    assert len(wspr_modes.BY_WSPRDAEMON_CODE) == len(wspr_modes.MODES)


def test_name_wins_over_stored_number():
    assert resolve({"mode": "f15", "pkt_mode": 2}).name == "F15"


@pytest.mark.parametrize("cols,name", [
    ({"mode": "PKT16", "pkt_mode": 16}, "F15"),   # wspr-recorder's old name
    ({"mode": "PKT31", "pkt_mode": 31}, "F30"),
    ({"pkt_mode": 6}, "F5"),
    ({"code": 3}, "F2"),
    ({"code": 16, "pkt_mode": 2}, "F15"),
])
def test_stored_number_resolves_when_the_name_does_not(cols, name):
    assert resolve(cols).name == name


@pytest.mark.parametrize("cols", [{}, {"mode": "PKT99", "pkt_mode": 99},
                                  {"pkt_mode": "x"}, {"pkt_mode": 5}])
def test_unknown_resolves_to_none(cols):
    assert resolve(cols) is None
