"""hs-uploader.service must start on a host without /var/lib/sigmond."""
import re
from pathlib import Path

UNIT = Path(__file__).resolve().parents[1] / "systemd" / "hs-uploader.service"


def test_unit_marks_the_sigmond_path_optional():
    found = 0
    for line in UNIT.read_text().splitlines():
        m = re.match(r"\s*ReadWritePaths=(.*)", line)
        if not m:
            continue
        for tok in m.group(1).split():
            if "/var/lib/sigmond" in tok:
                found += 1
                assert tok.startswith("-"), tok
    assert found, "no /var/lib/sigmond ReadWritePaths entry"
