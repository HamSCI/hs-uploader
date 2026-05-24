#!/bin/bash
#
# hs-uploader installation/upgrade script
#
# hs-uploader is consumed two ways:
#
#   1. As a LIBRARY by mag-recorder / psk-recorder / wspr-recorder
#      (and wsprdaemon-client).  Each of those projects installs
#      hs-uploader editable into ITS OWN per-client venv at install
#      time -- nothing here touches that path.
#
#   2. As an OPS CLI (`hs-uploader status|peek|reset-cursor|kick`)
#      against /var/lib/hs-uploader/watermarks.db.  THIS script installs
#      that CLI into /opt/hs-uploader/venv and symlinks the binary onto
#      $PATH so operators can invoke it.
#
# Tooling: uv (https://astral.sh/uv) is the canonical venv + installer
# across the sigmond suite.  If it's not present, this script installs
# it system-wide to /usr/local/bin via the Astral installer.
#
# Idempotent.  Installs or upgrades:
#   - uv (if missing) at /usr/local/bin/uv
#   - Python venv at /opt/hs-uploader/venv
#   - /usr/local/bin/hs-uploader -> /opt/hs-uploader/venv/bin/hs-uploader
#
# No service user, no systemd unit, no udev rule -- the CLI runs
# interactively (typically as root or via a sigmond-group member who
# can read /var/lib/hs-uploader/watermarks.db).
#
# Usage:
#   sudo ./install.sh              # install or upgrade
#   sudo ./install.sh --uninstall  # remove
#
# To force-recreate the venv (e.g. after a Python upgrade):
#   sudo rm -rf /opt/hs-uploader/venv && sudo ./install.sh
#

set -e

INSTALL_DIR="/opt/hs-uploader"

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

check_root() {
    [[ $EUID -eq 0 ]] || error "Run as root (sudo)."
}

# Ensures `uv` is on PATH.  If missing, installs from astral.sh into
# /usr/local/bin so root + every interactive user sees it (service
# users don't need uv at runtime -- once the venv exists, Python
# imports are self-contained).
_ensure_uv() {
    if command -v uv >/dev/null 2>&1; then
        info "  uv $(uv --version 2>/dev/null | awk '{print $2}') at $(command -v uv)"
        return
    fi
    info "  uv not found -- installing system-wide to /usr/local/bin"
    command -v curl >/dev/null || error "curl not found (apt install curl)"
    # Astral installer honors XDG_BIN_HOME for target dir; UV_NO_MODIFY_PATH=1
    # because /usr/local/bin is already on every shell's PATH (the older
    # --no-modify-path flag is deprecated as of uv 0.11.x).
    if ! curl -LsSf https://astral.sh/uv/install.sh | env XDG_BIN_HOME=/usr/local/bin UV_NO_MODIFY_PATH=1 sh; then
        error "uv installer failed"
    fi
    command -v uv >/dev/null || error "uv installer ran but uv is still not on PATH"
    info "  uv $(uv --version 2>/dev/null | awk '{print $2}') installed"
}

check_dependencies() {
    info "Checking dependencies..."
    command -v python3 >/dev/null || error "python3 not found"
    _ensure_uv
}

install_application() {
    info "Installing Python application to ${INSTALL_DIR}..."
    if [[ ! -d "$INSTALL_DIR/venv" ]]; then
        install -d -m 0755 "$INSTALL_DIR"
        # --seed populates pip/setuptools/wheel for compatibility with
        # tooling that shells out to pip; harmless overhead otherwise.
        uv venv "$INSTALL_DIR/venv" --python 3.11 --seed --quiet
    fi
    # Pre-clean any leftover egg-info from prior dev installs in the
    # source tree -- setuptools' "Cannot update time stamp" check
    # inside the build sandbox would abort the editable install if
    # ownership has drifted.  Safe to delete; uv recreates it.
    rm -rf "$REPO_ROOT/src"/*.egg-info "$REPO_ROOT"/*.egg-info 2>/dev/null || true
    # Editable install: $REPO_ROOT is the canonical source.  Bug fixes
    # there take effect immediately for the next CLI invocation, no
    # re-install required.
    uv pip install --quiet --python "$INSTALL_DIR/venv/bin/python3" -e "$REPO_ROOT"
    # Symlink the venv entry point so `hs-uploader` works on $PATH.
    # setuptools generates the script with shebang
    # `#!/opt/hs-uploader/venv/bin/python3`, so the symlink resolves
    # to the venv interpreter automatically.
    ln -sfn "$INSTALL_DIR/venv/bin/hs-uploader" /usr/local/bin/hs-uploader
    info "  $(/usr/local/bin/hs-uploader --help 2>&1 | head -1 || echo 'hs-uploader installed')"
}

uninstall() {
    info "Removing hs-uploader CLI install (library installs in client venvs are untouched)..."
    rm -f /usr/local/bin/hs-uploader
    info "Removed /usr/local/bin/hs-uploader."
    info "Kept (delete by hand if desired): ${INSTALL_DIR}"
    info "Per-client venv installs (mag/psk/wspr-recorder, wsprdaemon-client) are untouched."
}

main() {
    check_root
    if [[ "${1:-}" == "--uninstall" ]]; then
        uninstall
        return
    fi
    check_dependencies
    install_application
    info "Install complete."
    info "Try:  hs-uploader status"
    info "      (reads /var/lib/hs-uploader/watermarks.db; needs sigmond group or root)"
}

main "$@"
