#!/bin/bash
#
# hs-uploader installation/upgrade script
#
# hs-uploader is consumed two ways:
#
#   1. As a LIBRARY by mag-recorder / psk-recorder / wspr-recorder
#      (and wsprdaemon-client).  Each of those projects pip-installs
#      hs-uploader editable into ITS OWN per-client venv at install
#      time -- nothing here touches that path.
#
#   2. As an OPS CLI (`hs-uploader status|peek|reset-cursor|kick`)
#      against /var/lib/hs-uploader/watermarks.db.  THIS script installs
#      that CLI into /opt/hs-uploader/venv and symlinks the binary onto
#      $PATH so operators can invoke it.
#
# Idempotent.  Installs or upgrades:
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

check_dependencies() {
    info "Checking dependencies..."
    command -v python3 >/dev/null || error "python3 not found"
    python3 -c "import venv" 2>/dev/null || error "python3-venv missing (apt install python3-venv)"
}

install_application() {
    info "Installing Python application to ${INSTALL_DIR}..."
    if [[ ! -d "$INSTALL_DIR/venv" ]]; then
        install -d -m 0755 "$INSTALL_DIR"
        python3 -m venv "$INSTALL_DIR/venv"
    fi
    # Pre-clean any leftover egg-info from prior dev installs in the
    # source tree -- if it's owned by a different user, setuptools'
    # "Cannot update time stamp" check inside the build sandbox would
    # abort the editable install.  Safe to delete; pip recreates it.
    rm -rf "$REPO_ROOT/src"/*.egg-info "$REPO_ROOT"/*.egg-info 2>/dev/null || true
    "$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip setuptools wheel
    # Editable install: the source tree at $REPO_ROOT is the canonical
    # location; bug fixes there take effect immediately for the next
    # CLI invocation, no re-install required.
    "$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade -e "$REPO_ROOT"
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
