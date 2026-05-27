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

# Ensures `uv` is on PATH.  Delegates to sigmond's shared helper if
# present; falls back to an inline copy otherwise (covers the bootstrap
# case where sigmond hasn't been cloned -- hs-uploader doesn't
# otherwise require sigmond, so we don't force-clone it).  Keep the
# inline body in sync with sigmond/scripts/install/ensure_uv.sh.
_ENSURE_UV_SH="/opt/git/sigmond/sigmond/scripts/install/ensure_uv.sh"
if [[ -r "$_ENSURE_UV_SH" ]]; then
    # shellcheck source=/dev/null
    source "$_ENSURE_UV_SH"
else
    _ensure_uv() {
        if command -v uv >/dev/null 2>&1; then
            printf '[INFO]  uv %s at %s\n' "$(uv --version 2>/dev/null | awk '{print $2}')" "$(command -v uv)"
            return 0
        fi
        printf '[INFO]  uv not found -- installing system-wide to /usr/local/bin\n'
        command -v curl >/dev/null || { printf '[ERROR] curl not found (apt install curl)\n' >&2; return 1; }
        if ! curl -LsSf https://astral.sh/uv/install.sh | env XDG_BIN_HOME=/usr/local/bin UV_NO_MODIFY_PATH=1 sh; then
            printf '[ERROR] uv installer failed\n' >&2
            return 1
        fi
        command -v uv >/dev/null || { printf '[ERROR] uv installer ran but uv is still not on PATH\n' >&2; return 1; }
        printf '[INFO]  uv %s installed\n' "$(uv --version 2>/dev/null | awk '{print $2}')"
    }
fi

check_dependencies() {
    info "Checking dependencies..."
    command -v python3 >/dev/null || error "python3 not found"
    _ensure_uv || error "_ensure_uv failed"
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

ensure_runtime_state() {
    # The watermark store at /var/lib/hs-uploader is shared across HamSCI
    # client users (pskrec, wsprrec, ...) that all belong to the sigmond
    # supplementary group.  Without setgid root:sigmond perms on the dir,
    # the first user to construct a SqliteWatermarkStore creates it with
    # their own group and a second user from a different account hits
    # "attempt to write a readonly database" on its first ack — observed
    # on bee1 2026-05-12 and B4-100 2026-05-14.  Fix in three parts:
    #
    #   1. ensure the `sigmond` group exists (normally created by
    #      sigmond/install.sh's `useradd --user-group sigmond`, but
    #      hs-uploader can be installed standalone without sigmond);
    #   2. install tmpfiles.d/hs-uploader.conf so systemd-tmpfiles can
    #      enforce the perms on every boot (recovery from ownership drift);
    #   3. trigger systemd-tmpfiles --create now so /var/lib/hs-uploader
    #      exists with the right perms before the first watermark write.

    if ! getent group sigmond >/dev/null 2>&1; then
        info "Creating sigmond system group"
        groupadd --system sigmond
    fi

    if [[ -f "$REPO_ROOT/tmpfiles.d/hs-uploader.conf" ]]; then
        info "Installing tmpfiles.d/hs-uploader.conf → /etc/tmpfiles.d/"
        install -m 0644 "$REPO_ROOT/tmpfiles.d/hs-uploader.conf" \
                /etc/tmpfiles.d/hs-uploader.conf
        systemd-tmpfiles --create /etc/tmpfiles.d/hs-uploader.conf
    else
        warn "tmpfiles.d/hs-uploader.conf not found in repo — skipping"
    fi
}

main() {
    check_root
    if [[ "${1:-}" == "--uninstall" ]]; then
        uninstall
        return
    fi
    check_dependencies
    ensure_runtime_state
    install_application
    info "Install complete."
    info "Try:  hs-uploader status"
    info "      (reads /var/lib/hs-uploader/watermarks.db; needs sigmond group or root)"
}

main "$@"
