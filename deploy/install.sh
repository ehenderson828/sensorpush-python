#!/usr/bin/env bash
# Installs the sensorpush systemd service.
# Run from anywhere on the target machine (e.g. the Pi):
#   sudo ./deploy/install.sh [service-user]
# Defaults to the user who invoked sudo.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo: sudo $0 [service-user]" >&2
    exit 1
fi

SERVICE_USER="${1:-${SUDO_USER:-root}}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_SRC="$REPO_DIR/deploy/sensorpush.service"
UNIT_DST="/etc/systemd/system/sensorpush.service"

# Prefer a repo-local venv if one exists, otherwise system python3
if [[ -x "$REPO_DIR/venv/bin/python" ]]; then
    PYTHON="$REPO_DIR/venv/bin/python"
elif [[ -x "$REPO_DIR/.venv/bin/python" ]]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    PYTHON="$(command -v python3)"
fi

if [[ ! -f "$REPO_DIR/.env" ]]; then
    echo "WARNING: $REPO_DIR/.env not found - Supabase writes will fail without SUPABASE_URL/SUPABASE_KEY" >&2
fi

sed -e "s|{{USER}}|$SERVICE_USER|g" \
    -e "s|{{WORKDIR}}|$REPO_DIR|g" \
    -e "s|{{PYTHON}}|$PYTHON|g" \
    "$UNIT_SRC" > "$UNIT_DST"

systemctl daemon-reload
systemctl enable --now sensorpush.service

echo "Installed and started sensorpush.service (user=$SERVICE_USER, python=$PYTHON)"
echo "Follow logs with: journalctl -u sensorpush -f"
