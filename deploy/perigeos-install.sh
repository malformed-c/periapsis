#!/bin/bash
set -euo pipefail

# Install perigeos binary and systemd service.
# Usage: ./perigeos-install.sh [binary-path]
#
# Defaults:
#   binary:  ./perigeos (from go build)
#   config:  /etc/perigeos/perigeos.toml
#   state:   /var/lib/perigeos/
#   socket:  /run/perigeos.sock

BINARY="${1:-./perigeos}"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/perigeos"
SERVICE_FILE="$(dirname "$0")/perigeos.service"

if [ "$(id -u)" -ne 0 ]; then
    echo "Must run as root" >&2
    exit 1
fi

if [ ! -f "$BINARY" ]; then
    echo "Binary not found: $BINARY" >&2
    echo "Build with: go build -o perigeos ./cmd/perigeos" >&2
    exit 1
fi

# Install binary
install -m 0755 "$BINARY" "$INSTALL_DIR/perigeos"
echo "Installed $INSTALL_DIR/perigeos"

# Create config dir if needed
mkdir -p "$CONFIG_DIR"
if [ ! -f "$CONFIG_DIR/perigeos.toml" ]; then
    echo "No config found at $CONFIG_DIR/perigeos.toml"
    echo "Copy a config from configs/ to get started:"
    echo "  cp configs/perigeos.toml $CONFIG_DIR/perigeos.toml"
fi

# Install systemd service
install -m 0644 "$SERVICE_FILE" /etc/systemd/system/perigeos.service
systemctl daemon-reload
echo "Installed perigeos.service"
echo ""
echo "Usage:"
echo "  systemctl enable --now perigeos    # start and enable on boot"
echo "  journalctl -u perigeos -f          # follow logs"
echo "  systemctl restart perigeos         # restart"
