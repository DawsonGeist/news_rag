#!/usr/bin/env bash
# Install (or update) the newsrag systemd user service for this checkout.
#   deploy/install-service.sh          install and enable, do not start
#   deploy/install-service.sh --start  install, enable and start now
# Run `loginctl enable-linger "$USER"` once so the service runs without a login session.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$unit_dir"
sed "s|@REPO@|$repo|g" "$repo/deploy/systemd/newsrag.service.in" > "$unit_dir/newsrag.service"
systemctl --user daemon-reload
systemctl --user enable newsrag.service
if [[ "${1:-}" == "--start" ]]; then
  systemctl --user restart newsrag.service
fi
systemctl --user --no-pager status newsrag.service | head -5 || true
