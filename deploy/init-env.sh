#!/usr/bin/env bash
# Create deploy/.env for compose.yml (once). Keeps an existing file untouched.
set -euo pipefail
env_file="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.env"
if [[ -e "$env_file" ]]; then echo "$env_file exists; leaving it"; exit 0; fi
umask 077
cat > "$env_file" <<EOT
NEO4J_PASSWORD=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)
NEWSRAG_DATA_DIR=${NEWSRAG_DATA_DIR:-$HOME/newsrag-data}
EOT
echo "wrote $env_file"
