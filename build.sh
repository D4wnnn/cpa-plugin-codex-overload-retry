#!/usr/bin/env bash
# Build the cpa-plugin-codex-overload-retry CPA plugin (.so) for linux/amd64.
#
# Usage:
#   bash build.sh              # build cpa-plugin-codex-overload-retry.so in this directory
#   bash build.sh --deploy     # build and copy into the CPA plugin directory
set -euo pipefail
cd "$(dirname "$0")"

CGO_ENABLED=1 go build -buildmode=c-shared -trimpath -o cpa-plugin-codex-overload-retry.so .
echo "built: $(pwd)/cpa-plugin-codex-overload-retry.so"

if [[ "${1:-}" == "--deploy" ]]; then
    DEST="${2:-/data/chaoliu/servers/cpa/plugins/linux/amd64}"
    cp -v cpa-plugin-codex-overload-retry.so "$DEST/cpa-plugin-codex-overload-retry.so"
    echo "deployed to: $DEST"
    echo "restart cli-proxy-api (or use the management API) to load the plugin"
fi
