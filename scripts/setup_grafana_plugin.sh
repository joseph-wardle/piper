#!/usr/bin/env bash
# Download and extract the motherduck-duckdb-datasource Grafana plugin.
#
# This plugin is unsigned and not in the Grafana catalog, so it must be
# installed manually.  Run this once before `podman compose up -d`.
#
# Usage:
#   scripts/setup_grafana_plugin.sh

set -euo pipefail

PLUGIN_VERSION="0.4.0"
PLUGIN_ID="motherduck-duckdb-datasource"
DOWNLOAD_URL="https://github.com/motherduckdb/grafana-duckdb-datasource/releases/download/v${PLUGIN_VERSION}/${PLUGIN_ID}-${PLUGIN_VERSION}.zip"
PLUGIN_DIR="$(cd "$(dirname "$0")/.." && pwd)/grafana/plugins"
TARGET="${PLUGIN_DIR}/${PLUGIN_ID}"

if [ -d "$TARGET" ]; then
    echo "Plugin already installed at $TARGET"
    exit 0
fi

mkdir -p "$PLUGIN_DIR"
TMP=$(mktemp /tmp/motherduck-XXXXXX.zip)
echo "Downloading ${PLUGIN_ID} v${PLUGIN_VERSION}..."
curl -fL -o "$TMP" "$DOWNLOAD_URL"
unzip -q "$TMP" -d "$PLUGIN_DIR"
rm "$TMP"
echo "Installed to $TARGET"
