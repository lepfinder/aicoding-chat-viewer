#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${SCRIPT_DIR}/stop.sh" || true
sleep 1
exec "${SCRIPT_DIR}/start.sh"
