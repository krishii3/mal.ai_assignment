#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
if command -v python3 >/dev/null 2>&1; then
    exec python3 "$ROOT_DIR/run.py" "$@"
fi

exec python "$ROOT_DIR/run.py" "$@"
