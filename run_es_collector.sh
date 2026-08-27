#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="$PROJECT_DIR/logs"

cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON" ]]; then
  print -u2 "Missing virtual environment: $PYTHON"
  exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="$PROJECT_DIR/gcs-sa.json"

exec caffeinate -dimsu "$PYTHON" "$PROJECT_DIR/data_collector_workflow.py"