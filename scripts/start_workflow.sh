#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export TZ="${TZ:-America/New_York}"
export PYTHONUNBUFFERED=1

LOG_DIR="$REPO_ROOT/logs"
LOG_FILE="$LOG_DIR/workflow.log"
LOCK_FILE="$LOG_DIR/workflow.pid"
PYTHON="$REPO_ROOT/.venv/bin/python"

mkdir -p "$LOG_DIR"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a "$LOG_FILE"
}

if [[ ! -x "$PYTHON" ]]; then
  log "ERROR: missing venv python at $PYTHON — run: python3 -m venv .venv && pip install -r requirements.txt"
  exit 1
fi

# Lock file / PID check
if [[ -f "$LOCK_FILE" ]]; then
  old_pid="$(cat "$LOCK_FILE" 2>/dev/null || true)"
  if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
    log "Workflow already running (pid $old_pid); skipping start."
    exit 0
  fi
  rm -f "$LOCK_FILE"
fi

if pgrep -f "${REPO_ROOT}/workflow.py" >/dev/null 2>&1; then
  log "Workflow process already running; skipping start."
  exit 0
fi

log "Starting workflow from $REPO_ROOT"

# Launch caffeinate in the background without exec so $! captures the PID
nohup caffeinate -dims "$PYTHON" -u "$REPO_ROOT/workflow.py" >>"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$LOCK_FILE"

log "Workflow started (PID $PID) — logging to $LOG_FILE"