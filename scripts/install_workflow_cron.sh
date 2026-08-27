#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START_SCRIPT="$REPO_ROOT/scripts/start_workflow.sh"
CRON_LINE="3 23 * * * $START_SCRIPT # live-trading-bot workflow"

chmod +x "$START_SCRIPT"

# Safely append or update the cron line without wiping existing crontab
existing_cron="$(crontab -l 2>/dev/null || true)"

if echo "$existing_cron" | grep -qF "$START_SCRIPT"; then
  echo "Cron job already exists for $START_SCRIPT"
else
  (echo "$existing_cron"; echo "$CRON_LINE") | crontab -
  echo "Cron job installed successfully."
fi

echo ""
crontab -l