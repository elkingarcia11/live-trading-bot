#!/usr/bin/env bash
# Install (or refresh) a daily cron job that starts the workflow if it is not already running.
# With 24/7 ES streaming the process stays up; cron is a reboot/crash safety net.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START_SCRIPT="$REPO_ROOT/scripts/start_workflow.sh"
CRON_LINE="3 23 * * * $START_SCRIPT # live-trading-bot workflow"

chmod +x "$START_SCRIPT"

printf '%s\n' "$CRON_LINE" | crontab -

echo "Installed cron job (11:03 PM daily, system local time):"
crontab -l
echo ""
echo "Ensure macOS timezone matches America/New_York, or edit the hour in:"
echo "  crontab -e"
echo ""
echo "Logs: $REPO_ROOT/logs/workflow.log"
echo "Manual start: $START_SCRIPT"
