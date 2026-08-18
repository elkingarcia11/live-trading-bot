#!/usr/bin/env bash
# Pull latest main and restart the live workflow (run on the GCE VM).
# Invoked by GitHub Actions after every push to main, or manually on the VM.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BRANCH="${DEPLOY_BRANCH:-main}"
PYTHON="${REPO_ROOT}/.venv/bin/python"
SERVICE_NAME="${SYSTEMD_SERVICE:-live-trading-bot}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

if [[ ! -d "${REPO_ROOT}/.git" ]]; then
  log "ERROR: ${REPO_ROOT} is not a git repository"
  exit 1
fi

log "Deploy started in ${REPO_ROOT} (branch=${BRANCH})"

git fetch origin "${BRANCH}"
git reset --hard "origin/${BRANCH}"

if [[ ! -x "${PYTHON}" ]]; then
  log "Creating venv and installing dependencies…"
  python3 -m venv "${REPO_ROOT}/.venv"
  PYTHON="${REPO_ROOT}/.venv/bin/python"
fi

"${PYTHON}" -m pip install --upgrade pip
"${REPO_ROOT}/.venv/bin/pip" install -r "${REPO_ROOT}/requirements.txt"

mkdir -p "${REPO_ROOT}/logs"

if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
  log "Restarting systemd service ${SERVICE_NAME}…"
  sudo systemctl restart "${SERVICE_NAME}"
  sudo systemctl --no-pager status "${SERVICE_NAME}" || true
elif [[ -x "${REPO_ROOT}/scripts/start_workflow.sh" ]]; then
  log "systemd service not active; using start_workflow.sh"
  "${REPO_ROOT}/scripts/start_workflow.sh"
else
  log "ERROR: no ${SERVICE_NAME} service and no start_workflow.sh"
  exit 1
fi

log "Deploy complete ($(git rev-parse --short HEAD))"
