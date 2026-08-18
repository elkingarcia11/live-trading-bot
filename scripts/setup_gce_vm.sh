#!/usr/bin/env bash
# One-time GCE VM bootstrap: clone repo, venv, systemd, GitHub deploy SSH key.
#
# Run ON the VM (after creating the instance), e.g.:
#   curl -fsSL https://raw.githubusercontent.com/elkingarcia11/live-trading-bot/main/scripts/setup_gce_vm.sh | bash
# Or copy this repo and: ./scripts/setup_gce_vm.sh
#
# Then add GitHub repository secrets (Settings → Secrets → Actions):
#   GCE_SSH_HOST     — VM external IP or hostname
#   GCE_SSH_USER     — user that ran this script (e.g. ubuntu)
#   GCE_SSH_PRIVATE_KEY — contents of ~/.ssh/github_deploy (private key)

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/elkingarcia11/live-trading-bot.git}"
REPO_ROOT="${REPO_ROOT:-${HOME}/live-trading-bot}"
BRANCH="${BRANCH:-main}"
SERVICE_NAME="${SYSTEMD_SERVICE:-live-trading-bot}"
DEPLOY_KEY="${HOME}/.ssh/github_deploy"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

log "Installing system packages…"
sudo apt-get update -qq
sudo apt-get install -y -qq git python3-venv python3-pip

if [[ ! -d "${REPO_ROOT}/.git" ]]; then
  log "Cloning ${REPO_URL} → ${REPO_ROOT}"
  git clone --branch "${BRANCH}" "${REPO_URL}" "${REPO_ROOT}"
else
  log "Repository already exists at ${REPO_ROOT}"
fi

cd "${REPO_ROOT}"
chmod +x scripts/*.sh 2>/dev/null || true

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  log "Creating ${REPO_ROOT}/.env from .env.example — fill in secrets before starting"
  cp .env.example .env
fi

log "Creating Python venv…"
python3 -m venv "${REPO_ROOT}/.venv"
"${REPO_ROOT}/.venv/bin/pip" install --upgrade pip
"${REPO_ROOT}/.venv/bin/pip" install -r requirements.txt

mkdir -p "${REPO_ROOT}/logs"

if [[ ! -f "${DEPLOY_KEY}" ]]; then
  log "Generating SSH key for GitHub Actions deploy (${DEPLOY_KEY})"
  ssh-keygen -t ed25519 -f "${DEPLOY_KEY}" -N "" -C "github-actions-deploy"
  cat "${DEPLOY_KEY}.pub" >> "${HOME}/.ssh/authorized_keys"
  chmod 600 "${DEPLOY_KEY}" "${HOME}/.ssh/authorized_keys"
  log "--- Add this private key to GitHub → Settings → Secrets → GCE_SSH_PRIVATE_KEY ---"
  cat "${DEPLOY_KEY}"
  log "--- End private key ---"
else
  log "Deploy key already exists at ${DEPLOY_KEY}"
fi

log "Installing systemd unit ${SERVICE_NAME}…"
sed -e "s|DEPLOY_USER|${USER}|g" \
    -e "s|REPO_ROOT|${REPO_ROOT}|g" \
    deploy/live-trading-bot.service | sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"

SUDOERS_FILE="/etc/sudoers.d/${SERVICE_NAME}"
if [[ ! -f "${SUDOERS_FILE}" ]]; then
  log "Allowing passwordless systemctl restart for ${USER}…"
  printf '%s ALL=(ALL) NOPASSWD: /bin/systemctl restart %s, /bin/systemctl status %s\n' \
    "${USER}" "${SERVICE_NAME}" "${SERVICE_NAME}" | sudo tee "${SUDOERS_FILE}" >/dev/null
  sudo chmod 440 "${SUDOERS_FILE}"
fi

log "Bootstrap complete."
log "  1. Edit ${REPO_ROOT}/.env with DATABENTO_API_KEY, GMAIL_APP_PASSWORD, etc."
log "  2. Ensure VM service account can write gs://${GOOGLE_CLOUD_PROJECT:-live-trading-bot}"
log "  3. Add GitHub secrets: GCE_SSH_HOST, GCE_SSH_USER=${USER}, GCE_SSH_PRIVATE_KEY"
log "  4. Start: sudo systemctl start ${SERVICE_NAME}"
log "  5. Logs: tail -f ${REPO_ROOT}/logs/workflow.log"
