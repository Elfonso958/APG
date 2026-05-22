#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SERVICE_NAME="${SERVICE_NAME:-apg-importer}"
BRANCH="${BRANCH:-}"

cd "$APP_DIR"

echo "==> Deploying from $APP_DIR"

if [[ -n "$BRANCH" ]]; then
  echo "==> Checking out $BRANCH"
  git fetch origin "$BRANCH"
  git checkout "$BRANCH"
fi

echo "==> Pulling latest changes"
git pull --ff-only

if [[ -d ".venv" ]]; then
  echo "==> Activating .venv"
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

echo "==> Running database migrations"
export FLASK_APP="${FLASK_APP:-wsgi.py}"
flask db upgrade

echo "==> Restarting $SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "==> Checking service status"
sudo systemctl --no-pager --full status "$SERVICE_NAME"

echo "==> Deploy complete"
