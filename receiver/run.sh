#!/usr/bin/env bash
# Runs the alert receiver. Creates the venv on first use. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  echo "Creating venv and installing dependencies..."
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt
fi

# Set RECEIVER_TOKEN to require `Authorization: Bearer <token>` on /alerts.
# See the README for wiring the same token into Alertmanager.
exec .venv/bin/python receiver.py
