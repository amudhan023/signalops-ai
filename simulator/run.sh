#!/usr/bin/env bash
# Runs the payment-api stream simulator. Creates the venv on first use, and
# reinstalls when requirements.txt changes. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ] || [ requirements.txt -nt .venv/.installed ]; then
  echo "Installing dependencies into $PWD/.venv ..."
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt
  touch .venv/.installed
fi

if [ "${1:-}" = "test" ]; then
  exec .venv/bin/python -m unittest discover -s tests -t . -v
fi
exec .venv/bin/python -m payment_stream "$@"
