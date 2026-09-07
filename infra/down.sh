#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ "${1:-}" = "--wipe" ]; then
  docker compose down -v --remove-orphans
  echo "Stopped. All volumes deleted."
else
  docker compose down --remove-orphans
  echo "Stopped. Data volumes kept — ./down.sh --wipe to delete them."
fi
