#!/usr/bin/env bash
# Brings up the whole SRE Copilot infrastructure. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"

# Compose reads .env for every image tag and the Postgres password. Without it
# each ${VAR} expands to an empty string and the failure is confusing, so stop
# here instead. .env is gitignored -- a fresh clone will not have one.
if [ ! -f .env ]; then
  echo "Missing $PWD/.env" >&2
  echo >&2
  echo "It is gitignored because it holds POSTGRES_PASSWORD. Create it from the" >&2
  echo "template, then set a password of your own:" >&2
  echo >&2
  echo "  cp $PWD/.env.example $PWD/.env" >&2
  echo >&2
  exit 1
fi

# OpenSearch refuses to start below this. Persist it so reboots keep working.
NEEDED=262144
CURRENT=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
if [ "$CURRENT" -lt "$NEEDED" ]; then
  echo "Raising vm.max_map_count $CURRENT -> $NEEDED (needs sudo, OpenSearch requires it)"
  sudo sysctl -w vm.max_map_count=$NEEDED
  if ! grep -q '^vm.max_map_count' /etc/sysctl.conf 2>/dev/null; then
    echo "vm.max_map_count=$NEEDED" | sudo tee -a /etc/sysctl.conf > /dev/null
  fi
fi

docker compose up -d

# The one-shot init containers exit when done, and `--wait` counts an exited
# container as a failure even on exit 0 -- so wait only on the long-running ones.
docker compose up -d --wait \
  prometheus alertmanager opensearch otel-collector kafka redis postgres grafana

# ...and check the one-shot ones ourselves.
for s in opensearch-init kafka-init; do
  code=$(docker inspect -f '{{.State.ExitCode}}' "$(docker compose ps -aq "$s")")
  if [ "$code" != 0 ]; then
    echo "$s failed (exit $code):" >&2
    docker compose logs "$s" >&2
    exit 1
  fi
done

cat <<'ENDPOINTS'

Infrastructure up. Connect your apps to:

  Prometheus        http://localhost:9090
  Alertmanager      http://localhost:9093    -> POSTs to host :8080/alerts
  Grafana           http://localhost:3000    (anonymous admin)
  OpenSearch        http://localhost:9200    indices: sre-logs, sre-traces
  OTLP gRPC         localhost:4317           send logs + traces here
  OTLP HTTP         localhost:4318
  Kafka             localhost:29092          topic: incidents (3 partitions)
  Redis             localhost:6379
  Postgres          localhost:5432           srecopilot/srecopilot

Still yours to build and run on the host:
  simulator (:8000, must export tenant+service labels), receiver (:8080),
  agent worker, MCP servers.

  ./down.sh          stop, keep data
  ./down.sh --wipe   stop, delete all volumes
ENDPOINTS
