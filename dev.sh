#!/usr/bin/env bash
# Runs the whole SignalOps stack on this machine: the containers in infra/
# plus the host processes (simulator, receiver, sigops-sim).
#
#   ./dev.sh up             start everything (idempotent)
#   ./dev.sh down [--wipe]  stop everything; --wipe also deletes container data
#   ./dev.sh restart        stop and start the host processes only
#   ./dev.sh status         what is running, and is it healthy
#   ./dev.sh logs [name]    follow a host process log (default: simulator)
#
# Host processes run in the background. Their pid files and logs live in
# .run/ (gitignored). Container logs: docker compose -f infra/docker-compose.yml logs
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$PWD
RUN=$ROOT/.run

# name | start command | health URL | comma-separated cmdline patterns
# The patterns guard against a stale pid file whose pid the OS has since
# given to some unrelated process -- we must never signal that one. run.sh
# matches while it builds the venv, the module name once it has exec'd.
SERVICES=(
  "receiver|$ROOT/receiver/run.sh|http://localhost:8080/healthz|receiver/run.sh,receiver\.py"
  "simulator|$ROOT/simulator/run.sh|http://localhost:8000/|simulator/run.sh,payment_stream"
)
# sigops-sim (checkout-api, driven by the Control Plane's Simulate page) lives
# in its own repo. Clone it next to this one, or point SIGOPS_SIM_DIR at it;
# without it the stack still runs and dev.sh says what it skipped.
SIGOPS_SIM_DIR=${SIGOPS_SIM_DIR:-$ROOT/../sigops-sim-service}
if [ -x "$SIGOPS_SIM_DIR/run.sh" ]; then
  SIGOPS_SIM_DIR=$(cd "$SIGOPS_SIM_DIR" && pwd)
  SERVICES+=("sigops-sim|$SIGOPS_SIM_DIR/run.sh|http://localhost:8200/|sigops-sim-service/run.sh,sim_service")
else
  echo "Note: no sigops-sim-service at $SIGOPS_SIM_DIR; skipping sigops-sim." >&2
fi
# The first start builds a venv and pip-installs, which can take a minute.
START_TIMEOUT=180
STOP_TIMEOUT=20   # the simulator spends up to 10s flushing Kafka on SIGTERM

field() { cut -d'|' -f"$2" <<<"$1"; }

pid_of() {   # prints the pid if the named process is ours and alive
  local name=$1 match=$2 pid
  [ -f "$RUN/$name.pid" ] || return 1
  pid=$(cat "$RUN/$name.pid")
  if kill -0 "$pid" 2>/dev/null && grep -qaE "${match//,/|}" "/proc/$pid/cmdline" 2>/dev/null; then
    echo "$pid"
  else
    rm -f "$RUN/$name.pid"
    return 1
  fi
}

start_one() {
  local svc=$1 name cmd url match pid
  name=$(field "$svc" 1); cmd=$(field "$svc" 2); url=$(field "$svc" 3); match=$(field "$svc" 4)
  if pid=$(pid_of "$name" "$match"); then
    echo "  $name already running (pid $pid)"
    return
  fi
  mkdir -p "$RUN"
  # setsid: its own session, so Ctrl-C in this terminal does not reach it.
  setsid nohup "$cmd" >>"$RUN/$name.log" 2>&1 < /dev/null &
  echo $! > "$RUN/$name.pid"
  printf "  %-10s starting (pid %s) " "$name" "$!"

  local deadline=$((SECONDS + START_TIMEOUT))
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    if ! kill -0 "$(cat "$RUN/$name.pid")" 2>/dev/null; then
      echo "exited. Last log lines:"
      tail -n 15 "$RUN/$name.log" | sed 's/^/    /'
      rm -f "$RUN/$name.pid"
      return 1
    fi
    if [ $SECONDS -ge $deadline ]; then
      echo "not healthy after ${START_TIMEOUT}s; see $RUN/$name.log"
      return 1
    fi
    printf "."
    sleep 2
  done
  echo " ok"
}

stop_one() {
  local svc=$1 name match pid
  name=$(field "$svc" 1); match=$(field "$svc" 4)
  if ! pid=$(pid_of "$name" "$match"); then
    echo "  $name not running"
    return
  fi
  printf "  %-10s stopping (pid %s) " "$name" "$pid"
  kill -TERM "$pid"
  local deadline=$((SECONDS + STOP_TIMEOUT))
  while kill -0 "$pid" 2>/dev/null; do
    if [ $SECONDS -ge $deadline ]; then
      echo "still alive after ${STOP_TIMEOUT}s, killing"
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
    sleep 1
  done
  rm -f "$RUN/$name.pid"
  echo "stopped"
}

start_hosts() {
  echo "Host processes:"
  # Receiver first: an alert that fires while it is down is retried by
  # Alertmanager, but there is no reason to make it.
  local svc
  for svc in "${SERVICES[@]}"; do start_one "$svc"; done
}

stop_hosts() {
  echo "Host processes:"
  # Reverse order: stop the producer of alerts before their consumer.
  local i
  for ((i = ${#SERVICES[@]} - 1; i >= 0; i--)); do stop_one "${SERVICES[$i]}"; done
}

status() {
  echo "Containers:"
  docker compose -f infra/docker-compose.yml ps --format '  {{.Service}}\t{{.State}}\t{{.Status}}' \
    | column -t -s $'\t' | sed 's/^/  /'
  echo "Host processes:"
  local svc name url match pid health
  for svc in "${SERVICES[@]}"; do
    name=$(field "$svc" 1); url=$(field "$svc" 3); match=$(field "$svc" 4)
    if pid=$(pid_of "$name" "$match"); then
      health=$(curl -fsS -o /dev/null -w ok "$url" 2>/dev/null || echo unhealthy)
      printf "  %-10s running  pid %-8s %s  %s\n" "$name" "$pid" "$health" "$url"
    else
      printf "  %-10s stopped\n" "$name"
    fi
  done
}

case "${1:-}" in
  up)
    ./infra/up.sh
    echo
    start_hosts
    cat <<'EOF'

Everything is up.
  Dashboard     http://localhost:3000/d/payment-api-stream
  Start the incident:   curl -X POST localhost:8000/break
  End it:               curl -X POST localhost:8000/heal
  Change event rate:    curl -X POST 'localhost:8000/rate?eps=2000'
  Watch the stream:     docker exec sre-copilot-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
                          --bootstrap-server localhost:9092 --topic payment-events
  checkout-api incidents (sigops-sim, :8200): use the Control Plane's Simulate page
EOF
    ;;
  down)
    stop_hosts
    shift
    ./infra/down.sh "$@"
    ;;
  restart)
    stop_hosts
    start_hosts
    ;;
  status)
    status
    ;;
  logs)
    name=${2:-simulator}
    [ -f "$RUN/$name.log" ] || { echo "no log for '$name' in $RUN" >&2; exit 1; }
    exec tail -n 50 -F "$RUN/$name.log"
    ;;
  *)
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
