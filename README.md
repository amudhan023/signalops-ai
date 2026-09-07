# SignalOps AI

A local observability stack that gives an AI incident-response agent something
real to investigate, plus a fake service that produces a reproducible incident
on demand.

The repository holds two things: the **infrastructure** (`infra/`) and an
**incident simulator** (`simulator/`). The agent itself is not here yet.

## The split: containers vs. host

`infra/docker-compose.yml` runs only the stateful plane — the parts you never
want to restart. Everything you edit constantly runs on the host and connects
to the published ports:

| Runs in Docker | Runs on the host |
| --- | --- |
| Prometheus, Alertmanager, OpenSearch, OTel Collector, Kafka, Redis, Postgres, Grafana | simulator, alert receiver, agent worker, MCP servers |

Containers reach host processes through `host.docker.internal`, which the
compose file maps to `host-gateway`. Prometheus uses it to scrape the
simulator; Alertmanager uses it to POST alerts to your receiver.

## Quick start

```bash
cp infra/.env.example infra/.env     # then set POSTGRES_PASSWORD
./infra/up.sh                        # needs sudo once, for vm.max_map_count
python3 simulator/simulator.py       # separate terminal
```

`up.sh` prints every endpoint when it finishes. Stop with `./infra/down.sh`,
or `./infra/down.sh --wipe` to delete the data volumes too.

`infra/.env` is gitignored because it holds a password. `up.sh` stops with a
clear message if it is missing.

## Ports

| Service | Port | Notes |
| --- | --- | --- |
| Grafana | 3000 | anonymous admin, datasources provisioned from code |
| OTLP gRPC / HTTP | 4317 / 4318 | send logs and traces here |
| Postgres | 5432 | pgvector + pg_trgm, for incident memory |
| Redis | 6379 | dedup state, nothing persisted |
| Prometheus | 9090 | 15s scrape, 15d retention |
| OpenSearch | 9200 | indices `sre-logs`, `sre-traces` |
| Alertmanager | 9093 | webhooks to host `:8080/alerts` |
| Kafka | 29092 | topic `incidents`, 3 partitions |
| OpenSearch Dashboards | 5601 | opt-in: `docker compose --profile dashboards up -d` |

## Producing an incident

The simulator serves one tenant (`acme`) and one service (`payment-api`) at 20
requests per second, and emits all four evidence sources an engineer would
check: metrics, logs, traces, and deployments.

```bash
curl -X POST localhost:8000/break   # connection pool 50 -> 10
curl -X POST localhost:8000/heal    # back to 50
```

`/break` changes one thing, and every signal moves at once:

- p99 latency crosses 4 seconds
- database timeouts appear in `sre-logs`
- the `payment -> database` span stretches from ~40 ms to ~3.8 s
- a deployment record appears saying `maxPoolSize: 50 -> 10`

Alerts fire about 90 seconds later: the rule waits `for: 1m`, then Alertmanager
holds for `group_wait: 30s`.

That last point is what makes this useful for testing an agent. The root cause
is known, so "did the agent get it right?" becomes a repeatable check instead
of a judgment call.

Endpoints: `GET /metrics`, `GET /deployments`, `GET /` (health and current
state), `POST /break`, `POST /heal`. Configure with `TENANT`, `SERVICE`,
`OTLP_HTTP`, `SIMULATOR_PORT`, `RPS`.

```bash
python3 simulator/simulator.py --selftest
```

The self-test asserts the shape of the incident: healthy p99 stays under the
1-second alert threshold, broken p99 crosses 4 seconds, histogram buckets stay
monotonic, and OTLP ids are the right length. Run it after editing the
simulator to confirm the demo still demos.

## Design decisions worth knowing

**Alertmanager groups by `tenant` + `service`, not by alert name.** Grouping by
name would deliver high latency and database errors as two separate pages.
Grouping by service delivers one incident with several symptoms, which is what
you want to hand an agent. This is why `prometheus.yml` insists the simulator
label its metrics with `tenant` and `service`.

**OpenSearch indices use `replicas: 0`.** A single node cannot allocate a
replica, so any other value leaves every index yellow forever.
`infra/opensearch/init.sh` sets this in the index templates.

**No attribute key may be a dotted prefix of another.** The OpenSearch exporter
flattens attributes, so sending both `service` and `service.name` asks the
index to map one key as a string and an object at once. It rejects that for the
life of the index. The simulator sends only `service.name`, and the self-test
guards the rule.

**Grafana reads spans with `startTime`, not `@timestamp`.** The exporter leaves
`@timestamp` at the zero time on spans. See
`infra/grafana/provisioning/datasources/datasources.yml`.

**There is no log rotation policy.** At 20 rps, `sre-traces` grows by roughly
900 MB per day. Add an ISM rollover policy before a long run, or wipe the
volumes between sessions with `./infra/down.sh --wipe`.

## Not included

The alert receiver (`:8080`), the agent worker, and the MCP servers. The stack
publishes the ports they need and Alertmanager is already configured to call
the receiver, but nothing listens there yet.
