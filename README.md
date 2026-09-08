# SignalOps AI

A local observability stack that gives an AI incident-response agent something
real to investigate, plus a fake service that produces a reproducible incident
on demand.

The repository holds three things: the **infrastructure** (`infra/`), an
**incident simulator** (`simulator/`), and the **alert receiver**
(`receiver/`) that turns a fired alert into an incident on a Kafka topic. The
agent worker that consumes that topic is not here yet.

## The split: containers vs. host

`infra/docker-compose.yml` runs only the stateful plane — the parts you never
want to restart. Everything you edit constantly runs on the host and connects
to the published ports:

| Runs in Docker | Runs on the host |
| --- | --- |
| Prometheus, Alertmanager, OpenSearch, OTel Collector, Kafka, Redis, Postgres, Grafana | simulator, alert receiver, agent worker, MCP servers |

The simulator and the receiver are in this repository. The agent worker and the
MCP servers are not.

Containers reach host processes through `host.docker.internal`, which the
compose file maps to `host-gateway`. Prometheus uses it to scrape the
simulator; Alertmanager uses it to POST alerts to your receiver.

## Quick start

```bash
cp infra/.env.example infra/.env     # then set POSTGRES_PASSWORD
./infra/up.sh                        # needs sudo once, for vm.max_map_count
python3 simulator/simulator.py       # separate terminal
./receiver/run.sh                    # separate terminal, builds its venv once
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
holds for `group_wait: 30s`. A measured run, `POST /break` at `t+0`:

| Time | What happened |
| --- | --- |
| `t+90s` | alert active in Alertmanager |
| `t+107s` | receiver files the incident (1 alert, `PaymentAPIHighLatency`) |
| `t+135s` | `POST /heal` |
| `t+407s` | re-notification, now 2 alerts — `group_interval: 5m` |
| `t+707s` | `status=resolved` incident filed |

Recovery is slow on purpose. Both rules read `rate(...[5m])`, so the metric has
to walk a five-minute window down past the threshold before Prometheus will
call the alert resolved.

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

## Alert receiver

`receiver/` listens on `:8080` for Alertmanager's webhook. It does three things
and stops:

1. **Verify the sender.** Set `RECEIVER_TOKEN` and it requires
   `Authorization: Bearer <token>`, compared in constant time. Unset, it
   accepts any caller that can reach the port and says so at startup. It always
   checks the payload is a webhook schema version 4 document.
2. **Resolve tenant and service.** It reads `groupLabels` first, since
   Alertmanager groups by exactly those two keys. `commonLabels` and the first
   alert's labels are fallbacks. If none carry them, it files the incident as
   `unknown/unknown` and logs a warning — a hard-to-triage incident beats a
   dropped one.
3. **Write one incident to Kafka.** Topic `incidents`, keyed `tenant/service`.

Diagnosis, retrieval, and notification belong to the agent worker, not here.

**Endpoints:** `POST /alerts`, `GET /healthz` (reports Kafka reachability, dedup
state, and whether a token is required).

**Configuration** (all environment variables, all with defaults):
`RECEIVER_PORT`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `REDIS_URL`,
`RECEIVER_TOKEN`, `DEDUP_TTL_SECONDS`.

### The message it writes

```json
{
  "schema": "sre-copilot.incident.v1",
  "incident_key": "acme/payment-api/1ecca9b9ae9a4dfb",
  "tenant": "acme",
  "service": "payment-api",
  "status": "firing",
  "severity": "critical",
  "alert_count": 2,
  "alertnames": ["PaymentAPIDatabaseErrors", "PaymentAPIHighLatency"],
  "alerts": [ "...full label and annotation set per alert..." ]
}
```

One message, two alertnames. That is the grouping key doing its job: the agent
receives one incident with several symptoms instead of two pages it would have
to correlate itself.

### Requiring a token

Alertmanager's config file does not expand environment variables, so do not
paste a token into `alertmanager.yml` — it is committed. Use a mounted file
instead:

```yaml
# infra/alertmanager/alertmanager.yml
- url: http://host.docker.internal:8080/alerts
  send_resolved: true
  http_config:
    authorization:
      type: Bearer
      credentials_file: /etc/alertmanager/receiver_token
```

Mount `./alertmanager/receiver_token:/etc/alertmanager/receiver_token:ro` in
`docker-compose.yml`, add that file to `.gitignore`, and start the receiver
with the same value in `RECEIVER_TOKEN`.

## Design decisions worth knowing

**The receiver keys Kafka messages by `tenant/service`.** Kafka hashes the key
to choose a partition, so every incident for one service lands on the same
partition and stays in order, while three partitions still let you run three
workers in parallel. Keying by incident id instead would scatter one service's
history across all three.

**Repeat notifications are suppressed in Redis, and the suppression fails
open.** Alertmanager re-sends a still-firing group every `repeat_interval` (4h),
and each repeat would otherwise become a fresh incident. The receiver holds a
`SET NX` key per incident for `DEDUP_TTL_SECONDS`. The status is part of the
key, so a `resolved` notification is never swallowed by the `firing` one. If
Redis is unreachable the receiver logs it and publishes anyway: Redis holds
throwaway state, so losing it should cost a duplicate, never a dropped alert.

**A failed Kafka write returns HTTP 500.** Alertmanager retries on 5xx, so a
broker blip delays an incident instead of losing it. Returning 200 on a failed
publish would drop it silently.

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

**There is no index rotation policy, and the stack writes about 1.1 GB per
day.** Measured on a 10-minute window at the default 20 rps:

| Index | Docs/sec | Bytes/doc | Growth |
| --- | --- | --- | --- |
| `sre-traces` | 40.1 | 286 | **0.99 GB/day** |
| `sre-logs` | 2.8 | 278 | 0.07 GB/day |

Traces dominate because the simulator emits two spans per request, so 20 rps
becomes 40 documents per second. Add an ISM rollover policy before a long run,
or wipe the volumes between sessions with `./infra/down.sh --wipe`.

Re-measure it yourself — take two samples ten minutes apart and multiply the
document rate by the average document size:

```bash
Q='http://localhost:9200/_cat/indices/sre-*?h=index,docs.count,pri.store.size&bytes=b&v'
curl -s "$Q"; sleep 600; curl -s "$Q"
```

Use the *document* delta, not the store-size delta. Segment merges rewrite
files in the background, so raw size jumps around; document count only goes
up.

## Not included

The agent worker and the MCP servers. The receiver files incidents onto the
`incidents` topic; nothing consumes them yet. Read what is waiting there with:

```bash
docker exec sre-copilot-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic incidents --from-beginning
```
