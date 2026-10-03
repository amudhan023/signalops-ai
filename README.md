# SignalOps AI

A local observability stack that gives an AI incident-response agent something
real to investigate, plus a fake service that produces a reproducible incident
on demand. The fake service is also a streaming producer: it writes every
request it handles to a Kafka topic, at hundreds of events per second.

The repository holds three things: the **infrastructure** (`infra/`), an
**incident simulator** (`simulator/`, a Kafka producer), and the **alert receiver**
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
./dev.sh up                          # needs sudo once, for vm.max_map_count
```

`dev.sh up` starts the containers (through `infra/up.sh`), then the receiver,
the simulator and sigops-sim in the background. It waits for each to answer its health
check and prints the endpoints. The first run builds two Python venvs, so it
takes a minute.

| Command | What it does |
| --- | --- |
| `./dev.sh up` | start everything; safe to re-run |
| `./dev.sh status` | containers, host processes, and their health |
| `./dev.sh logs [simulator\|receiver\|sigops-sim]` | follow a host process log |
| `./dev.sh restart` | restart the host processes only, e.g. after editing code |
| `./dev.sh down` | stop everything, keep data |
| `./dev.sh down --wipe` | stop everything, delete all container volumes |

Host process pid files and logs live in `.run/` (gitignored). You can still run
any piece in the foreground instead: `./infra/up.sh`, `./simulator/run.sh`,
`./receiver/run.sh`.

**sigops-sim** is the checkout-api simulator behind the Control Plane's Simulate
page ([sigops-sim-service](https://github.com/amudhan023/sigops-sim-service),
port 8200). It lives in its own repo: `dev.sh` looks for it next to this one,
or wherever `SIGOPS_SIM_DIR` points, and skips it with a note if it is missing.

`infra/.env` is gitignored because it holds a password. `up.sh` stops with a
clear message if it is missing.

## Ports

| Service | Port | Notes |
| --- | --- | --- |
| Simulator | 8000 | host process: metrics, fault controls |
| Receiver | 8080 | host process: Alertmanager webhook |
| Grafana | 3000 | anonymous admin, datasources and dashboard provisioned from code |
| OTLP gRPC / HTTP | 4317 / 4318 | send logs and traces here |
| Postgres | 5432 | pgvector + pg_trgm, for incident memory |
| Redis | 6379 | dedup state, nothing persisted |
| Prometheus | 9090 | 15s scrape, 15d retention |
| OpenSearch | 9200 | indices `sre-logs`, `sre-traces` |
| Alertmanager | 9093 | webhooks to host `:8080/alerts` |
| Kafka | 29092 | topics `incidents` (3 partitions), `payment-events` (6 partitions, 5-minute retention) |
| OpenSearch Dashboards | 5601 | opt-in: `docker compose --profile dashboards up -d` |

## The simulator: a streaming payment service

`simulator/` is a Python package, `payment_stream`. It pretends to be
`payment-api` for tenant `acme`. It runs one loop, 10 ticks per second, and
for every simulated request it does four things:

1. Records request metrics for Prometheus to scrape on `:8000/metrics`.
2. Produces one JSON event to the Kafka topic `payment-events`, keyed by
   customer id, so one customer's events stay in order on one partition.
3. For some requests, sends a trace (3 spans) over OTLP to the collector, which
   writes it to OpenSearch `sre-traces`.
4. For some requests, sends a log line the same way, into `sre-logs`.

An event looks like this:

```json
{"schema": "signalops.payment-event.v1", "event_id": "23b998a0-...",
 "occurred_at_ms": 1790825657930, "tenant": "acme", "service": "payment-api",
 "trace_id": null,
 "payment": {"customer_id": "cus_03712", "merchant_id": "mer_010",
             "amount_minor": 422157, "currency": "USD", "method": "wallet"},
 "outcome": {"status": "approved", "latency_ms": 82.2, "db_pool_max": 50}}
```

When the request has a trace, `trace_id` is set and the Kafka record carries a
W3C `traceparent` header. A consumer can then continue the same trace.

### Which requests get a trace

Every failed request gets one. Healthy requests are sampled at 2%
(`TRACE_SAMPLE_RATIO`). The workload decides this after it knows the outcome,
in `Workload.should_trace` (`simulator/payment_stream/workload.py`). Plain
head sampling would also drop 98% of the timeouts, and an error log whose
trace was never stored is a dead end for whoever investigates it.

Healthy requests also log an INFO line at 1% (`INFO_LOG_RATIO`). Every timeout
logs an ERROR line, inside its trace, so the log carries the trace id.

### The topic and its 5-minute retention

`infra/docker-compose.yml` creates `payment-events` with `retention.ms=300000`.
Kafka only deletes *closed* log segments, and by default a segment closes after
a week or 1 GB. So the topic also sets `segment.ms=60000`, and the broker checks
retention every 30 s instead of every 5 minutes. With all three, an event lives
between 5 and about 6.5 minutes. With `retention.ms` alone, nothing would ever
be deleted at this volume.

The broker has `auto.create.topics.enable=false`. A typo in `KAFKA_TOPIC` fails
at startup with a clear message instead of quietly creating a new topic with
default retention. The simulator checks the topic before it starts.

Change the retention with `PAYMENT_EVENTS_RETENTION_MS` in `infra/.env`, then
`docker compose -f infra/docker-compose.yml up kafka-init`. The init step is
safe to re-run: it creates what is missing and re-applies the config.

Watch the stream:

```bash
docker exec sre-copilot-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment-events --property print.headers=true
```

### Backpressure and broker outages

The producer uses `acks=all` with idempotence, so a retry never writes a
duplicate. If the local queue fills (100k events), the loop blocks until Kafka
catches up and counts each wait in `payment_events_backpressure_total`. If the
broker stays down longer than `KAFKA_DELIVERY_TIMEOUT_MS` (30 s), the producer
drops the event and counts it in `payment_events_delivery_failures_total{reason}`.

After an outage the loop does not burst the missed events. A burst would hide
the outage in the throughput graph.

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVENTS_PER_SECOND` | `500` | target rate, 1 to 20000 |
| `KAFKA_BOOTSTRAP` | `localhost:29092` | |
| `KAFKA_TOPIC` | `payment-events` | must already exist |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `30000` | give up on an event after this long |
| `OTLP_HTTP` | `http://localhost:4318` | collector, for traces and logs |
| `TRACE_SAMPLE_RATIO` | `0.02` | share of *healthy* requests traced |
| `INFO_LOG_RATIO` | `0.01` | share of healthy, untraced requests logged |
| `TENANT` / `SERVICE` | `acme` / `payment-api` | labels on every signal |
| `SIMULATOR_PORT` | `8000` | |

The receiver also reads `KAFKA_TOPIC` (default `incidents`). Do not export it
in a shell that runs `./dev.sh up`, or both processes get the same topic.

### HTTP endpoints

| Endpoint | What it does |
| --- | --- |
| `GET /` | health and current state |
| `GET /metrics` | Prometheus exposition |
| `GET /deployments` | change history, the "what changed recently?" evidence |
| `POST /break` | connection pool 50 -> 10, recorded as deploy `v1.42.0` |
| `POST /heal` | back to 50, recorded as deploy `v1.42.1` |
| `POST /rate?eps=N` | change the event rate live |

### Tests

```bash
./simulator/run.sh test
```

The tests check the shape of the incident and of the telemetry. Healthy p99
stays under the 1-second alert threshold, and broken p99 crosses 4 seconds.
Every error log links to a stored trace. The `traceparent` header matches the
event's `trace_id`. No attribute key breaks the OpenSearch mapping. Kafka and
the collector are replaced by in-memory fakes, so the tests need no running
stack.

## Producing an incident

```bash
curl -X POST localhost:8000/break   # connection pool 50 -> 10
curl -X POST localhost:8000/heal    # back to 50
```

`/break` changes one thing, and every signal moves at once:

- p99 latency crosses 4 seconds
- database timeouts appear in `sre-logs`, each with a trace id
- the `SELECT payments.transaction` span stretches from ~40 ms to ~3.8 s
- events on `payment-events` start to carry `"status": "timeout"`
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

### Alerts

| Alert | Fires when |
| --- | --- |
| `PaymentAPIHighLatency` | request p99 > 1 s for 1m |
| `PaymentAPIDatabaseErrors` | database timeouts > 1/s for 1m |
| `PaymentEventsDeliveryFailing` | any event fails Kafka delivery, for 1m |
| `PaymentEventsThroughputLow` | acknowledged rate < 80% of target, for 2m |
| `PaymentEventsDeliveryLatencyHigh` | Kafka ack p99 > 1 s, for 2m |
| `SimulatorDown` | Prometheus cannot scrape `:8000` or sigops-sim on `:8200`, for 1m |
| `CheckoutAPIErrors` | one error type is > 5% of checkout-api requests over 5m, for 2m (one alert per `error_type`) |
| `CheckoutAPIHighLatency` | checkout-api request p99 > 2 s, for 2m |
| `CheckoutAPIMemoryHigh` | checkout-api memory > 1.2 GiB (60% of its limit), for 1m |
| `CheckoutAPIOOMKilled` | checkout-api restarted after an OOMKill in the last 15m |
| `CheckoutAPICertificateExpired` | the payments-gateway certificate has expired, for 1m |

Try the stream alerts with `docker stop sre-copilot-kafka-1`. Start it again
with `docker start sre-copilot-kafka-1`.

### Dashboard

Grafana provisions **SignalOps → payment-api — stream and requests** from
`infra/grafana/dashboards/payment-api.json`
(<http://localhost:3000/d/payment-api-stream>). It shows throughput against
target, delivery failures, Kafka ack latency, the producer queue, request
latency, database timeouts, pool size, and the latest error logs from
OpenSearch. Firing alerts appear as red annotations.

Edits made in the Grafana UI are lost on restart. Export the JSON and commit
it instead. Grafana's Alerting pages also read Alertmanager through a
provisioned datasource.

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
life of the index. The simulator sends only `service.name`, and the tests
guard the rule.

**Grafana reads spans with `startTime`, not `@timestamp`.** The exporter leaves
`@timestamp` at the zero time on spans. See
`infra/grafana/provisioning/datasources/datasources.yml`.

**There is no index rotation policy, and the incident is what fills the disk.**
Sampling keeps the healthy case small, but every failed request keeps its
trace. Rates at the default 500 events/s, about 265 bytes per document:

| State | Spans/sec | Logs/sec | Growth |
| --- | --- | --- | --- |
| healthy | ~30 (2% of 500, 3 spans each) | ~15 | ~1 GB/day |
| `/break`, measured | 329 (~21% of requests time out) | 109 | ~10 GB/day |

An hour of incident costs about as much as ten hours of healthy traffic. Add
an ISM rollover policy before a long run, or wipe the volumes between sessions
with `./dev.sh down --wipe`. Raising `EVENTS_PER_SECOND` raises these figures
in proportion.

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
