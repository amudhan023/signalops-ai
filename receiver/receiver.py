#!/usr/bin/env python3
"""
Alert receiver — the bridge from Alertmanager to the incident event log.

Alertmanager POSTs a grouped alert notification here (see
infra/alertmanager/alertmanager.yml). This process does three things and
nothing else:

  1. verify the sender
  2. resolve tenant + service
  3. write one incident to the Kafka `incidents` topic

Everything downstream -- diagnosis, retrieval, notification -- belongs to the
agent worker, which consumes the topic.

  POST /alerts    Alertmanager webhook, schema version 4
  GET  /healthz   liveness, plus Kafka and Redis reachability

Run:  python3 receiver/receiver.py
"""
import hashlib
import hmac
import json
import os
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Producer

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

# ---------- config ----------

PORT = int(os.environ.get("RECEIVER_PORT", "8080"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "incidents")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Optional shared secret. When set, Alertmanager must send
# `Authorization: Bearer <token>`. Unset means no token check -- fine on a
# laptop where only Docker can reach the port, not fine anywhere else.
TOKEN = os.environ.get("RECEIVER_TOKEN", "")

# Alertmanager re-sends a still-firing group every repeat_interval (4h).
# Without this, each repeat becomes another incident and the agent
# investigates the same outage again.
DEDUP_TTL = int(os.environ.get("DEDUP_TTL_SECONDS", "3600"))

MAX_BODY = 4 * 1024 * 1024   # Alertmanager batches; 4 MB is generous.
SCHEMA = "sre-copilot.incident.v1"

# ---------- helpers ----------

def log(msg):
    print("%s receiver: %s" % (datetime.now(timezone.utc).isoformat(timespec="seconds"), msg),
          flush=True)


def resolve_target(payload):
    """Find tenant + service.

    Alertmanager groups by ["tenant", "service"], so groupLabels carries both
    and is the authoritative source. commonLabels and the first alert are
    fallbacks for a hand-rolled POST or a misconfigured route.
    """
    for source in ("groupLabels", "commonLabels"):
        labels = payload.get(source) or {}
        tenant, service = labels.get("tenant"), labels.get("service")
        if tenant and service:
            return tenant, service, source
    for alert in payload.get("alerts") or []:
        labels = alert.get("labels") or {}
        tenant, service = labels.get("tenant"), labels.get("service")
        if tenant and service:
            return tenant, service, "alerts[0].labels"
    return "unknown", "unknown", "missing"


def build_incident(payload, tenant, service, resolved_from):
    alerts = payload.get("alerts") or []
    fingerprints = sorted(a.get("fingerprint", "") for a in alerts)

    # Stable id for this exact set of alerts in this state. Same outage
    # re-notified -> same key, which is what dedup keys on.
    digest = hashlib.sha256(
        ("%s|%s|%s|%s" % (tenant, service, payload.get("status", ""), ",".join(fingerprints))).encode()
    ).hexdigest()[:16]

    severities = [(a.get("labels") or {}).get("severity") for a in alerts]
    for level in ("critical", "warning", "info"):
        if level in severities:
            severity = level
            break
    else:
        severity = "unknown"

    starts = sorted(a.get("startsAt") for a in alerts if a.get("startsAt"))

    return {
        "schema": SCHEMA,
        "incident_key": "%s/%s/%s" % (tenant, service, digest),
        "tenant": tenant,
        "service": service,
        "status": payload.get("status", "unknown"),
        "severity": severity,
        "received_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "started_at": starts[0] if starts else None,
        "alert_count": len(alerts),
        "alertnames": sorted({(a.get("labels") or {}).get("alertname", "") for a in alerts}),
        "group_key": payload.get("groupKey"),
        "tenant_service_resolved_from": resolved_from,
        "external_url": payload.get("externalURL"),
        "alerts": [
            {
                "status": a.get("status"),
                "labels": a.get("labels") or {},
                "annotations": a.get("annotations") or {},
                "startsAt": a.get("startsAt"),
                "endsAt": a.get("endsAt"),
                "fingerprint": a.get("fingerprint"),
                "generatorURL": a.get("generatorURL"),
            }
            for a in alerts
        ],
    }


class Dedup:
    """Redis-backed suppression of repeat notifications.

    Fails open on purpose. Redis holds throwaway state, so losing it should
    cost us a duplicate incident, never a dropped one.
    """

    def __init__(self, url):
        self.client = None
        if redis_lib is None:
            log("redis library not installed -- dedup disabled")
            return
        try:
            self.client = redis_lib.from_url(url, socket_timeout=2, socket_connect_timeout=2)
            self.client.ping()
            log("dedup active via %s (ttl %ds)" % (url, DEDUP_TTL))
        except Exception as exc:
            self.client = None
            log("redis unreachable (%s) -- dedup disabled, duplicates will pass through" % exc)

    def is_duplicate(self, incident_key):
        if self.client is None:
            return False
        try:
            # SET NX returns None when the key already exists.
            return not self.client.set("dedup:%s" % incident_key, "1", nx=True, ex=DEDUP_TTL)
        except Exception as exc:
            log("redis error (%s) -- passing through" % exc)
            return False


# ---------- kafka ----------

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "client.id": "sre-copilot-receiver",
})
dedup = Dedup(REDIS_URL)


def publish(incident):
    """Produce one incident. Returns (ok, detail).

    The key is tenant/service, not the incident id. Kafka hashes the key to
    pick a partition, so every incident for one service lands on the same
    partition and stays in order -- while three partitions still let you run
    three workers.
    """
    result = {}
    done = threading.Event()

    def on_delivery(err, msg):
        result["err"] = err
        if msg is not None and err is None:
            result["partition"] = msg.partition()
            result["offset"] = msg.offset()
        done.set()

    producer.produce(
        TOPIC,
        key=("%s/%s" % (incident["tenant"], incident["service"])).encode(),
        value=json.dumps(incident, separators=(",", ":")).encode(),
        callback=on_delivery,
    )
    producer.flush(10)

    if not done.wait(1):
        return False, "delivery callback did not fire within timeout"
    if result.get("err") is not None:
        return False, str(result["err"])
    return True, "partition %s offset %s" % (result.get("partition"), result.get("offset"))


# ---------- http ----------

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass   # We do our own logging; the default writes to stderr per request.

    def _reply(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self):
        if not TOKEN:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        # Constant-time compare so a wrong token cannot be guessed by timing.
        return hmac.compare_digest(header[7:], TOKEN)

    def do_GET(self):
        if self.path != "/healthz":
            self._reply(404, {"error": "not found"})
            return
        try:
            kafka_ok = producer.list_topics(timeout=3).topics is not None
        except Exception:
            kafka_ok = False
        self._reply(200 if kafka_ok else 503, {
            "status": "ok" if kafka_ok else "degraded",
            "kafka": {"bootstrap": BOOTSTRAP, "topic": TOPIC, "reachable": kafka_ok},
            "dedup": {"enabled": dedup.client is not None, "ttl_seconds": DEDUP_TTL},
            "auth": {"token_required": bool(TOKEN)},
        })

    def do_POST(self):
        if self.path != "/alerts":
            self._reply(404, {"error": "not found"})
            return

        if not self._authorized():
            log("rejected unauthorized POST from %s" % self.client_address[0])
            self._reply(401, {"error": "unauthorized"})
            return

        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY:
            self._reply(413 if length > MAX_BODY else 400, {"error": "bad content-length"})
            return

        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._reply(400, {"error": "invalid json: %s" % exc})
            return

        # Shape check. Alertmanager's webhook schema is version "4".
        if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
            self._reply(400, {"error": "not an alertmanager webhook payload"})
            return
        if payload.get("version") not in ("4", None):
            self._reply(400, {"error": "unsupported webhook version %r" % payload.get("version")})
            return

        tenant, service, resolved_from = resolve_target(payload)
        if resolved_from == "missing":
            # Accept it anyway. A hard-to-triage incident beats a lost one.
            log("WARNING: no tenant/service label on groupKey=%r -- filing as unknown/unknown"
                % payload.get("groupKey"))

        incident = build_incident(payload, tenant, service, resolved_from)

        if dedup.is_duplicate(incident["incident_key"]):
            log("duplicate %s (%s) suppressed" % (incident["incident_key"], incident["status"]))
            self._reply(200, {"result": "duplicate", "incident_key": incident["incident_key"]})
            return

        ok, detail = publish(incident)
        if not ok:
            # 5xx makes Alertmanager retry. Never swallow a delivery failure.
            log("ERROR publishing %s: %s" % (incident["incident_key"], detail))
            self._reply(500, {"error": "kafka publish failed", "detail": detail})
            return

        log("filed %s status=%s severity=%s alerts=%d -> %s"
            % (incident["incident_key"], incident["status"], incident["severity"],
               incident["alert_count"], detail))
        self._reply(200, {"result": "filed", "incident_key": incident["incident_key"],
                          "detail": detail})


def main():
    log("listening on :%d  kafka=%s topic=%s  auth=%s"
        % (PORT, BOOTSTRAP, TOPIC, "bearer token" if TOKEN else "none"))
    if not TOKEN:
        log("no RECEIVER_TOKEN set -- any host that can reach :%d can file incidents" % PORT)
    try:
        ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        log("shutting down, flushing producer")
        producer.flush(10)
        sys.exit(0)


if __name__ == "__main__":
    main()
