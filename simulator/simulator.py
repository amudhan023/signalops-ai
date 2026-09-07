#!/usr/bin/env python3
"""
Traffic simulator for the SRE Copilot stack.

Produces the four evidence sources deep dive 17 walks through, for one
service (payment-api) belonging to one tenant:

  metrics      GET /metrics          scraped by Prometheus
  logs         OTLP -> collector     indexed into sre-logs
  traces       OTLP -> collector     indexed into sre-traces
  deployments  GET /deployments      the "recent changes" step

Healthy, it answers in about 80 ms. POST /break reduces the connection pool
from 50 to 10 -- exactly the change in the walkthrough -- and every signal
moves at once: p99 crosses four seconds, database timeouts appear in the
logs, and the payment->database span goes from ~40 ms to ~3.8 s.

    python3 simulator.py              # run it, no dependencies
    curl -XPOST localhost:8000/break  # start the incident
    curl -XPOST localhost:8000/heal   # roll it back
    python3 simulator.py --selftest   # check the signal model
"""
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TENANT = os.environ.get("TENANT", "acme")
SERVICE = os.environ.get("SERVICE", "payment-api")
OTLP = os.environ.get("OTLP_HTTP", "http://localhost:4318").rstrip("/")
PORT = int(os.environ.get("SIMULATOR_PORT", "8000"))
RPS = int(os.environ.get("RPS", "20"))

# Finer than the default buckets between 2.5s and 5s, so histogram_quantile
# interpolates the p99 honestly instead of guessing across a 2.5s-wide bucket.
BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1,
           2.5, 3, 3.5, 4, 4.5, 5, 10]

POOL_HEALTHY, POOL_BROKEN = 50, 10

_lock = threading.Lock()
_counts = [0] * (len(BUCKETS) + 1)   # last slot is +Inf
_sum = 0.0
_total = 0
_db_errors = 0
_pool = POOL_HEALTHY
_deploys = [{
    "version": "v1.41.0", "tenant": TENANT, "service": SERVICE,
    "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3 * 86400)),
    "change": "routine dependency bump",
}]


def observe(seconds):
    """Record one request duration into the histogram."""
    global _sum, _total
    i = 0
    while i < len(BUCKETS) and seconds > BUCKETS[i]:
        i += 1
    with _lock:
        _counts[i] += 1
        _sum += seconds
        _total += 1


def render_metrics():
    labels = 'tenant="%s",service="%s"' % (TENANT, SERVICE)
    with _lock:
        counts, total, sm, errs, pool = list(_counts), _total, _sum, _db_errors, _pool
    out = [
        "# HELP payment_api_request_duration_seconds End-to-end request duration.",
        "# TYPE payment_api_request_duration_seconds histogram",
    ]
    running = 0
    for le, c in zip(BUCKETS, counts):
        running += c
        out.append('payment_api_request_duration_seconds_bucket{%s,le="%g"} %d'
                   % (labels, le, running))
    out += [
        'payment_api_request_duration_seconds_bucket{%s,le="+Inf"} %d' % (labels, total),
        "payment_api_request_duration_seconds_sum{%s} %f" % (labels, sm),
        "payment_api_request_duration_seconds_count{%s} %d" % (labels, total),
        "# HELP payment_api_database_errors_total Database calls that timed out.",
        "# TYPE payment_api_database_errors_total counter",
        "payment_api_database_errors_total{%s} %d" % (labels, errs),
        "# HELP payment_api_db_pool_size Configured maximum database connections.",
        "# TYPE payment_api_db_pool_size gauge",
        "payment_api_db_pool_size{%s} %d" % (labels, pool),
        "",
    ]
    return "\n".join(out)


def sample_request():
    """One request: (app_seconds, db_seconds, timed_out)."""
    app = max(0.001, random.gauss(0.040, 0.010))
    broken = _pool == POOL_BROKEN
    # With the pool cut to 10, most requests still find a free connection.
    # The rest queue behind one, and that wait is the whole incident.
    if broken and random.random() < 0.35:
        db = max(0.001, random.gauss(3.8, 0.35))
        return app, db, random.random() < 0.60
    return app, max(0.001, random.gauss(0.040, 0.008)), False


def _res():
    # Only `service.name`, never a plain `service` alongside it: the exporter
    # flattens these to resource.* and OpenSearch cannot map resource.service
    # as both a string and an object.
    return {"attributes": [
        {"key": "service.name", "value": {"stringValue": SERVICE}},
        {"key": "tenant", "value": {"stringValue": TENANT}},
    ]}


def _attrs(d):
    # Floats go over as doubleValue so OpenSearch maps them numerically and
    # the agent can aggregate on them; everything else is a string.
    return [{"key": k,
             "value": {"doubleValue": v} if isinstance(v, float)
                      else {"stringValue": str(v)}}
            for k, v in d.items()]


def build_payloads(requests):
    """Turn sampled requests into OTLP/JSON log and trace payloads."""
    spans, logs = [], []
    for app, db, timed_out in requests:
        trace_id = "%032x" % random.getrandbits(128)
        root, child = "%016x" % random.getrandbits(64), "%016x" % random.getrandbits(64)
        end = time.time_ns()
        start = end - int((app + db) * 1e9)
        db_start = start + int(app * 1e9)
        spans.append({
            "traceId": trace_id, "spanId": root, "name": "POST /payments", "kind": 2,
            "startTimeUnixNano": str(start), "endTimeUnixNano": str(end),
            "attributes": _attrs({"http.method": "POST", "http.route": "/payments",
                                  "http.status_code": 504 if timed_out else 200,
                                  "duration_ms": round((app + db) * 1000, 1)}),
            "status": {"code": 2 if timed_out else 1},
        })
        spans.append({
            "traceId": trace_id, "spanId": child, "parentSpanId": root,
            "name": "SELECT payments.transaction", "kind": 3,
            "startTimeUnixNano": str(db_start), "endTimeUnixNano": str(end),
            "attributes": _attrs({"db.system": "postgresql",
                                  "db.statement": "SELECT * FROM transaction WHERE id = $1",
                                  "db.pool.max": _pool,
                                  "duration_ms": round(db * 1000, 1)}),
            "status": {"code": 2 if timed_out else 1},
        })
        if timed_out:
            body = ("Database connection timeout after %dms waiting for a pooled "
                    "connection (pool max=%d)" % (db * 1000, _pool))
            sev, sev_text = 17, "ERROR"
        elif random.random() < 0.10:
            body = "Payment processed in %dms" % ((app + db) * 1000)
            sev, sev_text = 9, "INFO"
        else:
            continue
        logs.append({
            "timeUnixNano": str(end), "severityNumber": sev, "severityText": sev_text,
            "body": {"stringValue": body}, "traceId": trace_id, "spanId": root,
        })
    return (
        {"resourceLogs": [{"resource": _res(), "scopeLogs": [{"logRecords": logs}]}]} if logs else None,
        {"resourceSpans": [{"resource": _res(), "scopeSpans": [{"spans": spans}]}]} if spans else None,
    )


_otlp_warned = False


def post_otlp(path, payload):
    global _otlp_warned
    if payload is None:
        return
    req = urllib.request.Request(
        OTLP + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5).read()
        _otlp_warned = False
    except (urllib.error.URLError, OSError) as e:
        if not _otlp_warned:
            print("otlp %s unreachable (%s); metrics still work" % (OTLP + path, e), flush=True)
            _otlp_warned = True


def workload():
    global _db_errors
    while True:
        started = time.time()
        requests = [sample_request() for _ in range(RPS)]
        errors = 0
        for app, db, timed_out in requests:
            observe(app + db)
            errors += timed_out
        with _lock:
            _db_errors += errors
        logs, traces = build_payloads(requests)
        post_otlp("/v1/logs", logs)
        post_otlp("/v1/traces", traces)
        time.sleep(max(0, 1 - (time.time() - started)))


def set_pool(size, version, change):
    global _pool
    _pool = size
    _deploys.append({
        "version": version, "tenant": TENANT, "service": SERVICE,
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "change": change,
    })


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, body, ctype="application/json"):
        body = body.encode() if isinstance(body, str) else body
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/metrics"):
            self._send(render_metrics(), "text/plain; version=0.0.4")
        elif self.path.startswith("/deployments"):
            self._send(json.dumps({"deployments": _deploys}, indent=2))
        elif self.path == "/":
            self._send(json.dumps({
                "tenant": TENANT, "service": SERVICE, "rps": RPS,
                "db_pool_max": _pool,
                "state": "incident" if _pool == POOL_BROKEN else "healthy",
            }, indent=2))
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/break":
            set_pool(POOL_BROKEN, "v1.42.0", "maxPoolSize: 50 -> 10")
            self._send(json.dumps({"state": "incident", "db_pool_max": _pool}))
        elif self.path == "/heal":
            set_pool(POOL_HEALTHY, "v1.42.1", "revert maxPoolSize: 10 -> 50")
            self._send(json.dumps({"state": "healthy", "db_pool_max": _pool}))
        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


def selftest():
    global _pool
    def p99(vals):
        s = sorted(vals)
        return s[int(len(s) * 0.99)]

    _pool = POOL_HEALTHY
    healthy = [sum(sample_request()[:2]) for _ in range(20000)]
    assert 0.06 < sum(healthy) / len(healthy) < 0.10, "baseline should be ~80ms"
    assert p99(healthy) < 1.0, "healthy p99 must stay under the 1s alert threshold"

    _pool = POOL_BROKEN
    broken = [sample_request() for _ in range(20000)]
    assert p99([a + d for a, d, _ in broken]) > 4.0, "incident p99 must cross 4s"
    err_rate = sum(t for _, _, t in broken) / len(broken) * RPS
    assert err_rate > 1.5, "error rate must clear the >1/s alert rule, got %.2f" % err_rate

    # Histogram buckets are cumulative and the +Inf bucket holds everything.
    for v in [0.0001, 0.03, 0.9, 3.7, 99]:
        observe(v)
    text = render_metrics()
    counts = [int(l.rsplit(" ", 1)[1]) for l in text.splitlines() if "_bucket{" in l]
    assert counts == sorted(counts), "buckets must be non-decreasing"
    assert counts[-1] == 5, "+Inf must count every observation, got %d" % counts[-1]

    logs, traces = build_payloads(broken[:50])
    span = traces["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert len(span["traceId"]) == 32 and len(span["spanId"]) == 16, "OTLP ids are hex"
    assert int(span["endTimeUnixNano"]) > int(span["startTimeUnixNano"])
    assert any(a["key"] == "duration_ms" and "doubleValue" in a["value"]
               for a in span["attributes"]), "duration must be numeric for aggregation"
    assert any("timeout" in r["body"]["stringValue"]
               for r in logs["resourceLogs"][0]["scopeLogs"][0]["logRecords"]), \
        "incident logs must contain database timeouts"

    # No attribute key may be a dotted prefix of another: the OpenSearch
    # exporter flattens them, and a key that is both a value and an object
    # is rejected for the life of the index.
    for payload, root_key, kind in [(logs, "resourceLogs", "scopeLogs"),
                                    (traces, "resourceSpans", "scopeSpans")]:
        scope = payload[root_key][0]
        keys = {a["key"] for a in scope["resource"]["attributes"]}
        for item in scope[kind][0].get("spans", scope[kind][0].get("logRecords", [])):
            keys |= {a["key"] for a in item.get("attributes", [])}
        for k in keys:
            assert not any(o != k and o.startswith(k + ".") for o in keys), \
                "%s is a prefix of another attribute key" % k
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        sys.exit(0)
    threading.Thread(target=workload, daemon=True).start()
    print("simulator: tenant=%s service=%s :%d -> otlp %s\n"
          "  GET  /metrics /deployments /\n"
          "  POST /break /heal" % (TENANT, SERVICE, PORT, OTLP), flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
