"""The streaming loop: simulate requests, emit telemetry, produce to Kafka.

One thread owns the loop and the producer. librdkafka runs its own I/O
threads underneath, but delivery callbacks fire only inside poll(), so every
callback here runs on this same thread and needs no locking.
"""
import random
import threading
import time
from collections import Counter

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .model import POOL_BROKEN, POOL_HEALTHY, Pacer, build_event, sample_request

TICKS_PER_SECOND = 10
FAILURE_SUMMARY_SECONDS = 10
# If a tick overruns by more than this, stop trying to catch up. Bursting
# the backlog after a broker outage would hide the outage in the rate graph.
MAX_LAG_SECONDS = 1.0

_propagator = TraceContextTextMapPropagator()


class Workload:
    def __init__(self, cfg, metrics, telemetry, producer, rng=None):
        self.cfg = cfg
        self.metrics = metrics
        self.tracer = telemetry.tracer
        self.app_log = telemetry.app_log
        self.ops_log = telemetry.ops_log
        self.producer = producer
        self.rng = rng or random.Random()

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._pool_max = POOL_HEALTHY
        self._rate = cfg.events_per_second
        self._deployments = [{
            "version": "v1.41.0", "tenant": cfg.tenant, "service": cfg.service,
            "at": _iso(time.time() - 3 * 86400), "change": "routine dependency bump",
        }]
        self._failures = Counter()
        metrics.pool_size.set(self._pool_max)
        metrics.target_rate.set(self._rate)

    # ---------- control, called from HTTP threads ----------

    def snapshot(self):
        with self._lock:
            return {
                "tenant": self.cfg.tenant, "service": self.cfg.service,
                "topic": self.cfg.kafka_topic, "events_per_second": self._rate,
                "db_pool_max": self._pool_max,
                "state": "incident" if self._pool_max == POOL_BROKEN else "healthy",
            }

    def deployments(self):
        with self._lock:
            return list(self._deployments)

    def set_pool(self, size, version, change):
        with self._lock:
            self._pool_max = size
            self._deployments.append({
                "version": version, "tenant": self.cfg.tenant, "service": self.cfg.service,
                "at": _iso(time.time()), "change": change,
            })
        self.metrics.pool_size.set(size)
        self.ops_log.warning("deploy %s: %s", version, change)

    def set_rate(self, events_per_second):
        with self._lock:
            self._rate = events_per_second
        self.metrics.target_rate.set(events_per_second)
        self.ops_log.info("target rate set to %d events/s", events_per_second)

    def stop(self):
        self._stop.set()

    # ---------- the loop ----------

    def run(self):
        pacer = Pacer(TICKS_PER_SECOND)
        tick = 1.0 / TICKS_PER_SECOND
        deadline = time.monotonic()
        next_summary = deadline + FAILURE_SUMMARY_SECONDS
        while not self._stop.is_set():
            with self._lock:
                rate, pool_max = self._rate, self._pool_max
            for _ in range(pacer.events_for_tick(rate)):
                if not self.handle_one(pool_max):
                    break

            deadline += tick
            now = time.monotonic()
            if now - deadline > MAX_LAG_SECONDS:
                deadline = now
            # Serve delivery callbacks while waiting for the next tick.
            while now < deadline and not self._stop.is_set():
                self.producer.poll(deadline - now)
                now = time.monotonic()
            self.producer.poll(0)

            if now >= next_summary:
                self._log_failures()
                next_summary = now + FAILURE_SUMMARY_SECONDS

    def should_trace(self, req):
        """Keep every failed request's trace; sample the healthy ones.

        Head sampling at 2% would drop 98% of the timeouts too, and an error
        log whose trace was never stored is a dead end for whoever reads it.
        """
        return req.timed_out or self.rng.random() < self.cfg.trace_sample_ratio

    def handle_one(self, pool_max):
        """Simulate one request end to end. Returns False if stopping."""
        req = sample_request(self.rng, pool_max)
        self.metrics.request_duration.observe(req.total_seconds)
        if req.timed_out:
            self.metrics.database_errors.inc()

        end_ns = time.time_ns()
        headers, publish_span, trace_id = None, None, None
        if self.should_trace(req):
            root = self._request_spans(req, pool_max, end_ns)
            trace_id = format(root.get_span_context().trace_id, "032x")
            with trace.use_span(root, end_on_exit=False):
                self._request_log(req, pool_max)
                publish_span = self.tracer.start_span(
                    "%s publish" % self.cfg.kafka_topic, kind=SpanKind.PRODUCER,
                    attributes={"messaging.system": "kafka",
                                "messaging.operation.type": "send",
                                "messaging.destination.name": self.cfg.kafka_topic})
                # traceparent rides in the record headers, so a consumer can
                # continue this trace instead of starting a new one.
                carrier = {}
                _propagator.inject(carrier, context=trace.set_span_in_context(publish_span))
                headers = [(k, v.encode()) for k, v in carrier.items()]
            # Pinned to the simulated end, not the wall clock at this line.
            root.end(end_time=end_ns)
        elif not req.timed_out and self.rng.random() < self.cfg.info_log_ratio:
            self._request_log(req, pool_max)

        key, value = build_event(self.rng, req, self.cfg.tenant, self.cfg.service,
                                 pool_max, end_ns // 1_000_000, trace_id)

        def on_done(err, msg):
            if err is not None:
                self._failures[err.name()] += 1
            if publish_span is None:
                return
            if err is None:
                publish_span.set_attribute("messaging.kafka.partition", msg.partition())
                publish_span.set_attribute("messaging.kafka.offset", msg.offset())
            else:
                publish_span.set_status(Status(StatusCode.ERROR, err.str()))
            publish_span.end()

        queued = self.producer.produce(key, value, headers, on_done, self._stop.is_set)
        if not queued and publish_span is not None:
            publish_span.end()
        return queued

    def _request_spans(self, req, pool_max, end_ns):
        """The request's own spans, back-dated to the simulated latency.

        Returns the root span still open, so the publish span and the log
        line can attach to it. The caller ends it at `end_ns`.
        """
        start_ns = end_ns - int(req.total_seconds * 1e9)
        db_start_ns = start_ns + int(req.app_seconds * 1e9)
        error = Status(StatusCode.ERROR, "database connection timeout") if req.timed_out else None
        root = self.tracer.start_span(
            "POST /payments", kind=SpanKind.SERVER, start_time=start_ns,
            attributes={"http.method": "POST", "http.route": "/payments",
                        "http.status_code": 504 if req.timed_out else 200,
                        "duration_ms": round(req.total_seconds * 1000, 1)})
        db = self.tracer.start_span(
            "SELECT payments.transaction", kind=SpanKind.CLIENT, start_time=db_start_ns,
            context=trace.set_span_in_context(root),
            attributes={"db.system": "postgresql",
                        "db.statement": "SELECT * FROM transaction WHERE id = $1",
                        "db.pool.max": pool_max,
                        "duration_ms": round(req.db_seconds * 1000, 1)})
        if error:
            db.set_status(error)
            root.set_status(error)
        db.end(end_time=end_ns)
        return root

    def _request_log(self, req, pool_max):
        # Called inside the root span's context, so the OTel handler stamps
        # this record with the trace and span id.
        extra = {"duration_ms": round(req.total_seconds * 1000, 1)}
        if req.timed_out:
            self.app_log.error(
                "Database connection timeout after %dms waiting for a pooled "
                "connection (pool max=%d)", req.db_seconds * 1000, pool_max, extra=extra)
        else:
            self.app_log.info("Payment processed in %dms",
                              req.total_seconds * 1000, extra=extra)

    def _log_failures(self):
        if not self._failures:
            return
        summary = ", ".join("%s=%d" % kv for kv in self._failures.most_common())
        self.ops_log.error("kafka delivery failed for %d events in the last %ds: %s",
                           sum(self._failures.values()), FAILURE_SUMMARY_SECONDS, summary)
        self._failures.clear()


def _iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
