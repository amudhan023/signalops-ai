"""Checks that the incident still looks like an incident.

These assert statistical and structural properties of what the simulator
emits -- the things the alerts, the OpenSearch mapping, and an agent reading
the evidence all depend on. Run after editing the simulator:

    ./run.sh test
"""
import json
import logging
import random
import unittest

from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import generate_latest

from payment_stream.config import Config
from payment_stream.model import (POOL_BROKEN, POOL_HEALTHY, Pacer, build_event,
                                  sample_request)
from payment_stream.telemetry import Metrics, resource
from payment_stream.workload import Workload

N = 20_000


def p99(values):
    s = sorted(values)
    return s[int(len(s) * 0.99)]


class LatencyModelTest(unittest.TestCase):
    def setUp(self):
        self.rng = random.Random(7)

    def test_healthy_stays_under_alert_threshold(self):
        totals = [sample_request(self.rng, POOL_HEALTHY).total_seconds for _ in range(N)]
        self.assertTrue(0.06 < sum(totals) / N < 0.10, "baseline should be ~80ms")
        self.assertLess(p99(totals), 1.0, "healthy p99 must stay under the 1s alert")
        self.assertFalse(any(sample_request(self.rng, POOL_HEALTHY).timed_out
                             for _ in range(N)))

    def test_broken_pool_crosses_both_alerts(self):
        reqs = [sample_request(self.rng, POOL_BROKEN) for _ in range(N)]
        self.assertGreater(p99([r.total_seconds for r in reqs]), 4.0)
        # PaymentAPIDatabaseErrors fires above 1 error/s. Even at a tenth of
        # the default rate, the incident must clear it.
        errors_per_second = sum(r.timed_out for r in reqs) / N * 50
        self.assertGreater(errors_per_second, 1.5)


class EventTest(unittest.TestCase):
    def test_event_shape_and_key(self):
        rng = random.Random(1)
        req = sample_request(rng, POOL_BROKEN)
        key, value = build_event(rng, req, "acme", "payment-api", POOL_BROKEN, 123, "ab" * 16)
        event = json.loads(value)
        self.assertEqual(key.decode(), event["payment"]["customer_id"])
        self.assertEqual(event["schema"], "signalops.payment-event.v1")
        self.assertEqual((event["tenant"], event["service"]), ("acme", "payment-api"))
        self.assertEqual(event["trace_id"], "ab" * 16)
        self.assertEqual(event["outcome"]["db_pool_max"], POOL_BROKEN)

    def test_timed_out_request_has_timeout_status(self):
        rng = random.Random(2)
        req = next(r for r in (sample_request(rng, POOL_BROKEN) for _ in range(1000))
                   if r.timed_out)
        _, value = build_event(rng, req, "acme", "payment-api", POOL_BROKEN, 0)
        self.assertEqual(json.loads(value)["outcome"]["status"], "timeout")


class PacerTest(unittest.TestCase):
    def test_fractional_rate_is_exact_over_time(self):
        pacer = Pacer(ticks_per_second=10)
        sent = sum(pacer.events_for_tick(333) for _ in range(10 * 60))
        self.assertIn(sent, (333 * 60 - 1, 333 * 60))


class ConfigTest(unittest.TestCase):
    def test_defaults_point_at_local_stack(self):
        cfg = Config.from_env({})
        self.assertEqual(cfg.kafka_bootstrap, "localhost:29092")
        self.assertEqual(cfg.kafka_topic, "payment-events")
        self.assertEqual(cfg.port, 8000)

    def test_rejects_bad_values(self):
        for env in ({"EVENTS_PER_SECOND": "0"}, {"EVENTS_PER_SECOND": "fast"},
                    {"EVENTS_PER_SECOND": "1000000"}, {"TRACE_SAMPLE_RATIO": "2"}):
            with self.assertRaises(ValueError, msg=env):
                Config.from_env(env)


class MetricsTest(unittest.TestCase):
    def test_histogram_cumulative_and_labelled(self):
        m = Metrics("acme", "payment-api", "payment-events")
        for v in (0.0001, 0.03, 0.9, 3.7, 99):
            m.request_duration.observe(v)
        text = generate_latest(m.registry).decode()
        buckets = [float(l.rsplit(" ", 1)[1]) for l in text.splitlines()
                   if l.startswith("payment_api_request_duration_seconds_bucket")]
        self.assertEqual(buckets, sorted(buckets), "buckets must be non-decreasing")
        self.assertEqual(buckets[-1], 5, "+Inf must count every observation")

        # Every application series needs tenant + service, or alerts built
        # on it reach the receiver as unknown/unknown.
        m.delivery_failed("_MSG_TIMED_OUT")
        for line in text.splitlines() + generate_latest(m.registry).decode().splitlines():
            if line.startswith("payment_"):
                self.assertIn('tenant="acme"', line)
                self.assertIn('service="payment-api"', line)


class FakeProducer:
    """Acknowledges every event immediately, on the next poll."""

    def __init__(self):
        self.records, self._pending = [], []

    def produce(self, key, value, headers, on_done, should_stop):
        self.records.append((key, value, headers))
        self._pending.append(on_done)
        return True

    def poll(self, timeout):
        pending, self._pending = self._pending, []
        for i, on_done in enumerate(pending):
            on_done(None, FakeMessage(i))


class FakeMessage:
    def __init__(self, offset):
        self._offset = offset

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeTelemetry:
    def __init__(self):
        res = resource("acme", "payment-api")
        self.spans = InMemorySpanExporter()
        tp = TracerProvider(resource=res)
        tp.add_span_processor(SimpleSpanProcessor(self.spans))
        self.tracer = tp.get_tracer("test")

        self.logs = InMemoryLogRecordExporter()
        lp = LoggerProvider(resource=res)
        lp.add_log_record_processor(SimpleLogRecordProcessor(self.logs))
        self.app_log = logging.getLogger("test.payment_api")
        self.app_log.handlers[:] = [LoggingHandler(logger_provider=lp)]
        self.app_log.setLevel(logging.INFO)
        self.app_log.propagate = False
        self.ops_log = logging.getLogger("test.simulator")
        self.ops_log.disabled = True


class WorkloadTest(unittest.TestCase):
    def setUp(self):
        self.cfg = Config.from_env({})
        self.tel = FakeTelemetry()
        self.producer = FakeProducer()
        self.workload = Workload(self.cfg, Metrics("acme", "payment-api", "payment-events"),
                                 self.tel, self.producer, random.Random(3))

    def run_requests(self, n, pool):
        for _ in range(n):
            self.workload.handle_one(pool)
        self.producer.poll(0)

    def test_every_error_log_links_to_a_stored_trace(self):
        self.run_requests(2000, POOL_BROKEN)
        trace_ids = {s.context.trace_id for s in self.tel.spans.get_finished_spans()}
        errors = [l.log_record for l in self.tel.logs.get_finished_logs()
                  if l.log_record.severity_text == "ERROR"]
        self.assertTrue(errors, "the incident must produce error logs")
        self.assertIn("timeout", errors[0].body)
        for record in errors:
            self.assertIn(record.trace_id, trace_ids)

    def test_traced_event_carries_traceparent_and_trace_id(self):
        self.run_requests(2000, POOL_BROKEN)
        traced = [(json.loads(v), h) for _, v, h in self.producer.records if h]
        self.assertTrue(traced)
        event, headers = traced[0]
        traceparent = dict(headers)["traceparent"].decode()
        self.assertEqual(traceparent.split("-")[1], event["trace_id"])

    def test_spans_are_well_formed(self):
        self.run_requests(500, POOL_BROKEN)
        spans = self.tel.spans.get_finished_spans()
        names = {s.name for s in spans}
        self.assertEqual(names, {"POST /payments", "SELECT payments.transaction",
                                 "payment-events publish"})
        for s in spans:
            self.assertGreater(s.end_time, s.start_time)
        publish = next(s for s in spans if s.name == "payment-events publish")
        self.assertEqual(publish.attributes["messaging.kafka.partition"], 0)
        db = next(s for s in spans if s.name == "SELECT payments.transaction"
                  and s.attributes["duration_ms"] > 1000)
        self.assertIsInstance(db.attributes["duration_ms"], float,
                              "duration must be numeric for aggregation")

    def test_healthy_traffic_is_sampled(self):
        self.run_requests(5000, POOL_HEALTHY)
        roots = [s for s in self.tel.spans.get_finished_spans() if s.parent is None]
        # 2% of 5000 is 100; allow wide slack, the point is "sampled, not all".
        self.assertTrue(40 < len(roots) < 200, len(roots))
        self.assertEqual(len(self.producer.records), 5000, "every request is an event")

    def test_no_attribute_key_is_a_dotted_prefix_of_another(self):
        # The OpenSearch exporter flattens attribute keys. If both `a` and
        # `a.b` arrive, OpenSearch must map `a` as a string and an object at
        # once, and rejects the document for the life of the index.
        self.run_requests(2000, POOL_BROKEN)
        keys = set(resource("acme", "payment-api").attributes)
        for s in self.tel.spans.get_finished_spans():
            keys |= set(s.attributes)
        for l in self.tel.logs.get_finished_logs():
            keys |= set(l.log_record.attributes)
        for k in keys:
            clash = [o for o in keys if o.startswith(k + ".")]
            self.assertFalse(clash, "%s is a prefix of %s" % (k, clash))


if __name__ == "__main__":
    unittest.main()
