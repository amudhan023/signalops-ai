"""Metrics, traces, and logs -- the three signals the observability stack reads.

  metrics  Prometheus scrapes GET /metrics      (infra/prometheus/prometheus.yml)
  traces   OTLP/HTTP -> collector -> sre-traces (infra/otel/collector.yaml)
  logs     OTLP/HTTP -> collector -> sre-logs
"""
import logging
import sys

from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import (CollectorRegistry, Counter, Gauge, Histogram,
                               GCCollector, PlatformCollector, ProcessCollector,
                               disable_created_metrics)

from .model import LATENCY_BUCKETS

# The *_created series double the series count and nothing here reads them.
disable_created_metrics()

DELIVERY_BUCKETS = (0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)


class Metrics:
    """Every series carries `tenant` and `service`.

    Alertmanager groups notifications by exactly those two labels, so an
    alert built from a series without them reaches the receiver as
    unknown/unknown.
    """

    def __init__(self, tenant, service, topic, registry=None):
        self.registry = registry or CollectorRegistry()
        if registry is None:
            ProcessCollector(registry=self.registry)
            PlatformCollector(registry=self.registry)
            GCCollector(registry=self.registry)
        ts = ("tenant", "service")
        r = self.registry

        # Request side -- the payment-api incident. Names are unchanged from
        # the first simulator so alerts.yml keeps working.
        self.request_duration = Histogram(
            "payment_api_request_duration_seconds", "End-to-end request duration.",
            ts, buckets=LATENCY_BUCKETS, registry=r).labels(tenant, service)
        self.database_errors = Counter(
            "payment_api_database_errors", "Database calls that timed out.",
            ts, registry=r).labels(tenant, service)
        self.pool_size = Gauge(
            "payment_api_db_pool_size", "Configured maximum database connections.",
            ts, registry=r).labels(tenant, service)

        # Stream side -- the Kafka producer.
        tst = ts + ("topic",)
        self.events_produced = Counter(
            "payment_events_produced", "Events the broker acknowledged.",
            tst, registry=r).labels(tenant, service, topic)
        self._delivery_failures = Counter(
            "payment_events_delivery_failures",
            "Events the producer gave up on, by librdkafka error name.",
            tst + ("reason",), registry=r)
        self._labels = (tenant, service, topic)
        self.delivery_latency = Histogram(
            "payment_events_delivery_latency_seconds",
            "Time from produce() to broker acknowledgement.",
            tst, buckets=DELIVERY_BUCKETS, registry=r).labels(tenant, service, topic)
        self.backpressure = Counter(
            "payment_events_backpressure",
            "Times produce() found the local queue full and had to wait.",
            tst, registry=r).labels(tenant, service, topic)
        self.target_rate = Gauge(
            "payment_events_target_rate", "Events per second the workload aims for.",
            tst, registry=r).labels(tenant, service, topic)
        self.queue_depth = Gauge(
            "payment_events_producer_queue",
            "Events produced but not yet acknowledged or failed.",
            tst, registry=r).labels(tenant, service, topic)

    def delivery_failed(self, reason):
        self._delivery_failures.labels(*self._labels, reason).inc()


def resource(tenant, service):
    # Only `service.name`, never a plain `service` alongside it: the
    # OpenSearch exporter flattens attributes, and OpenSearch cannot map one
    # key as both a string and an object. tests/ guards this rule.
    return Resource.create({"service.name": service, "tenant": tenant})


class Telemetry:
    """Owns the OTel providers so shutdown can flush them in one place."""

    def __init__(self, cfg):
        res = resource(cfg.tenant, cfg.service)

        # The SDK samples everything it is handed. The workload decides which
        # requests get a trace at all (Workload.should_trace), because that
        # decision needs the request's outcome -- see the README.
        self.tracer_provider = TracerProvider(resource=res)
        self.tracer_provider.add_span_processor(BatchSpanProcessor(
            OTLPSpanExporter(endpoint=cfg.otlp_endpoint + "/v1/traces", timeout=5),
            max_queue_size=8192, max_export_batch_size=1024, schedule_delay_millis=2000))
        self.tracer = self.tracer_provider.get_tracer("payment_stream")

        self.logger_provider = LoggerProvider(resource=res)
        self.logger_provider.add_log_record_processor(BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=cfg.otlp_endpoint + "/v1/logs", timeout=5),
            max_queue_size=8192, max_export_batch_size=1024, schedule_delay_millis=2000))
        otlp = LoggingHandler(level=logging.INFO, logger_provider=self.logger_provider)

        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))

        # Two loggers on purpose. `payment_api` is the application's own
        # per-request log: it goes to OpenSearch only, or a busy stream would
        # bury the terminal. `simulator` is operational (startup, faults,
        # broker trouble) and goes to both.
        self.app_log = _logger("payment_api", otlp)
        self.ops_log = _logger("simulator", otlp, console)

        # The OTLP exporters log through `opentelemetry`. Route that to the
        # console only: shipping "cannot reach the collector" to the
        # collector would loop.
        _logger("opentelemetry", console).setLevel(logging.WARNING)

    def shutdown(self):
        self.tracer_provider.shutdown()
        self.logger_provider.shutdown()


def _logger(name, *handlers):
    log = logging.getLogger(name)
    log.handlers[:] = handlers
    log.setLevel(logging.INFO)
    log.propagate = False
    return log
