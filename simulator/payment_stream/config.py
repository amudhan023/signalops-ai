"""Runtime configuration, read once from environment variables.

Every setting has a default that works against infra/docker-compose.yml on
the same machine, so `./run.sh` needs no configuration at all.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    tenant: str = "acme"
    service: str = "payment-api"
    port: int = 8000

    kafka_bootstrap: str = "localhost:29092"
    kafka_topic: str = "payment-events"
    # How long the producer keeps retrying one event before it reports a
    # delivery failure. librdkafka's default is 5 minutes, which would hide a
    # dead broker from the failure metric for as long as the topic's whole
    # retention window.
    kafka_delivery_timeout_ms: int = 30_000

    events_per_second: int = 500
    otlp_endpoint: str = "http://localhost:4318"
    # Fraction of *healthy* requests that get a trace. Failed requests are
    # always traced -- see Workload.should_trace.
    trace_sample_ratio: float = 0.02
    # Fraction of healthy requests that write an INFO log line.
    info_log_ratio: float = 0.01

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env

        def get(name, default, cast=str):
            raw = env.get(name)
            if raw is None or raw == "":
                return default
            try:
                return cast(raw)
            except ValueError:
                raise ValueError("%s=%r is not a valid %s" % (name, raw, cast.__name__))

        d = cls()
        cfg = cls(
            tenant=get("TENANT", d.tenant),
            service=get("SERVICE", d.service),
            port=get("SIMULATOR_PORT", d.port, int),
            kafka_bootstrap=get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
            kafka_topic=get("KAFKA_TOPIC", d.kafka_topic),
            kafka_delivery_timeout_ms=get("KAFKA_DELIVERY_TIMEOUT_MS",
                                          d.kafka_delivery_timeout_ms, int),
            events_per_second=get("EVENTS_PER_SECOND", d.events_per_second, int),
            otlp_endpoint=get("OTLP_HTTP", d.otlp_endpoint).rstrip("/"),
            trace_sample_ratio=get("TRACE_SAMPLE_RATIO", d.trace_sample_ratio, float),
            info_log_ratio=get("INFO_LOG_RATIO", d.info_log_ratio, float),
        )
        cfg.validate()
        return cfg

    def validate(self):
        if not 1 <= self.events_per_second <= MAX_EVENTS_PER_SECOND:
            raise ValueError("EVENTS_PER_SECOND must be 1..%d, got %d"
                             % (MAX_EVENTS_PER_SECOND, self.events_per_second))
        for name in ("trace_sample_ratio", "info_log_ratio"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError("%s must be between 0 and 1, got %s" % (name.upper(), value))
        if not self.tenant or not self.service:
            raise ValueError("TENANT and SERVICE must be non-empty: alerts group on them")


# One Python process with librdkafka comfortably sustains this; past it the
# event generation loop, not Kafka, becomes the bottleneck.
MAX_EVENTS_PER_SECOND = 20_000
