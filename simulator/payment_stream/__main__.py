"""Entry point: python -m payment_stream (or ./run.sh)."""
import signal
import sys
import threading

from .config import Config
from .http_api import make_server
from .producer import EventProducer, TopicNotReady
from .telemetry import Metrics, Telemetry
from .workload import Workload

SHUTDOWN_FLUSH_SECONDS = 10


def main():
    try:
        cfg = Config.from_env()
    except ValueError as e:
        sys.exit("config: %s" % e)

    telemetry = Telemetry(cfg)
    log = telemetry.ops_log
    metrics = Metrics(cfg.tenant, cfg.service, cfg.kafka_topic)
    producer = EventProducer(cfg, metrics, log)

    try:
        retention_ms = producer.check_topic()
    except TopicNotReady as e:
        telemetry.shutdown()
        sys.exit("kafka: %s" % e)

    workload = Workload(cfg, metrics, telemetry, producer)
    server = make_server(cfg.port, workload, metrics.registry)
    threading.Thread(target=server.serve_forever, name="http", daemon=True).start()

    def on_signal(signum, _frame):
        log.info("received %s, stopping", signal.Signals(signum).name)
        workload.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    log.info("tenant=%s service=%s rate=%d/s -> kafka %s topic=%s (retention %ds), "
             "otlp %s, http :%d",
             cfg.tenant, cfg.service, cfg.events_per_second, cfg.kafka_bootstrap,
             cfg.kafka_topic, retention_ms // 1000, cfg.otlp_endpoint, cfg.port)
    workload.run()

    # Order matters: stop taking requests, drain Kafka (its callbacks end the
    # last publish spans), then flush spans and logs to the collector.
    server.shutdown()
    unsent = producer.flush(SHUTDOWN_FLUSH_SECONDS)
    if unsent:
        log.error("%d events still undelivered after %ds; dropping them",
                  unsent, SHUTDOWN_FLUSH_SECONDS)
    log.info("stopped")
    telemetry.shutdown()


if __name__ == "__main__":
    main()
