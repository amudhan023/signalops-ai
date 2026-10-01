"""The Kafka side: a thin wrapper over confluent_kafka.Producer.

librdkafka does the heavy lifting -- batching, compression, retries -- on its
own threads. This class adds the three things the simulator needs on top:
blocking backpressure, delivery metrics, and a startup check on the topic.
"""
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, ConfigResource


class TopicNotReady(RuntimeError):
    pass


class EventProducer:
    def __init__(self, cfg, metrics, ops_log):
        self.topic = cfg.kafka_topic
        self.metrics = metrics
        self.ops_log = ops_log
        self._last_broker_error = 0.0
        self._conf = {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "client.id": "%s-simulator" % cfg.service,
        }
        self._producer = Producer({
            **self._conf,
            # Durable and ordered: every in-sync replica acknowledges, and
            # idempotence stops a retried batch from writing duplicates or
            # reordering one customer's events.
            "acks": "all",
            "enable.idempotence": True,
            # Throughput: wait up to 20 ms to fill a batch, then compress it.
            # At 500 events/s that turns ~500 requests/s into ~50.
            "linger.ms": 20,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 100_000,
            "message.timeout.ms": cfg.kafka_delivery_timeout_ms,
            # Never let a typo create a topic with broker-default retention.
            "allow.auto.create.topics": False,
            "error_cb": self._on_broker_error,
        })

    def check_topic(self, timeout=10):
        """Fail fast if the topic is missing; return its retention in ms.

        infra/docker-compose.yml creates the topic. Without this check a
        missing topic shows up only as a stream of delivery failures.
        """
        admin = AdminClient(self._conf)
        resource = ConfigResource(ConfigResource.Type.TOPIC, self.topic)
        try:
            configs = admin.describe_configs([resource])[resource].result(timeout=timeout)
        except KafkaException as e:
            raise TopicNotReady(
                "topic %r is not available (%s). Start the stack with ./infra/up.sh, "
                "which creates it." % (self.topic, e.args[0])) from None
        return int(configs["retention.ms"].value)

    def produce(self, key, value, headers, on_done, should_stop):
        """Queue one event. Blocks while the local queue is full.

        Blocking is the point: if the broker cannot keep up, the workload
        slows down, and the throughput alert sees it. Dropping events here
        instead would hide the problem behind a healthy-looking rate.
        """
        started = time.monotonic()

        def delivered(err, msg):
            if err is None:
                self.metrics.events_produced.inc()
                self.metrics.delivery_latency.observe(time.monotonic() - started)
            else:
                self.metrics.delivery_failed(err.name())
            on_done(err, msg)

        while True:
            try:
                self._producer.produce(self.topic, value=value, key=key,
                                       headers=headers, on_delivery=delivered)
                return True
            except BufferError:
                self.metrics.backpressure.inc()
                self._producer.poll(0.05)
                if should_stop():
                    return False

    def poll(self, timeout):
        """Run delivery callbacks. They fire only inside poll() or flush()."""
        self._producer.poll(timeout)
        self.metrics.queue_depth.set(len(self._producer))

    def flush(self, timeout):
        """Wait for in-flight events; return how many never got an answer."""
        return self._producer.flush(timeout)

    def _on_broker_error(self, err):
        # librdkafka reports "all brokers down" on every reconnect attempt.
        # One line every 10 seconds tells the operator; the failure metric
        # tells the alert.
        now = time.monotonic()
        if now - self._last_broker_error >= 10:
            self._last_broker_error = now
            self.ops_log.warning("kafka: %s", err.str())
