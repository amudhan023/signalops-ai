"""The pure part of the simulator: no I/O, no clocks it does not receive.

Everything here is deterministic given a `random.Random`, which is what lets
tests/ assert the shape of the incident instead of eyeballing a dashboard.
"""
import json
import uuid
from dataclasses import dataclass

POOL_HEALTHY, POOL_BROKEN = 50, 10

# Finer than the default buckets between 2.5s and 5s, so histogram_quantile
# interpolates the p99 honestly instead of guessing across a 2.5s-wide bucket.
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1,
                   2.5, 3, 3.5, 4, 4.5, 5, 10)

CURRENCIES = ("USD", "EUR", "GBP", "INR")
METHODS = ("card", "card", "card", "wallet", "bank_transfer")
CUSTOMERS = 10_000
MERCHANTS = 200

SCHEMA = "signalops.payment-event.v1"


@dataclass(frozen=True)
class Request:
    app_seconds: float
    db_seconds: float
    timed_out: bool

    @property
    def total_seconds(self):
        return self.app_seconds + self.db_seconds


def sample_request(rng, pool_max):
    """One payment request handled with a connection pool of `pool_max`."""
    app = max(0.001, rng.gauss(0.040, 0.010))
    # With the pool cut to 10, most requests still find a free connection.
    # The rest queue behind one, and that wait is the whole incident.
    if pool_max <= POOL_BROKEN and rng.random() < 0.35:
        db = max(0.001, rng.gauss(3.8, 0.35))
        return Request(app, db, rng.random() < 0.60)
    return Request(app, max(0.001, rng.gauss(0.040, 0.008)), False)


def build_event(rng, req, tenant, service, pool_max, occurred_at_ms, trace_id=None):
    """The record written to Kafka for one request: (key, value bytes).

    Keyed by customer, so one customer's payments land on one partition and
    a downstream consumer sees them in order.
    """
    customer = "cus_%05d" % rng.randrange(CUSTOMERS)
    status = "timeout" if req.timed_out else ("declined" if rng.random() < 0.03 else "approved")
    event = {
        "schema": SCHEMA,
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "occurred_at_ms": occurred_at_ms,
        "tenant": tenant,
        "service": service,
        "trace_id": trace_id,
        "payment": {
            "customer_id": customer,
            "merchant_id": "mer_%03d" % rng.randrange(MERCHANTS),
            "amount_minor": rng.randrange(100, 500_000),
            "currency": rng.choice(CURRENCIES),
            "method": rng.choice(METHODS),
        },
        "outcome": {
            "status": status,
            "latency_ms": round(req.total_seconds * 1000, 1),
            "db_pool_max": pool_max,
        },
    }
    return customer.encode(), json.dumps(event, separators=(",", ":")).encode()


class Pacer:
    """Turns a per-second rate into a whole number of events per tick.

    500 events/s at 10 ticks/s is 50 per tick, but 333 events/s is 33.3. The
    carry keeps the fractional part, so the long-run rate is exact instead of
    rounding down to 330.
    """

    def __init__(self, ticks_per_second):
        self.ticks_per_second = ticks_per_second
        self._carry = 0.0

    def events_for_tick(self, events_per_second):
        due = events_per_second / self.ticks_per_second + self._carry
        whole = int(due)
        self._carry = due - whole
        return whole
