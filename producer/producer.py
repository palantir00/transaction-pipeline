"""Mock transaction producer.

Generates synthetic card transactions and publishes them to Kafka.
This is the streaming side of the pipeline: it runs continuously and
emits events one at a time, as they "happen".

The module is split so that everything except create_producer/main is a
pure function - no network, no clock, no global random state - which is
what makes it testable without a running Kafka.
"""
from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer

LOGGER = logging.getLogger("producer")

# Reference data for the mock stream.
CURRENCIES = ("PLN", "EUR", "USD", "GBP")
COUNTRIES = ("PL", "DE", "GB", "US", "FR")
USER_IDS = tuple(f"u_{i:03d}" for i in range(1, 51))

# Amounts are drawn in whole cents, so they are exact by construction.
MIN_CENTS = 100        # 1.00
MAX_CENTS = 1_000_000  # 10000.00

# Default random source. Tests pass their own seeded instance instead.
_RNG = random.Random()

_shutdown = False


def build_transaction(
    rng: random.Random | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build one synthetic transaction.

    Both the random source and the clock are parameters rather than
    globals: given the same rng and now, this returns the same
    transaction, which is what makes the generator testable.
    """
    rng = rng if rng is not None else _RNG
    now = now if now is not None else datetime.now(timezone.utc)

    # A UUID drawn from the injected rng, so it is reproducible under a seed.
    transaction_id = uuid.UUID(int=rng.getrandbits(128), version=4)

    # Whole cents -> Decimal, never float. quantize pins it to two places.
    amount = (Decimal(rng.randint(MIN_CENTS, MAX_CENTS)) / 100).quantize(Decimal("0.01"))

    # Events are always slightly in the past: a card tap reaches us with lag.
    occurred_at = now - timedelta(milliseconds=rng.randint(0, 5_000))

    return {
        "transaction_id": transaction_id,
        "user_id": rng.choice(USER_IDS),
        "amount": amount,
        "currency": rng.choice(CURRENCIES),
        "country": rng.choice(COUNTRIES),
        "occurred_at": occurred_at,
    }


def serialize(transaction: dict[str, Any]) -> bytes:
    """Encode a transaction as UTF-8 JSON bytes.

    The amount travels as a STRING on purpose. Every JSON number is a
    double, so sending 19.99 as a number risks distorting it in transit;
    as text it arrives byte for byte and the consumer parses it back
    into a Decimal.
    """
    payload = {
        "transaction_id": str(transaction["transaction_id"]),
        "user_id": transaction["user_id"],
        "amount": str(transaction["amount"]),
        "currency": transaction["currency"],
        "country": transaction["country"],
        "occurred_at": transaction["occurred_at"].isoformat(),
    }
    return json.dumps(payload).encode("utf-8")


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Open the connection to Kafka. The only function here that does I/O.

    acks="all" means the broker confirms a write only once it is safely
    stored. With a single broker that is cheap; in a real cluster it is
    the setting that decides whether a message can be lost when a node
    dies. acks=1 is faster and can lose data, acks=0 is fastest and gives
    no guarantee at all - a classic latency-versus-durability trade-off.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=50,        # batch messages for up to 50ms before sending
    )


def _request_shutdown(signum, _frame) -> None:
    global _shutdown
    LOGGER.info("received signal %s, finishing current batch", signum)
    _shutdown = True


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )
    # kafka-python logs every connection and metadata refresh at INFO,
    # which drowns out our own messages. Only warnings and errors from it.
    logging.getLogger("kafka").setLevel(logging.WARNING)

    # Kafka has two addresses depending on who is asking: kafka:9092 from
    # inside the Docker network, localhost:29092 from the host machine.
    # KAFKA_BOOTSTRAP_SERVERS holds the container view; the _HOST override
    # lets the same script run straight from a terminal.
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST") or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    rate = float(os.getenv("PRODUCER_RATE_PER_SECOND", "2"))
    interval = 1 / rate if rate > 0 else 0

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    LOGGER.info("publishing to topic %r via %s at %.1f msg/s", topic, bootstrap, rate)
    producer = create_producer(bootstrap)
    sent = 0

    try:
        while not _shutdown:
            transaction = build_transaction()
            producer.send(
                topic,
                # The key decides the partition, and Kafka only guarantees
                # ordering within a partition. Keying by user_id keeps one
                # user's transactions in order relative to each other.
                key=transaction["user_id"].encode("utf-8"),
                value=serialize(transaction),
            )
            sent += 1
            if sent % 10 == 0:
                LOGGER.info("sent %d transactions", sent)
            time.sleep(interval)
    finally:
        # flush() blocks until everything buffered has been acknowledged,
        # so a Ctrl+C does not silently drop the last few messages.
        producer.flush()
        producer.close()
        LOGGER.info("stopped after %d transactions", sent)


if __name__ == "__main__":
    main()
