"""Tests for the transaction producer.

Written before the implementation exists: they describe what the producer
is supposed to guarantee, not what it happens to do.
"""
import json
import os
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from producer.producer import build_transaction, serialize

REQUIRED_FIELDS = {
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "country",
    "occurred_at",
}


# ---------------------------------------------------------------------
# What a transaction must look like
# ---------------------------------------------------------------------

def test_transaction_contains_exactly_the_required_fields():
    """No missing fields, and no surprise extras the consumer cannot map."""
    tx = build_transaction()
    assert set(tx) == REQUIRED_FIELDS


def test_amount_is_decimal_with_two_places_and_non_zero():
    """Money is Decimal, never float - and the raw table rejects amount = 0."""
    for _ in range(50):
        amount = build_transaction()["amount"]
        assert isinstance(amount, Decimal), f"expected Decimal, got {type(amount)}"
        assert amount != 0
        assert amount.as_tuple().exponent == -2, f"{amount} is not rounded to cents"


def test_currency_and_country_match_the_database_column_widths():
    """CHAR(3) and CHAR(2) in the schema - anything else would be truncated."""
    tx = build_transaction()
    assert len(tx["currency"]) == 3
    assert tx["currency"].isupper()
    assert len(tx["country"]) == 2
    assert tx["country"].isupper()


def test_occurred_at_is_timezone_aware_and_in_utc():
    """A naive timestamp would make cross-country aggregation ambiguous."""
    occurred_at = build_transaction()["occurred_at"]
    assert isinstance(occurred_at, datetime)
    assert occurred_at.tzinfo is not None, "timestamp must carry a timezone"
    assert occurred_at.utcoffset().total_seconds() == 0, "timestamp must be UTC"


def test_transaction_id_is_a_unique_uuid():
    """The consumer relies on this for deduplication, so it must never repeat."""
    ids = [build_transaction()["transaction_id"] for _ in range(200)]
    for value in ids:
        uuid.UUID(str(value))          # raises if it is not a valid UUID
    assert len(set(ids)) == len(ids)


def test_amount_stays_within_a_sane_business_range():
    """NUMERIC(18,2) would accept absurd values; a broken generator would not
    be caught by the type checks alone."""
    for _ in range(200):
        amount = build_transaction()["amount"]
        assert Decimal("0.01") <= amount <= Decimal("10000.00"), amount


def test_user_id_is_never_blank():
    """NOT NULL in the schema does not stop an empty string, which would
    produce a daily report belonging to nobody."""
    for _ in range(50):
        assert build_transaction()["user_id"].strip()


# ---------------------------------------------------------------------
# Reproducibility: the same seed must give the same transaction
# ---------------------------------------------------------------------

def test_same_seed_produces_the_same_transaction():
    """An injectable random source is what makes the generator testable."""
    fixed_time = datetime(2026, 8, 19, 10, 0, tzinfo=timezone.utc)

    first = build_transaction(rng=random.Random(42), now=fixed_time)
    second = build_transaction(rng=random.Random(42), now=fixed_time)

    assert first == second


# ---------------------------------------------------------------------
# Serialization: the wire format must not corrupt the amount
# ---------------------------------------------------------------------

def test_serialize_returns_utf8_json_bytes():
    payload = serialize(build_transaction())
    assert isinstance(payload, bytes)
    json.loads(payload.decode("utf-8"))     # raises if it is not valid JSON


def test_serialized_amount_is_a_string_not_a_json_number():
    """JSON numbers are doubles; sending 19.99 as a number can distort it."""
    tx = build_transaction()
    decoded = json.loads(serialize(tx).decode("utf-8"))

    assert isinstance(decoded["amount"], str), "amount must travel as text"
    assert Decimal(decoded["amount"]) == tx["amount"], "amount changed in transit"


def test_serialized_timestamp_survives_the_round_trip():
    tx = build_transaction()
    decoded = json.loads(serialize(tx).decode("utf-8"))

    assert datetime.fromisoformat(decoded["occurred_at"]) == tx["occurred_at"]


# ---------------------------------------------------------------------
# Integration: does the message actually reach Kafka?
# Needs the Docker stack. Skip with: pytest -m "not integration"
# ---------------------------------------------------------------------

@pytest.mark.integration
def test_message_reaches_kafka_and_keeps_its_key():
    from kafka import KafkaAdminClient, KafkaConsumer

    from producer.producer import create_producer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
    topic = f"test-transactions-{uuid.uuid4().hex[:8]}"

    tx = build_transaction()
    try:
        producer = create_producer(bootstrap)
        producer.send(topic, key=tx["user_id"].encode("utf-8"), value=serialize(tx))
        producer.flush()
        producer.close()

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=15_000,
        )
        received = next(iter(consumer), None)
        consumer.close()
    finally:
        # A test that leaves topics behind pollutes the broker for every
        # later run. Clean up even when the assertions below fail.
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        admin.delete_topics([topic])
        admin.close()

    assert received is not None, "no message came back from Kafka"
    # The key decides the partition, which is what preserves per-user ordering
    assert received.key.decode("utf-8") == tx["user_id"]
    assert json.loads(received.value.decode("utf-8"))["transaction_id"] == str(
        tx["transaction_id"]
    )
