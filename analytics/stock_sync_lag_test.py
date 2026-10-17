import json
import time
from datetime import datetime, timezone

import happybase
from kafka import KafkaProducer, KafkaConsumer, TopicPartition


BOOTSTRAP = "localhost:9092"
POS_TOPIC = "pos_sales"

STORE_ID = "S163"
SKU = "SKU03782"
ROW_KEY = f"{STORE_ID}#{SKU}"

TEST_TX = f"LAG_TEST_{int(time.time())}"


def utc_now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------
# Connect to HBase
# ---------------------------------------------------------

print("Connecting to HBase...")

hbase = happybase.Connection(
    host="localhost",
    port=9090,
    timeout=10000,
    transport="buffered",
    protocol="binary",
    compat="0.96"
)

table = hbase.table("inventory_current")

print("HBase connection ready.")


# ---------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)


# ---------------------------------------------------------
# Create and send POS event
# ---------------------------------------------------------

event_time = utc_now()

event = {
    "transaction_id": TEST_TX,
    "line_id": 1,
    "store_id": STORE_ID,
    "sku": SKU,
    "quantity": 1,
    "unit_price": 100.0,
    "discount_pct": 0.0,
    "transaction_timestamp": event_time.isoformat(),
    "channel": "STORE",
    "payment_type": "UPI",
    "replay_source": "latency_test"
}

print()
print("Sending event:", TEST_TX)
print("Event time:", event_time.isoformat())

future = producer.send(
    POS_TOPIC,
    value=event
)

metadata = future.get(timeout=10)
producer.flush()

partition = metadata.partition
offset = metadata.offset

print("Kafka event sent.")
print(f"Partition={partition}, Offset={offset}")


# ---------------------------------------------------------
# Read EXACT Kafka event using returned partition + offset
# ---------------------------------------------------------

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

topic_partition = TopicPartition(
    POS_TOPIC,
    partition
)

consumer.assign([topic_partition])

# Start exactly at the offset Kafka returned.
consumer.seek(
    topic_partition,
    offset
)

print()
print("Verifying exact Kafka event...")

seen = False
kafka_seen_time = None

deadline = time.time() + 10

while time.time() < deadline:

    records = consumer.poll(
        timeout_ms=1000,
        max_records=10
    )

    for _, messages in records.items():

        for message in messages:

            value = message.value

            if (
                message.offset == offset
                and value.get("transaction_id") == TEST_TX
            ):

                kafka_seen_time = utc_now()
                seen = True

                print("Kafka event verified.")
                print(
                    "Transaction:",
                    value.get("transaction_id")
                )
                print(
                    "Partition:",
                    message.partition
                )
                print(
                    "Offset:",
                    message.offset
                )

                break

        if seen:
            break

    if seen:
        break


if not seen:

    print()
    print("FAIL: exact Kafka event could not be read.")

    producer.close()
    consumer.close()
    hbase.close()

    raise SystemExit(1)


kafka_lag = (
    kafka_seen_time - event_time
).total_seconds()

print(
    f"Kafka delivery/verification lag: "
    f"{kafka_lag:.2f} seconds"
)


# ---------------------------------------------------------
# Check whether inventory processing updated HBase
# ---------------------------------------------------------

print()
print("Waiting for HBase inventory update...")

deadline = time.time() + 20

while time.time() < deadline:

    row = table.row(ROW_KEY)

    last_tx_raw = row.get(
        b"meta:last_transaction_id"
    )

    if last_tx_raw:

        last_tx = last_tx_raw.decode("utf-8")

        if last_tx == TEST_TX:

            hbase_time = utc_now()

            total_lag = (
                hbase_time - event_time
            ).total_seconds()

            qty_raw = row.get(
                b"state:quantity_on_hand"
            )

            quantity = (
                qty_raw.decode("utf-8")
                if qty_raw
                else "UNKNOWN"
            )

            print()
            print("HBase inventory updated.")
            print("Transaction:", TEST_TX)
            print("Store:", STORE_ID)
            print("SKU:", SKU)
            print(
                "Quantity on hand:",
                quantity
            )

            print()
            print(
                f"Stock synchronization lag: "
                f"{total_lag:.2f} seconds"
            )
            print("Required target: < 30 seconds")

            if total_lag < 30:
                print("RESULT: PASS")
            else:
                print("RESULT: FAIL")

            producer.close()
            consumer.close()
            hbase.close()

            raise SystemExit(0)

    time.sleep(1)


print()
print("Kafka stage: PASS")
print(
    f"Kafka lag: {kafka_lag:.2f} seconds"
)

print()
print("HBase stage: NOT UPDATED")

print(
    "Reason: no inventory processing stream "
    "is currently consuming this POS event "
    "and applying the stock decrement to HBase."
)

print()
print(
    "Next step: run one lightweight "
    "Kafka -> HBase streaming processor."
)

producer.close()
consumer.close()
hbase.close()
