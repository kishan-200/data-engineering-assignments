import json
import time
from pathlib import Path

import pyarrow.dataset as ds
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SALES_PATH = PROJECT_ROOT / "data_lake" / "bronze" / "sales"

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "pos_sales"

# Start small. We validate the pipeline before increasing this.
MAX_EVENTS = 10
EVENT_DELAY_SECONDS = 0.5


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8") if key else None,
    acks="all",
    retries=5,
)


# ============================================================
# READ FROZEN BRONZE SALES
# ============================================================

print("=" * 70)
print("POS SALES KAFKA PRODUCER")
print("=" * 70)

print(f"\nSource: {SALES_PATH}")
print(f"Topic : {KAFKA_TOPIC}")
print(f"Limit : {MAX_EVENTS} events")

dataset = ds.dataset(
    SALES_PATH,
    format="parquet",
    partitioning="hive",
)

columns = [
    "transaction_id",
    "line_id",
    "store_id",
    "sku",
    "quantity",
    "unit_price",
    "discount_pct",
    "transaction_timestamp",
    "channel",
    "payment_type",
]

table = dataset.head(
    MAX_EVENTS,
    columns=columns,
)

records = table.to_pylist()

print(f"\n[OK] Loaded {len(records)} frozen sales records")


# ============================================================
# PUBLISH EVENTS
# ============================================================

print("\nPublishing POS events...\n")

for event_number, record in enumerate(records, start=1):

    # Convert timestamp into JSON-safe representation
    if record["transaction_timestamp"] is not None:
        record["transaction_timestamp"] = (
            record["transaction_timestamp"].isoformat()
        )

    transaction_id = str(record["transaction_id"])

    future = producer.send(
        KAFKA_TOPIC,
        key=transaction_id,
        value=record,
    )

    metadata = future.get(timeout=10)

    print(
        f"[{event_number:02d}/{len(records):02d}] "
        f"transaction={transaction_id} "
        f"store={record['store_id']} "
        f"sku={record['sku']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )

    time.sleep(EVENT_DELAY_SECONDS)


# ============================================================
# CLEANUP
# ============================================================

producer.flush()
producer.close()

print("\n" + "=" * 70)
print("POS SALES KAFKA PRODUCER COMPLETE")
print("=" * 70)

print(f"Events published: {len(records)}")
print(f"Kafka topic     : {KAFKA_TOPIC}")
