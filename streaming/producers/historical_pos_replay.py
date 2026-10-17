import json
import time
import pyarrow.dataset as ds
from kafka import KafkaProducer

SALES_PATH = "data_lake/silver/sales_clean"
TOPIC = "pos_sales"
KAFKA = "localhost:9092"

EVENTS_TO_REPLAY = 1000
EVENTS_PER_SECOND = 5

dataset = ds.dataset(
    SALES_PATH,
    format="parquet",
    partitioning="hive"
)

table = dataset.head(EVENTS_TO_REPLAY)
rows = table.to_pylist()

producer = KafkaProducer(
    bootstrap_servers=KAFKA,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1,
    request_timeout_ms=60000
)

print("===== HISTORICAL POS REPLAY =====")
print("Topic:", TOPIC)
print("Events:", len(rows))
print("Rate:", EVENTS_PER_SECOND, "events/sec")

delay = 1 / EVENTS_PER_SECOND

for i, row in enumerate(rows, start=1):
    event = {
        "transaction_id": row["transaction_id"],
        "line_id": int(row["line_id"]),
        "store_id": row["store_id"],
        "sku": row["sku"],
        "quantity": int(row["quantity"]),
        "unit_price": float(row["unit_price"]),
        "discount_pct": float(row["discount_pct"]),
        "transaction_timestamp": row["transaction_timestamp"].isoformat(),
        "channel": row["channel"],
        "payment_type": row["payment_type"],
        "replay_source": "historical_sales",
        "replay_sequence": i
    }

    producer.send(TOPIC, value=event)

    if i % 100 == 0:
        print(f"Sent {i}/{len(rows)} events")

    time.sleep(delay)

producer.flush()
producer.close()

print("\n===== REPLAY COMPLETE =====")
print("Events sent:", len(rows))
