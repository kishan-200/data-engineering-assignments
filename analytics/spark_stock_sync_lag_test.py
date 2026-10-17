import json
import time
from datetime import datetime, timezone
from pathlib import Path

import happybase
from kafka import KafkaProducer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


BOOTSTRAP = "localhost:9092"
TOPIC = "pos_sales"

STORE_ID = "S163"
SKU = "SKU03782"

TEST_TX = f"SPARK_LAG_TEST_{int(time.time())}"
ROW_KEY = f"{STORE_ID}#{SKU}"

PROJECT_ROOT = Path.home() / "omnichannel-inventory"

CHECKPOINT = (
    PROJECT_ROOT
    / "streaming"
    / "checkpoints"
    / "lag_test"
    / TEST_TX
)


def utc_now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------
# Spark
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Stock-Sync-Lag-Test")
    .master("local[1]")
    .config("spark.driver.memory", "768m")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# POS schema
# ---------------------------------------------------------

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("line_id", LongType()),
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("quantity", LongType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("transaction_timestamp", StringType()),
    StructField("channel", StringType()),
    StructField("payment_type", StringType()),
    StructField("replay_source", StringType()),
])


# ---------------------------------------------------------
# Read Kafka
# ---------------------------------------------------------

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .filter(
        col("replay_source") == "spark_latency_test"
    )
)


# ---------------------------------------------------------
# Update HBase for each micro-batch
# ---------------------------------------------------------

def update_hbase(batch_df, batch_id):

    rows = batch_df.collect()

    if not rows:
        return

    connection = happybase.Connection(
        host="localhost",
        port=9090,
        timeout=10000,
        transport="buffered",
        protocol="binary",
        compat="0.96",
    )

    table = connection.table("inventory_current")

    for row in rows:

        row_key = f"{row.store_id}#{row.sku}"

        current = table.row(row_key)

        if not current:
            print(
                f"Inventory row not found: {row_key}"
            )
            continue

        event_marker = (
            f"events:{row.transaction_id}"
            f"#{row.line_id}"
        ).encode()

        # Idempotency
        if event_marker in current:
            print(
                "Duplicate event skipped:",
                row.transaction_id
            )
            continue

        current_qty = int(
            current.get(
                b"state:quantity_on_hand",
                b"0"
            ).decode()
        )

        reorder_point = int(
            current.get(
                b"state:reorder_point",
                b"0"
            ).decode()
        )

        new_qty = max(
            0,
            current_qty - int(row.quantity)
        )

        if new_qty == 0:
            status = "CRITICAL"
        elif new_qty <= reorder_point:
            status = "LOW"
        else:
            status = "HEALTHY"

        event_time = datetime.fromisoformat(
            row.transaction_timestamp
        )

        update_time = utc_now()

        lag_seconds = (
            update_time - event_time
        ).total_seconds()

        table.put(
            row_key,
            {
                b"state:quantity_on_hand":
                    str(new_qty).encode(),

                b"state:stock_status":
                    status.encode(),

                b"meta:last_transaction_id":
                    row.transaction_id.encode(),

                b"meta:last_updated":
                    update_time.isoformat().encode(),

                b"meta:last_sync_lag_seconds":
                    f"{lag_seconds:.3f}".encode(),

                event_marker:
                    b"APPLIED",
            }
        )

        print()
        print("EVENT APPLIED TO HBASE")
        print(
            "Transaction:",
            row.transaction_id
        )
        print(
            "Inventory:",
            current_qty,
            "->",
            new_qty
        )
        print(
            f"Processing lag: "
            f"{lag_seconds:.3f} seconds"
        )

    connection.close()


query = (
    events.writeStream
    .foreachBatch(update_hbase)
    .option(
        "checkpointLocation",
        str(CHECKPOINT)
    )
    .trigger(processingTime="2 seconds")
    .start()
)


print()
print("Spark stream started.")
print("Initializing Kafka offsets...")

# Ensure Spark establishes its starting offset BEFORE
# we produce the test event.
query.processAllAvailable()

print("Spark stream ready.")


# ---------------------------------------------------------
# Produce one controlled POS event
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

event_time = utc_now()

event = {
    "transaction_id": TEST_TX,
    "line_id": 1,
    "store_id": STORE_ID,
    "sku": SKU,
    "quantity": 1,
    "unit_price": 100.0,
    "discount_pct": 0.0,
    "transaction_timestamp":
        event_time.isoformat(),
    "channel": "STORE",
    "payment_type": "UPI",
    "replay_source":
        "spark_latency_test",
}

print()
print("Sending controlled POS event...")
print("Transaction:", TEST_TX)
print(
    "Event timestamp:",
    event_time.isoformat()
)

metadata = producer.send(
    TOPIC,
    value=event
).get(timeout=10)

producer.flush()

print(
    f"Kafka acknowledged: "
    f"partition={metadata.partition}, "
    f"offset={metadata.offset}"
)


# ---------------------------------------------------------
# Wait for Spark/HBase
# ---------------------------------------------------------

deadline = time.time() + 30

success = False

while time.time() < deadline:

    # Force any available micro-batch to finish
    query.processAllAvailable()

    connection = happybase.Connection(
        host="localhost",
        port=9090,
        timeout=10000,
        transport="buffered",
        protocol="binary",
        compat="0.96",
    )

    table = connection.table(
        "inventory_current"
    )

    result = table.row(ROW_KEY)

    connection.close()

    last_tx = result.get(
        b"meta:last_transaction_id"
    )

    if (
        last_tx
        and last_tx.decode() == TEST_TX
    ):

        finish_time = utc_now()

        total_lag = (
            finish_time - event_time
        ).total_seconds()

        stored_lag = result.get(
            b"meta:last_sync_lag_seconds"
        )

        current_qty = result.get(
            b"state:quantity_on_hand"
        )

        print()
        print("=" * 55)
        print("STOCK SYNC LATENCY RESULT")
        print("=" * 55)

        print(
            "Transaction:",
            TEST_TX
        )

        print(
            "Store / SKU:",
            STORE_ID,
            SKU
        )

        if current_qty:
            print(
                "Current quantity:",
                current_qty.decode()
            )

        if stored_lag:
            print(
                "Spark/HBase processing lag:",
                stored_lag.decode(),
                "seconds"
            )

        print(
            f"Observed end-to-end lag: "
            f"{total_lag:.3f} seconds"
        )

        print(
            "Required target: < 30 seconds"
        )

        if total_lag < 30:
            print("RESULT: PASS")
        else:
            print("RESULT: FAIL")

        print("=" * 55)

        success = True
        break

    time.sleep(1)


if not success:
    print()
    print(
        "RESULT: FAIL - inventory was not "
        "updated within 30 seconds."
    )


query.stop()
producer.close()
spark.stop()
