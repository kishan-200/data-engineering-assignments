from datetime import datetime, timezone

import happybase

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "inventory_updates"

HBASE_HOST = "localhost"
HBASE_PORT = 9090

INVENTORY_TABLE = "inventory_current"
AUDIT_TABLE = "processed_inventory_events"

CHECKPOINT = "streaming/checkpoints/hbase_inventory_updater"


schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("line_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("sku", StringType(), False),

    StructField("quantity_sold", IntegerType(), True),
    StructField("quantity_change", IntegerType(), True),

    StructField("event_type", StringType(), True),

    StructField("source_transaction_timestamp", StringType(), True),
    StructField("inventory_event_timestamp", StringType(), True),

    StructField("source_partition", IntegerType(), True),
    StructField("source_offset", LongType(), True),
])


def bytes_to_int(value, default=0):
    if value is None:
        return default

    try:
        return int(value.decode("utf-8"))
    except Exception:
        return default


def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"[BATCH {batch_id}] No records")
        return

    print()
    print("=" * 70)
    print(f"PROCESSING MICRO-BATCH {batch_id}")
    print("=" * 70)

    connection = happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=30000,
        transport="buffered",
        protocol="binary",
        compat="0.96"
    )

    inventory_table = connection.table(INVENTORY_TABLE)
    audit_table = connection.table(AUDIT_TABLE)

    processed = 0
    duplicates = 0
    missing_inventory = 0
    rejected = 0

    try:

        rows = batch_df.select(
            "transaction_id",
            "line_id",
            "store_id",
            "sku",
            "quantity_sold",
            "quantity_change",
            "event_type",
            "inventory_event_timestamp",
            "source_partition",
            "source_offset"
        ).toLocalIterator()

        for row in rows:

            transaction_id = row["transaction_id"]
            line_id = row["line_id"]
            store_id = row["store_id"]
            sku = row["sku"]

            quantity_sold = row["quantity_sold"]

            if (
                transaction_id is None
                or line_id is None
                or store_id is None
                or sku is None
                or quantity_sold is None
                or quantity_sold <= 0
            ):
                rejected += 1
                print("[REJECTED] Invalid inventory event")
                continue

            event_id = f"{transaction_id}#{line_id}"
            row_key = f"{store_id}#{sku}"

            row_key_bytes = row_key.encode("utf-8")

            event_column = (
                f"events:{event_id}"
                .encode("utf-8")
            )

            # Read current stock + event marker.
            current = inventory_table.row(
                row_key_bytes,
                columns=[
                    b"state:quantity_on_hand",
                    b"state:reorder_point",
                    b"state:max_stock_level",
                    b"state:stock_status",
                    event_column,
                ],
            )

            if not current:
                missing_inventory += 1

                print(
                    f"[MISSING] {event_id} -> "
                    f"inventory row {row_key} not found"
                )

                continue

            # ------------------------------------------------------
            # DUPLICATE PROTECTION
            # ------------------------------------------------------

            if event_column in current:

                duplicates += 1

                print(
                    f"[DUPLICATE SKIPPED] "
                    f"{event_id} | {row_key}"
                )

                continue

            old_quantity = bytes_to_int(
                current.get(b"state:quantity_on_hand")
            )

            reorder_point = bytes_to_int(
                current.get(b"state:reorder_point")
            )

            max_stock_level = bytes_to_int(
                current.get(b"state:max_stock_level")
            )

            new_quantity = max(
                0,
                old_quantity - quantity_sold
            )

            # Recalculate stock state.
            if new_quantity <= 0:
                stock_status = "CRITICAL"

            elif new_quantity <= reorder_point:
                stock_status = "LOW"

            else:
                stock_status = "HEALTHY"

            stock_gap = new_quantity - reorder_point

            if new_quantity <= reorder_point:
                reorder_quantity = max(
                    0,
                    max_stock_level - new_quantity
                )
            else:
                reorder_quantity = 0

            updated_at = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            # ------------------------------------------------------
            # ONE HBASE ROW MUTATION
            #
            # quantity + derived state + event marker
            # are written together on the inventory row.
            # ------------------------------------------------------

            inventory_table.put(
                row_key_bytes,
                {
                    b"state:quantity_on_hand":
                        str(new_quantity).encode(),

                    b"state:stock_status":
                        stock_status.encode(),

                    b"state:stock_gap":
                        str(stock_gap).encode(),

                    b"state:reorder_quantity":
                        str(reorder_quantity).encode(),

                    b"meta:last_updated":
                        updated_at.encode(),

                    b"meta:last_transaction_id":
                        transaction_id.encode(),

                    b"meta:last_event_id":
                        event_id.encode(),

                    event_column:
                        b"APPLIED",
                }
            )

            # Separate table is retained as an audit ledger.
            audit_table.put(
                event_id.encode(),
                {
                    b"event:status": b"APPLIED",
                    b"event:row_key": row_key.encode(),
                    b"event:transaction_id":
                        transaction_id.encode(),
                    b"event:line_id":
                        line_id.encode(),
                    b"event:quantity_sold":
                        str(quantity_sold).encode(),
                    b"event:processed_at":
                        updated_at.encode(),
                }
            )

            processed += 1

            print(
                f"[APPLIED] {event_id} | "
                f"{row_key} | "
                f"{old_quantity} -> {new_quantity} | "
                f"sold={quantity_sold} | "
                f"status={stock_status}"
            )

    finally:
        connection.close()

    print()
    print("-" * 70)
    print(f"BATCH {batch_id} SUMMARY")
    print("-" * 70)
    print(f"Applied           : {processed}")
    print(f"Duplicates skipped: {duplicates}")
    print(f"Missing inventory : {missing_inventory}")
    print(f"Rejected          : {rejected}")
    print("-" * 70)


spark = (
    SparkSession.builder
    .appName("KafkaToHBaseInventoryUpdater")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    # We only want NEW events during this test.
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


parsed = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("event"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "event.*",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
)


valid_events = (
    parsed
    .filter(col("transaction_id").isNotNull())
    .filter(col("line_id").isNotNull())
    .filter(col("store_id").isNotNull())
    .filter(col("sku").isNotNull())
    .filter(col("quantity_sold") > 0)
    .filter(col("event_type") == "SALE")
)


query = (
    valid_events.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT
    )
    .trigger(processingTime="20 seconds")
    .start()
)


print()
print("=" * 70)
print("REAL-TIME HBASE INVENTORY UPDATER STARTED")
print("=" * 70)
print(f"Kafka topic : {KAFKA_TOPIC}")
print(f"HBase table : {INVENTORY_TABLE}")
print(f"Checkpoint  : {CHECKPOINT}")
print()
print("Waiting for NEW inventory events...")
print("=" * 70)


query.awaitTermination()
