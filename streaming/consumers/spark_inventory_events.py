from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    current_timestamp,
    to_json,
    struct,
    to_timestamp,
    round as spark_round,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "streaming"
    / "checkpoints"
    / "inventory_events"
)

KAFKA_BOOTSTRAP = "localhost:9092"

SOURCE_TOPIC = "pos_sales"
TARGET_TOPIC = "inventory_updates"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("InventoryEventStreaming")
    .master("local[1]")
    .config("spark.driver.memory", "2g")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# POS EVENT SCHEMA
# ============================================================

sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("line_id", LongType(), True),
    StructField("store_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("payment_type", StringType(), True),
])


print("=" * 70)
print("REAL-TIME INVENTORY EVENT PIPELINE")
print("=" * 70)

print(f"Source topic : {SOURCE_TOPIC}")
print(f"Target topic : {TARGET_TOPIC}")
print(f"Checkpoint   : {CHECKPOINT_PATH}")


# ============================================================
# READ POS EVENTS
# ============================================================

source = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

print("\n[OK] POS Kafka streaming source created")


# ============================================================
# PARSE JSON
# ============================================================

parsed = (
    source
    .select(
        from_json(
            col("value").cast("string"),
            sales_schema
        ).alias("sale"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset")
    )
    .select(
        "sale.*",
        "source_partition",
        "source_offset"
    )
)


# ============================================================
# VALIDATION
# ============================================================

valid = parsed.filter(
    col("transaction_id").isNotNull()
    & col("store_id").isNotNull()
    & col("sku").isNotNull()
    & col("quantity").isNotNull()
    & (col("quantity") > 0)
)


# ============================================================
# CREATE INVENTORY EVENTS
# ============================================================

inventory_events = (
    valid
    .select(
        col("transaction_id"),
        col("line_id").alias("line_id"),
        col("store_id"),
        col("sku"),

        col("quantity").alias("quantity_sold"),

        (-col("quantity")).alias("quantity_change"),

        lit("SALE").alias("event_type"),

        col("transaction_timestamp")
            .alias("source_transaction_timestamp"),

        current_timestamp()
            .alias("inventory_event_timestamp"),

        col("source_partition"),
        col("source_offset")
    )

    .withColumn(
        "source_pos_ts",
        to_timestamp(
            col("source_transaction_timestamp")
        )
    )

    .withColumn(
        "stock_sync_latency_seconds",
        spark_round(
            col("inventory_event_timestamp").cast("double")
            -
            col("source_pos_ts").cast("double"),
            3
        )
    )

    .withColumn(
        "sla_result",
        when(
            col("stock_sync_latency_seconds") < 30,
            lit("PASS")
        ).otherwise(
            lit("FAIL")
        )
    )
)


# ============================================================
# CONVERT TO KAFKA KEY/VALUE
# ============================================================

kafka_output = inventory_events.select(
    col("store_id")
        .cast("string")
        .alias("key"),

    to_json(
        struct(
            "transaction_id",
            "line_id",
            "store_id",
            "sku",
            "quantity_sold",
            "quantity_change",
            "event_type",
            "source_transaction_timestamp",
            "inventory_event_timestamp",
            "stock_sync_latency_seconds",
            "sla_result",
            "source_partition",
            "source_offset"
        )
    ).alias("value")
)


# ============================================================
# WRITE TO INVENTORY TOPIC
# ============================================================

query = (
    kafka_output.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "topic",
        TARGET_TOPIC
    )
    .option(
        "checkpointLocation",
        str(CHECKPOINT_PATH)
    )
    .outputMode("append")
    .start()
)


print("[OK] Inventory event stream started")

print("\nPipeline:")
print("pos_sales -> Spark -> inventory_updates")

print("\nWaiting for NEW POS sales...")
print("Press Ctrl+C to stop.\n")


try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping inventory event pipeline...")

finally:
    query.stop()
    spark.stop()

    print("=" * 70)
    print("INVENTORY EVENT PIPELINE STOPPED")
    print("=" * 70)
