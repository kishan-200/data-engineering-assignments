from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "bronze" / "pos_stream"
CHECKPOINT_PATH = PROJECT_ROOT / "streaming" / "checkpoints" / "pos_sales"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "pos_sales"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("POSSalesStreaming")
    .master("local[*]")
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
    StructField("transaction_id", StringType(), False),
    StructField("line_id", LongType(), False),
    StructField("store_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("quantity", LongType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount_pct", DoubleType(), False),
    StructField("transaction_timestamp", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("payment_type", StringType(), False),
])


print("=" * 70)
print("SPARK STRUCTURED STREAMING - POS SALES")
print("=" * 70)

print(f"Kafka topic : {KAFKA_TOPIC}")
print(f"Output      : {OUTPUT_PATH}")
print(f"Checkpoint  : {CHECKPOINT_PATH}")


# ============================================================
# READ KAFKA STREAM
# ============================================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

print("\n[OK] Kafka streaming source created")


# ============================================================
# PARSE JSON
# ============================================================

json_stream = kafka_stream.select(
    col("value").cast("string").alias("json_value"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
)

parsed = (
    json_stream
    .select(
        from_json(col("json_value"), sales_schema).alias("sale"),
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    .select(
        "sale.*",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
)


# ============================================================
# VALIDATE EVENTS
# ============================================================

valid_sales = parsed.filter(
    col("transaction_id").isNotNull()
    & col("store_id").isNotNull()
    & col("sku").isNotNull()
    & (col("quantity") > 0)
    & (col("unit_price") > 0)
    & (col("discount_pct") >= 0)
    & (col("discount_pct") <= 100)
)


# ============================================================
# SALES METRICS
# ============================================================

processed = (
    valid_sales
    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_timestamp"))
    )
    .withColumn(
        "gross_amount",
        spark_round(
            col("quantity") * col("unit_price"),
            2
        )
    )
    .withColumn(
        "discount_amount",
        spark_round(
            col("quantity")
            * col("unit_price")
            * col("discount_pct")
            / 100,
            2
        )
    )
    .withColumn(
        "net_sales_amount",
        spark_round(
            col("quantity")
            * col("unit_price")
            * (1 - col("discount_pct") / 100),
            2
        )
    )
)


# ============================================================
# WRITE STREAM
# ============================================================

query = (
    processed.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(OUTPUT_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("[OK] Spark Structured Streaming query started")
print("\nWaiting for POS events...")
print("Press Ctrl+C to stop safely.\n")


try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping POS streaming pipeline...")

finally:
    query.stop()
    spark.stop()

    print("=" * 70)
    print("POS STREAMING PIPELINE STOPPED")
    print("=" * 70)
