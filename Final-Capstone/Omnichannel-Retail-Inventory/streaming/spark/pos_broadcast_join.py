import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    broadcast
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


PROJECT_ROOT = os.path.expanduser("~/omnichannel-inventory")

PRODUCTS_CSV = f"{PROJECT_ROOT}/data/generated/products.csv"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "pos_sales"


spark = (
    SparkSession.builder
    .appName("POS-Broadcast-Product-Join")
    .master("local[1]")
    .config("spark.driver.memory", "768m")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Static product master
# ---------------------------------------------------------

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTS_CSV)
    .select(
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "selling_price",
        "demand_class"
    )
)

print("Product master rows:", products.count())

# Force broadcast join
products_broadcast = broadcast(products)


# ---------------------------------------------------------
# Kafka POS schema
# ---------------------------------------------------------

pos_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("line_id", IntegerType()),
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("transaction_timestamp", StringType()),
    StructField("channel", StringType()),
    StructField("payment_type", StringType()),
    StructField("replay_source", StringType()),
    StructField("original_stock_code", StringType())
])


# ---------------------------------------------------------
# Kafka stream
# ---------------------------------------------------------

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            pos_schema
        ).alias("data")
    )
    .select("data.*")
)


# ---------------------------------------------------------
# Broadcast product metadata join
# ---------------------------------------------------------

enriched = (
    parsed.alias("pos")
    .join(
        products_broadcast.alias("product"),
        col("pos.sku") == col("product.sku"),
        "left"
    )
    .select(
        col("pos.transaction_id"),
        col("pos.line_id"),
        col("pos.store_id"),
        col("pos.sku"),

        col("product.product_name"),
        col("product.category"),
        col("product.subcategory"),
        col("product.brand"),
        col("product.demand_class"),

        col("pos.quantity"),
        col("pos.unit_price"),
        col("pos.transaction_timestamp"),
        col("pos.channel"),
        col("pos.replay_source")
    )
)


query = (
    enriched.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)


print()
print("Structured Streaming broadcast join started")
print("Listening to Kafka topic:", TOPIC)
print("Waiting for new POS events...")
print()

query.awaitTermination()
