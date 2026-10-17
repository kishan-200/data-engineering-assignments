from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format
)

spark = (
    SparkSession.builder
    .appName("PrepareInventoryForHBase")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SOURCE = "data_lake/silver/inventory_clean"
OUTPUT = "hdfs://localhost:9000/tmp/inventory_hbase_bootstrap"

df = spark.read.parquet(SOURCE)

inventory = (
    df.select(
        concat_ws("#", col("store_id"), col("sku")).alias("row_key"),

        # state column family
        col("opening_stock").cast("string").alias("quantity_on_hand"),
        col("reorder_level").cast("string").alias("reorder_point"),
        col("max_stock_level").cast("string").alias("max_stock_level"),
        col("stock_status").alias("stock_status"),
        col("stock_gap").cast("string").alias("stock_gap"),
        col("reorder_quantity").cast("string").alias("reorder_quantity"),

        # meta column family
        col("store_id"),
        col("sku"),
        date_format(
            col("last_updated"),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("last_updated")
    )
)

print("========================================")
print("HBASE BOOTSTRAP PREPARATION")
print("========================================")
print("Rows:", inventory.count())

inventory.show(5, truncate=False)

print()
print("Writing TSV to:")
print(OUTPUT)

(
    inventory
    .coalesce(8)
    .write
    .mode("overwrite")
    .option("sep", "\t")
    .option("header", "false")
    .csv(OUTPUT)
)

print()
print("HDFS bootstrap export completed successfully.")

spark.stop()
