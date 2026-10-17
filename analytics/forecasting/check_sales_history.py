from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    min as spark_min,
    max as spark_max,
    sum as spark_sum
)

SALES_PATH = "data_lake/silver/sales_clean"

spark = (
    SparkSession.builder
    .appName("ForecastDataCheck")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

print("\n===== FORECAST DATA CHECK =====")

sales.select(
    spark_min("transaction_date").alias("start_date"),
    spark_max("transaction_date").alias("end_date"),
    countDistinct("transaction_date").alias("distinct_days"),
    countDistinct("sku").alias("distinct_skus"),
    countDistinct("store_id").alias("distinct_stores")
).show(truncate=False)

daily_sku = (
    sales
    .groupBy("transaction_date", "sku")
    .agg(
        spark_sum("quantity").alias("daily_quantity")
    )
)

print("\n===== DAILY SKU DATA =====")
print(f"Daily SKU rows: {daily_sku.count():,}")

print("\n===== TOP 10 SKUs BY TOTAL QUANTITY =====")

(
    daily_sku
    .groupBy("sku")
    .agg(spark_sum("daily_quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
    .show(10, truncate=False)
)

spark.stop()
