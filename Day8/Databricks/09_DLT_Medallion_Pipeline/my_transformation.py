from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ============================================================
# BRONZE LAYER
# Read raw JSON orders using Auto Loader
# ============================================================

@dp.table(
    comment="Raw Bronze ingestion from storage"
)
def dlt_bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/workspace/lab_db/lab_landing/orders/")
    )


# ============================================================
# SILVER LAYER
# Clean data, convert amount to numeric, remove duplicates
# ============================================================

@dp.table(
    comment="Cleansed Silver orders table"
)
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def dlt_silver_orders():
    return (
        dp.read_stream("dlt_bronze_orders")
        .withColumn("amount", col("amount").cast("double"))
        .dropDuplicates(["order_id"])
    )


# ============================================================
# GOLD LAYER
# Calculate total spending for each customer
# ============================================================

@dp.materialized_view(
    comment="Gold aggregations materialized view"
)
def dlt_gold_daily_sales():
    return (
        dp.read("dlt_silver_orders")
        .groupBy("customer_id")
        .sum("amount")
        .withColumnRenamed("sum(amount)", "total_spent")
    )