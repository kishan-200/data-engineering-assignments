from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    row_number,
    to_date,
    date_add,
    round as spark_round,
    countDistinct,
    sum as spark_sum
)
from pyspark.sql.window import Window

INPUT_PATH = "data_lake/gold/replenishment_recommendations"
OUTPUT_PATH = "data_lake/gold/draft_purchase_orders"

spark = (
    SparkSession.builder
    .appName("GenerateDraftPurchaseOrders")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

replenishment = spark.read.parquet(INPUT_PATH)

# One draft PO per store + supplier
po_base = (
    replenishment
    .withColumn(
        "draft_po_id",
        concat_ws(
            "_",
            lit("DRAFT"),
            lit("202601"),
            col("store_id"),
            col("supplier_id")
        )
    )
    .withColumn(
        "order_date",
        to_date(lit("2025-12-31"))
    )
    .withColumn(
        "expected_delivery_date",
        date_add(
            to_date(lit("2025-12-31")),
            col("lead_time_days")
        )
    )
)

# Assign line numbers inside each PO
window_spec = (
    Window
    .partitionBy("draft_po_id")
    .orderBy("sku")
)

draft_pos = (
    po_base
    .withColumn(
        "po_line_id",
        row_number().over(window_spec)
    )
    .withColumn(
        "status",
        lit("DRAFT")
    )
    .withColumn(
        "estimated_po_value",
        spark_round(col("estimated_po_value"), 2)
    )
    .select(
        "draft_po_id",
        "po_line_id",
        "store_id",
        "supplier_id",
        "sku",
        "order_date",
        "expected_delivery_date",
        "forecast_month",
        "opening_stock",
        "forecast_quantity",
        "projected_stock_after_forecast",
        "reorder_level",
        "recommended_order_qty",
        "supplier_cost",
        "estimated_po_value",
        "lead_time_days",
        "status"
    )
)

draft_pos.write.mode("overwrite").parquet(OUTPUT_PATH)

summary = (
    draft_pos
    .agg(
        countDistinct("draft_po_id").alias("draft_po_count"),
        spark_sum("recommended_order_qty").alias("total_units"),
        spark_sum("estimated_po_value").alias("total_po_value")
    )
    .first()
)

print("\n===== DRAFT PURCHASE ORDERS =====")
print("Draft POs :", summary["draft_po_count"])
print("PO lines  :", draft_pos.count())
print("Units     :", summary["total_units"])
print(f"Value     : {summary['total_po_value']:.2f}")
print("Status    : DRAFT")
print("Output    :", OUTPUT_PATH)

print("\n===== SAMPLE DRAFT PO LINES =====")

draft_pos.orderBy(
    "draft_po_id",
    "po_line_id"
).show(10, truncate=False)

spark.stop()
