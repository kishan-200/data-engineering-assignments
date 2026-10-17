from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    greatest,
    lit,
    ceil
)

FORECAST_PATH = "data_lake/gold/store_sku_forecast_monthly"
INVENTORY_PATH = "data_lake/silver/inventory_clean"
SUPPLIER_PRODUCTS_PATH = "data/generated/supplier_products.csv"
OUTPUT_PATH = "data_lake/gold/replenishment_recommendations"

spark = (
    SparkSession.builder
    .appName("ReplenishmentRecommendations")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

forecast = spark.read.parquet(FORECAST_PATH)
inventory = spark.read.parquet(INVENTORY_PATH)
supplier_products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SUPPLIER_PRODUCTS_PATH)
)

# Preferred supplier only
preferred = (
    supplier_products
    .filter(col("preferred_supplier") == 1)
    .select(
        "sku",
        "supplier_id",
        "supplier_cost",
        "minimum_order_qty",
        "lead_time_days"
    )
)

replenishment = (
    inventory.alias("i")
    .join(
        forecast.alias("f"),
        ["store_id", "sku"],
        "left"
    )
    .join(
        preferred.alias("s"),
        ["sku"],
        "left"
    )
    .withColumn(
        "forecast_quantity",
        when(col("forecast_quantity").isNull(), lit(0))
        .otherwise(col("forecast_quantity"))
    )
    .withColumn(
        "projected_stock_after_forecast",
        col("opening_stock") - col("forecast_quantity")
    )
    .withColumn(
        "replenishment_needed",
        when(
            col("projected_stock_after_forecast") <= col("reorder_level"),
            lit(1)
        ).otherwise(lit(0))
    )
    .withColumn(
        "raw_reorder_qty",
        greatest(
            lit(0),
            col("max_stock_level") - col("projected_stock_after_forecast")
        )
    )
    .withColumn(
        "recommended_order_qty",
        when(
            col("replenishment_needed") == 1,
            greatest(
                col("raw_reorder_qty"),
                col("minimum_order_qty")
            )
        ).otherwise(lit(0))
    )
    .withColumn(
        "estimated_po_value",
        col("recommended_order_qty") * col("supplier_cost")
    )
    .withColumn(
        "forecast_month",
        lit("2026-01")
    )
)

output = (
    replenishment
    .filter(col("replenishment_needed") == 1)
    .select(
        "store_id",
        "sku",
        "forecast_month",
        "opening_stock",
        "forecast_quantity",
        "projected_stock_after_forecast",
        "reorder_level",
        "max_stock_level",
        "supplier_id",
        "lead_time_days",
        "minimum_order_qty",
        "recommended_order_qty",
        "supplier_cost",
        "estimated_po_value"
    )
)

output.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\n===== REPLENISHMENT RECOMMENDATIONS =====")
print("Recommendations:", output.count())
print("Output:", OUTPUT_PATH)

print("\n===== TOP 10 REPLENISHMENT NEEDS =====")

output.orderBy(
    col("recommended_order_qty").desc()
).show(10, truncate=False)

spark.stop()
