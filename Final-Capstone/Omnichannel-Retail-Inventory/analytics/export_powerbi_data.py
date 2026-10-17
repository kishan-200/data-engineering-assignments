from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("PowerBIExport")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 1. STOCK-OUT RISK
# --------------------------------------------------

replenishment = spark.read.parquet(
    "data_lake/gold/replenishment_recommendations"
)

stock_risk = (
    replenishment
    .withColumn(
        "stock_out_risk",
        F.when(
            F.col("projected_stock_after_forecast") <= 0,
            "HIGH"
        )
        .when(
            F.col("projected_stock_after_forecast")
            <= F.col("reorder_level"),
            "MEDIUM"
        )
        .otherwise("LOW")
    )
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
        "recommended_order_qty",
        "estimated_po_value",
        "stock_out_risk"
    )
)

stock_risk.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("powerbi/data/stock_out_risk")


# --------------------------------------------------
# 2. SELL-THROUGH RATE
# --------------------------------------------------

sales = spark.read.parquet(
    "data_lake/silver/sales_clean"
)

inventory = spark.read.parquet(
    "data_lake/silver/inventory_clean"
)

sales_summary = (
    sales
    .groupBy("store_id", "sku")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("net_sales_amount").alias("net_sales")
    )
)

sell_through = (
    inventory
    .join(
        sales_summary,
        ["store_id", "sku"],
        "left"
    )
    .fillna({
        "units_sold": 0,
        "net_sales": 0
    })
    .withColumn(
        "sell_through_rate_pct",
        F.when(
            (F.col("opening_stock") + F.col("units_sold")) > 0,
            (
                F.col("units_sold")
                /
                (
                    F.col("opening_stock")
                    + F.col("units_sold")
                )
            ) * 100
        ).otherwise(0)
    )
    .select(
        "store_id",
        "sku",
        "opening_stock",
        "units_sold",
        "net_sales",
        F.round(
            "sell_through_rate_pct", 2
        ).alias("sell_through_rate_pct")
    )
)

sell_through.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("powerbi/data/sell_through")


# --------------------------------------------------
# 3. SUPPLIER LEAD TIME
# --------------------------------------------------

po = spark.read.parquet(
    "data_lake/silver/purchase_orders_clean"
)

supplier_lead = (
    po
    .groupBy(
        "supplier_id",
        "supplier_name",
        "supplier_region"
    )
    .agg(
        F.countDistinct("po_id").alias("purchase_orders"),
        F.avg(
            "actual_lead_time_days"
        ).alias("avg_actual_lead_time_days"),
        F.avg(
            "expected_lead_time_days"
        ).alias("avg_expected_lead_time_days"),
        F.avg(
            F.col("is_late").cast("double")
        ).alias("late_rate"),
        F.avg(
            "fill_rate"
        ).alias("avg_fill_rate"),
        F.avg(
            "reliability_score"
        ).alias("reliability_score")
    )
    .withColumn(
        "late_rate_pct",
        F.round(F.col("late_rate") * 100, 2)
    )
    .withColumn(
        "avg_actual_lead_time_days",
        F.round("avg_actual_lead_time_days", 2)
    )
    .withColumn(
        "avg_expected_lead_time_days",
        F.round("avg_expected_lead_time_days", 2)
    )
    .withColumn(
        "avg_fill_rate",
        F.round("avg_fill_rate", 2)
    )
    .drop("late_rate")
)

supplier_lead.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("powerbi/data/supplier_lead_time")


print("\n===== POWER BI EXPORT COMPLETE =====")
print("Stock-out risk rows :", stock_risk.count())
print("Sell-through rows   :", sell_through.count())
print("Supplier rows       :", supplier_lead.count())

spark.stop()
