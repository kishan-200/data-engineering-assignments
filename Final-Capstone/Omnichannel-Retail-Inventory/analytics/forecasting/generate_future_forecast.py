from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    lit,
    round as spark_round
)

SALES_PATH = "data_lake/silver/sales_clean"
OUTPUT_PATH = "data_lake/gold/demand_forecast_monthly"

spark = (
    SparkSession.builder
    .appName("FutureDemandForecast")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

# Winning model:
# Same month from previous year.
#
# Forecast January 2026 using January 2025 actual demand.

jan_2025 = (
    sales
    .filter(
        (col("year") == 2025) &
        (col("month") == 1)
    )
    .groupBy("sku")
    .agg(
        spark_sum("quantity").alias("forecast_quantity")
    )
)

forecast = (
    jan_2025
    .withColumn("forecast_month", lit("2026-01"))
    .withColumn("model", lit("same_month_last_year"))
    .withColumn("validation_mape_pct", lit(12.94))
    .select(
        "forecast_month",
        "sku",
        spark_round(col("forecast_quantity"), 0)
            .cast("long")
            .alias("forecast_quantity"),
        "model",
        "validation_mape_pct"
    )
)

forecast.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\n===== JANUARY 2026 DEMAND FORECAST =====")
print("Model : Same Month Last Year")
print("MAPE  : 12.94%")
print("Output:", OUTPUT_PATH)
print("SKUs  :", forecast.count())

print("\n===== TOP 10 FORECAST DEMAND =====")

forecast.orderBy(
    col("forecast_quantity").desc()
).show(10, truncate=False)

spark.stop()
