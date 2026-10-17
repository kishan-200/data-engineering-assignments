from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = (
    SparkSession.builder
    .appName("NightlyDemandForecast")
    .getOrCreate()
)

INPUT = "s3://omnichannel-retail-kishan-034362072403/emr/input/"
OUTPUT = "s3://omnichannel-retail-kishan-034362072403/emr/output/jan2026/"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT)
)

# Same-month-last-year model:
# January 2025 demand becomes January 2026 forecast.
forecast = (
    df.filter(
        (col("year") == 2025) &
        (col("month") == 1)
    )
    .select(
        "sku",
        col("monthly_quantity").alias("forecast_quantity")
    )
    .withColumn("forecast_year", lit(2026))
    .withColumn("forecast_month", lit(1))
    .withColumn("model", lit("same_month_last_year"))
    .withColumn("validation_mape_pct", lit(12.94))
)

forecast.write.mode("overwrite").parquet(OUTPUT)

print("===== EMR FORECAST COMPLETE =====")
print("Forecast rows:", forecast.count())
print("Output:", OUTPUT)

forecast.orderBy(
    col("forecast_quantity").desc()
).show(10, truncate=False)

spark.stop()
