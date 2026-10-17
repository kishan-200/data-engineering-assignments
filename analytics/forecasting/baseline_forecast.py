from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import pandas as pd
import numpy as np

SALES_PATH = "data_lake/silver/sales_clean"
TEST_DAYS = 30
TOP_SKUS = 100

spark = (
    SparkSession.builder
    .appName("DemandForecastBaseline")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

# Daily demand by SKU
daily = (
    sales.groupBy("transaction_date", "sku")
    .agg(spark_sum("quantity").alias("actual"))
)

# Find top 100 SKUs
top_skus = (
    daily.groupBy("sku")
    .agg(spark_sum("actual").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
    .limit(TOP_SKUS)
    .select("sku")
)

top_daily = daily.join(top_skus, "sku")

pdf = top_daily.toPandas()
pdf["transaction_date"] = pd.to_datetime(pdf["transaction_date"])

# ---------------------------------------------------------
# Fill missing SKU/date combinations with zero demand
# ---------------------------------------------------------
all_dates = pd.date_range(
    pdf["transaction_date"].min(),
    pdf["transaction_date"].max(),
    freq="D"
)

skus = pdf["sku"].unique()

full_index = pd.MultiIndex.from_product(
    [skus, all_dates],
    names=["sku", "transaction_date"]
)

pdf = (
    pdf.set_index(["sku", "transaction_date"])
       .reindex(full_index, fill_value=0)
       .reset_index()
)

pdf = pdf.sort_values(["sku", "transaction_date"])

# ---------------------------------------------------------
# Forecast models
# ---------------------------------------------------------

# Yesterday's demand
pdf["forecast_naive"] = (
    pdf.groupby("sku")["actual"]
    .shift(1)
)

# 7-day moving average
pdf["forecast_ma7"] = (
    pdf.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(7).mean())
)

# 28-day moving average
pdf["forecast_ma28"] = (
    pdf.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(28).mean())
)

# Same weekday last week
pdf["forecast_weekly"] = (
    pdf.groupby("sku")["actual"]
    .shift(7)
)

# ---------------------------------------------------------
# Last 30 days = test period
# ---------------------------------------------------------
max_date = pdf["transaction_date"].max()
test_start = max_date - pd.Timedelta(days=TEST_DAYS - 1)

test = pdf[pdf["transaction_date"] >= test_start].copy()

print("\n===== FORECAST TEST PERIOD =====")
print("Start:", test_start.date())
print("End  :", max_date.date())
print("SKUs :", len(test["sku"].unique()))
print("Rows :", len(test))

# MAPE is undefined when actual = 0.
# Therefore calculate MAPE only for positive-demand observations.
positive = test[test["actual"] > 0].copy()

def mape(actual, forecast):
    mask = forecast.notna()
    actual = actual[mask]
    forecast = forecast[mask]

    return np.mean(
        np.abs((actual - forecast) / actual)
    ) * 100

models = {
    "Naive Previous Day": "forecast_naive",
    "7-Day Moving Average": "forecast_ma7",
    "28-Day Moving Average": "forecast_ma28",
    "Same Day Last Week": "forecast_weekly"
}

print("\n===== FORECAST MAPE =====")

results = []

for name, column in models.items():
    score = mape(
        positive["actual"],
        positive[column]
    )

    results.append((name, score))
    print(f"{name:25s}: {score:.2f}%")

best_name, best_mape = min(results, key=lambda x: x[1])

print("\n===== BEST BASELINE MODEL =====")
print("Model :", best_name)
print(f"MAPE  : {best_mape:.2f}%")

if best_mape < 25:
    print("Target: PASSED (<25%)")
else:
    print("Target: NOT YET MET (<25% required)")

# Save results
result_df = pd.DataFrame(results, columns=["model", "mape"])
result_df.to_csv(
    "analytics/forecasting/baseline_mape_results.csv",
    index=False
)

spark.stop()
