from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import pandas as pd
import numpy as np

SALES_PATH = "data_lake/silver/sales_clean"
TOP_SKUS = 100
TEST_WEEKS = 8

spark = (
    SparkSession.builder
    .appName("WeeklyDemandForecast")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

# Daily quantity per SKU
daily = (
    sales.groupBy("transaction_date", "sku")
    .agg(spark_sum("quantity").alias("quantity"))
)

# Top 100 SKUs
top_skus = (
    daily.groupBy("sku")
    .agg(spark_sum("quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
    .limit(TOP_SKUS)
    .select("sku")
)

pdf = daily.join(top_skus, "sku").toPandas()

pdf["transaction_date"] = pd.to_datetime(pdf["transaction_date"])

# --------------------------------------------------
# Weekly aggregation
# --------------------------------------------------
pdf["week"] = (
    pdf["transaction_date"]
    .dt.to_period("W-SUN")
    .apply(lambda x: x.start_time)
)

weekly = (
    pdf.groupby(["sku", "week"], as_index=False)["quantity"]
    .sum()
    .rename(columns={"quantity": "actual"})
)

# Fill missing SKU/week combinations with zero
all_weeks = pd.date_range(
    weekly["week"].min(),
    weekly["week"].max(),
    freq="7D"
)

skus = weekly["sku"].unique()

full_index = pd.MultiIndex.from_product(
    [skus, all_weeks],
    names=["sku", "week"]
)

weekly = (
    weekly.set_index(["sku", "week"])
    .reindex(full_index, fill_value=0)
    .reset_index()
    .sort_values(["sku", "week"])
)

# --------------------------------------------------
# Forecast models
# --------------------------------------------------

# Previous week's demand
weekly["naive"] = (
    weekly.groupby("sku")["actual"].shift(1)
)

# Average previous 4 weeks
weekly["ma4"] = (
    weekly.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(4).mean())
)

# Average previous 8 weeks
weekly["ma8"] = (
    weekly.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(8).mean())
)

# Average previous 12 weeks
weekly["ma12"] = (
    weekly.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(12).mean())
)

# Weighted recent demand
weekly["weighted4"] = (
    weekly.groupby("sku")["actual"]
    .transform(
        lambda x:
        x.shift(1) * 0.4 +
        x.shift(2) * 0.3 +
        x.shift(3) * 0.2 +
        x.shift(4) * 0.1
    )
)

# --------------------------------------------------
# Last 8 weeks as test data
# --------------------------------------------------
max_week = weekly["week"].max()
test_start = max_week - pd.Timedelta(weeks=TEST_WEEKS - 1)

test = weekly[weekly["week"] >= test_start].copy()

print("\n===== WEEKLY FORECAST TEST =====")
print("Start week :", test_start.date())
print("End week   :", max_week.date())
print("SKUs       :", test["sku"].nunique())
print("Rows       :", len(test))

def mape(actual, forecast):
    mask = (
        actual.notna() &
        forecast.notna() &
        (actual > 0)
    )

    a = actual[mask]
    f = forecast[mask]

    return np.mean(np.abs((a - f) / a)) * 100

models = {
    "Previous Week": "naive",
    "4-Week Moving Average": "ma4",
    "8-Week Moving Average": "ma8",
    "12-Week Moving Average": "ma12",
    "Weighted 4-Week Average": "weighted4"
}

results = []

print("\n===== WEEKLY FORECAST MAPE =====")

for name, column in models.items():
    score = mape(test["actual"], test[column])
    results.append((name, score))
    print(f"{name:28s}: {score:.2f}%")

best_name, best_score = min(results, key=lambda x: x[1])

print("\n===== BEST WEEKLY MODEL =====")
print("Model :", best_name)
print(f"MAPE  : {best_score:.2f}%")

if best_score < 25:
    print("Target: PASSED (<25%)")
else:
    print("Target: NOT YET MET (<25% required)")

pd.DataFrame(
    results,
    columns=["model", "mape"]
).to_csv(
    "analytics/forecasting/weekly_mape_results.csv",
    index=False
)

spark.stop()
