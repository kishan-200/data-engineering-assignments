from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import pandas as pd
import numpy as np

SALES_PATH = "data_lake/silver/sales_clean"
TOP_SKUS = 100
TEST_MONTHS = 4

spark = (
    SparkSession.builder
    .appName("MonthlyDemandForecast")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

# Aggregate sales by SKU/date first
daily = (
    sales.groupBy("transaction_date", "sku")
    .agg(spark_sum("quantity").alias("quantity"))
)

# Top 100 SKUs across complete history
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
# Monthly demand
# --------------------------------------------------
pdf["month"] = (
    pdf["transaction_date"]
    .dt.to_period("M")
    .dt.to_timestamp()
)

monthly = (
    pdf.groupby(["sku", "month"], as_index=False)["quantity"]
    .sum()
    .rename(columns={"quantity": "actual"})
)

# Complete SKU/month grid
all_months = pd.date_range(
    monthly["month"].min(),
    monthly["month"].max(),
    freq="MS"
)

skus = monthly["sku"].unique()

full_index = pd.MultiIndex.from_product(
    [skus, all_months],
    names=["sku", "month"]
)

monthly = (
    monthly.set_index(["sku", "month"])
    .reindex(full_index, fill_value=0)
    .reset_index()
    .sort_values(["sku", "month"])
)

# --------------------------------------------------
# Forecast models
# --------------------------------------------------

# Previous month
monthly["previous_month"] = (
    monthly.groupby("sku")["actual"].shift(1)
)

# 3-month moving average
monthly["ma3"] = (
    monthly.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(3).mean())
)

# 6-month moving average
monthly["ma6"] = (
    monthly.groupby("sku")["actual"]
    .transform(lambda x: x.shift(1).rolling(6).mean())
)

# Same month previous year
monthly["seasonal_12"] = (
    monthly.groupby("sku")["actual"].shift(12)
)

# Weighted recent three months
monthly["weighted3"] = (
    monthly.groupby("sku")["actual"]
    .transform(
        lambda x:
            x.shift(1) * 0.5 +
            x.shift(2) * 0.3 +
            x.shift(3) * 0.2
    )
)

# --------------------------------------------------
# Holdout: last 4 months
# --------------------------------------------------
max_month = monthly["month"].max()

test_months = sorted(monthly["month"].unique())[-TEST_MONTHS:]

test = monthly[
    monthly["month"].isin(test_months)
].copy()

print("\n===== MONTHLY FORECAST TEST =====")
print("Start month :", min(test_months).date())
print("End month   :", max(test_months).date())
print("SKUs        :", test["sku"].nunique())
print("Rows        :", len(test))

def mape(actual, forecast):
    mask = (
        actual.notna() &
        forecast.notna() &
        (actual > 0)
    )

    a = actual[mask].astype(float)
    f = forecast[mask].astype(float)

    return np.mean(np.abs((a - f) / a)) * 100

models = {
    "Previous Month": "previous_month",
    "3-Month Moving Average": "ma3",
    "6-Month Moving Average": "ma6",
    "Same Month Last Year": "seasonal_12",
    "Weighted 3-Month Average": "weighted3"
}

results = []

print("\n===== MONTHLY FORECAST MAPE =====")

for name, column in models.items():
    score = mape(test["actual"], test[column])
    results.append((name, score))
    print(f"{name:28s}: {score:.2f}%")

best_name, best_score = min(results, key=lambda x: x[1])

print("\n===== BEST MONTHLY MODEL =====")
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
    "analytics/forecasting/monthly_mape_results.csv",
    index=False
)

spark.stop()
