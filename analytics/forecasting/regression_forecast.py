from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
import numpy as np

SALES_PATH = "data_lake/silver/sales_clean"
TOP_SKUS = 100

spark = (
    SparkSession.builder
    .appName("RetailDemandRegression")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

# Top 100 SKUs
top_skus = (
    sales.groupBy("sku")
    .agg(F.sum("quantity").alias("total_qty"))
    .orderBy(F.col("total_qty").desc())
    .limit(TOP_SKUS)
    .select("sku")
)

monthly = (
    sales.join(top_skus, "sku")
    .groupBy("sku", "year", "month")
    .agg(F.sum("quantity").alias("actual"))
)

w = Window.partitionBy("sku").orderBy("year", "month")

# Lag / seasonality features
monthly = (
    monthly
    .withColumn("lag1", F.lag("actual", 1).over(w))
    .withColumn("lag2", F.lag("actual", 2).over(w))
    .withColumn("lag3", F.lag("actual", 3).over(w))
    .withColumn("lag12", F.lag("actual", 12).over(w))
    .withColumn(
        "rolling3",
        (
            F.col("lag1") +
            F.col("lag2") +
            F.col("lag3")
        ) / 3.0
    )
)

model_data = (
    monthly
    .dropna(
        subset=[
            "actual",
            "lag1",
            "lag2",
            "lag3",
            "lag12",
            "rolling3"
        ]
    )
)

# Last 4 months of 2025 = test set
train = model_data.filter(
    (F.col("year") < 2025) |
    (
        (F.col("year") == 2025) &
        (F.col("month") <= 8)
    )
)

test = model_data.filter(
    (F.col("year") == 2025) &
    (F.col("month") >= 9)
)

features = [
    "month",
    "lag1",
    "lag2",
    "lag3",
    "lag12",
    "rolling3"
]

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features"
)

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="actual",
    predictionCol="prediction",
    numTrees=80,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(
    stages=[
        assembler,
        rf
    ]
)

print("\n===== REGRESSION DATA =====")
print("Training rows:", train.count())
print("Test rows    :", test.count())

model = pipeline.fit(train)

predictions = model.transform(test)

# MAPE for positive-demand observations
result = (
    predictions
    .filter(F.col("actual") > 0)
    .withColumn(
        "ape",
        F.abs(
            (F.col("actual") - F.col("prediction"))
            / F.col("actual")
        )
    )
)

mape = result.agg(
    F.avg("ape").alias("mape")
).first()["mape"] * 100

print("\n===== REGRESSION FORECAST RESULT =====")
print("Model : Random Forest Regression")
print(f"MAPE  : {mape:.2f}%")

if mape < 25:
    print("Target: PASSED (<25%)")
else:
    print("Target: NOT YET MET (<25% required)")

print("\n===== SAMPLE PREDICTIONS =====")

(
    predictions
    .select(
        "sku",
        "year",
        "month",
        "actual",
        F.round("prediction", 2).alias("prediction"),
        "lag12"
    )
    .orderBy("sku", "month")
    .show(20, truncate=False)
)

spark.stop()
