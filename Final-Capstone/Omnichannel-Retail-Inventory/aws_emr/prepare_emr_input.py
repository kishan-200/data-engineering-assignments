from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

SALES_PATH = "data_lake/silver/sales_clean"
OUTPUT_PATH = "aws_emr/monthly_top100"

spark = (
    SparkSession.builder
    .appName("PrepareEMRForecastInput")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet(SALES_PATH)

top_skus = (
    sales.groupBy("sku")
    .agg(spark_sum("quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
    .limit(100)
    .select("sku")
)

monthly = (
    sales.join(top_skus, "sku")
    .groupBy("sku", "year", "month")
    .agg(spark_sum("quantity").alias("monthly_quantity"))
    .orderBy("sku", "year", "month")
)

monthly.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)

print("\n===== EMR INPUT PREPARED =====")
print("Rows:", monthly.count())
print("Output:", OUTPUT_PATH)

spark.stop()
