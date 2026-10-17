from pyspark.sql import SparkSession
from pathlib import Path
import os

PROJECT_ROOT = Path.home() / "omnichannel-inventory"
SOURCE_ROOT = PROJECT_ROOT / "data_lake" / "silver" / "sales_clean"

TARGET = (
    "hdfs://localhost:9000"
    "/warehouse/omnichannel/silver/fact_sales_region_date"
)

spark = (
    SparkSession.builder
    .appName("Region-Date-Fact-Sales-Monthly")
    .master("local[1]")
    .config("spark.driver.memory", "768m")
    .config("spark.executor.memory", "768m")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

columns = [
    "transaction_id",
    "line_id",
    "store_id",
    "sku",
    "transaction_timestamp",
    "quantity",
    "unit_price",
    "discount_pct",
    "gross_amount",
    "discount_amount",
    "net_sales_amount",
    "channel",
    "payment_type",
    "store_name",
    "city",
    "state",
    "store_type",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "demand_class",
    "region",
    "transaction_date"
]

months_written = 0

for year in [2024, 2025]:
    for month in range(1, 13):

        path = SOURCE_ROOT / f"year={year}" / f"month={month}"

        if not path.exists():
            print(f"SKIP: {path}")
            continue

        print()
        print("=" * 60)
        print(f"Processing year={year}, month={month}")
        print("=" * 60)

        df = (
            spark.read
            .parquet(str(path))
            .select(*columns)
        )

        (
            df.write
            .mode("append")
            .partitionBy("region", "transaction_date")
            .parquet(TARGET)
        )

        months_written += 1

        print(
            f"COMPLETED year={year}, month={month} "
            f"({months_written}/24 months)"
        )

        spark.catalog.clearCache()

print()
print("ALL MONTHS COMPLETE")
print("Months written:", months_written)
print("Target:", TARGET)

spark.stop()
