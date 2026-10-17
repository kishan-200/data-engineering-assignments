from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE = PROJECT_ROOT / "data_lake" / "bronze"
SILVER = PROJECT_ROOT / "data_lake" / "silver"

SALES_PATH = BRONZE / "sales"
STORES_PATH = BRONZE / "stores" / "stores.parquet"
PRODUCTS_PATH = BRONZE / "products" / "products.parquet"

OUTPUT_PATH = SILVER / "sales_clean"

EXPECTED_ROWS = 5_000_000


# ============================================================
# SPARK
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("BuildSilverSales")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# MAIN
# ============================================================

def main():

    start = time.time()

    print("=" * 70)
    print("SILVER SALES TRANSFORMATION")
    print("=" * 70)

    spark = create_spark()

    try:

        # ====================================================
        # READ BRONZE
        # ====================================================

        print("\nReading Bronze datasets...")

        sales = (
            spark.read
            .option("basePath", str(SALES_PATH))
            .parquet(str(SALES_PATH))
        )

        stores = spark.read.parquet(
            str(STORES_PATH)
        )

        products = spark.read.parquet(
            str(PRODUCTS_PATH)
        )

        print("[OK] Bronze datasets loaded")

        # ====================================================
        # SELECT MASTER DATA
        # ====================================================

        stores_dim = stores.select(
            "store_id",
            "store_name",
            "city",
            "state",
            "region",
            "store_type",
        )

        products_dim = products.select(
            "sku",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "demand_class",
        )

        # ====================================================
        # DERIVE SALES METRICS
        # ====================================================

        print("\nCreating sales metrics...")

        sales_clean = (
            sales

            .withColumn(
                "transaction_date",
                F.to_date("transaction_timestamp")
            )

            .withColumn(
                "day",
                F.dayofmonth("transaction_timestamp")
            )

            .withColumn(
                "hour",
                F.hour("transaction_timestamp")
            )

            .withColumn(
                "day_of_week",
                F.date_format(
                    "transaction_timestamp",
                    "EEEE"
                )
            )

            .withColumn(
                "is_weekend",
                F.dayofweek(
                    "transaction_timestamp"
                ).isin(1, 7)
            )

            .withColumn(
                "gross_amount",
                F.round(
                    F.col("quantity")
                    * F.col("unit_price"),
                    2
                )
            )

            .withColumn(
                "discount_amount",
                F.round(
                    F.col("quantity")
                    * F.col("unit_price")
                    * F.col("discount_pct")
                    / 100,
                    2
                )
            )

            .withColumn(
                "net_sales_amount",
                F.round(
                    (
                        F.col("quantity")
                        * F.col("unit_price")
                    )
                    * (
                        1
                        - F.col("discount_pct") / 100
                    ),
                    2
                )
            )
        )

        # ====================================================
        # ENRICH WITH MASTER DATA
        # ====================================================

        print("Joining store dimension...")

        sales_clean = sales_clean.join(
            F.broadcast(stores_dim),
            on="store_id",
            how="left",
        )

        print("Joining product dimension...")

        sales_clean = sales_clean.join(
            F.broadcast(products_dim),
            on="sku",
            how="left",
        )

        # ====================================================
        # FINAL COLUMN ORDER
        # ====================================================

        sales_clean = sales_clean.select(

            "transaction_id",
            "line_id",

            "store_id",
            "sku",

            "transaction_timestamp",
            "transaction_date",

            "year",
            "month",
            "day",
            "hour",
            "day_of_week",
            "is_weekend",

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
            "region",
            "store_type",

            "product_name",
            "category",
            "subcategory",
            "brand",
            "demand_class",
        )

        # ====================================================
        # PRE-WRITE VALIDATION
        # ====================================================

        print("\nValidating transformed dataset...")

        row_count = sales_clean.count()

        print(
            f"Silver candidate rows: "
            f"{row_count:,}"
        )

        if row_count != EXPECTED_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_ROWS:,}, "
                f"found {row_count:,}"
            )

        print("[OK] Row count preserved")

        # Check enrichment

        unmatched_stores = (
            sales_clean
            .filter(
                F.col("store_name").isNull()
            )
            .count()
        )

        unmatched_products = (
            sales_clean
            .filter(
                F.col("product_name").isNull()
            )
            .count()
        )

        print(
            f"Unmatched stores   : "
            f"{unmatched_stores:,}"
        )

        print(
            f"Unmatched products : "
            f"{unmatched_products:,}"
        )

        if unmatched_stores != 0:
            raise ValueError(
                "Store enrichment failed"
            )

        if unmatched_products != 0:
            raise ValueError(
                "Product enrichment failed"
            )

        print("[OK] Master-data enrichment valid")

        # ====================================================
        # BUSINESS VALIDATION
        # ====================================================

        invalid_amounts = (
            sales_clean
            .filter(
                (F.col("gross_amount") <= 0)
                | (F.col("net_sales_amount") < 0)
                | (
                    F.col("discount_amount")
                    < 0
                )
            )
            .count()
        )

        print(
            f"Invalid monetary rows: "
            f"{invalid_amounts:,}"
        )

        if invalid_amounts != 0:
            raise ValueError(
                "Invalid monetary values detected"
            )

        print("[OK] Monetary values valid")

        # ====================================================
        # WRITE SILVER
        # ====================================================

        print("\nWriting Silver sales...")

        (
            sales_clean
            .repartition(
                24,
                "year",
                "month"
            )
            .write
            .mode("overwrite")
            .partitionBy(
                "year",
                "month"
            )
            .parquet(
                str(OUTPUT_PATH)
            )
        )

        print("[OK] Silver sales written")

        # ====================================================
        # READ BACK
        # ====================================================

        print("\nVerifying written Silver dataset...")

        written = spark.read.parquet(
            str(OUTPUT_PATH)
        )

        written_count = written.count()

        print(
            f"Written rows: "
            f"{written_count:,}"
        )

        if written_count != EXPECTED_ROWS:
            raise ValueError(
                "Silver write verification failed"
            )

        print("[OK] Silver row count verified")

        # ====================================================
        # OUTPUT FILE COUNT
        # ====================================================

        parquet_files = list(
            OUTPUT_PATH.rglob("*.parquet")
        )

        print(
            f"Silver Parquet files: "
            f"{len(parquet_files):,}"
        )

        # ====================================================
        # SAMPLE
        # ====================================================

        print("\nSilver sample:")

        written.select(
            "transaction_id",
            "store_id",
            "sku",
            "quantity",
            "gross_amount",
            "discount_amount",
            "net_sales_amount",
            "region",
            "category",
            "demand_class",
        ).show(
            5,
            truncate=False,
        )

        # ====================================================
        # COMPLETE
        # ====================================================

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("SILVER SALES BUILD COMPLETE")
        print("=" * 70)

        print(
            f"Rows         : "
            f"{written_count:,}"
        )

        print(
            f"Parquet files: "
            f"{len(parquet_files):,}"
        )

        print(
            f"Output       : "
            f"{OUTPUT_PATH}"
        )

        print(
            f"Elapsed time : "
            f"{elapsed:.2f} seconds"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
