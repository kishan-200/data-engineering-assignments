from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SALES_PATH = (
    PROJECT_ROOT
    / "data_lake"
    / "bronze"
    / "sales"
)

EXPECTED_ROWS = 5_000_000


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("InspectBronzeSales")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
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
    print("BRONZE SALES - SPARK INSPECTION")
    print("=" * 70)

    print(f"\nSource: {SALES_PATH}")

    spark = create_spark()

    try:

        # ----------------------------------------------------
        # READ BRONZE SALES
        # ----------------------------------------------------

        print("\nReading Bronze sales dataset...")

        sales = (
            spark.read
            .option("basePath", str(SALES_PATH))
            .parquet(str(SALES_PATH))
        )

        print("[OK] Bronze sales loaded")

        # ----------------------------------------------------
        # SCHEMA
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("SPARK SCHEMA")
        print("=" * 70)

        sales.printSchema()

        # ----------------------------------------------------
        # PARTITION INFORMATION
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("SPARK PARTITION INFORMATION")
        print("=" * 70)

        print(
            "Input DataFrame partitions:",
            sales.rdd.getNumPartitions()
        )

        # ----------------------------------------------------
        # ROW COUNT
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("ROW COUNT")
        print("=" * 70)

        row_count = sales.count()

        print(
            f"Rows detected: {row_count:,}"
        )

        if row_count != EXPECTED_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_ROWS:,} rows, "
                f"found {row_count:,}"
            )

        print(
            "[OK] Exact 5,000,000 sales rows"
        )

        # ----------------------------------------------------
        # YEAR / MONTH DISTRIBUTION
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("YEAR / MONTH DISTRIBUTION")
        print("=" * 70)

        (
            sales
            .groupBy("year", "month")
            .count()
            .orderBy("year", "month")
            .show(24, truncate=False)
        )

        # ----------------------------------------------------
        # NULL CHECK
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("NULL CHECK")
        print("=" * 70)

        null_expressions = [
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1
                ).otherwise(0)
            ).alias(column)
            for column in sales.columns
        ]

        null_counts = (
            sales
            .agg(*null_expressions)
            .collect()[0]
            .asDict()
        )

        total_nulls = 0

        for column, count in null_counts.items():

            print(
                f"{column:<25} {count:,}"
            )

            total_nulls += count

        if total_nulls == 0:
            print("\n[OK] No NULL values detected")
        else:
            print(
                f"\n[WARNING] "
                f"{total_nulls:,} NULL values detected"
            )

        # ----------------------------------------------------
        # SAMPLE
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("SAMPLE SALES RECORDS")
        print("=" * 70)

        sales.show(
            5,
            truncate=False
        )

        # ----------------------------------------------------
        # BASIC BUSINESS STATISTICS
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("BASIC SALES STATISTICS")
        print("=" * 70)

        stats = sales.agg(

            F.countDistinct(
                "transaction_id"
            ).alias(
                "unique_transactions"
            ),

            F.countDistinct(
                "store_id"
            ).alias(
                "stores"
            ),

            F.countDistinct(
                "sku"
            ).alias(
                "products"
            ),

            F.sum(
                "quantity"
            ).alias(
                "units_sold"
            ),

            F.avg(
                "quantity"
            ).alias(
                "avg_quantity"
            ),

        )

        stats.show(
            truncate=False
        )

        # ----------------------------------------------------
        # FINAL
        # ----------------------------------------------------

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("BRONZE SALES SPARK INSPECTION PASSED")
        print("=" * 70)

        print(
            f"Rows verified : {row_count:,}"
        )

        print(
            f"Elapsed time  : {elapsed:.2f} seconds"
        )

        print(
            "\nBronze sales are ready "
            "for Silver transformation."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
