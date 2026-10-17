from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_SALES = PROJECT_ROOT / "data_lake" / "bronze" / "sales"
SILVER_SALES = PROJECT_ROOT / "data_lake" / "silver" / "sales_clean"

EXPECTED_ROWS = 5_000_000


def create_spark():
    spark = (
        SparkSession.builder
        .appName("ValidateSilverSales")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    start = time.time()

    print("=" * 70)
    print("SILVER SALES VALIDATION")
    print("=" * 70)

    spark = create_spark()

    try:

        print("\nReading Bronze and Silver...")

        bronze = spark.read.parquet(
            str(BRONZE_SALES)
        )

        silver = spark.read.parquet(
            str(SILVER_SALES)
        )

        print("[OK] Datasets loaded")

        # ----------------------------------------------------
        # ROW COUNTS
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("ROW COUNT VALIDATION")
        print("=" * 70)

        bronze_count = bronze.count()
        silver_count = silver.count()

        print(f"Bronze rows : {bronze_count:,}")
        print(f"Silver rows : {silver_count:,}")

        if bronze_count != EXPECTED_ROWS:
            raise ValueError(
                f"Bronze expected {EXPECTED_ROWS:,}, "
                f"found {bronze_count:,}"
            )

        if silver_count != EXPECTED_ROWS:
            raise ValueError(
                f"Silver expected {EXPECTED_ROWS:,}, "
                f"found {silver_count:,}"
            )

        if bronze_count != silver_count:
            raise ValueError(
                "Bronze/Silver row count mismatch"
            )

        print("[OK] All 5,000,000 rows preserved")

        # ----------------------------------------------------
        # NULL CHECK
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("NULL VALIDATION")
        print("=" * 70)

        columns_to_check = [
            "transaction_id",
            "line_id",
            "store_id",
            "sku",
            "transaction_timestamp",
            "transaction_date",
            "quantity",
            "unit_price",
            "discount_pct",
            "gross_amount",
            "discount_amount",
            "net_sales_amount",
            "channel",
            "payment_type",
            "store_name",
            "region",
            "product_name",
            "category",
            "demand_class",
        ]

        null_exprs = [
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in columns_to_check
        ]

        null_result = (
            silver
            .agg(*null_exprs)
            .collect()[0]
            .asDict()
        )

        total_nulls = 0

        for column, count in null_result.items():
            print(f"{column:<25} {count:,}")
            total_nulls += count

        if total_nulls != 0:
            raise ValueError(
                f"Silver contains {total_nulls:,} NULL values"
            )

        print("[OK] Required Silver columns contain no NULLs")

        # ----------------------------------------------------
        # BUSINESS RULES
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("BUSINESS RULE VALIDATION")
        print("=" * 70)

        invalid = (
            silver
            .filter(
                (F.col("quantity") <= 0)
                | (F.col("unit_price") <= 0)
                | (F.col("discount_pct") < 0)
                | (F.col("discount_pct") > 100)
                | (F.col("gross_amount") <= 0)
                | (F.col("discount_amount") < 0)
                | (F.col("net_sales_amount") < 0)
            )
            .count()
        )

        print(f"Invalid rows: {invalid:,}")

        if invalid != 0:
            raise ValueError(
                "Silver business-rule validation failed"
            )

        print("[OK] Business rules valid")

        # ----------------------------------------------------
        # CALCULATION VALIDATION
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("SALES CALCULATION VALIDATION")
        print("=" * 70)

        calculation_errors = (
            silver
            .filter(
                F.abs(
                    F.col("gross_amount")
                    - F.round(
                        F.col("quantity")
                        * F.col("unit_price"),
                        2
                    )
                ) > 0.01
            )
            .count()
        )

        print(
            f"Gross amount errors: "
            f"{calculation_errors:,}"
        )

        if calculation_errors != 0:
            raise ValueError(
                "Gross amount calculation mismatch"
            )

        net_errors = (
            silver
            .filter(
                F.abs(
                    F.col("net_sales_amount")
                    -
                    (
                        F.col("gross_amount")
                        - F.col("discount_amount")
                    )
                ) > 0.02
            )
            .count()
        )

        print(
            f"Net sales calculation errors: "
            f"{net_errors:,}"
        )

        if net_errors != 0:
            raise ValueError(
                "Net sales calculation mismatch"
            )

        print("[OK] Sales calculations valid")

        # ----------------------------------------------------
        # MASTER DATA ENRICHMENT
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("ENRICHMENT VALIDATION")
        print("=" * 70)

        missing_store = (
            silver
            .filter(
                F.col("store_name").isNull()
            )
            .count()
        )

        missing_product = (
            silver
            .filter(
                F.col("product_name").isNull()
            )
            .count()
        )

        print(
            f"Missing store enrichment   : "
            f"{missing_store:,}"
        )

        print(
            f"Missing product enrichment : "
            f"{missing_product:,}"
        )

        if missing_store != 0 or missing_product != 0:
            raise ValueError(
                "Master-data enrichment validation failed"
            )

        print("[OK] Enrichment valid")

        # ----------------------------------------------------
        # CARDINALITY
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("SILVER STATISTICS")
        print("=" * 70)

        stats = silver.agg(
            F.countDistinct(
                "transaction_id"
            ).alias("transactions"),

            F.countDistinct(
                "store_id"
            ).alias("stores"),

            F.countDistinct(
                "sku"
            ).alias("products"),

            F.sum(
                "quantity"
            ).alias("units_sold"),

            F.sum(
                "net_sales_amount"
            ).alias("net_sales"),
        ).collect()[0]

        print(
            f"Transactions : "
            f"{stats['transactions']:,}"
        )

        print(
            f"Stores       : "
            f"{stats['stores']:,}"
        )

        print(
            f"Products     : "
            f"{stats['products']:,}"
        )

        print(
            f"Units sold   : "
            f"{stats['units_sold']:,}"
        )

        print(
            f"Net sales    : "
            f"{stats['net_sales']:,.2f}"
        )

        # ----------------------------------------------------
        # PARTITIONS
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print("YEAR / MONTH VALIDATION")
        print("=" * 70)

        periods = (
            silver
            .select(
                "year",
                "month"
            )
            .distinct()
            .count()
        )

        print(
            f"Year/month combinations: "
            f"{periods}"
        )

        if periods != 24:
            raise ValueError(
                f"Expected 24 periods, found {periods}"
            )

        print("[OK] 24 year/month periods")

        # ----------------------------------------------------
        # FINAL
        # ----------------------------------------------------

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("SILVER SALES VALIDATION PASSED")
        print("=" * 70)

        print(
            f"Rows verified : "
            f"{silver_count:,}"
        )

        print(
            f"Elapsed time  : "
            f"{elapsed:.2f} seconds"
        )

        print(
            "\nSilver sales are ready "
            "for downstream processing."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
