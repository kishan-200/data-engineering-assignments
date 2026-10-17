from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT
    / "data_lake"
    / "bronze"
    / "inventory"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data_lake"
    / "silver"
    / "inventory_clean"
)

EXPECTED_ROWS = 471_000


def create_spark():

    spark = (
        SparkSession.builder
        .appName("ValidateSilverInventory")
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
    print("SILVER INVENTORY VALIDATION")
    print("=" * 70)

    spark = create_spark()

    try:

        # ====================================================
        # READ DATA
        # ====================================================

        print("\nReading Bronze and Silver inventory...")

        bronze = spark.read.parquet(
            str(BRONZE_PATH)
        )

        silver = spark.read.parquet(
            str(SILVER_PATH)
        )

        print("[OK] Datasets loaded")

        # ====================================================
        # ROW COUNT
        # ====================================================

        print("\n" + "=" * 70)
        print("ROW COUNT VALIDATION")
        print("=" * 70)

        bronze_count = bronze.count()
        silver_count = silver.count()

        print(f"Bronze rows : {bronze_count:,}")
        print(f"Silver rows : {silver_count:,}")

        if bronze_count != EXPECTED_ROWS:
            raise ValueError(
                f"Unexpected Bronze count: {bronze_count:,}"
            )

        if silver_count != EXPECTED_ROWS:
            raise ValueError(
                f"Unexpected Silver count: {silver_count:,}"
            )

        if bronze_count != silver_count:
            raise ValueError(
                "Bronze/Silver row counts do not match"
            )

        print("[OK] All 471,000 inventory rows preserved")

        # ====================================================
        # COMPOSITE KEY
        # ====================================================

        print("\n" + "=" * 70)
        print("KEY VALIDATION")
        print("=" * 70)

        duplicate_keys = (
            silver
            .groupBy(
                "store_id",
                "sku"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        print(
            f"Duplicate store/SKU keys: "
            f"{duplicate_keys:,}"
        )

        if duplicate_keys != 0:
            raise ValueError(
                "Duplicate inventory keys detected"
            )

        print("[OK] Store/SKU composite keys unique")

        # ====================================================
        # NULL VALIDATION
        # ====================================================

        print("\n" + "=" * 70)
        print("NULL VALIDATION")
        print("=" * 70)

        required_columns = [
            "store_id",
            "sku",
            "store_name",
            "region",
            "store_type",
            "channel",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "demand_class",
            "opening_stock",
            "safety_stock",
            "reorder_level",
            "max_stock_level",
            "stock_status",
            "stock_gap",
            "reorder_quantity",
            "base_cost",
            "selling_price",
            "inventory_value",
            "retail_inventory_value",
            "last_updated"
        ]

        null_expressions = [
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1
                ).otherwise(0)
            ).alias(column)
            for column in required_columns
        ]

        result = (
            silver
            .agg(*null_expressions)
            .collect()[0]
            .asDict()
        )

        total_nulls = 0

        for column, count in result.items():

            print(
                f"{column:<25} "
                f"{count:,}"
            )

            total_nulls += count

        if total_nulls != 0:
            raise ValueError(
                f"Found {total_nulls:,} NULL values"
            )

        print("[OK] Required columns contain no NULLs")

        # ====================================================
        # INVENTORY BUSINESS RULES
        # ====================================================

        print("\n" + "=" * 70)
        print("INVENTORY BUSINESS RULES")
        print("=" * 70)

        invalid_stock = (
            silver
            .filter(
                (F.col("opening_stock") < 0)
                | (F.col("safety_stock") < 0)
                | (
                    F.col("reorder_level")
                    < F.col("safety_stock")
                )
                | (
                    F.col("max_stock_level")
                    < F.col("reorder_level")
                )
            )
            .count()
        )

        print(
            f"Invalid stock-level rows: "
            f"{invalid_stock:,}"
        )

        if invalid_stock != 0:
            raise ValueError(
                "Invalid stock levels detected"
            )

        print("[OK] Stock-level relationships valid")

        # ====================================================
        # STOCK STATUS LOGIC
        # ====================================================

        print("\n" + "=" * 70)
        print("STOCK STATUS VALIDATION")
        print("=" * 70)

        expected_status = (
            F.when(
                F.col("opening_stock")
                <= F.col("safety_stock"),
                F.lit("CRITICAL")
            )
            .when(
                F.col("opening_stock")
                <= F.col("reorder_level"),
                F.lit("LOW")
            )
            .otherwise(
                F.lit("HEALTHY")
            )
        )

        status_errors = (
            silver
            .filter(
                F.col("stock_status")
                != expected_status
            )
            .count()
        )

        print(
            f"Incorrect stock statuses: "
            f"{status_errors:,}"
        )

        if status_errors != 0:
            raise ValueError(
                "Stock-status calculation invalid"
            )

        print("[OK] Stock status logic valid")

        # ====================================================
        # REORDER QUANTITY
        # ====================================================

        print("\n" + "=" * 70)
        print("REORDER QUANTITY VALIDATION")
        print("=" * 70)

        expected_reorder = (
            F.when(
                F.col("opening_stock")
                <= F.col("reorder_level"),
                F.greatest(
                    F.col("max_stock_level")
                    - F.col("opening_stock"),
                    F.lit(0)
                )
            )
            .otherwise(
                F.lit(0)
            )
        )

        reorder_errors = (
            silver
            .filter(
                F.col("reorder_quantity")
                != expected_reorder
            )
            .count()
        )

        print(
            f"Incorrect reorder quantities: "
            f"{reorder_errors:,}"
        )

        if reorder_errors != 0:
            raise ValueError(
                "Reorder quantity calculation invalid"
            )

        print("[OK] Reorder quantities valid")

        # ====================================================
        # INVENTORY VALUE
        # ====================================================

        print("\n" + "=" * 70)
        print("INVENTORY VALUE VALIDATION")
        print("=" * 70)

        value_errors = (
            silver
            .filter(
                F.abs(
                    F.col("inventory_value")
                    -
                    F.round(
                        F.col("opening_stock")
                        * F.col("base_cost"),
                        2
                    )
                ) > 0.01
            )
            .count()
        )

        print(
            f"Inventory value errors: "
            f"{value_errors:,}"
        )

        if value_errors != 0:
            raise ValueError(
                "Inventory value calculation invalid"
            )

        print("[OK] Inventory values valid")

        # ====================================================
        # STOCK DISTRIBUTION
        # ====================================================

        print("\n" + "=" * 70)
        print("STOCK STATUS DISTRIBUTION")
        print("=" * 70)

        distribution = (
            silver
            .groupBy("stock_status")
            .count()
            .orderBy("stock_status")
        )

        distribution.show()

        # ====================================================
        # REPLENISHMENT STATISTICS
        # ====================================================

        print("\n" + "=" * 70)
        print("REPLENISHMENT STATISTICS")
        print("=" * 70)

        replenishment = (
            silver
            .filter(
                F.col("reorder_quantity") > 0
            )
        )

        reorder_records = replenishment.count()

        reorder_units = (
            replenishment
            .agg(
                F.sum(
                    "reorder_quantity"
                ).alias("units")
            )
            .collect()[0]["units"]
        )

        print(
            f"SKUs requiring reorder : "
            f"{reorder_records:,}"
        )

        print(
            f"Suggested reorder units : "
            f"{reorder_units:,}"
        )

        # ====================================================
        # FINAL
        # ====================================================

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("SILVER INVENTORY VALIDATION PASSED")
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
            "\nSilver inventory is ready "
            "for downstream processing."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
