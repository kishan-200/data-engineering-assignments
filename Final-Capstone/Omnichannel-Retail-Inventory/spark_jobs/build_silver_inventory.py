from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data_lake" / "silver"

INVENTORY_PATH = BRONZE_DIR / "inventory"
STORES_PATH = BRONZE_DIR / "stores"
PRODUCTS_PATH = BRONZE_DIR / "products"

OUTPUT_PATH = SILVER_DIR / "inventory_clean"

EXPECTED_ROWS = 471_000


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("BuildSilverInventory")
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
    print("SILVER INVENTORY TRANSFORMATION")
    print("=" * 70)

    spark = create_spark()

    try:

        # ----------------------------------------------------
        # READ BRONZE DATA
        # ----------------------------------------------------

        print("\nReading Bronze datasets...")

        inventory = spark.read.parquet(
            str(INVENTORY_PATH)
        )

        stores = spark.read.parquet(
            str(STORES_PATH)
        )

        products = spark.read.parquet(
            str(PRODUCTS_PATH)
        )

        print("[OK] Bronze inventory loaded")
        print("[OK] Bronze stores loaded")
        print("[OK] Bronze products loaded")

        # ----------------------------------------------------
        # BASE INVENTORY
        # ----------------------------------------------------

        print("\nPreparing inventory data...")

        inventory = (
            inventory
            .select(
                "store_id",
                "sku",
                "opening_stock",
                "safety_stock",
                "reorder_level",
                "max_stock_level",
                "last_updated"
            )
        )

        # ----------------------------------------------------
        # STORE DIMENSION
        # ----------------------------------------------------

        store_dim = (
            stores
            .select(
                "store_id",
                "store_name",
                "region",
                "store_type",
                "channel"
            )
        )

        # ----------------------------------------------------
        # PRODUCT DIMENSION
        # ----------------------------------------------------

        product_dim = (
            products
            .select(
                "sku",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "selling_price",
                "base_cost",
                "demand_class"
            )
        )

        # ----------------------------------------------------
        # ENRICH INVENTORY
        # ----------------------------------------------------

        print("Joining store dimension...")

        silver = inventory.join(
            F.broadcast(store_dim),
            on="store_id",
            how="left"
        )

        print("Joining product dimension...")

        silver = silver.join(
            F.broadcast(product_dim),
            on="sku",
            how="left"
        )

        # ----------------------------------------------------
        # INVENTORY FEATURES
        # ----------------------------------------------------

        print("Creating inventory metrics...")

        silver = (
            silver

            # Stock status
            .withColumn(
                "stock_status",
                F.when(
                    F.col("opening_stock") <= F.col("safety_stock"),
                    F.lit("CRITICAL")
                )
                .when(
                    F.col("opening_stock") <= F.col("reorder_level"),
                    F.lit("LOW")
                )
                .otherwise(
                    F.lit("HEALTHY")
                )
            )

            # Difference from reorder threshold
            .withColumn(
                "stock_gap",
                F.col("opening_stock")
                - F.col("reorder_level")
            )

            # Suggested quantity to reach max stock
            .withColumn(
                "reorder_quantity",
                F.when(
                    F.col("opening_stock") <= F.col("reorder_level"),
                    F.greatest(
                        F.col("max_stock_level")
                        - F.col("opening_stock"),
                        F.lit(0)
                    )
                )
                .otherwise(F.lit(0))
                .cast("long")
            )

            # Inventory value at cost
            .withColumn(
                "inventory_value",
                F.round(
                    F.col("opening_stock")
                    * F.col("base_cost"),
                    2
                )
            )

            # Potential retail value
            .withColumn(
                "retail_inventory_value",
                F.round(
                    F.col("opening_stock")
                    * F.col("selling_price"),
                    2
                )
            )
        )

        # ----------------------------------------------------
        # SELECT FINAL SILVER SCHEMA
        # ----------------------------------------------------

        silver = silver.select(

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
        )

        # ----------------------------------------------------
        # VALIDATE BEFORE WRITE
        # ----------------------------------------------------

        print("\nValidating transformed dataset...")

        row_count = silver.count()

        print(
            f"Silver candidate rows: "
            f"{row_count:,}"
        )

        if row_count != EXPECTED_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_ROWS:,} rows, "
                f"found {row_count:,}"
            )

        print("[OK] Row count preserved")

        # Check enrichment
        unmatched_stores = (
            silver
            .filter(
                F.col("store_name").isNull()
            )
            .count()
        )

        unmatched_products = (
            silver
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

        # ----------------------------------------------------
        # INVENTORY RULE VALIDATION
        # ----------------------------------------------------

        invalid = (
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
                | (F.col("reorder_quantity") < 0)
                | (F.col("inventory_value") < 0)
            )
            .count()
        )

        print(
            f"Invalid inventory rows: "
            f"{invalid:,}"
        )

        if invalid != 0:
            raise ValueError(
                "Inventory business-rule validation failed"
            )

        print("[OK] Inventory business rules valid")

        # ----------------------------------------------------
        # WRITE SILVER
        # ----------------------------------------------------

        print("\nWriting Silver inventory...")

        (
            silver
            .repartition(8)
            .write
            .mode("overwrite")
            .parquet(
                str(OUTPUT_PATH)
            )
        )

        print("[OK] Silver inventory written")

        # ----------------------------------------------------
        # VERIFY OUTPUT
        # ----------------------------------------------------

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
                "Written Silver inventory row count mismatch"
            )

        print("[OK] Silver row count verified")

        parquet_files = len(
            list(
                OUTPUT_PATH.glob("*.parquet")
            )
        )

        print(
            f"Silver Parquet files: "
            f"{parquet_files}"
        )

        # ----------------------------------------------------
        # STOCK STATUS DISTRIBUTION
        # ----------------------------------------------------

        print("\nStock status distribution:")

        (
            written
            .groupBy("stock_status")
            .count()
            .orderBy("stock_status")
            .show()
        )

        # ----------------------------------------------------
        # SAMPLE
        # ----------------------------------------------------

        print("\nSilver inventory sample:")

        (
            written
            .select(
                "store_id",
                "sku",
                "opening_stock",
                "safety_stock",
                "reorder_level",
                "max_stock_level",
                "stock_status",
                "reorder_quantity",
                "inventory_value",
                "region",
                "category",
                "demand_class"
            )
            .show(5, truncate=False)
        )

        # ----------------------------------------------------
        # COMPLETE
        # ----------------------------------------------------

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("SILVER INVENTORY BUILD COMPLETE")
        print("=" * 70)

        print(
            f"Rows          : "
            f"{written_count:,}"
        )

        print(
            f"Parquet files : "
            f"{parquet_files}"
        )

        print(
            f"Output        : "
            f"{OUTPUT_PATH}"
        )

        print(
            f"Elapsed time  : "
            f"{elapsed:.2f} seconds"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
