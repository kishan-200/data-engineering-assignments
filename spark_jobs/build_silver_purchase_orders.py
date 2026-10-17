from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PATHS
# ============================================================

ROOT = Path.cwd()

BRONZE_DIR = ROOT / "data_lake" / "bronze"
SILVER_DIR = ROOT / "data_lake" / "silver"

PO_PATH = str(BRONZE_DIR / "purchase_orders")
SUPPLIERS_PATH = str(BRONZE_DIR / "suppliers")
STORES_PATH = str(BRONZE_DIR / "stores")
PRODUCTS_PATH = str(BRONZE_DIR / "products")

OUTPUT_PATH = str(SILVER_DIR / "purchase_orders_clean")

EXPECTED_ROWS = 150_000


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SilverPurchaseOrders")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start = perf_counter()

print("=" * 70)
print("SILVER PURCHASE ORDER TRANSFORMATION")
print("=" * 70)


# ============================================================
# READ BRONZE
# ============================================================

print("\nReading Bronze datasets...")

po = spark.read.parquet(PO_PATH)
suppliers = spark.read.parquet(SUPPLIERS_PATH)
stores = spark.read.parquet(STORES_PATH)
products = spark.read.parquet(PRODUCTS_PATH)

print("[OK] Bronze purchase_orders loaded")
print("[OK] Bronze suppliers loaded")
print("[OK] Bronze stores loaded")
print("[OK] Bronze products loaded")


# ============================================================
# PREPARE DIMENSIONS
# ============================================================

print("\nPreparing dimensions...")

supplier_dim = suppliers.select(
    "supplier_id",
    "supplier_name",
    F.col("region").alias("supplier_region"),
    "default_lead_time_days",
    "reliability_score"
)

store_dim = stores.select(
    "store_id",
    "store_name",
    F.col("region").alias("store_region"),
    "store_type",
    "channel"
)

product_dim = products.select(
    "sku",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "demand_class"
)


# ============================================================
# CREATE PROCUREMENT METRICS
# ============================================================

print("Creating procurement metrics...")

silver = (
    po

    # Ordered monetary value
    .withColumn(
        "ordered_value",
        F.round(
            F.col("ordered_quantity") * F.col("unit_cost"),
            2
        )
    )

    # Received monetary value
    .withColumn(
        "received_value",
        F.round(
            F.col("received_quantity") * F.col("unit_cost"),
            2
        )
    )

    # Unreceived quantity
    .withColumn(
        "unreceived_quantity",
        F.col("ordered_quantity") - F.col("received_quantity")
    )

    # Fill rate
    .withColumn(
        "fill_rate",
        F.when(
            F.col("ordered_quantity") > 0,
            F.round(
                F.col("received_quantity") /
                F.col("ordered_quantity"),
                4
            )
        ).otherwise(F.lit(0.0))
    )

    # Expected lead time
    .withColumn(
        "expected_lead_time_days",
        F.datediff(
            F.col("expected_delivery_date"),
            F.col("order_date")
        )
    )

    # Actual lead time
    .withColumn(
        "actual_lead_time_days",
        F.when(
            F.col("actual_delivery_date").isNotNull(),
            F.datediff(
                F.col("actual_delivery_date"),
                F.col("order_date")
            )
        )
    )

    # Delivery delay
    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col("actual_delivery_date").isNotNull(),
            F.greatest(
                F.datediff(
                    F.col("actual_delivery_date"),
                    F.col("expected_delivery_date")
                ),
                F.lit(0)
            )
        )
    )

    # Late delivery flag
    .withColumn(
        "is_late",
        F.when(
            F.col("actual_delivery_date").isNotNull() &
            (
                F.col("actual_delivery_date") >
                F.col("expected_delivery_date")
            ),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # Full delivery flag
    .withColumn(
        "is_fully_received",
        F.col("received_quantity") == F.col("ordered_quantity")
    )

    # Partial delivery flag
    .withColumn(
        "is_partial_delivery",
        (
            (F.col("received_quantity") > 0) &
            (F.col("received_quantity") < F.col("ordered_quantity"))
        )
    )

    # Useful partition columns
    .withColumn(
        "order_year",
        F.year("order_date")
    )
    .withColumn(
        "order_month",
        F.month("order_date")
    )
)


# ============================================================
# ENRICH WITH MASTER DATA
# ============================================================

print("Joining supplier dimension...")

silver = silver.join(
    F.broadcast(supplier_dim),
    on="supplier_id",
    how="left"
)

print("Joining store dimension...")

silver = silver.join(
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


# ============================================================
# VALIDATE BEFORE WRITE
# ============================================================

print("\nValidating transformed dataset...")

row_count = silver.count()

print(f"Silver candidate rows: {row_count:,}")

if row_count != EXPECTED_ROWS:
    raise ValueError(
        f"Row count mismatch: expected {EXPECTED_ROWS:,}, "
        f"found {row_count:,}"
    )

print("[OK] Row count preserved")


# ------------------------------------------------------------
# MASTER DATA ENRICHMENT
# ------------------------------------------------------------

missing_suppliers = silver.filter(
    F.col("supplier_name").isNull()
).count()

missing_stores = silver.filter(
    F.col("store_name").isNull()
).count()

missing_products = silver.filter(
    F.col("product_name").isNull()
).count()

print(f"Unmatched suppliers : {missing_suppliers:,}")
print(f"Unmatched stores    : {missing_stores:,}")
print(f"Unmatched products  : {missing_products:,}")

if (
    missing_suppliers != 0
    or missing_stores != 0
    or missing_products != 0
):
    raise ValueError("Master-data enrichment failed")

print("[OK] Master-data enrichment valid")


# ------------------------------------------------------------
# PROCUREMENT BUSINESS RULES
# ------------------------------------------------------------

invalid_rows = silver.filter(
    (F.col("ordered_quantity") <= 0)
    | (F.col("received_quantity") < 0)
    | (F.col("received_quantity") > F.col("ordered_quantity"))
    | (F.col("unit_cost") <= 0)
    | (F.col("expected_delivery_date") < F.col("order_date"))
).count()

print(f"Invalid procurement rows: {invalid_rows:,}")

if invalid_rows != 0:
    raise ValueError("Procurement business-rule validation failed")

print("[OK] Procurement business rules valid")


# ------------------------------------------------------------
# DUPLICATE KEY CHECK
# ------------------------------------------------------------

duplicate_keys = (
    silver
    .groupBy("po_id", "po_line_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate PO-line keys: {duplicate_keys:,}")

if duplicate_keys != 0:
    raise ValueError("Duplicate PO-line keys detected")

print("[OK] PO-line keys unique")


# ============================================================
# WRITE SILVER
# ============================================================

print("\nWriting Silver purchase orders...")

(
    silver
    .write
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .parquet(OUTPUT_PATH)
)

print("[OK] Silver purchase orders written")


# ============================================================
# VERIFY OUTPUT
# ============================================================

print("\nVerifying written Silver dataset...")

written = spark.read.parquet(OUTPUT_PATH)

written_count = written.count()

print(f"Written rows: {written_count:,}")

if written_count != EXPECTED_ROWS:
    raise ValueError(
        f"Written row count mismatch: {written_count:,}"
    )

print("[OK] Silver row count verified")


# ============================================================
# STATISTICS
# ============================================================

print("\nPurchase order statistics:")

stats = written.agg(
    F.countDistinct("po_id").alias("purchase_orders"),
    F.countDistinct("supplier_id").alias("suppliers"),
    F.countDistinct("store_id").alias("stores"),
    F.countDistinct("sku").alias("products"),
    F.round(
        F.avg("fill_rate") * 100,
        2
    ).alias("avg_fill_rate_pct"),
    F.sum(
        F.when(F.col("is_late"), 1).otherwise(0)
    ).alias("late_lines"),
    F.sum(
        F.when(F.col("is_partial_delivery"), 1).otherwise(0)
    ).alias("partial_lines")
)

stats.show(truncate=False)


print("\nSilver purchase order sample:")

written.select(
    "po_id",
    "po_line_id",
    "supplier_id",
    "store_id",
    "sku",
    "ordered_quantity",
    "received_quantity",
    "fill_rate",
    "ordered_value",
    "delivery_delay_days",
    "is_late",
    "supplier_name",
    "product_name"
).show(5, truncate=False)


# ============================================================
# COMPLETE
# ============================================================

elapsed = perf_counter() - start

parquet_files = len(
    list(
        Path(OUTPUT_PATH).rglob("*.parquet")
    )
)

print("\n" + "=" * 70)
print("SILVER PURCHASE ORDER BUILD COMPLETE")
print("=" * 70)

print(f"Rows          : {written_count:,}")
print(f"Parquet files : {parquet_files}")
print(f"Output        : {OUTPUT_PATH}")
print(f"Elapsed time  : {elapsed:.2f} seconds")

spark.stop()
