from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PATHS
# ============================================================

ROOT = Path.cwd()

SILVER_DIR = ROOT / "data_lake" / "silver"
GOLD_DIR = ROOT / "data_lake" / "gold"

SALES_PATH = str(SILVER_DIR / "sales_clean")
INVENTORY_PATH = str(SILVER_DIR / "inventory_clean")

OUTPUT_PATH = str(GOLD_DIR / "store_sku_performance")


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GoldStoreSKUPerformance")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start = perf_counter()

print("=" * 70)
print("GOLD STORE-SKU PERFORMANCE BUILD")
print("=" * 70)


# ============================================================
# READ SILVER DATA
# ============================================================

print("\nReading Silver datasets...")

sales = spark.read.parquet(SALES_PATH)
inventory = spark.read.parquet(INVENTORY_PATH)

print("[OK] Silver sales loaded")
print("[OK] Silver inventory loaded")

sales_rows = sales.count()
inventory_rows = inventory.count()

print(f"Sales rows     : {sales_rows:,}")
print(f"Inventory rows : {inventory_rows:,}")


# ============================================================
# SALES DATE RANGE
# ============================================================

print("\nDetermining sales history range...")

date_stats = sales.agg(
    F.min("transaction_date").alias("min_date"),
    F.max("transaction_date").alias("max_date")
).first()

min_date = date_stats["min_date"]
max_date = date_stats["max_date"]

print(f"Sales start : {min_date}")
print(f"Sales end   : {max_date}")


# ============================================================
# STORE-SKU SALES AGGREGATION
# ============================================================

print("\nAggregating 5M sales rows to store/SKU level...")

sales_agg = (
    sales
    .groupBy(
        "store_id",
        "sku"
    )
    .agg(
        F.sum("quantity").alias("units_sold"),

        F.round(
            F.sum("gross_amount"),
            2
        ).alias("gross_sales"),

        F.round(
            F.sum("discount_amount"),
            2
        ).alias("discount_amount"),

        F.round(
            F.sum("net_sales_amount"),
            2
        ).alias("net_sales"),

        F.countDistinct(
            "transaction_id"
        ).alias("transactions"),

        F.countDistinct(
            "transaction_date"
        ).alias("selling_days"),

        F.min(
            "transaction_date"
        ).alias("first_sale_date"),

        F.max(
            "transaction_date"
        ).alias("last_sale_date"),

        F.round(
            F.avg("quantity"),
            4
        ).alias("avg_units_per_line")
    )
)

sales_agg = (
    sales_agg
    .withColumn(
        "active_sales_days",
        F.datediff(
            F.col("last_sale_date"),
            F.col("first_sale_date")
        ) + F.lit(1)
    )
    .withColumn(
        "avg_daily_demand",
        F.round(
            F.col("units_sold")
            / F.col("active_sales_days"),
            4
        )
    )
    .withColumn(
        "avg_units_per_transaction",
        F.round(
            F.col("units_sold")
            / F.col("transactions"),
            4
        )
    )
)


sales_store_skus = sales_agg.count()

print(
    f"[OK] Sales aggregated to "
    f"{sales_store_skus:,} store/SKU combinations"
)


# ============================================================
# PREPARE INVENTORY
# ============================================================

print("\nPreparing Silver inventory...")

inventory_selected = inventory.select(
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


# ============================================================
# JOIN SALES + INVENTORY
# ============================================================

print("\nJoining sales performance with inventory...")

gold = (
    inventory_selected
    .join(
        sales_agg,
        on=["store_id", "sku"],
        how="left"
    )
)


# ============================================================
# HANDLE INVENTORY WITH NO SALES
# ============================================================

print("Handling inventory combinations with no historical sales...")

numeric_zero_columns = [
    "units_sold",
    "gross_sales",
    "discount_amount",
    "net_sales",
    "transactions",
    "selling_days",
    "active_sales_days",
    "avg_daily_demand",
    "avg_units_per_transaction",
    "avg_units_per_line"
]

gold = gold.fillna(
    0,
    subset=numeric_zero_columns
)


# ============================================================
# BUSINESS METRICS
# ============================================================

print("Creating Gold business metrics...")


# ------------------------------------------------------------
# Sales velocity
# ------------------------------------------------------------

gold = gold.withColumn(
    "sales_velocity",
    F.when(
        F.col("avg_daily_demand") >= 2,
        F.lit("FAST")
    )
    .when(
        F.col("avg_daily_demand") >= 0.5,
        F.lit("MEDIUM")
    )
    .when(
        F.col("avg_daily_demand") > 0,
        F.lit("SLOW")
    )
    .otherwise(
        F.lit("NO_SALES")
    )
)


# ------------------------------------------------------------
# Approximate days of stock
# ------------------------------------------------------------

gold = gold.withColumn(
    "days_of_stock",
    F.when(
        F.col("avg_daily_demand") > 0,
        F.round(
            F.col("opening_stock")
            / F.col("avg_daily_demand"),
            2
        )
    )
)


# ------------------------------------------------------------
# Gross margin
# ------------------------------------------------------------

gold = gold.withColumn(
    "gross_margin_pct",
    F.when(
        F.col("selling_price") > 0,
        F.round(
            (
                (
                    F.col("selling_price")
                    - F.col("base_cost")
                )
                / F.col("selling_price")
            ) * 100,
            2
        )
    )
)


# ------------------------------------------------------------
# Reorder flag
# ------------------------------------------------------------

gold = gold.withColumn(
    "requires_reorder",
    F.col("opening_stock") <= F.col("reorder_level")
)


# ------------------------------------------------------------
# Inventory risk classification
# ------------------------------------------------------------

gold = gold.withColumn(
    "inventory_risk",
    F.when(
        (F.col("opening_stock") <= F.col("safety_stock"))
        & (F.col("avg_daily_demand") >= 2),
        F.lit("HIGH")
    )
    .when(
        F.col("opening_stock") <= F.col("reorder_level"),
        F.lit("MEDIUM")
    )
    .otherwise(
        F.lit("LOW")
    )
)


# ------------------------------------------------------------
# Estimated revenue opportunity of reorder
# ------------------------------------------------------------

gold = gold.withColumn(
    "reorder_retail_value",
    F.round(
        F.col("reorder_quantity")
        * F.col("selling_price"),
        2
    )
)


# ============================================================
# VALIDATION
# ============================================================

print("\nValidating Gold dataset...")

gold_count = gold.count()

print(f"Gold candidate rows: {gold_count:,}")

if gold_count != inventory_rows:
    raise ValueError(
        "Gold row count must equal Silver inventory row count. "
        f"Inventory={inventory_rows:,}, Gold={gold_count:,}"
    )

print("[OK] One Gold row per inventory store/SKU")


# ------------------------------------------------------------
# KEY UNIQUENESS
# ------------------------------------------------------------

duplicate_keys = (
    gold
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

print(f"Duplicate store/SKU keys: {duplicate_keys:,}")

if duplicate_keys != 0:
    raise ValueError(
        "Duplicate Gold store/SKU keys detected"
    )

print("[OK] Store/SKU keys unique")


# ------------------------------------------------------------
# SALES SANITY
# ------------------------------------------------------------

invalid_sales = gold.filter(
    (F.col("units_sold") < 0)
    | (F.col("gross_sales") < 0)
    | (F.col("discount_amount") < 0)
    | (F.col("net_sales") < 0)
).count()

print(f"Invalid sales metrics: {invalid_sales:,}")

if invalid_sales != 0:
    raise ValueError(
        "Invalid Gold sales metrics detected"
    )

print("[OK] Sales metrics valid")


# ------------------------------------------------------------
# INVENTORY SANITY
# ------------------------------------------------------------

invalid_inventory = gold.filter(
    (F.col("opening_stock") < 0)
    | (F.col("safety_stock") < 0)
    | (F.col("reorder_level") < F.col("safety_stock"))
    | (F.col("max_stock_level") < F.col("reorder_level"))
).count()

print(f"Invalid inventory metrics: {invalid_inventory:,}")

if invalid_inventory != 0:
    raise ValueError(
        "Invalid Gold inventory metrics detected"
    )

print("[OK] Inventory metrics valid")


# ============================================================
# WRITE GOLD
# ============================================================

print("\nWriting Gold store/SKU performance...")

(
    gold
    .repartition(8, "region")
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(OUTPUT_PATH)
)

print("[OK] Gold dataset written")


# ============================================================
# VERIFY WRITTEN DATA
# ============================================================

print("\nVerifying written Gold dataset...")

written = spark.read.parquet(OUTPUT_PATH)

written_count = written.count()

print(f"Written rows: {written_count:,}")

if written_count != gold_count:
    raise ValueError(
        "Written Gold row count mismatch"
    )

print("[OK] Gold row count verified")


# ============================================================
# GOLD STATISTICS
# ============================================================

print("\n" + "=" * 70)
print("GOLD STORE/SKU STATISTICS")
print("=" * 70)

written.agg(
    F.countDistinct(
        "store_id"
    ).alias("stores"),

    F.countDistinct(
        "sku"
    ).alias("products"),

    F.sum(
        "units_sold"
    ).alias("units_sold"),

    F.round(
        F.sum("net_sales"),
        2
    ).alias("net_sales"),

    F.sum(
        F.when(
            F.col("requires_reorder"),
            1
        ).otherwise(0)
    ).alias("reorder_store_skus"),

    F.sum(
        F.when(
            F.col("inventory_risk") == "HIGH",
            1
        ).otherwise(0)
    ).alias("high_risk_store_skus")
).show(truncate=False)


# ============================================================
# SALES VELOCITY DISTRIBUTION
# ============================================================

print("\nSales velocity distribution:")

(
    written
    .groupBy("sales_velocity")
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(truncate=False)
)


# ============================================================
# INVENTORY RISK DISTRIBUTION
# ============================================================

print("\nInventory risk distribution:")

(
    written
    .groupBy("inventory_risk")
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(truncate=False)
)


# ============================================================
# SAMPLE
# ============================================================

print("\nGold sample:")

written.select(
    "store_id",
    "sku",
    "product_name",
    "category",
    "opening_stock",
    "units_sold",
    "avg_daily_demand",
    "days_of_stock",
    "stock_status",
    "requires_reorder",
    "reorder_quantity",
    "sales_velocity",
    "inventory_risk"
).show(
    10,
    truncate=False
)


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
print("GOLD STORE-SKU PERFORMANCE BUILD COMPLETE")
print("=" * 70)

print(f"Rows          : {written_count:,}")
print(f"Parquet files : {parquet_files}")
print(f"Output        : {OUTPUT_PATH}")
print(f"Elapsed time  : {elapsed:.2f} seconds")

spark.stop()
