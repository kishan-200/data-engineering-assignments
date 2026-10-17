from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIG
# ============================================================

ROOT = Path.cwd()

BRONZE_PATH = str(
    ROOT / "data_lake" / "bronze" / "purchase_orders"
)

SILVER_PATH = str(
    ROOT / "data_lake" / "silver" / "purchase_orders_clean"
)

EXPECTED_ROWS = 150_000


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ValidateSilverPurchaseOrders")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start = perf_counter()

print("=" * 70)
print("SILVER PURCHASE ORDER VALIDATION")
print("=" * 70)


# ============================================================
# READ DATA
# ============================================================

print("\nReading Bronze and Silver purchase orders...")

bronze = spark.read.parquet(BRONZE_PATH)
silver = spark.read.parquet(SILVER_PATH)

print("[OK] Datasets loaded")


# ============================================================
# ROW COUNT
# ============================================================

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
    raise ValueError("Bronze/Silver row count mismatch")

print("[OK] All 150,000 PO lines preserved")


# ============================================================
# KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("KEY VALIDATION")
print("=" * 70)

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
# REQUIRED NULL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("REQUIRED COLUMN NULL VALIDATION")
print("=" * 70)

required_columns = [
    "po_id",
    "po_line_id",
    "supplier_id",
    "store_id",
    "sku",
    "order_date",
    "expected_delivery_date",
    "ordered_quantity",
    "received_quantity",
    "unit_cost",
    "status",
    "ordered_value",
    "received_value",
    "unreceived_quantity",
    "fill_rate",
    "expected_lead_time_days",
    "is_late",
    "is_fully_received",
    "is_partial_delivery",
    "supplier_name",
    "store_name",
    "product_name",
    "category",
    "demand_class",
]

null_counts = silver.select(
    [
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in required_columns
    ]
).collect()[0].asDict()

for column, count in null_counts.items():
    print(f"{column:<30} {count:,}")

invalid_nulls = {
    column: count
    for column, count in null_counts.items()
    if count != 0
}

if invalid_nulls:
    raise ValueError(
        f"Required columns contain NULLs: {invalid_nulls}"
    )

print("[OK] Required columns contain no NULLs")

print(
    "\nNote: actual_delivery_date, actual_lead_time_days and "
    "delivery_delay_days may legitimately be NULL."
)


# ============================================================
# QUANTITY BUSINESS RULES
# ============================================================

print("\n" + "=" * 70)
print("QUANTITY VALIDATION")
print("=" * 70)

invalid_quantities = silver.filter(
    (F.col("ordered_quantity") <= 0)
    | (F.col("received_quantity") < 0)
    | (
        F.col("received_quantity")
        > F.col("ordered_quantity")
    )
    | (F.col("unreceived_quantity") < 0)
).count()

print(f"Invalid quantity rows: {invalid_quantities:,}")

if invalid_quantities != 0:
    raise ValueError("Quantity validation failed")

print("[OK] Quantity relationships valid")


# ============================================================
# FILL RATE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FILL RATE VALIDATION")
print("=" * 70)

fill_rate_errors = silver.filter(
    F.abs(
        F.col("fill_rate")
        - F.round(
            F.col("received_quantity")
            / F.col("ordered_quantity"),
            4
        )
    ) > 0.0001
).count()

invalid_fill_range = silver.filter(
    (F.col("fill_rate") < 0)
    | (F.col("fill_rate") > 1)
).count()

print(f"Fill-rate calculation errors : {fill_rate_errors:,}")
print(f"Fill-rate range errors       : {invalid_fill_range:,}")

if fill_rate_errors != 0 or invalid_fill_range != 0:
    raise ValueError("Fill-rate validation failed")

print("[OK] Fill-rate calculations valid")


# ============================================================
# MONETARY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("MONETARY VALIDATION")
print("=" * 70)

ordered_value_errors = silver.filter(
    F.abs(
        F.col("ordered_value")
        - F.round(
            F.col("ordered_quantity")
            * F.col("unit_cost"),
            2
        )
    ) > 0.01
).count()

received_value_errors = silver.filter(
    F.abs(
        F.col("received_value")
        - F.round(
            F.col("received_quantity")
            * F.col("unit_cost"),
            2
        )
    ) > 0.01
).count()

invalid_costs = silver.filter(
    F.col("unit_cost") <= 0
).count()

print(f"Ordered-value errors : {ordered_value_errors:,}")
print(f"Received-value errors: {received_value_errors:,}")
print(f"Invalid unit costs   : {invalid_costs:,}")

if (
    ordered_value_errors != 0
    or received_value_errors != 0
    or invalid_costs != 0
):
    raise ValueError("Monetary validation failed")

print("[OK] Procurement monetary calculations valid")


# ============================================================
# DATE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("DELIVERY DATE VALIDATION")
print("=" * 70)

invalid_expected_dates = silver.filter(
    F.col("expected_delivery_date") < F.col("order_date")
).count()

invalid_actual_dates = silver.filter(
    F.col("actual_delivery_date").isNotNull()
    & (
        F.col("actual_delivery_date")
        < F.col("order_date")
    )
).count()

print(f"Invalid expected delivery dates: {invalid_expected_dates:,}")
print(f"Invalid actual delivery dates  : {invalid_actual_dates:,}")

if invalid_expected_dates != 0 or invalid_actual_dates != 0:
    raise ValueError("Delivery date validation failed")

print("[OK] Delivery dates valid")


# ============================================================
# LATE DELIVERY LOGIC
# ============================================================

print("\n" + "=" * 70)
print("LATE DELIVERY VALIDATION")
print("=" * 70)

late_flag_errors = silver.filter(
    F.col("is_late")
    != (
        F.col("actual_delivery_date").isNotNull()
        & (
            F.col("actual_delivery_date")
            > F.col("expected_delivery_date")
        )
    )
).count()

print(f"Incorrect late flags: {late_flag_errors:,}")

if late_flag_errors != 0:
    raise ValueError("Late-delivery flag validation failed")

print("[OK] Late-delivery logic valid")


# ============================================================
# DELIVERY STATUS LOGIC
# ============================================================

print("\n" + "=" * 70)
print("DELIVERY COMPLETENESS VALIDATION")
print("=" * 70)

full_flag_errors = silver.filter(
    F.col("is_fully_received")
    != (
        F.col("received_quantity")
        == F.col("ordered_quantity")
    )
).count()

partial_flag_errors = silver.filter(
    F.col("is_partial_delivery")
    != (
        (F.col("received_quantity") > 0)
        & (
            F.col("received_quantity")
            < F.col("ordered_quantity")
        )
    )
).count()

print(f"Incorrect full-delivery flags   : {full_flag_errors:,}")
print(f"Incorrect partial-delivery flags: {partial_flag_errors:,}")

if full_flag_errors != 0 or partial_flag_errors != 0:
    raise ValueError("Delivery completeness validation failed")

print("[OK] Delivery completeness logic valid")


# ============================================================
# ENRICHMENT VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("ENRICHMENT VALIDATION")
print("=" * 70)

missing_supplier = silver.filter(
    F.col("supplier_name").isNull()
).count()

missing_store = silver.filter(
    F.col("store_name").isNull()
).count()

missing_product = silver.filter(
    F.col("product_name").isNull()
).count()

print(f"Missing supplier enrichment: {missing_supplier:,}")
print(f"Missing store enrichment   : {missing_store:,}")
print(f"Missing product enrichment : {missing_product:,}")

if missing_supplier or missing_store or missing_product:
    raise ValueError("Master-data enrichment validation failed")

print("[OK] Master-data enrichment valid")


# ============================================================
# PROCUREMENT STATISTICS
# ============================================================

print("\n" + "=" * 70)
print("PROCUREMENT STATISTICS")
print("=" * 70)

silver.agg(
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
        F.when(
            F.col("is_partial_delivery"),
            1
        ).otherwise(0)
    ).alias("partial_lines")
).show(truncate=False)


# ============================================================
# YEAR / MONTH VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("ORDER YEAR / MONTH VALIDATION")
print("=" * 70)

periods = (
    silver
    .select("order_year", "order_month")
    .distinct()
    .count()
)

print(f"Year/month combinations: {periods}")

silver.groupBy(
    "order_year",
    "order_month"
).count().orderBy(
    "order_year",
    "order_month"
).show(30, truncate=False)


# ============================================================
# COMPLETE
# ============================================================

elapsed = perf_counter() - start

print("\n" + "=" * 70)
print("SILVER PURCHASE ORDER VALIDATION PASSED")
print("=" * 70)

print(f"Rows verified : {silver_count:,}")
print(f"Elapsed time  : {elapsed:.2f} seconds")

print(
    "\nSilver purchase orders are ready "
    "for downstream processing."
)

spark.stop()
