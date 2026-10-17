import pandas as pd
import pyarrow.dataset as ds
import duckdb

from config import GENERATED_DIR


SALES_DIR = GENERATED_DIR / "sales_final"
EXPECTED_ROWS = 5_000_000


def main():

    print("=" * 70)
    print("FINAL 5M SALES DATASET VALIDATION")
    print("=" * 70)

    errors = []

    # ========================================================
    # OPEN DATASET
    # ========================================================

    dataset = ds.dataset(
        SALES_DIR,
        format="parquet",
        partitioning="hive",
    )

    row_count = dataset.count_rows()

    print("\nDATASET")
    print("-" * 70)
    print(f"Sales lines: {row_count:,}")

    if row_count == EXPECTED_ROWS:
        print("[OK] Exact row count")
    else:
        errors.append(
            f"Expected {EXPECTED_ROWS:,}, "
            f"found {row_count:,}"
        )

    # ========================================================
    # PARTITION VALIDATION
    # ========================================================

    partition_paths = set()

    for fragment in dataset.get_fragments():

        path = str(fragment.path)

        year = None
        month = None

        for part in path.split("/"):

            if part.startswith("year="):
                year = int(part.split("=")[1])

            if part.startswith("month="):
                month = int(part.split("=")[1])

        if year is not None and month is not None:
            partition_paths.add(
                (year, month)
            )

    expected_partitions = {
        (year, month)
        for year in [2024, 2025]
        for month in range(1, 13)
    }

    print(
        f"Year/month partitions: "
        f"{len(partition_paths)}"
    )

    if partition_paths == expected_partitions:
        print("[OK] All 24 partitions present")
    else:
        errors.append(
            "Partition structure invalid"
        )

    # ========================================================
    # LOAD SMALL MASTER DATA
    # ========================================================

    stores = pd.read_csv(
        GENERATED_DIR / "stores.csv",
        usecols=["store_id"],
    )

    products = pd.read_csv(
        GENERATED_DIR / "products.csv",
        usecols=[
            "sku",
            "demand_class",
        ],
    )

    valid_stores = set(
        stores["store_id"]
    )

    valid_skus = set(
        products["sku"]
    )

    product_demand = dict(
        zip(
            products["sku"],
            products["demand_class"],
        )
    )

    # ========================================================
    # COUNTERS
    # ========================================================

    invalid_stores = 0
    invalid_skus = 0
    invalid_quantities = 0
    invalid_prices = 0
    invalid_discounts = 0
    invalid_dates = 0
    invalid_partitions = 0

    demand_counts = {
        "HIGH": 0,
        "MEDIUM": 0,
        "LOW": 0,
    }

    weekday_counts = {
        "Monday": 0,
        "Tuesday": 0,
        "Wednesday": 0,
        "Thursday": 0,
        "Friday": 0,
        "Saturday": 0,
        "Sunday": 0,
    }

    # ========================================================
    # MEMORY-SAFE BATCH SCAN
    # ========================================================

    print("\nSCANNING SALES DATA")
    print("-" * 70)

    scanner = dataset.scanner(
        columns=[
            "store_id",
            "sku",
            "quantity",
            "unit_price",
            "discount_pct",
            "transaction_timestamp",
            "year",
            "month",
        ],
        batch_size=50_000,
    )

    scanned = 0
    next_progress = 500_000

    for batch in scanner.to_batches():

        df = batch.to_pandas()

        scanned += len(df)

        # Foreign keys

        invalid_stores += int(
            (
                ~df["store_id"].isin(
                    valid_stores
                )
            ).sum()
        )

        invalid_skus += int(
            (
                ~df["sku"].isin(
                    valid_skus
                )
            ).sum()
        )

        # Quantities

        invalid_quantities += int(
            (
                ~df["quantity"].between(
                    1,
                    5,
                )
            ).sum()
        )

        # Prices

        invalid_prices += int(
            (
                df["unit_price"] <= 0
            ).sum()
        )

        # Discounts

        invalid_discounts += int(
            (
                ~df["discount_pct"].isin(
                    [0, 5, 10, 15, 20]
                )
            ).sum()
        )

        # Dates

        timestamps = pd.to_datetime(
            df["transaction_timestamp"]
        )

        invalid_dates += int(
            (
                (timestamps < "2024-01-01")
                |
                (timestamps >= "2026-01-01")
            ).sum()
        )

        # Partition correctness

        invalid_partitions += int(
            (
                (timestamps.dt.year != df["year"])
                |
                (
                    timestamps.dt.month
                    != df["month"]
                )
            ).sum()
        )

        # Demand distribution

        sku_counts = (
            df["sku"]
            .value_counts()
        )

        for sku, count in sku_counts.items():

            demand_class = product_demand.get(
                sku
            )

            if demand_class:
                demand_counts[
                    demand_class
                ] += int(count)

        # Weekdays

        day_counts = (
            timestamps
            .dt.day_name()
            .value_counts()
        )

        for day, count in day_counts.items():

            weekday_counts[day] += int(
                count
            )

        # Progress

        if (
            scanned >= next_progress
            or
            scanned == EXPECTED_ROWS
        ):

            print(
                f"Scanned "
                f"{scanned:,}"
                f"/{EXPECTED_ROWS:,} "
                f"("
                f"{scanned / EXPECTED_ROWS * 100:.1f}%"
                f")"
            )

            next_progress += 500_000

        del df

    # ========================================================
    # BASIC RESULTS
    # ========================================================

    print("\nBASIC VALIDATION")
    print("-" * 70)

    checks = [
        (
            scanned == EXPECTED_ROWS,
            "All rows scanned",
        ),
        (
            invalid_stores == 0,
            "All store references valid",
        ),
        (
            invalid_skus == 0,
            "All SKU references valid",
        ),
        (
            invalid_quantities == 0,
            "All quantities valid",
        ),
        (
            invalid_prices == 0,
            "All prices positive",
        ),
        (
            invalid_discounts == 0,
            "All discounts valid",
        ),
        (
            invalid_dates == 0,
            "All timestamps valid",
        ),
        (
            invalid_partitions == 0,
            "Partition matches timestamp",
        ),
    ]

    for passed, message in checks:

        if passed:
            print(f"[OK] {message}")

        else:
            print(f"[ERROR] {message}")
            errors.append(message)

    # ========================================================
    # DUCKDB VALIDATION
    # ========================================================

    print("\nTRANSACTION VALIDATION (DUCKDB)")
    print("-" * 70)

    con = duckdb.connect()

    # Keep DuckDB memory bounded.
    con.execute(
        "SET memory_limit='1GB'"
    )

    con.execute(
        "SET threads=2"
    )

    parquet_pattern = (
        str(SALES_DIR)
        + "/year=*/month=*/*.parquet"
    )

    # --------------------------------------------------------
    # Unique transactions
    # --------------------------------------------------------

    transaction_count = con.execute(
        f"""
        SELECT COUNT(DISTINCT transaction_id)
        FROM read_parquet(
            '{parquet_pattern}',
            hive_partitioning=true
        )
        """
    ).fetchone()[0]

    print(
        f"Unique transactions: "
        f"{transaction_count:,}"
    )

    print(
        f"Average basket size: "
        f"{row_count / transaction_count:.2f}"
    )

    # --------------------------------------------------------
    # Duplicate transaction-line keys
    # --------------------------------------------------------

    duplicate_groups = con.execute(
        f"""
        SELECT COUNT(*)
        FROM
        (
            SELECT
                transaction_id,
                line_id
            FROM read_parquet(
                '{parquet_pattern}',
                hive_partitioning=true
            )
            GROUP BY
                transaction_id,
                line_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_groups == 0:

        print(
            "[OK] Transaction-line keys unique"
        )

    else:

        print(
            f"[ERROR] "
            f"{duplicate_groups:,} duplicate keys"
        )

        errors.append(
            "Duplicate transaction-line keys"
        )

    con.close()

    # ========================================================
    # DISTRIBUTIONS
    # ========================================================

    print("\nDEMAND CLASS DISTRIBUTION")
    print("-" * 70)

    for demand_class in [
        "HIGH",
        "MEDIUM",
        "LOW",
    ]:

        count = demand_counts[
            demand_class
        ]

        print(
            f"{demand_class:<10}"
            f"{count:>12,} "
            f"("
            f"{count / row_count * 100:6.2f}%"
            f")"
        )

    print("\nWEEKDAY DISTRIBUTION")
    print("-" * 70)

    for day in [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]:

        count = weekday_counts[day]

        print(
            f"{day:<12}"
            f"{count:>12,} "
            f"("
            f"{count / row_count * 100:6.2f}%"
            f")"
        )

    # ========================================================
    # FINAL RESULT
    # ========================================================

    print("\n" + "=" * 70)

    if errors:

        print(
            f"FINAL SALES VALIDATION FAILED "
            f"({len(errors)} errors)"
        )

        print("=" * 70)

        for error in errors:
            print(
                f"[ERROR] {error}"
            )

        raise SystemExit(1)

    print(
        "FINAL 5M SALES DATASET "
        "VALIDATION PASSED"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
