from pathlib import Path
import sys

import pandas as pd
import pyarrow.dataset as ds


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE = PROJECT_ROOT / "data_lake" / "bronze"


EXPECTED = {
    "sales": 5_000_000,
    "purchase_orders": 150_000,
    "stores": 201,
    "products": 5_000,
    "suppliers": 100,
    "supplier_products": 12_264,
    "inventory": 471_000,
}


# ============================================================
# HELPERS
# ============================================================

def check(name, actual):
    expected = EXPECTED[name]

    if actual != expected:
        print(
            f"[FAIL] {name:<20} "
            f"{actual:,} rows "
            f"(expected {expected:,})"
        )
        return False

    print(
        f"[OK]   {name:<20} "
        f"{actual:,} rows"
    )

    return True


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("BRONZE DATA LAKE VALIDATION")
    print("=" * 70)

    results = []

    # --------------------------------------------------------
    # SALES
    # --------------------------------------------------------

    sales_path = BRONZE / "sales"

    sales = ds.dataset(
        sales_path,
        format="parquet",
        partitioning="hive",
    )

    sales_rows = sales.count_rows()

    results.append(
        check("sales", sales_rows)
    )

    # Check sales partitions

    parquet_files = list(
        sales_path.rglob("*.parquet")
    )

    year_dirs = sorted(
        p.name
        for p in sales_path.iterdir()
        if p.is_dir()
    )

    month_dirs = list(
        sales_path.glob(
            "year=*/month=*"
        )
    )

    print(
        f"       Sales parquet files: "
        f"{len(parquet_files):,}"
    )

    print(
        f"       Sales year partitions: "
        f"{len(year_dirs)}"
    )

    print(
        f"       Sales month partitions: "
        f"{len(month_dirs)}"
    )

    if len(month_dirs) != 24:
        print(
            "[FAIL] Expected 24 sales "
            "year/month partitions"
        )
        results.append(False)
    else:
        print(
            "[OK]   24 sales year/month "
            "partitions found"
        )

    # --------------------------------------------------------
    # OTHER BRONZE TABLES
    # --------------------------------------------------------

    files = {
        "purchase_orders":
            BRONZE
            / "purchase_orders"
            / "purchase_orders.parquet",

        "stores":
            BRONZE
            / "stores"
            / "stores.parquet",

        "products":
            BRONZE
            / "products"
            / "products.parquet",

        "suppliers":
            BRONZE
            / "suppliers"
            / "suppliers.parquet",

        "supplier_products":
            BRONZE
            / "supplier_products"
            / "supplier_products.parquet",

        "inventory":
            BRONZE
            / "inventory"
            / "inventory.parquet",
    }

    for name, path in files.items():

        if not path.exists():
            print(
                f"[FAIL] {name:<20} "
                "file missing"
            )
            results.append(False)
            continue

        dataset = ds.dataset(
            path,
            format="parquet",
        )

        rows = dataset.count_rows()

        results.append(
            check(name, rows)
        )

    # --------------------------------------------------------
    # BASIC DATA QUALITY
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("BASIC BRONZE QUALITY CHECKS")
    print("=" * 70)

    stores = pd.read_parquet(
        files["stores"]
    )

    products = pd.read_parquet(
        files["products"]
    )

    suppliers = pd.read_parquet(
        files["suppliers"]
    )

    print(
        "[OK] Store IDs unique"
        if stores["store_id"].is_unique
        else "[FAIL] Duplicate store IDs"
    )

    results.append(
        stores["store_id"].is_unique
    )

    print(
        "[OK] Product SKUs unique"
        if products["sku"].is_unique
        else "[FAIL] Duplicate product SKUs"
    )

    results.append(
        products["sku"].is_unique
    )

    print(
        "[OK] Supplier IDs unique"
        if suppliers["supplier_id"].is_unique
        else "[FAIL] Duplicate supplier IDs"
    )

    results.append(
        suppliers["supplier_id"].is_unique
    )

    # --------------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------------

    print("\n" + "=" * 70)

    if all(results):

        print("BRONZE DATA LAKE VALIDATION PASSED")
        print("=" * 70)

        print(
            "\nBronze layer is ready "
            "for Spark processing."
        )

    else:

        print("BRONZE DATA LAKE VALIDATION FAILED")
        print("=" * 70)

        sys.exit(1)


if __name__ == "__main__":
    main()
