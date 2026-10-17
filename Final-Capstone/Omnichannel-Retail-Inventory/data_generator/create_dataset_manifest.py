from pathlib import Path
from datetime import datetime
import hashlib

import pandas as pd
import pyarrow.dataset as ds

from config import GENERATED_DIR


def sha256_file(path):

    sha = hashlib.sha256()

    with open(path, "rb") as f:

        while True:

            chunk = f.read(1024 * 1024)

            if not chunk:
                break

            sha.update(chunk)

    return sha.hexdigest()


def main():

    print("=" * 70)
    print("FINAL DATASET MANIFEST")
    print("=" * 70)

    files = [
        "stores.csv",
        "products.csv",
        "suppliers.csv",
        "supplier_products.csv",
        "opening_inventory.parquet",
        "purchase_orders_final.parquet",
    ]

    print("\nMASTER / PROCUREMENT DATA")
    print("-" * 70)

    total_rows = 0

    for filename in files:

        path = GENERATED_DIR / filename

        if filename.endswith(".csv"):

            rows = len(
                pd.read_csv(path)
            )

        else:

            rows = len(
                pd.read_parquet(path)
            )

        total_rows += rows

        size_mb = (
            path.stat().st_size
            / 1024
            / 1024
        )

        checksum = sha256_file(path)

        print(
            f"{filename:<35}"
            f"{rows:>12,} rows   "
            f"{size_mb:>8.2f} MB"
        )

        print(
            f"  SHA256: {checksum}"
        )

    # --------------------------------------------------------
    # SALES DATASET
    # --------------------------------------------------------

    sales_path = (
        GENERATED_DIR / "sales_final"
    )

    sales = ds.dataset(
        sales_path,
        format="parquet",
        partitioning="hive",
    )

    sales_rows = sales.count_rows()

    sales_files = list(
        sales_path.rglob("*.parquet")
    )

    sales_size = sum(
        p.stat().st_size
        for p in sales_files
    )

    total_rows += sales_rows

    print("\nHISTORICAL SALES")
    print("-" * 70)

    print(
        f"Rows:             "
        f"{sales_rows:,}"
    )

    print(
        f"Parquet files:    "
        f"{len(sales_files):,}"
    )

    print(
        f"Year/month parts: "
        f"24"
    )

    print(
        f"Size:             "
        f"{sales_size / 1024 / 1024:.2f} MB"
    )

    # --------------------------------------------------------
    # TOTAL
    # --------------------------------------------------------

    print("\n" + "=" * 70)

    print(
        f"TOTAL FINAL RECORDS: "
        f"{total_rows:,}"
    )

    print(
        f"Manifest created: "
        f"{datetime.now()}"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
