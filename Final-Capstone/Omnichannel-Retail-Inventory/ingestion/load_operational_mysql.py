import os
import sys
import time
from pathlib import Path

import pandas as pd
import mysql.connector
from dotenv import load_dotenv


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "generated"
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

required_variables = [
    "MYSQL_HOST",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
]

missing = [
    variable
    for variable in required_variables
    if not os.getenv(variable)
]

if missing:
    raise RuntimeError(
        f"Missing environment variables: {', '.join(missing)}"
    )


DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}


BATCH_SIZE = 5000


# ============================================================
# DATABASE CONNECTION
# ============================================================

def connect_mysql():

    connection = mysql.connector.connect(**DB_CONFIG)

    if not connection.is_connected():
        raise RuntimeError("Could not connect to MySQL")

    return connection


# ============================================================
# LOAD SOURCE DATA
# ============================================================

def read_sources():

    print("=" * 70)
    print("OPERATIONAL MYSQL INGESTION")
    print("=" * 70)

    print("\nReading frozen source datasets...")

    stores = pd.read_csv(
        DATA_DIR / "stores.csv"
    )

    products = pd.read_csv(
        DATA_DIR / "products.csv"
    )

    suppliers = pd.read_csv(
        DATA_DIR / "suppliers.csv"
    )

    supplier_products = pd.read_csv(
        DATA_DIR / "supplier_products.csv"
    )

    inventory = pd.read_parquet(
        DATA_DIR / "opening_inventory.parquet"
    )

    datasets = {
        "stores": stores,
        "products": products,
        "suppliers": suppliers,
        "supplier_products": supplier_products,
        "inventory": inventory,
    }

    print("\nValidating NULL values...")

    for name, df in datasets.items():

        null_count = int(df.isnull().sum().sum())

        if null_count > 0:
            raise ValueError(
                f"{name} contains {null_count:,} NULL value(s)"
            )

        print(f"[OK] {name}: no NULL values")

    print(f"Stores:             {len(stores):,}")
    print(f"Products:           {len(products):,}")
    print(f"Suppliers:          {len(suppliers):,}")
    print(f"Supplier products:  {len(supplier_products):,}")
    print(f"Inventory:          {len(inventory):,}")

    total = (
        len(stores)
        + len(products)
        + len(suppliers)
        + len(supplier_products)
        + len(inventory)
    )

    print("-" * 70)
    print(f"Total rows:         {total:,}")

    return (
        stores,
        products,
        suppliers,
        supplier_products,
        inventory,
    )


# ============================================================
# BATCH INSERT
# ============================================================

def load_dataframe(
    connection,
    dataframe,
    table_name,
    columns,
):

    total_rows = len(dataframe)

    placeholders = ", ".join(
        ["%s"] * len(columns)
    )

    column_sql = ", ".join(
        f"`{column}`" for column in columns
    )

    sql = f"""
        INSERT INTO `{table_name}`
        ({column_sql})
        VALUES ({placeholders})
    """

    print("\n" + "-" * 70)
    print(f"Loading table: {table_name}")
    print(f"Rows: {total_rows:,}")
    print("-" * 70)

    cursor = connection.cursor()

    try:

        for start in range(
            0,
            total_rows,
            BATCH_SIZE
        ):

            end = min(
                start + BATCH_SIZE,
                total_rows
            )

            batch = dataframe.iloc[start:end]

            rows = list(
                batch[columns].itertuples(
                    index=False,
                    name=None
                )
            )

            cursor.executemany(
                sql,
                rows
            )

            connection.commit()

            percentage = (
                end / total_rows
            ) * 100

            print(
                f"{end:,}/{total_rows:,} "
                f"({percentage:.1f}%)"
            )

    except Exception:

        connection.rollback()
        raise

    finally:

        cursor.close()

    print(
        f"[OK] {table_name}: "
        f"{total_rows:,} rows loaded"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    (
        stores,
        products,
        suppliers,
        supplier_products,
        inventory,
    ) = read_sources()

    connection = None

    try:

        print("\nConnecting to MySQL...")

        connection = connect_mysql()

        print("[OK] MySQL connection successful")

        # 1. STORES
        load_dataframe(
            connection,
            stores,
            "stores",
            [
                "store_id",
                "store_name",
                "city",
                "state",
                "region",
                "store_type",
                "channel",
                "opening_date",
                "active",
            ],
        )

        # 2. PRODUCTS
        load_dataframe(
            connection,
            products,
            "products",
            [
                "sku",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "selling_price",
                "base_cost",
                "shelf_life_days",
                "demand_class",
                "active",
            ],
        )

        # 3. SUPPLIERS
        load_dataframe(
            connection,
            suppliers,
            "suppliers",
            [
                "supplier_id",
                "supplier_name",
                "city",
                "state",
                "region",
                "default_lead_time_days",
                "reliability_score",
                "minimum_order_value",
                "active",
            ],
        )

        # 4. SUPPLIER PRODUCTS
        load_dataframe(
            connection,
            supplier_products,
            "supplier_products",
            [
                "supplier_id",
                "sku",
                "supplier_cost",
                "minimum_order_qty",
                "lead_time_days",
                "preferred_supplier",
            ],
        )

        # 5. INVENTORY
        load_dataframe(
            connection,
            inventory,
            "inventory",
            [
                "store_id",
                "sku",
                "opening_stock",
                "safety_stock",
                "reorder_level",
                "max_stock_level",
                "last_updated",
            ],
        )

        elapsed = time.time() - start_time

        print("\n" + "=" * 70)
        print("OPERATIONAL MYSQL INGESTION COMPLETE")
        print("=" * 70)

        print("\nExpected table counts:")
        print(f"stores:              {len(stores):,}")
        print(f"products:            {len(products):,}")
        print(f"suppliers:           {len(suppliers):,}")
        print(
            f"supplier_products:   "
            f"{len(supplier_products):,}"
        )
        print(f"inventory:           {len(inventory):,}")

        print(f"\nElapsed time: {elapsed:.2f} seconds")

    except Exception as exc:

        print("\n" + "=" * 70)
        print("INGESTION FAILED")
        print("=" * 70)

        print(type(exc).__name__)
        print(exc)

        sys.exit(1)

    finally:

        if (
            connection is not None
            and connection.is_connected()
        ):

            connection.close()

            print("\nMySQL connection closed.")


if __name__ == "__main__":
    main()
