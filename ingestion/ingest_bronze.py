import os
import sys
import shutil
import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow.dataset as ds
import mysql.connector
from dotenv import load_dotenv


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

GENERATED_DIR = PROJECT_ROOT / "data" / "generated"
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
LOG_DIR = PROJECT_ROOT / "logs"

ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)

LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# MYSQL CONFIG
# ============================================================

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}


# ============================================================
# EXPECTED COUNTS
# ============================================================

EXPECTED_COUNTS = {
    "sales": 5_000_000,
    "purchase_orders": 150_000,
    "stores": 201,
    "products": 5_000,
    "suppliers": 100,
    "supplier_products": 12_264,
    "inventory": 471_000,
}


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_DIR / "bronze_ingestion.log"


def log(message):

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    line = f"[{timestamp}] {message}"

    print(line)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ============================================================
# VALIDATION
# ============================================================

def validate_count(name, actual):

    expected = EXPECTED_COUNTS[name]

    if actual != expected:
        raise ValueError(
            f"{name}: expected {expected:,} rows, "
            f"found {actual:,}"
        )

    log(
        f"[OK] {name}: "
        f"{actual:,} rows"
    )


# ============================================================
# SALES INGESTION
# ============================================================

def ingest_sales():

    log("Starting historical sales ingestion")

    source = GENERATED_DIR / "sales_final"
    destination = BRONZE_DIR / "sales"

    dataset = ds.dataset(
        source,
        format="parquet",
        partitioning="hive",
    )

    rows = dataset.count_rows()

    validate_count(
        "sales",
        rows,
    )

    if destination.exists():
        shutil.rmtree(destination)

    shutil.copytree(
        source,
        destination,
    )

    bronze_dataset = ds.dataset(
        destination,
        format="parquet",
        partitioning="hive",
    )

    bronze_rows = bronze_dataset.count_rows()

    if bronze_rows != rows:
        raise RuntimeError(
            "Sales Bronze verification failed"
        )

    log(
        f"[OK] sales copied to Bronze: "
        f"{bronze_rows:,} rows"
    )


# ============================================================
# PURCHASE ORDER INGESTION
# ============================================================

def ingest_purchase_orders():

    log("Starting purchase order ingestion")

    source = (
        GENERATED_DIR /
        "purchase_orders_final.parquet"
    )

    destination = (
        BRONZE_DIR /
        "purchase_orders" /
        "purchase_orders.parquet"
    )

    df = pd.read_parquet(source)

    validate_count(
        "purchase_orders",
        len(df),
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        destination,
        index=False,
    )

    check = pd.read_parquet(destination)

    if len(check) != len(df):
        raise RuntimeError(
            "Purchase order Bronze verification failed"
        )

    log(
        f"[OK] purchase_orders written to Bronze: "
        f"{len(check):,} rows"
    )


# ============================================================
# MYSQL EXTRACTION
# ============================================================

def ingest_mysql_table(
    connection,
    table_name,
):

    log(
        f"Extracting MySQL table: "
        f"{table_name}"
    )

    query = f"""
        SELECT *
        FROM `{table_name}`
    """

    df = pd.read_sql(
        query,
        connection,
    )

    validate_count(
        table_name,
        len(df),
    )

    destination = (
        BRONZE_DIR /
        table_name /
        f"{table_name}.parquet"
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        destination,
        index=False,
    )

    check = pd.read_parquet(
        destination
    )

    if len(check) != len(df):
        raise RuntimeError(
            f"{table_name} Bronze verification failed"
        )

    log(
        f"[OK] {table_name} written to Bronze: "
        f"{len(check):,} rows"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    start = time.time()

    print("=" * 70)
    print("BRONZE LAYER INGESTION")
    print("=" * 70)

    connection = None

    try:

        # ----------------------------------------
        # Historical files
        # ----------------------------------------

        ingest_sales()

        ingest_purchase_orders()

        # ----------------------------------------
        # Operational MySQL
        # ----------------------------------------

        log("Connecting to operational MySQL")

        connection = mysql.connector.connect(
            **DB_CONFIG
        )

        if not connection.is_connected():
            raise RuntimeError(
                "Could not connect to MySQL"
            )

        log("[OK] MySQL connection successful")

        for table in [
            "stores",
            "products",
            "suppliers",
            "supplier_products",
            "inventory",
        ]:
            ingest_mysql_table(
                connection,
                table,
            )

        elapsed = time.time() - start

        print("\n" + "=" * 70)
        print("BRONZE INGESTION COMPLETE")
        print("=" * 70)

        print(
            f"Elapsed time: "
            f"{elapsed:.2f} seconds"
        )

        print(
            f"Bronze location: "
            f"{BRONZE_DIR}"
        )

    except Exception as exc:

        print("\n" + "=" * 70)
        print("BRONZE INGESTION FAILED")
        print("=" * 70)

        print(
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        sys.exit(1)

    finally:

        if (
            connection is not None
            and connection.is_connected()
        ):
            connection.close()
            log("MySQL connection closed")


if __name__ == "__main__":
    main()
