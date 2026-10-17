from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(
    "/home/kishan/omnichannel-inventory"
)

SUPPLIER_FILE = (
    PROJECT_ROOT
    / "data/generated/suppliers.csv"
)

REPLENISHMENT_PATH = (
    PROJECT_ROOT
    / "data_lake/gold/replenishment_recommendations"
)

S3_BUCKET = "omnichannel-retail-kishan-034362072403"


# ============================================================
# TASK FUNCTIONS
# ============================================================

def supplier_file_exists():
    """
    Wait until the supplier master file is available.
    """
    exists = SUPPLIER_FILE.exists()

    print(
        f"Supplier file: {SUPPLIER_FILE}"
    )
    print(
        f"Exists: {exists}"
    )

    return exists


def choose_replenishment_branch():
    """
    The Gold replenishment dataset contains store-SKU
    combinations requiring replenishment.

    If valid parquet files exist, generate draft POs.
    Otherwise take the no-action branch.
    """

    if not REPLENISHMENT_PATH.exists():
        print(
            "Replenishment directory does not exist."
        )
        return "no_action"

    parquet_files = list(
        REPLENISHMENT_PATH.rglob("*.parquet")
    )

    valid_files = [
        f
        for f in parquet_files
        if f.is_file()
        and f.stat().st_size > 0
    ]

    print(
        f"Valid replenishment parquet files: "
        f"{len(valid_files)}"
    )

    if valid_files:
        return "generate_draft_po"

    return "no_action"


def verify_gold_dataset():
    """
    Verify that the Gold replenishment dataset
    exists after replenishment processing.
    """

    if not REPLENISHMENT_PATH.exists():
        raise RuntimeError(
            "Gold replenishment path does not exist."
        )

    parquet_files = list(
        REPLENISHMENT_PATH.rglob("*.parquet")
    )

    valid_files = [
        f
        for f in parquet_files
        if f.is_file()
        and f.stat().st_size > 0
    ]

    print("=" * 60)
    print("GOLD DATASET VERIFICATION")
    print("=" * 60)

    print(
        "Gold path:",
        REPLENISHMENT_PATH
    )

    print(
        "Valid parquet files:",
        len(valid_files)
    )

    if not valid_files:
        raise RuntimeError(
            "No valid Gold replenishment files found."
        )

    print(
        "Gold replenishment dataset: VERIFIED"
    )
    print("=" * 60)

    return True


# ============================================================
# DEFAULT AIRFLOW SETTINGS
# ============================================================

default_args = {
    "owner": "omnichannel-retail",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="omnichannel_replenishment",
    description=(
        "Supplier sensor -> S3 verification -> "
        "low-stock branch -> draft PO -> "
        "Gold analytics verification"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "retail",
        "inventory",
        "replenishment",
        "aws",
        "analytics",
    ],
) as dag:

    # --------------------------------------------------------
    # 1. Supplier file sensor
    # --------------------------------------------------------

    supplier_file_sensor = PythonSensor(
        task_id="supplier_file_sensor",
        python_callable=supplier_file_exists,
        poke_interval=20,
        timeout=300,
        mode="poke",
    )


    # --------------------------------------------------------
    # 2. Verify AWS S3 connectivity
    # --------------------------------------------------------

    verify_s3_bucket = BashOperator(
        task_id="verify_s3_bucket",
        bash_command=f"""
        set -e

        echo "========================================"
        echo "VERIFYING AWS S3"
        echo "========================================"

        aws s3 ls s3://{S3_BUCKET}/ > /dev/null

        echo "S3 bucket reachable:"
        echo "s3://{S3_BUCKET}/"
        echo "S3 verification: PASS"
        """,
    )


    # --------------------------------------------------------
    # 3. Check whether replenishment is required
    # --------------------------------------------------------

    check_low_stock = BranchPythonOperator(
        task_id="check_low_stock",
        python_callable=choose_replenishment_branch,
    )


    # --------------------------------------------------------
    # 4A. Generate draft purchase orders
    # --------------------------------------------------------

    generate_draft_po = BashOperator(
        task_id="generate_draft_po",
        bash_command="""
        set -e

        cd /home/kishan/omnichannel-inventory

        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PYSPARK_PYTHON=/usr/bin/python3.11
        export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

        echo "Generating draft purchase orders..."

        spark-submit \
          --master local[1] \
          --driver-memory 768m \
          --conf spark.sql.shuffle.partitions=1 \
          analytics/forecasting/generate_draft_purchase_orders.py

        echo "Draft PO generation completed."
        """,
    )


    # --------------------------------------------------------
    # 4B. No replenishment required
    # --------------------------------------------------------

    no_action = EmptyOperator(
        task_id="no_action"
    )


    # --------------------------------------------------------
    # 5. Join branch results
    # --------------------------------------------------------

    branch_complete = EmptyOperator(
        task_id="branch_complete",
        trigger_rule=(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        ),
    )


    # --------------------------------------------------------
    # 6. Verify Gold analytics dataset
    # --------------------------------------------------------

    verify_gold_output = PythonOperator(
        task_id="verify_gold_output",
        python_callable=verify_gold_dataset,
    )


    # --------------------------------------------------------
    # 7. Workflow completion
    # --------------------------------------------------------

    done = EmptyOperator(
        task_id="done"
    )


    # ========================================================
    # DAG DEPENDENCIES
    # ========================================================

    supplier_file_sensor \
        >> verify_s3_bucket \
        >> check_low_stock

    check_low_stock >> [
        generate_draft_po,
        no_action,
    ]

    generate_draft_po >> branch_complete
    no_action >> branch_complete

    branch_complete \
        >> verify_gold_output \
        >> done
