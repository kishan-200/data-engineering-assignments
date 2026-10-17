import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import GENERATED_DIR, RANDOM_SEED


# ============================================================
# CONFIGURATION
# ============================================================

TOTAL_SALES_LINES = 5_000_000

HISTORY_START = datetime(2024, 1, 1)
HISTORY_END = datetime(2025, 12, 31)

OUTPUT_DIR = GENERATED_DIR / "sales_final"

# Write frequently so RAM stays controlled.
WRITE_BATCH_SIZE = 100_000

random.seed(RANDOM_SEED + 100)
np.random.seed(RANDOM_SEED + 100)


# ============================================================
# BUSINESS DISTRIBUTIONS
# ============================================================

DEMAND_WEIGHT = {
    "HIGH": 5.0,
    "MEDIUM": 2.5,
    "LOW": 1.0,
}

STORE_TRAFFIC_WEIGHT = {
    "SMALL": 0.70,
    "MEDIUM": 1.00,
    "LARGE": 1.40,
    "ONLINE": 1.60,
}

DAY_WEIGHT = {
    0: 0.85,   # Monday
    1: 0.90,
    2: 0.95,
    3: 1.00,
    4: 1.15,
    5: 1.35,
    6: 1.25,   # Sunday
}

QUANTITY_VALUES = [1, 2, 3, 4, 5]
QUANTITY_WEIGHTS = [60, 25, 9, 4, 2]

BASKET_VALUES = [1, 2, 3, 4, 5, 6, 7]
BASKET_WEIGHTS = [10, 18, 25, 22, 14, 7, 4]

DISCOUNT_VALUES = [0, 5, 10, 15, 20]
DISCOUNT_WEIGHTS = [70, 10, 10, 6, 4]

PHYSICAL_PAYMENTS = [
    "UPI",
    "CARD",
    "CASH",
    "WALLET",
]

ONLINE_PAYMENTS = [
    "UPI",
    "CARD",
    "WALLET",
    "NET_BANKING",
    "COD",
]


# ============================================================
# SEASONALITY
# ============================================================

def seasonal_multiplier(category, month):

    if month in [3, 4, 5, 6]:
        if category in ["Beverages", "Frozen"]:
            return 1.45

    if month in [7, 8, 9]:
        if category in ["Snacks", "Beverages"]:
            return 1.15

    if month in [10, 11, 12]:
        if category in [
            "Snacks",
            "Electronics",
            "Home",
            "Personal Care",
        ]:
            return 1.30

    if month in [1, 2, 12]:
        if category in ["Bakery", "Grocery"]:
            return 1.15

    return 1.0


# ============================================================
# DATE GENERATION
# ============================================================

def build_dates():

    dates = []

    current = HISTORY_START

    while current <= HISTORY_END:
        dates.append(current)
        current += timedelta(days=1)

    weights = np.array(
        [
            DAY_WEIGHT[d.weekday()]
            for d in dates
        ],
        dtype=float,
    )

    probabilities = weights / weights.sum()

    return dates, probabilities


# ============================================================
# TIME GENERATION
# ============================================================

def add_time(date_value, online):

    if online:

        hour = random.choices(
            list(range(24)),
            weights=[
                1, 1, 1, 1, 1, 1,
                2, 3, 5, 6, 7, 8,
                8, 7, 7, 7, 8, 9,
                10, 10, 9, 7, 4, 2,
            ],
            k=1,
        )[0]

    else:

        hour = random.choices(
            list(range(9, 23)),
            weights=[
                3, 5, 7, 8, 8, 7, 6,
                6, 7, 9, 10, 9, 6, 3,
            ],
            k=1,
        )[0]

    return date_value.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )


# ============================================================
# OUTPUT
# ============================================================

def write_batch(rows, batch_number):

    if not rows:
        return

    df = pd.DataFrame(rows)

    df["transaction_timestamp"] = pd.to_datetime(
        df["transaction_timestamp"]
    )

    df["year"] = (
        df["transaction_timestamp"]
        .dt.year
        .astype("int16")
    )

    df["month"] = (
        df["transaction_timestamp"]
        .dt.month
        .astype("int8")
    )

    # Partition each batch by year/month.
    for (year, month), group in df.groupby(
        ["year", "month"],
        sort=False,
    ):

        partition_dir = (
            OUTPUT_DIR
            / f"year={year}"
            / f"month={month:02d}"
        )

        partition_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        # year/month are encoded by directory names,
        # so they don't need to be stored in each record.
        group = group.drop(
            columns=["year", "month"]
        )

        output_file = (
            partition_dir
            / f"part-{batch_number:05d}.parquet"
        )

        table = pa.Table.from_pandas(
            group,
            preserve_index=False,
        )

        pq.write_table(
            table,
            output_file,
            compression="snappy",
        )


# ============================================================
# MAIN
# ============================================================

def generate():

    print("=" * 70)
    print("FINAL HISTORICAL SALES GENERATOR")
    print("=" * 70)

    print(
        f"Target sales lines: "
        f"{TOTAL_SALES_LINES:,}"
    )

    print(
        f"Batch size:         "
        f"{WRITE_BATCH_SIZE:,}"
    )

    print(
        f"Output:             "
        f"{OUTPUT_DIR}"
    )

    # --------------------------------------------------------
    # Load source data
    # --------------------------------------------------------

    stores = pd.read_csv(
        GENERATED_DIR / "stores.csv"
    )

    products = pd.read_csv(
        GENERATED_DIR / "products.csv"
    )

    inventory = pd.read_parquet(
        GENERATED_DIR
        / "opening_inventory.parquet"
    )

    # --------------------------------------------------------
    # Prepare output
    # --------------------------------------------------------

    if OUTPUT_DIR.exists():

        print(
            "\nRemoving previous sales_final directory..."
        )

        shutil.rmtree(OUTPUT_DIR)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # Product lookup
    # --------------------------------------------------------

    product_lookup = (
        products
        .set_index("sku")
    )

    # --------------------------------------------------------
    # Store lookup
    # --------------------------------------------------------

    stores = stores.reset_index(drop=True)

    store_weights = (
        stores["store_type"]
        .map(STORE_TRAFFIC_WEIGHT)
        .astype(float)
        .to_numpy()
    )

    store_probabilities = (
        store_weights
        / store_weights.sum()
    )

    store_indices = stores.index.to_numpy()

    # --------------------------------------------------------
    # Inventory lookup
    # --------------------------------------------------------

    inventory_by_store = {}

    for store_id, group in inventory.groupby(
        "store_id"
    ):

        skus = group["sku"].to_numpy()

        product_info = product_lookup.loc[
            skus
        ]

        base_weights = (
            product_info["demand_class"]
            .map(DEMAND_WEIGHT)
            .astype(float)
            .to_numpy()
        )

        categories = (
            product_info["category"]
            .to_numpy()
        )

        inventory_by_store[store_id] = {
            "skus": skus,
            "base_weights": base_weights,
            "categories": categories,
        }

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    dates, date_probabilities = build_dates()

    # --------------------------------------------------------
    # Generation
    # --------------------------------------------------------

    rows = []

    generated_lines = 0
    transaction_number = 1
    batch_number = 1

    while generated_lines < TOTAL_SALES_LINES:

        # Choose store
        store_index = np.random.choice(
            store_indices,
            p=store_probabilities,
        )

        store = stores.loc[store_index]

        store_id = store["store_id"]

        online = (
            store["store_type"] == "ONLINE"
        )

        channel = (
            "ONLINE"
            if online
            else "STORE"
        )

        # Choose date
        date_index = np.random.choice(
            len(dates),
            p=date_probabilities,
        )

        selected_date = dates[date_index]

        timestamp = add_time(
            selected_date,
            online,
        )

        # Basket
        basket_size = random.choices(
            BASKET_VALUES,
            weights=BASKET_WEIGHTS,
            k=1,
        )[0]

        remaining = (
            TOTAL_SALES_LINES
            - generated_lines
        )

        basket_size = min(
            basket_size,
            remaining,
        )

        store_data = (
            inventory_by_store[store_id]
        )

        skus = store_data["skus"]

        base_weights = (
            store_data["base_weights"]
        )

        categories = (
            store_data["categories"]
        )

        # Apply seasonal effects
        season_weights = np.array(
            [
                seasonal_multiplier(
                    category,
                    timestamp.month,
                )
                for category in categories
            ],
            dtype=float,
        )

        weights = (
            base_weights
            * season_weights
        )

        probabilities = (
            weights
            / weights.sum()
        )

        selected_positions = np.random.choice(
            len(skus),
            size=basket_size,
            replace=False,
            p=probabilities,
        )

        selected_skus = (
            skus[selected_positions]
        )

        transaction_id = (
            f"TXF{transaction_number:012d}"
        )

        if online:
            payment_type = random.choice(
                ONLINE_PAYMENTS
            )
        else:
            payment_type = random.choice(
                PHYSICAL_PAYMENTS
            )

        discount_pct = random.choices(
            DISCOUNT_VALUES,
            weights=DISCOUNT_WEIGHTS,
            k=1,
        )[0]

        # ----------------------------------------------------
        # Transaction lines
        # ----------------------------------------------------

        for line_id, sku in enumerate(
            selected_skus,
            start=1,
        ):

            product = product_lookup.loc[
                sku
            ]

            quantity = random.choices(
                QUANTITY_VALUES,
                weights=QUANTITY_WEIGHTS,
                k=1,
            )[0]

            rows.append(
                {
                    "transaction_id":
                        transaction_id,

                    "line_id":
                        line_id,

                    "store_id":
                        store_id,

                    "sku":
                        sku,

                    "quantity":
                        quantity,

                    "unit_price":
                        round(
                            float(
                                product[
                                    "selling_price"
                                ]
                            ),
                            2,
                        ),

                    "discount_pct":
                        discount_pct,

                    "transaction_timestamp":
                        timestamp,

                    "channel":
                        channel,

                    "payment_type":
                        payment_type,
                }
            )

            generated_lines += 1

        transaction_number += 1

        # ----------------------------------------------------
        # Flush batch
        # ----------------------------------------------------

        if (
            len(rows) >= WRITE_BATCH_SIZE
            or
            generated_lines
            == TOTAL_SALES_LINES
        ):

            write_batch(
                rows,
                batch_number,
            )

            print(
                f"Batch {batch_number:03d} | "
                f"{generated_lines:,}"
                f"/{TOTAL_SALES_LINES:,} "
                f"({generated_lines / TOTAL_SALES_LINES * 100:.1f}%)"
            )

            rows.clear()

            batch_number += 1

    print("\n" + "=" * 70)
    print("FINAL SALES GENERATION COMPLETE")
    print("=" * 70)

    print(
        f"Sales lines:    "
        f"{generated_lines:,}"
    )

    print(
        f"Transactions:   "
        f"{transaction_number - 1:,}"
    )

    print(
        f"Partitions:     "
        f"24 year/month partitions"
    )

    print(
        f"Output:         "
        f"{OUTPUT_DIR}"
    )


if __name__ == "__main__":
    generate()
