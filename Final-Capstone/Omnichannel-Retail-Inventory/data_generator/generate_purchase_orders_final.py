import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import GENERATED_DIR, RANDOM_SEED


# ============================================================
# CONFIGURATION
# ============================================================

TARGET_PO_LINES = 150_000

HISTORY_START = datetime(2024, 1, 1)
HISTORY_END = datetime(2025, 12, 31)

OUTPUT_FILE = (
    GENERATED_DIR
    / "purchase_orders_final.parquet"
)

random.seed(RANDOM_SEED + 200)
np.random.seed(RANDOM_SEED + 200)


# ============================================================
# RANDOM DATE
# ============================================================

def random_order_date():

    days = (
        HISTORY_END - HISTORY_START
    ).days

    return (
        HISTORY_START
        + timedelta(
            days=random.randint(0, days)
        )
    )


# ============================================================
# GENERATOR
# ============================================================

def generate():

    print("=" * 70)
    print("FINAL PURCHASE ORDER GENERATOR")
    print("=" * 70)

    print(
        f"Target PO lines: "
        f"{TARGET_PO_LINES:,}"
    )

    # --------------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------------

    inventory = pd.read_parquet(
        GENERATED_DIR
        / "opening_inventory.parquet"
    )

    suppliers = pd.read_csv(
        GENERATED_DIR
        / "suppliers.csv"
    )

    mappings = pd.read_csv(
        GENERATED_DIR
        / "supplier_products.csv"
    )

    # --------------------------------------------------------
    # SUPPLIER RELIABILITY
    # --------------------------------------------------------

    reliability_lookup = (
        suppliers
        .set_index("supplier_id")
        ["reliability_score"]
        .to_dict()
    )

    # --------------------------------------------------------
    # VALID STORE/SKU PAIRS
    # --------------------------------------------------------

    inventory_pairs = set(
        zip(
            inventory["store_id"],
            inventory["sku"],
        )
    )

    # --------------------------------------------------------
    # PREPARE SUPPLIER PRODUCT GROUPS
    # --------------------------------------------------------

    supplier_groups = {}

    for supplier_id, group in mappings.groupby(
        "supplier_id"
    ):

        supplier_groups[supplier_id] = (
            group.reset_index(drop=True)
        )

    supplier_ids = list(
        supplier_groups.keys()
    )

    # --------------------------------------------------------
    # GENERATION
    # --------------------------------------------------------

    rows = []

    po_number = 1
    generated_lines = 0

    while generated_lines < TARGET_PO_LINES:

        supplier_id = random.choice(
            supplier_ids
        )

        supplier_products = (
            supplier_groups[supplier_id]
        )

        # Pick a SKU first.
        first_row = supplier_products.sample(
            n=1
        ).iloc[0]

        first_sku = first_row["sku"]

        # Find stores that actually stock this SKU.
        possible_stores = inventory.loc[
            inventory["sku"] == first_sku,
            "store_id",
        ].tolist()

        if not possible_stores:
            continue

        store_id = random.choice(
            possible_stores
        )

        # Products from this supplier that are also
        # stocked by this store.
        available = supplier_products[
            supplier_products["sku"].apply(
                lambda sku:
                (store_id, sku)
                in inventory_pairs
            )
        ]

        if available.empty:
            continue

        # ----------------------------------------------------
        # MULTI-LINE PO
        # ----------------------------------------------------

        desired_lines = random.choices(
            [1, 2, 3, 4, 5, 6],
            weights=[
                5,
                15,
                25,
                25,
                20,
                10,
            ],
            k=1,
        )[0]

        remaining = (
            TARGET_PO_LINES
            - generated_lines
        )

        line_count = min(
            desired_lines,
            len(available),
            remaining,
        )

        selected = available.sample(
            n=line_count,
            replace=False,
        )

        po_id = (
            f"POF{po_number:09d}"
        )

        order_date = random_order_date()

        reliability = float(
            reliability_lookup[
                supplier_id
            ]
        )

        # ----------------------------------------------------
        # PO LINES
        # ----------------------------------------------------

        for line_number, (
            _,
            product,
        ) in enumerate(
            selected.iterrows(),
            start=1,
        ):

            lead_time = int(
                product["lead_time_days"]
            )

            minimum_qty = int(
                product[
                    "minimum_order_qty"
                ]
            )

            supplier_cost = float(
                product["supplier_cost"]
            )

            expected_delivery = (
                order_date
                + timedelta(
                    days=lead_time
                )
            )

            # ----------------------------------------------
            # DELIVERY BEHAVIOUR
            # ----------------------------------------------

            if random.random() <= reliability:

                delay = random.choices(
                    [-1, 0, 0, 0, 1],
                    weights=[
                        5,
                        55,
                        15,
                        15,
                        10,
                    ],
                    k=1,
                )[0]

            else:

                delay = random.randint(
                    2,
                    8,
                )

            actual_delivery = (
                expected_delivery
                + timedelta(days=delay)
            )

            if actual_delivery < order_date:
                actual_delivery = order_date

            # ----------------------------------------------
            # QUANTITY
            # ----------------------------------------------

            multiplier = random.choice(
                [1, 1, 2, 2, 3, 4]
            )

            ordered_quantity = (
                minimum_qty
                * multiplier
            )

            # ----------------------------------------------
            # RECEIVED QUANTITY
            # ----------------------------------------------

            if random.random() <= reliability:

                received_quantity = (
                    ordered_quantity
                )

            else:

                shortage = random.uniform(
                    0.05,
                    0.30,
                )

                received_quantity = max(
                    1,
                    round(
                        ordered_quantity
                        * (1 - shortage)
                    ),
                )

            # ----------------------------------------------
            # COST VARIATION
            # ----------------------------------------------

            unit_cost = round(
                supplier_cost
                * random.uniform(
                    0.95,
                    1.05,
                ),
                2,
            )

            rows.append(
                {
                    "po_id":
                        po_id,

                    "po_line_id":
                        line_number,

                    "supplier_id":
                        supplier_id,

                    "store_id":
                        store_id,

                    "sku":
                        product["sku"],

                    "order_date":
                        order_date.date(),

                    "expected_delivery_date":
                        expected_delivery.date(),

                    "actual_delivery_date":
                        actual_delivery.date(),

                    "ordered_quantity":
                        ordered_quantity,

                    "received_quantity":
                        received_quantity,

                    "unit_cost":
                        unit_cost,

                    "status":
                        "RECEIVED",
                }
            )

            generated_lines += 1

        po_number += 1

        if (
            generated_lines % 10_000
            < 6
            or
            generated_lines
            == TARGET_PO_LINES
        ):

            print(
                f"\rGenerated "
                f"{generated_lines:,}"
                f"/{TARGET_PO_LINES:,}",
                end="",
                flush=True,
            )

    print()

    # --------------------------------------------------------
    # DATAFRAME
    # --------------------------------------------------------

    df = pd.DataFrame(rows)

    df = df.sort_values(
        [
            "order_date",
            "po_id",
            "po_line_id",
        ]
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("FINAL PURCHASE ORDER GENERATION COMPLETE")
    print("=" * 70)

    print(
        f"PO lines:          "
        f"{len(df):,}"
    )

    print(
        f"Purchase orders:   "
        f"{df['po_id'].nunique():,}"
    )

    print(
        f"Avg lines / PO:    "
        f"{len(df) / df['po_id'].nunique():.2f}"
    )

    print(
        f"Suppliers used:    "
        f"{df['supplier_id'].nunique():,}"
    )

    print(
        f"Stores represented:"
        f" {df['store_id'].nunique():,}"
    )

    print(
        f"Products represented:"
        f" {df['sku'].nunique():,}"
    )

    expected = pd.to_datetime(
        df["expected_delivery_date"]
    )

    actual = pd.to_datetime(
        df["actual_delivery_date"]
    )

    late = actual > expected

    partial = (
        df["received_quantity"]
        <
        df["ordered_quantity"]
    )

    print(
        f"Late deliveries:   "
        f"{late.sum():,} "
        f"({late.mean() * 100:.2f}%)"
    )

    print(
        f"Partial deliveries:"
        f" {partial.sum():,} "
        f"({partial.mean() * 100:.2f}%)"
    )

    print(
        f"Output:            "
        f"{OUTPUT_FILE}"
    )


if __name__ == "__main__":
    generate()
