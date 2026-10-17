import random
import numpy as np
import pandas as pd

from config import GENERATED_DIR, RANDOM_SEED


random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


# Number of SKUs carried by each store type
ASSORTMENT_SIZE = {
    "SMALL": 1500,
    "MEDIUM": 2500,
    "LARGE": 3500,
    "ONLINE": 5000,
}


# Base stock ranges according to product demand
BASE_STOCK_RANGE = {
    "HIGH": (80, 160),
    "MEDIUM": (35, 90),
    "LOW": (10, 40),
}


# Larger stores normally carry more units
STORE_STOCK_MULTIPLIER = {
    "SMALL": 0.75,
    "MEDIUM": 1.00,
    "LARGE": 1.40,
    "ONLINE": 2.00,
}


# Higher-demand products are more likely
# to be included in a store's assortment.
DEMAND_SELECTION_WEIGHT = {
    "HIGH": 5.0,
    "MEDIUM": 2.5,
    "LOW": 1.0,
}


# Small percentage of inventory starts near
# its reorder point so we have useful
# replenishment scenarios later.
LOW_STOCK_RATE = 0.03


def select_products(products, count):

    weights = (
        products["demand_class"]
        .map(DEMAND_SELECTION_WEIGHT)
        .astype(float)
        .to_numpy()
    )

    probabilities = weights / weights.sum()

    selected_indexes = np.random.choice(
        products.index.to_numpy(),
        size=count,
        replace=False,
        p=probabilities,
    )

    return products.loc[selected_indexes]


def calculate_stock(product, store_type):

    demand_class = product["demand_class"]

    minimum, maximum = BASE_STOCK_RANGE[demand_class]

    base_stock = random.randint(
        minimum,
        maximum,
    )

    multiplier = STORE_STOCK_MULTIPLIER[
        store_type
    ]

    normal_stock = max(
        1,
        round(base_stock * multiplier),
    )

    # Safety stock: roughly 15–25% of normal stock
    safety_stock = max(
        1,
        round(
            normal_stock
            * random.uniform(0.15, 0.25)
        ),
    )

    # Reorder level sits above safety stock
    reorder_level = max(
        safety_stock + 1,
        round(
            normal_stock
            * random.uniform(0.35, 0.50)
        ),
    )

    # Maximum stock is above normal opening stock
    max_stock_level = max(
        reorder_level + 1,
        round(
            normal_stock
            * random.uniform(1.30, 1.70)
        ),
    )

    # Usually inventory starts in a healthy state.
    opening_stock = random.randint(
        reorder_level,
        max_stock_level,
    )

    # Approximately 3% intentionally start
    # below the reorder level.
    if random.random() < LOW_STOCK_RATE:

        opening_stock = random.randint(
            max(0, safety_stock // 2),
            max(0, reorder_level - 1),
        )

    return (
        opening_stock,
        safety_stock,
        reorder_level,
        max_stock_level,
    )


def generate_inventory():

    stores_file = GENERATED_DIR / "stores.csv"
    products_file = GENERATED_DIR / "products.csv"

    if not stores_file.exists():
        raise FileNotFoundError(
            "stores.csv not found. "
            "Run generate_stores.py first."
        )

    if not products_file.exists():
        raise FileNotFoundError(
            "products.csv not found. "
            "Run generate_products.py first."
        )

    stores = pd.read_csv(stores_file)
    products = pd.read_csv(products_file)

    inventory = []

    for _, store in stores.iterrows():

        store_id = store["store_id"]
        store_type = store["store_type"]

        assortment_count = ASSORTMENT_SIZE[
            store_type
        ]

        selected_products = select_products(
            products,
            assortment_count,
        )

        for _, product in selected_products.iterrows():

            (
                opening_stock,
                safety_stock,
                reorder_level,
                max_stock_level,
            ) = calculate_stock(
                product,
                store_type,
            )

            inventory.append(
                {
                    "store_id": store_id,
                    "sku": product["sku"],
                    "opening_stock": opening_stock,
                    "safety_stock": safety_stock,
                    "reorder_level": reorder_level,
                    "max_stock_level": max_stock_level,
                    "last_updated": "2025-12-31 23:59:59",
                }
            )

    df = pd.DataFrame(inventory)

    df = df.sort_values(
        ["store_id", "sku"]
    ).reset_index(drop=True)

    # Parquet is our primary format for this
    # larger dataset.
    parquet_file = (
        GENERATED_DIR
        / "opening_inventory.parquet"
    )

    df.to_parquet(
        parquet_file,
        index=False,
    )

    # Keep CSV too for now so it is easy
    # for us to inspect manually.
    csv_file = (
        GENERATED_DIR
        / "opening_inventory.csv"
    )

    df.to_csv(
        csv_file,
        index=False,
    )

    print("=" * 55)
    print("OPENING INVENTORY GENERATED")
    print("=" * 55)

    print(
        f"Inventory records: {len(df):,}"
    )

    print(
        f"Stores represented: "
        f"{df['store_id'].nunique():,}"
    )

    print(
        f"Products represented: "
        f"{df['sku'].nunique():,}"
    )

    print("\nOpening stock summary:")

    print(
        df["opening_stock"]
        .describe()
        .round(2)
    )

    low_stock = (
        df["opening_stock"]
        < df["reorder_level"]
    ).sum()

    print(
        f"\nLow-stock records: {low_stock:,}"
    )

    print(
        "Low-stock percentage: "
        f"{low_stock / len(df) * 100:.2f}%"
    )

    print(f"\nCSV:     {csv_file}")
    print(f"Parquet: {parquet_file}")

    return df


if __name__ == "__main__":
    generate_inventory()
