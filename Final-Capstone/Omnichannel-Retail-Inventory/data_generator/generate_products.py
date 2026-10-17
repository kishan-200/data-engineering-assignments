import random
import numpy as np
import pandas as pd

from config import (
    GENERATED_DIR,
    NUM_PRODUCTS,
    PRODUCT_CATEGORIES,
    RANDOM_SEED,
)


random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


BRANDS = [
    "FreshDay",
    "DailyChoice",
    "UrbanBasket",
    "HomeSelect",
    "PrimeMart",
    "GoodLife",
    "NatureFresh",
    "ValuePlus",
    "PureChoice",
    "SmartBuy",
]


def get_demand_class():
    return random.choices(
        population=["HIGH", "MEDIUM", "LOW"],
        weights=[20, 50, 30],
        k=1,
    )[0]


def get_price(category):
    price_ranges = {
        "Grocery": (30, 1200),
        "Dairy": (20, 600),
        "Beverages": (15, 800),
        "Snacks": (10, 500),
        "Bakery": (20, 600),
        "Personal Care": (30, 1500),
        "Household": (30, 2000),
        "Frozen": (40, 1000),
        "Electronics": (150, 5000),
        "Home": (50, 3000),
    }

    minimum, maximum = price_ranges[category]

    return round(
        random.uniform(minimum, maximum),
        2,
    )


def get_shelf_life(category):
    ranges = {
        "Dairy": (3, 30),
        "Bakery": (2, 15),
        "Frozen": (60, 365),
        "Grocery": (90, 730),
        "Beverages": (90, 730),
        "Snacks": (90, 365),
        "Personal Care": (365, 1095),
        "Household": (365, 1095),
        "Electronics": (730, 1825),
        "Home": (730, 1825),
    }

    low, high = ranges[category]

    return random.randint(low, high)


def generate_products():

    products = []

    categories = list(PRODUCT_CATEGORIES.keys())

    for i in range(1, NUM_PRODUCTS + 1):

        category = random.choice(categories)

        subcategory = random.choice(
            PRODUCT_CATEGORIES[category]
        )

        brand = random.choice(BRANDS)

        selling_price = get_price(category)

        # Cost is approximately 55–80% of selling price
        cost_ratio = random.uniform(0.55, 0.80)

        base_cost = round(
            selling_price * cost_ratio,
            2,
        )

        demand_class = get_demand_class()

        products.append(
            {
                "sku": f"SKU{i:05d}",
                "product_name": (
                    f"{brand} {subcategory} {i:04d}"
                ),
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "selling_price": selling_price,
                "base_cost": base_cost,
                "shelf_life_days": get_shelf_life(category),
                "demand_class": demand_class,
                "active": True,
            }
        )

    df = pd.DataFrame(products)

    output_file = GENERATED_DIR / "products.csv"

    df.to_csv(
        output_file,
        index=False,
    )

    print(f"Generated {len(df):,} products")
    print(f"Saved to: {output_file}")

    print("\nCategory distribution:")
    print(df["category"].value_counts())

    print("\nDemand class distribution:")
    print(df["demand_class"].value_counts())

    print("\nPrice summary:")
    print(
        df["selling_price"]
        .describe()
        .round(2)
    )

    return df


if __name__ == "__main__":
    generate_products()
