import random
import pandas as pd

from config import (
    GENERATED_DIR,
    RANDOM_SEED,
)


random.seed(RANDOM_SEED)


def generate_supplier_products():

    products_file = GENERATED_DIR / "products.csv"
    suppliers_file = GENERATED_DIR / "suppliers.csv"

    if not products_file.exists():
        raise FileNotFoundError(
            "products.csv not found. Run generate_products.py first."
        )

    if not suppliers_file.exists():
        raise FileNotFoundError(
            "suppliers.csv not found. Run generate_suppliers.py first."
        )

    products = pd.read_csv(products_file)
    suppliers = pd.read_csv(suppliers_file)

    relationships = []

    supplier_ids = suppliers["supplier_id"].tolist()

    for _, product in products.iterrows():

        sku = product["sku"]
        base_cost = float(product["base_cost"])

        # Each product will have between 1 and 4 suppliers.
        # Most products have 2 or 3.
        num_suppliers = random.choices(
            population=[1, 2, 3, 4],
            weights=[10, 45, 35, 10],
            k=1,
        )[0]

        selected_suppliers = random.sample(
            supplier_ids,
            num_suppliers,
        )

        # Exactly one preferred supplier per product.
        preferred_supplier = random.choice(
            selected_suppliers
        )

        for supplier_id in selected_suppliers:

            supplier_row = suppliers.loc[
                suppliers["supplier_id"] == supplier_id
            ].iloc[0]

            default_lead_time = int(
                supplier_row["default_lead_time_days"]
            )

            # Supplier-specific cost varies around base cost.
            cost_multiplier = random.uniform(
                0.92,
                1.08,
            )

            supplier_cost = round(
                base_cost * cost_multiplier,
                2,
            )

            # Prevent invalid/zero costs.
            supplier_cost = max(
                supplier_cost,
                1.0,
            )

            # Supplier-product lead time may vary slightly
            # from supplier's default.
            lead_time_days = max(
                1,
                default_lead_time
                + random.choice([-1, 0, 0, 0, 1]),
            )

            minimum_order_qty = random.choice(
                [
                    10,
                    20,
                    25,
                    50,
                    75,
                    100,
                    150,
                    200,
                ]
            )

            relationships.append(
                {
                    "supplier_id": supplier_id,
                    "sku": sku,
                    "supplier_cost": supplier_cost,
                    "minimum_order_qty": minimum_order_qty,
                    "lead_time_days": lead_time_days,
                    "preferred_supplier": (
                        supplier_id == preferred_supplier
                    ),
                }
            )

    df = pd.DataFrame(relationships)

    # Stable ordering makes the generated file easier
    # to compare between runs.
    df = df.sort_values(
        ["sku", "supplier_id"]
    ).reset_index(drop=True)

    output_file = (
        GENERATED_DIR
        / "supplier_products.csv"
    )

    df.to_csv(
        output_file,
        index=False,
    )

    print(
        f"Generated {len(df):,} "
        "supplier-product relationships"
    )

    print(f"Saved to: {output_file}")

    print("\nSuppliers per product:")
    print(
        df.groupby("sku")["supplier_id"]
        .count()
        .describe()
        .round(2)
    )

    print("\nPreferred supplier count:")
    print(
        df["preferred_supplier"]
        .value_counts()
    )

    return df


if __name__ == "__main__":
    generate_supplier_products()
