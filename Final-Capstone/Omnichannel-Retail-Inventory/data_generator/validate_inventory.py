import pandas as pd

from config import GENERATED_DIR


EXPECTED_ASSORTMENT = {
    "SMALL": 1500,
    "MEDIUM": 2500,
    "LARGE": 3500,
    "ONLINE": 5000,
}


def validate():

    stores = pd.read_csv(
        GENERATED_DIR / "stores.csv"
    )

    products = pd.read_csv(
        GENERATED_DIR / "products.csv"
    )

    inventory = pd.read_parquet(
        GENERATED_DIR / "opening_inventory.parquet"
    )

    errors = []

    # --------------------------------------------------
    # Primary key
    # --------------------------------------------------

    duplicate_keys = inventory.duplicated(
        subset=["store_id", "sku"]
    )

    if duplicate_keys.any():
        errors.append(
            f"{duplicate_keys.sum()} duplicate "
            "(store_id, sku) keys"
        )

    # --------------------------------------------------
    # Foreign keys
    # --------------------------------------------------

    invalid_stores = (
        set(inventory["store_id"])
        - set(stores["store_id"])
    )

    if invalid_stores:
        errors.append(
            f"{len(invalid_stores)} invalid stores"
        )

    invalid_skus = (
        set(inventory["sku"])
        - set(products["sku"])
    )

    if invalid_skus:
        errors.append(
            f"{len(invalid_skus)} invalid SKUs"
        )

    # --------------------------------------------------
    # Stock constraints
    # --------------------------------------------------

    if (inventory["opening_stock"] < 0).any():
        errors.append(
            "Negative opening stock found"
        )

    if (inventory["safety_stock"] <= 0).any():
        errors.append(
            "Invalid safety stock found"
        )

    invalid_reorder = (
        inventory["reorder_level"]
        <= inventory["safety_stock"]
    )

    if invalid_reorder.any():
        errors.append(
            "reorder_level <= safety_stock found"
        )

    invalid_max = (
        inventory["max_stock_level"]
        <= inventory["reorder_level"]
    )

    if invalid_max.any():
        errors.append(
            "max_stock_level <= reorder_level found"
        )

    # --------------------------------------------------
    # Store assortment
    # --------------------------------------------------

    counts = (
        inventory.groupby("store_id")
        .size()
        .to_dict()
    )

    bad_assortments = []

    for _, store in stores.iterrows():

        store_id = store["store_id"]
        store_type = store["store_type"]

        expected = EXPECTED_ASSORTMENT[
            store_type
        ]

        actual = counts.get(
            store_id,
            0,
        )

        if actual != expected:
            bad_assortments.append(
                (
                    store_id,
                    expected,
                    actual,
                )
            )

    if bad_assortments:
        errors.append(
            f"{len(bad_assortments)} stores have "
            "incorrect assortment sizes"
        )

    # --------------------------------------------------
    # Product coverage
    # --------------------------------------------------

    missing_products = (
        set(products["sku"])
        - set(inventory["sku"])
    )

    if missing_products:
        errors.append(
            f"{len(missing_products)} products are "
            "not stocked anywhere"
        )

    # --------------------------------------------------
    # Low-stock statistics
    # --------------------------------------------------

    low_stock = inventory[
        inventory["opening_stock"]
        < inventory["reorder_level"]
    ]

    low_stock_pct = (
        len(low_stock)
        / len(inventory)
        * 100
    )

    # --------------------------------------------------
    # Output
    # --------------------------------------------------

    print("=" * 60)
    print("OPENING INVENTORY VALIDATION")
    print("=" * 60)

    print(
        f"Inventory records:   "
        f"{len(inventory):,}"
    )

    print(
        f"Stores represented:  "
        f"{inventory['store_id'].nunique():,}"
    )

    print(
        f"Products represented:"
        f" {inventory['sku'].nunique():,}"
    )

    print(
        f"Low-stock records:   "
        f"{len(low_stock):,}"
    )

    print(
        f"Low-stock rate:      "
        f"{low_stock_pct:.2f}%"
    )

    print("-" * 60)

    if errors:

        print("VALIDATION FAILED")

        for error in errors:
            print(f"[ERROR] {error}")

    else:

        print("ALL VALIDATION CHECKS PASSED")

        print("[OK] Composite keys unique")
        print("[OK] Store references valid")
        print("[OK] SKU references valid")
        print("[OK] Opening stock non-negative")
        print("[OK] Safety stock valid")
        print("[OK] Reorder levels valid")
        print("[OK] Maximum stock levels valid")
        print("[OK] Store assortments correct")
        print("[OK] All products stocked")


if __name__ == "__main__":
    validate()
