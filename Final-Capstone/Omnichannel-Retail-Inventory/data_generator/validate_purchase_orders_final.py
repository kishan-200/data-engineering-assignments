import pandas as pd

from config import GENERATED_DIR


PO_FILE = (
    GENERATED_DIR
    / "purchase_orders_final.parquet"
)

EXPECTED_LINES = 150_000


def main():

    print("=" * 70)
    print("FINAL PURCHASE ORDER VALIDATION")
    print("=" * 70)

    errors = []

    # ========================================================
    # LOAD DATA
    # ========================================================

    po = pd.read_parquet(PO_FILE)

    stores = pd.read_csv(
        GENERATED_DIR / "stores.csv"
    )

    products = pd.read_csv(
        GENERATED_DIR / "products.csv"
    )

    suppliers = pd.read_csv(
        GENERATED_DIR / "suppliers.csv"
    )

    mappings = pd.read_csv(
        GENERATED_DIR / "supplier_products.csv"
    )

    inventory = pd.read_parquet(
        GENERATED_DIR
        / "opening_inventory.parquet"
    )

    # ========================================================
    # COUNTS
    # ========================================================

    print("\nDATASET COUNTS")
    print("-" * 70)

    print(
        f"PO lines:          "
        f"{len(po):,}"
    )

    print(
        f"Purchase orders:   "
        f"{po['po_id'].nunique():,}"
    )

    print(
        f"Suppliers:         "
        f"{po['supplier_id'].nunique():,}"
    )

    print(
        f"Stores:            "
        f"{po['store_id'].nunique():,}"
    )

    print(
        f"Products:          "
        f"{po['sku'].nunique():,}"
    )

    if len(po) == EXPECTED_LINES:
        print("[OK] Exact row count")
    else:
        errors.append("Incorrect row count")

    # ========================================================
    # PRIMARY KEY
    # ========================================================

    print("\nKEY VALIDATION")
    print("-" * 70)

    duplicates = po.duplicated(
        [
            "po_id",
            "po_line_id",
        ]
    )

    if not duplicates.any():

        print(
            "[OK] PO-line keys unique"
        )

    else:

        count = int(
            duplicates.sum()
        )

        print(
            f"[ERROR] "
            f"{count:,} duplicate keys"
        )

        errors.append(
            "Duplicate PO-line keys"
        )

    # ========================================================
    # FOREIGN KEYS
    # ========================================================

    print("\nREFERENTIAL INTEGRITY")
    print("-" * 70)

    valid_stores = set(
        stores["store_id"]
    )

    valid_products = set(
        products["sku"]
    )

    valid_suppliers = set(
        suppliers["supplier_id"]
    )

    if set(po["store_id"]).issubset(
        valid_stores
    ):

        print(
            "[OK] All store references valid"
        )

    else:

        errors.append(
            "Invalid store references"
        )

    if set(po["sku"]).issubset(
        valid_products
    ):

        print(
            "[OK] All product references valid"
        )

    else:

        errors.append(
            "Invalid product references"
        )

    if set(po["supplier_id"]).issubset(
        valid_suppliers
    ):

        print(
            "[OK] All supplier references valid"
        )

    else:

        errors.append(
            "Invalid supplier references"
        )

    # ========================================================
    # STORE / SKU RELATIONSHIP
    # ========================================================

    print("\nSTORE / PRODUCT VALIDATION")
    print("-" * 70)

    inventory_pairs = set(
        zip(
            inventory["store_id"],
            inventory["sku"],
        )
    )

    invalid_store_sku = sum(
        pair not in inventory_pairs
        for pair in zip(
            po["store_id"],
            po["sku"],
        )
    )

    if invalid_store_sku == 0:

        print(
            "[OK] Every PO SKU is stocked "
            "by its store"
        )

    else:

        print(
            f"[ERROR] "
            f"{invalid_store_sku:,} "
            f"invalid store/SKU pairs"
        )

        errors.append(
            "Invalid store/SKU relationships"
        )

    # ========================================================
    # SUPPLIER / SKU RELATIONSHIP
    # ========================================================

    print("\nSUPPLIER / PRODUCT VALIDATION")
    print("-" * 70)

    supplier_pairs = set(
        zip(
            mappings["supplier_id"],
            mappings["sku"],
        )
    )

    invalid_supplier_sku = sum(
        pair not in supplier_pairs
        for pair in zip(
            po["supplier_id"],
            po["sku"],
        )
    )

    if invalid_supplier_sku == 0:

        print(
            "[OK] Every supplier supplies "
            "its PO SKU"
        )

    else:

        print(
            f"[ERROR] "
            f"{invalid_supplier_sku:,} "
            f"invalid supplier/SKU pairs"
        )

        errors.append(
            "Invalid supplier/SKU relationships"
        )

    # ========================================================
    # MULTI-LINE PO CONSISTENCY
    # ========================================================

    print("\nPURCHASE ORDER CONSISTENCY")
    print("-" * 70)

    consistency = (
        po.groupby("po_id")
        .agg(
            suppliers=(
                "supplier_id",
                "nunique",
            ),
            stores=(
                "store_id",
                "nunique",
            ),
            order_dates=(
                "order_date",
                "nunique",
            ),
        )
    )

    bad_supplier = (
        consistency["suppliers"] != 1
    ).sum()

    bad_store = (
        consistency["stores"] != 1
    ).sum()

    bad_date = (
        consistency["order_dates"] != 1
    ).sum()

    if bad_supplier == 0:

        print(
            "[OK] One supplier per PO"
        )

    else:

        errors.append(
            "PO contains multiple suppliers"
        )

    if bad_store == 0:

        print(
            "[OK] One store per PO"
        )

    else:

        errors.append(
            "PO contains multiple stores"
        )

    if bad_date == 0:

        print(
            "[OK] One order date per PO"
        )

    else:

        errors.append(
            "PO contains multiple order dates"
        )

    # ========================================================
    # LINE NUMBERING
    # ========================================================

    def valid_line_numbers(group):

        actual = sorted(
            group["po_line_id"].tolist()
        )

        expected = list(
            range(
                1,
                len(group) + 1,
            )
        )

        return actual == expected

    line_number_check = (
        po.groupby("po_id")
        .apply(
            valid_line_numbers,
            include_groups=False,
        )
    )

    if line_number_check.all():

        print(
            "[OK] PO line numbering valid"
        )

    else:

        errors.append(
            "Invalid PO line numbering"
        )

    # ========================================================
    # QUANTITY / COST RULES
    # ========================================================

    print("\nBUSINESS RULES")
    print("-" * 70)

    if (
        po["ordered_quantity"] > 0
    ).all():

        print(
            "[OK] Ordered quantities positive"
        )

    else:

        errors.append(
            "Invalid ordered quantities"
        )

    received_valid = (
        (po["received_quantity"] > 0)
        &
        (
            po["received_quantity"]
            <= po["ordered_quantity"]
        )
    )

    if received_valid.all():

        print(
            "[OK] Received quantities valid"
        )

    else:

        errors.append(
            "Invalid received quantities"
        )

    if (
        po["unit_cost"] > 0
    ).all():

        print(
            "[OK] Unit costs positive"
        )

    else:

        errors.append(
            "Invalid unit costs"
        )

    # ========================================================
    # DATE RULES
    # ========================================================

    order_date = pd.to_datetime(
        po["order_date"]
    )

    expected_date = pd.to_datetime(
        po["expected_delivery_date"]
    )

    actual_date = pd.to_datetime(
        po["actual_delivery_date"]
    )

    if (
        expected_date >= order_date
    ).all():

        print(
            "[OK] Expected delivery dates valid"
        )

    else:

        errors.append(
            "Expected delivery before order"
        )

    if (
        actual_date >= order_date
    ).all():

        print(
            "[OK] Actual delivery dates valid"
        )

    else:

        errors.append(
            "Actual delivery before order"
        )

    # ========================================================
    # STATISTICS
    # ========================================================

    print("\nPURCHASE ORDER STATISTICS")
    print("-" * 70)

    po_count = (
        po["po_id"].nunique()
    )

    print(
        f"Average lines / PO: "
        f"{len(po) / po_count:.2f}"
    )

    late = (
        actual_date > expected_date
    )

    partial = (
        po["received_quantity"]
        <
        po["ordered_quantity"]
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

    # PO size distribution

    sizes = (
        po.groupby("po_id")
        .size()
        .value_counts()
        .sort_index()
    )

    print(
        "\nPO SIZE DISTRIBUTION"
    )

    for lines, count in sizes.items():

        print(
            f"{lines} line(s): "
            f"{count:,} POs"
        )

    # ========================================================
    # FINAL
    # ========================================================

    print("\n" + "=" * 70)

    if errors:

        print(
            f"FINAL PURCHASE ORDER "
            f"VALIDATION FAILED "
            f"({len(errors)} errors)"
        )

        print("=" * 70)

        for error in errors:
            print(
                f"[ERROR] {error}"
            )

        raise SystemExit(1)

    print(
        "FINAL PURCHASE ORDER "
        "DATASET VALIDATION PASSED"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
