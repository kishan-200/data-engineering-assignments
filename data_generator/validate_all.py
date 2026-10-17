import pandas as pd

from config import GENERATED_DIR


def ok(message):
    print(f"[OK] {message}")


def fail(errors, message):
    errors.append(message)
    print(f"[ERROR] {message}")


def main():

    print("=" * 65)
    print("OMNICHANNEL RETAIL DATASET - FINAL VALIDATION")
    print("=" * 65)

    # ---------------------------------------------------------
    # LOAD DATA
    # ---------------------------------------------------------

    stores = pd.read_csv(
        GENERATED_DIR / "stores.csv"
    )

    products = pd.read_csv(
        GENERATED_DIR / "products.csv"
    )

    suppliers = pd.read_csv(
        GENERATED_DIR / "suppliers.csv"
    )

    supplier_products = pd.read_csv(
        GENERATED_DIR / "supplier_products.csv"
    )

    inventory = pd.read_parquet(
        GENERATED_DIR / "opening_inventory.parquet"
    )

    sales = pd.read_parquet(
        GENERATED_DIR / "sales_dev.parquet"
    )

    purchase_orders = pd.read_parquet(
        GENERATED_DIR / "purchase_orders_dev.parquet"
    )

    errors = []

    # ---------------------------------------------------------
    # DATASET COUNTS
    # ---------------------------------------------------------

    print("\nDATASET COUNTS")
    print("-" * 65)

    print(f"Stores:                    {len(stores):,}")
    print(f"Products:                  {len(products):,}")
    print(f"Suppliers:                 {len(suppliers):,}")
    print(
        f"Supplier-product mappings: "
        f"{len(supplier_products):,}"
    )
    print(f"Inventory records:         {len(inventory):,}")
    print(f"Sales lines:               {len(sales):,}")
    print(
        f"Purchase-order lines:      "
        f"{len(purchase_orders):,}"
    )

    # ---------------------------------------------------------
    # MASTER KEYS
    # ---------------------------------------------------------

    print("\nMASTER KEY VALIDATION")
    print("-" * 65)

    if stores["store_id"].is_unique:
        ok("Store IDs unique")
    else:
        fail(errors, "Duplicate store IDs")

    if products["sku"].is_unique:
        ok("Product SKUs unique")
    else:
        fail(errors, "Duplicate product SKUs")

    if suppliers["supplier_id"].is_unique:
        ok("Supplier IDs unique")
    else:
        fail(errors, "Duplicate supplier IDs")

    # ---------------------------------------------------------
    # COMPOSITE KEYS
    # ---------------------------------------------------------

    inventory_duplicates = inventory.duplicated(
        ["store_id", "sku"]
    ).any()

    if not inventory_duplicates:
        ok("Inventory composite keys unique")
    else:
        fail(errors, "Duplicate inventory keys")

    supplier_mapping_duplicates = (
        supplier_products.duplicated(
            ["supplier_id", "sku"]
        ).any()
    )

    if not supplier_mapping_duplicates:
        ok("Supplier-product keys unique")
    else:
        fail(
            errors,
            "Duplicate supplier-product keys",
        )

    sales_duplicates = sales.duplicated(
        ["transaction_id", "line_id"]
    ).any()

    if not sales_duplicates:
        ok("Sales transaction-line keys unique")
    else:
        fail(errors, "Duplicate sales keys")

    po_duplicates = purchase_orders.duplicated(
        ["po_id", "po_line_id"]
    ).any()

    if not po_duplicates:
        ok("Purchase-order line keys unique")
    else:
        fail(errors, "Duplicate PO line keys")

    # ---------------------------------------------------------
    # FOREIGN KEYS
    # ---------------------------------------------------------

    print("\nREFERENTIAL INTEGRITY")
    print("-" * 65)

    store_ids = set(stores["store_id"])
    skus = set(products["sku"])
    supplier_ids = set(suppliers["supplier_id"])

    datasets_with_stores = {
        "Inventory": inventory,
        "Sales": sales,
        "Purchase Orders": purchase_orders,
    }

    for name, df in datasets_with_stores.items():

        invalid = (
            set(df["store_id"])
            - store_ids
        )

        if not invalid:
            ok(f"{name} -> Stores")
        else:
            fail(
                errors,
                f"{name} contains invalid stores",
            )

    datasets_with_products = {
        "Inventory": inventory,
        "Sales": sales,
        "Supplier Products": supplier_products,
        "Purchase Orders": purchase_orders,
    }

    for name, df in datasets_with_products.items():

        invalid = (
            set(df["sku"])
            - skus
        )

        if not invalid:
            ok(f"{name} -> Products")
        else:
            fail(
                errors,
                f"{name} contains invalid SKUs",
            )

    supplier_datasets = {
        "Supplier Products": supplier_products,
        "Purchase Orders": purchase_orders,
    }

    for name, df in supplier_datasets.items():

        invalid = (
            set(df["supplier_id"])
            - supplier_ids
        )

        if not invalid:
            ok(f"{name} -> Suppliers")
        else:
            fail(
                errors,
                f"{name} contains invalid suppliers",
            )

    # ---------------------------------------------------------
    # STORE/SKU RELATIONSHIPS
    # ---------------------------------------------------------

    print("\nSTORE / PRODUCT RELATIONSHIPS")
    print("-" * 65)

    valid_inventory_pairs = set(
        zip(
            inventory["store_id"],
            inventory["sku"],
        )
    )

    invalid_sales_pairs = sum(
        pair not in valid_inventory_pairs
        for pair in zip(
            sales["store_id"],
            sales["sku"],
        )
    )

    if invalid_sales_pairs == 0:
        ok("Every sale uses a SKU stocked by its store")
    else:
        fail(
            errors,
            f"{invalid_sales_pairs} invalid sale/store/SKU pairs",
        )

    invalid_po_pairs = sum(
        pair not in valid_inventory_pairs
        for pair in zip(
            purchase_orders["store_id"],
            purchase_orders["sku"],
        )
    )

    if invalid_po_pairs == 0:
        ok("Every PO SKU is stocked by its store")
    else:
        fail(
            errors,
            f"{invalid_po_pairs} invalid PO/store/SKU pairs",
        )

    # ---------------------------------------------------------
    # SUPPLIER / PRODUCT RELATIONSHIPS
    # ---------------------------------------------------------

    print("\nSUPPLIER / PRODUCT RELATIONSHIPS")
    print("-" * 65)

    valid_supplier_pairs = set(
        zip(
            supplier_products["supplier_id"],
            supplier_products["sku"],
        )
    )

    invalid_po_supplier_pairs = sum(
        pair not in valid_supplier_pairs
        for pair in zip(
            purchase_orders["supplier_id"],
            purchase_orders["sku"],
        )
    )

    if invalid_po_supplier_pairs == 0:
        ok("Every PO supplier supplies its SKU")
    else:
        fail(
            errors,
            f"{invalid_po_supplier_pairs} invalid supplier/SKU PO pairs",
        )

    preferred = supplier_products[
        supplier_products["preferred_supplier"] == True
    ]

    preferred_counts = preferred.groupby(
        "sku"
    ).size()

    valid_preferred = (
        len(preferred_counts) == len(products)
        and
        (preferred_counts == 1).all()
    )

    if valid_preferred:
        ok("Exactly one preferred supplier per product")
    else:
        fail(
            errors,
            "Preferred supplier rule violated",
        )

    # ---------------------------------------------------------
    # NUMERIC BUSINESS RULES
    # ---------------------------------------------------------

    print("\nBUSINESS RULES")
    print("-" * 65)

    if (products["selling_price"] > 0).all():
        ok("Product selling prices positive")
    else:
        fail(errors, "Invalid product prices")

    if (products["base_cost"] > 0).all():
        ok("Product base costs positive")
    else:
        fail(errors, "Invalid product costs")

    if (
        products["base_cost"]
        < products["selling_price"]
    ).all():
        ok("Product base cost below selling price")
    else:
        fail(
            errors,
            "Some base costs >= selling price",
        )

    if (inventory["opening_stock"] >= 0).all():
        ok("Opening stock non-negative")
    else:
        fail(errors, "Negative inventory found")

    if (
        inventory["reorder_level"]
        > inventory["safety_stock"]
    ).all():
        ok("Reorder level above safety stock")
    else:
        fail(
            errors,
            "Invalid reorder/safety relationship",
        )

    if (
        inventory["max_stock_level"]
        > inventory["reorder_level"]
    ).all():
        ok("Maximum stock above reorder level")
    else:
        fail(
            errors,
            "Invalid maximum stock level",
        )

    if (sales["quantity"] > 0).all():
        ok("Sales quantities positive")
    else:
        fail(errors, "Invalid sales quantities")

    if (
        purchase_orders["received_quantity"]
        <= purchase_orders["ordered_quantity"]
    ).all():
        ok("PO received quantity <= ordered quantity")
    else:
        fail(errors, "Invalid PO quantities")

    # ---------------------------------------------------------
    # DATA QUALITY STATISTICS
    # ---------------------------------------------------------

    print("\nDATA QUALITY STATISTICS")
    print("-" * 65)

    low_stock = (
        inventory["opening_stock"]
        < inventory["reorder_level"]
    )

    print(
        f"Low-stock inventory: "
        f"{low_stock.mean() * 100:.2f}%"
    )

    sales["transaction_timestamp"] = pd.to_datetime(
        sales["transaction_timestamp"]
    )

    print(
        "Historical sales range: "
        f"{sales['transaction_timestamp'].min()} "
        "to "
        f"{sales['transaction_timestamp'].max()}"
    )

    print(
        "Unique transactions: "
        f"{sales['transaction_id'].nunique():,}"
    )

    avg_basket = (
        len(sales)
        / sales["transaction_id"].nunique()
    )

    print(
        f"Average basket size: "
        f"{avg_basket:.2f} lines"
    )

    # ---------------------------------------------------------
    # FINAL RESULT
    # ---------------------------------------------------------

    print("\n" + "=" * 65)

    if errors:

        print(
            f"FINAL VALIDATION FAILED "
            f"({len(errors)} errors)"
        )

        print("=" * 65)

        for error in errors:
            print(f"[ERROR] {error}")

        raise SystemExit(1)

    else:

        print("FINAL DATASET VALIDATION PASSED")
        print("=" * 65)

        print(
            "All datasets are structurally and "
            "relationally consistent."
        )


if __name__ == "__main__":
    main()
