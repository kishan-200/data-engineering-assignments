import pandas as pd

from config import GENERATED_DIR


def validate():

    products = pd.read_csv(
        GENERATED_DIR / "products.csv"
    )

    suppliers = pd.read_csv(
        GENERATED_DIR / "suppliers.csv"
    )

    mappings = pd.read_csv(
        GENERATED_DIR / "supplier_products.csv"
    )

    errors = []

    # --------------------------------------------------------
    # 1. Duplicate supplier IDs
    # --------------------------------------------------------

    if suppliers["supplier_id"].duplicated().any():
        errors.append(
            "Duplicate supplier_id found"
        )

    # --------------------------------------------------------
    # 2. Duplicate supplier-product relationships
    # --------------------------------------------------------

    duplicate_mapping = mappings.duplicated(
        subset=["supplier_id", "sku"]
    ).any()

    if duplicate_mapping:
        errors.append(
            "Duplicate supplier-product relationship found"
        )

    # --------------------------------------------------------
    # 3. Every mapping SKU must exist
    # --------------------------------------------------------

    invalid_skus = (
        set(mappings["sku"])
        - set(products["sku"])
    )

    if invalid_skus:
        errors.append(
            f"{len(invalid_skus)} invalid SKU references"
        )

    # --------------------------------------------------------
    # 4. Every mapping supplier must exist
    # --------------------------------------------------------

    invalid_suppliers = (
        set(mappings["supplier_id"])
        - set(suppliers["supplier_id"])
    )

    if invalid_suppliers:
        errors.append(
            f"{len(invalid_suppliers)} invalid supplier references"
        )

    # --------------------------------------------------------
    # 5. Every product must have at least one supplier
    # --------------------------------------------------------

    products_with_supplier = set(
        mappings["sku"]
    )

    missing_supplier = (
        set(products["sku"])
        - products_with_supplier
    )

    if missing_supplier:
        errors.append(
            f"{len(missing_supplier)} products have no supplier"
        )

    # --------------------------------------------------------
    # 6. Exactly one preferred supplier per SKU
    # --------------------------------------------------------

    preferred = mappings[
        mappings["preferred_supplier"] == True
    ]

    preferred_counts = (
        preferred.groupby("sku")
        .size()
    )

    invalid_preferred = preferred_counts[
        preferred_counts != 1
    ]

    missing_preferred = (
        set(products["sku"])
        - set(preferred_counts.index)
    )

    if len(invalid_preferred) > 0:
        errors.append(
            "Some products have multiple preferred suppliers"
        )

    if missing_preferred:
        errors.append(
            f"{len(missing_preferred)} products "
            "have no preferred supplier"
        )

    # --------------------------------------------------------
    # 7. Supplier cost must be positive
    # --------------------------------------------------------

    if (mappings["supplier_cost"] <= 0).any():
        errors.append(
            "Invalid supplier cost found"
        )

    # --------------------------------------------------------
    # 8. Lead time must be positive
    # --------------------------------------------------------

    if (mappings["lead_time_days"] <= 0).any():
        errors.append(
            "Invalid lead time found"
        )

    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

    print("=" * 55)
    print("SUPPLIER DATA VALIDATION")
    print("=" * 55)

    print(
        f"Products:              {len(products):,}"
    )

    print(
        f"Suppliers:             {len(suppliers):,}"
    )

    print(
        "Supplier relationships:"
        f" {len(mappings):,}"
    )

    print(
        "Products with supplier:"
        f" {mappings['sku'].nunique():,}"
    )

    print(
        "Preferred suppliers:   "
        f"{len(preferred):,}"
    )

    print("-" * 55)

    if errors:

        print("VALIDATION FAILED")

        for error in errors:
            print(f"[ERROR] {error}")

    else:

        print("ALL VALIDATION CHECKS PASSED")

        print("[OK] Supplier IDs unique")
        print("[OK] Mapping keys unique")
        print("[OK] All SKU references valid")
        print("[OK] All supplier references valid")
        print("[OK] Every product has a supplier")
        print(
            "[OK] Exactly one preferred supplier per product"
        )
        print("[OK] Supplier costs valid")
        print("[OK] Lead times valid")


if __name__ == "__main__":
    validate()
