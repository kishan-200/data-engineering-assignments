import random
import pandas as pd

from config import (
    GENERATED_DIR,
    NUM_SUPPLIERS,
    RANDOM_SEED,
)


random.seed(RANDOM_SEED)


REGION_LOCATIONS = {
    "North": [
        ("Delhi", "Delhi"),
        ("Jaipur", "Rajasthan"),
        ("Lucknow", "Uttar Pradesh"),
        ("Gurugram", "Haryana"),
    ],
    "South": [
        ("Hyderabad", "Telangana"),
        ("Bengaluru", "Karnataka"),
        ("Chennai", "Tamil Nadu"),
        ("Kochi", "Kerala"),
    ],
    "East": [
        ("Kolkata", "West Bengal"),
        ("Bhubaneswar", "Odisha"),
        ("Patna", "Bihar"),
        ("Guwahati", "Assam"),
    ],
    "West": [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Ahmedabad", "Gujarat"),
        ("Surat", "Gujarat"),
    ],
    "Central": [
        ("Bhopal", "Madhya Pradesh"),
        ("Indore", "Madhya Pradesh"),
        ("Raipur", "Chhattisgarh"),
        ("Nagpur", "Maharashtra"),
    ],
}


SUPPLIER_PREFIXES = [
    "Fresh",
    "Prime",
    "National",
    "Reliable",
    "Metro",
    "United",
    "Smart",
    "Rapid",
    "Quality",
    "Evergreen",
]


SUPPLIER_SUFFIXES = [
    "Distributors",
    "Supply Co",
    "Trading",
    "Wholesale",
    "Enterprises",
    "Logistics",
    "Supplies",
]


def generate_suppliers():

    suppliers = []

    regions = list(REGION_LOCATIONS.keys())

    for i in range(1, NUM_SUPPLIERS + 1):

        region = random.choice(regions)

        city, state = random.choice(
            REGION_LOCATIONS[region]
        )

        supplier_name = (
            f"{random.choice(SUPPLIER_PREFIXES)} "
            f"{random.choice(SUPPLIER_SUFFIXES)} "
            f"{i:03d}"
        )

        # Most suppliers deliver within 2–8 days.
        # A few may take slightly longer.
        lead_time = random.choices(
            population=[2, 3, 4, 5, 6, 7, 8, 10],
            weights=[8, 14, 20, 20, 15, 10, 8, 5],
            k=1,
        )[0]

        # Supplier reliability between 82% and 99%.
        reliability = round(
            random.uniform(0.82, 0.99),
            3,
        )

        minimum_order_value = random.choice(
            [
                2000,
                3000,
                5000,
                7500,
                10000,
                15000,
            ]
        )

        suppliers.append(
            {
                "supplier_id": f"SUP{i:03d}",
                "supplier_name": supplier_name,
                "city": city,
                "state": state,
                "region": region,
                "default_lead_time_days": lead_time,
                "reliability_score": reliability,
                "minimum_order_value": minimum_order_value,
                "active": True,
            }
        )

    df = pd.DataFrame(suppliers)

    output_file = GENERATED_DIR / "suppliers.csv"

    df.to_csv(
        output_file,
        index=False,
    )

    print(f"Generated {len(df):,} suppliers")
    print(f"Saved to: {output_file}")

    print("\nRegion distribution:")
    print(df["region"].value_counts())

    print("\nLead time summary:")
    print(
        df["default_lead_time_days"]
        .describe()
        .round(2)
    )

    print("\nReliability summary:")
    print(
        df["reliability_score"]
        .describe()
        .round(3)
    )

    return df


if __name__ == "__main__":
    generate_suppliers()
