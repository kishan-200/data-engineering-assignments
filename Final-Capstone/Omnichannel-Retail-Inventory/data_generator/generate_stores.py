import random
import pandas as pd
from faker import Faker

from config import (
    GENERATED_DIR,
    NUM_PHYSICAL_STORES,
    RANDOM_SEED,
    REGIONS,
)


random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)

fake = Faker("en_IN")


REGION_LOCATIONS = {
    "North": [
        ("Delhi", "Delhi"),
        ("Jaipur", "Rajasthan"),
        ("Lucknow", "Uttar Pradesh"),
        ("Chandigarh", "Chandigarh"),
        ("Gurugram", "Haryana"),
    ],
    "South": [
        ("Hyderabad", "Telangana"),
        ("Bengaluru", "Karnataka"),
        ("Chennai", "Tamil Nadu"),
        ("Kochi", "Kerala"),
        ("Visakhapatnam", "Andhra Pradesh"),
    ],
    "East": [
        ("Kolkata", "West Bengal"),
        ("Bhubaneswar", "Odisha"),
        ("Patna", "Bihar"),
        ("Guwahati", "Assam"),
        ("Ranchi", "Jharkhand"),
    ],
    "West": [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Ahmedabad", "Gujarat"),
        ("Surat", "Gujarat"),
        ("Goa", "Goa"),
    ],
    "Central": [
        ("Bhopal", "Madhya Pradesh"),
        ("Indore", "Madhya Pradesh"),
        ("Raipur", "Chhattisgarh"),
        ("Nagpur", "Maharashtra"),
        ("Jabalpur", "Madhya Pradesh"),
    ],
}


def choose_store_type():
    return random.choices(
        population=["SMALL", "MEDIUM", "LARGE"],
        weights=[35, 45, 20],
        k=1,
    )[0]


def generate_stores():
    stores = []

    for i in range(1, NUM_PHYSICAL_STORES + 1):

        region = random.choice(REGIONS)

        city, state = random.choice(
            REGION_LOCATIONS[region]
        )

        store_type = choose_store_type()

        opening_date = fake.date_between(
            start_date="-12y",
            end_date="-1y",
        )

        stores.append(
            {
                "store_id": f"S{i:03d}",
                "store_name": f"{city} Store {i:03d}",
                "city": city,
                "state": state,
                "region": region,
                "store_type": store_type,
                "channel": "PHYSICAL",
                "opening_date": opening_date,
                "active": True,
            }
        )

    # Add e-commerce channel
    stores.append(
        {
            "store_id": "ECOM001",
            "store_name": "Online Store",
            "city": "Online",
            "state": "NA",
            "region": "National",
            "store_type": "ONLINE",
            "channel": "ONLINE",
            "opening_date": pd.Timestamp("2020-01-01").date(),
            "active": True,
        }
    )

    df = pd.DataFrame(stores)

    output_file = GENERATED_DIR / "stores.csv"

    df.to_csv(
        output_file,
        index=False,
    )

    print(f"Generated {len(df):,} stores")
    print(f"Saved to: {output_file}")

    print("\nStore type distribution:")
    print(df["store_type"].value_counts())

    print("\nRegion distribution:")
    print(df["region"].value_counts())

    return df


if __name__ == "__main__":
    generate_stores()
