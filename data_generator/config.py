from pathlib import Path

# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
GENERATED_DIR = DATA_DIR / "generated"

GENERATED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# RANDOM SEED
# ============================================================

RANDOM_SEED = 42


# ============================================================
# MASTER DATA SIZE
# ============================================================

NUM_PHYSICAL_STORES = 200
NUM_PRODUCTS = 5_000
NUM_SUPPLIERS = 100


# ============================================================
# SALES DATA SCALE
# ============================================================

DATA_MODE = "DEV"

SALES_SCALE = {
    "DEV": 10_000,
    "TEST": 100_000,
    "PERFORMANCE": 1_000_000,
    "FINAL": 5_000_000,
}

NUM_SALES_LINES = SALES_SCALE[DATA_MODE]


# ============================================================
# PROJECT TIME PERIOD
# ============================================================

HISTORY_START_DATE = "2024-01-01"
HISTORY_END_DATE = "2025-12-31"


# ============================================================
# BUSINESS CONFIGURATION
# ============================================================

REGIONS = [
    "North",
    "South",
    "East",
    "West",
    "Central",
]

STORE_TYPES = [
    "SMALL",
    "MEDIUM",
    "LARGE",
]

PRODUCT_CATEGORIES = {
    "Grocery": [
        "Rice",
        "Flour",
        "Pulses",
        "Cooking Oil",
        "Spices",
    ],
    "Dairy": [
        "Milk",
        "Curd",
        "Butter",
        "Cheese",
        "Paneer",
    ],
    "Beverages": [
        "Soft Drinks",
        "Juice",
        "Tea",
        "Coffee",
        "Water",
    ],
    "Snacks": [
        "Chips",
        "Biscuits",
        "Chocolate",
        "Namkeen",
        "Nuts",
    ],
    "Bakery": [
        "Bread",
        "Cake",
        "Cookies",
        "Buns",
        "Pastries",
    ],
    "Personal Care": [
        "Shampoo",
        "Soap",
        "Toothpaste",
        "Face Wash",
        "Deodorant",
    ],
    "Household": [
        "Detergent",
        "Dish Wash",
        "Floor Cleaner",
        "Tissues",
        "Garbage Bags",
    ],
    "Frozen": [
        "Ice Cream",
        "Frozen Snacks",
        "Frozen Vegetables",
        "Frozen Desserts",
        "Ready Meals",
    ],
    "Electronics": [
        "Earphones",
        "Chargers",
        "Power Banks",
        "Cables",
        "Accessories",
    ],
    "Home": [
        "Kitchenware",
        "Storage",
        "Cleaning Tools",
        "Home Decor",
        "Utility",
    ],
}
# ============================================================
# SALES GENERATION
# ============================================================

SALES_BATCH_SIZE = 100_000

PAYMENT_TYPES_PHYSICAL = [
    "UPI",
    "CARD",
    "CASH",
    "WALLET",
]

PAYMENT_TYPES_ONLINE = [
    "UPI",
    "CARD",
    "WALLET",
    "NET_BANKING",
    "COD",
]
# ============================================================
# PURCHASE ORDER SCALE
# ============================================================

PO_SCALE = {
    "DEV": 10_000,
    "TEST": 25_000,
    "PERFORMANCE": 75_000,
    "FINAL": 150_000,
}

NUM_PO_LINES = PO_SCALE[DATA_MODE]
