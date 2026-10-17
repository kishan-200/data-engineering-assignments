import json
import time
import hashlib
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]

UCI_FILE = PROJECT_ROOT / "data" / "external" / "uci" / "Online Retail.xlsx"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "pos_sales"

MAX_EVENTS = 1000
EVENTS_PER_SECOND = 5


def normalize_sku(stock_code):
    value = str(stock_code).strip()

    # Convert UCI StockCode into our project SKU-like format.
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()
    numeric = int(digest[:8], 16) % 5000 + 1

    return f"SKU{numeric:05d}"


def choose_store(invoice_no):
    digest = hashlib.md5(str(invoice_no).encode("utf-8")).hexdigest()
    store_num = int(digest[:6], 16) % 200 + 1
    return f"S{store_num:03d}"


def main():
    print("Reading UCI Online Retail dataset...")
    print("File:", UCI_FILE)

    df = pd.read_excel(
        UCI_FILE,
        engine="openpyxl"
    )

    print("Original rows:", len(df))

    # Keep valid retail sales only.
    df = df.dropna(
        subset=[
            "InvoiceNo",
            "StockCode",
            "Quantity",
            "InvoiceDate",
            "UnitPrice"
        ]
    )

    df = df[
        (df["Quantity"] > 0) &
        (df["UnitPrice"] > 0)
    ].copy()

    # Exclude cancelled invoices.
    df = df[
        ~df["InvoiceNo"].astype(str).str.startswith("C")
    ]

    df = df.head(MAX_EVENTS).reset_index(drop=True)

    print("Valid rows selected for replay:", len(df))

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=5
    )

    sent = 0

    for index, row in df.iterrows():
        invoice_no = str(row["InvoiceNo"])
        sku = normalize_sku(row["StockCode"])
        store_id = choose_store(invoice_no)

        event = {
            "transaction_id": f"UCI-{invoice_no}",
            "line_id": index + 1,
            "store_id": store_id,
            "sku": sku,
            "quantity": int(row["Quantity"]),
            "unit_price": round(float(row["UnitPrice"]), 2),
            "discount_pct": 0.0,
            "transaction_timestamp": pd.Timestamp(
                row["InvoiceDate"]
            ).isoformat(),
            "channel": "STORE",
            "payment_type": "UNKNOWN",
            "replay_source": "UCI_Online_Retail",
            "original_stock_code": str(row["StockCode"])
        }

        producer.send(
            TOPIC,
            key=store_id,
            value=event
        )

        sent += 1

        if sent % 100 == 0:
            print(f"Sent {sent}/{len(df)} events")

        time.sleep(1 / EVENTS_PER_SECOND)

    producer.flush()

    print()
    print("UCI REPLAY COMPLETE")
    print("Topic:", TOPIC)
    print("Events sent:", sent)
    print("Rate:", EVENTS_PER_SECOND, "events/sec")

    producer.close()


if __name__ == "__main__":
    main()
