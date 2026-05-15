"""
Generate synthetic sample data for local development.

Produces:
    data/sample/orders.csv     (~500 rows)
    data/sample/customers.csv  (~100 rows)

The generator is deterministic (seeded) so tests and demos are repeatable.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

OUT_DIR = Path(__file__).resolve().parent

FIRST_NAMES = [
    "Alex", "Sam", "Jordan", "Taylor", "Casey", "Riley", "Avery", "Quinn",
    "Morgan", "Drew", "Skyler", "Reese", "Cameron", "Hayden", "Parker",
]
LAST_NAMES = [
    "Lee", "Patel", "Garcia", "Smith", "Khan", "Nguyen", "Brown", "Wilson",
    "Cohen", "Singh", "Martinez", "Davis", "Rao", "Silva", "Park",
]
COUNTRIES = ["US", "IN", "BR", "DE", "GB", "CA", "AU", "JP"]
PRODUCTS = [f"PROD-{i:03d}" for i in range(100, 130)]
STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_METHODS = ["credit_card", "PayPal", "Debit_Card", "bank_transfer"]

NUM_CUSTOMERS = 100
NUM_ORDERS = 500
START_DATE = datetime(2024, 1, 1)


def generate_customers() -> list[dict]:
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        signup = START_DATE + timedelta(days=random.randint(0, 30))
        ingest = signup + timedelta(hours=random.randint(0, 48))
        customers.append(
            {
                "customer_id": f"CUST-{1000 + i}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{i}@example.com",
                "country": random.choice(COUNTRIES),
                "signup_date": signup.strftime("%Y-%m-%d"),
                "_ingestion_timestamp": ingest.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return customers


def generate_orders(customer_ids: list[str]) -> list[dict]:
    orders = []
    for i in range(1, NUM_ORDERS + 1):
        order_dt = START_DATE + timedelta(
            days=random.randint(0, 90), hours=random.randint(0, 23)
        )
        ingest = order_dt + timedelta(minutes=random.randint(5, 240))
        qty = random.randint(1, 5)
        price = round(random.uniform(9.99, 499.99), 2)
        discount = round(random.choice([0, 0, 0, 5, 10, 25]), 2)
        orders.append(
            {
                "order_id": f"ORD-{i:05d}",
                "customer_id": random.choice(customer_ids),
                "product_id": random.choice(PRODUCTS),
                "order_date": order_dt.strftime("%Y-%m-%d"),
                "order_timestamp": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity": qty,
                "unit_price": price,
                "discount_amount": discount,
                "order_status": random.choice(STATUSES),
                "payment_method": random.choice(PAYMENT_METHODS),
                "_ingestion_timestamp": ingest.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return orders


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


def main() -> None:
    customers = generate_customers()
    orders = generate_orders([c["customer_id"] for c in customers])
    write_csv(OUT_DIR / "customers.csv", customers)
    write_csv(OUT_DIR / "orders.csv", orders)


if __name__ == "__main__":
    main()
