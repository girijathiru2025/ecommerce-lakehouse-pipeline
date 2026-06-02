"""
Run locally to generate sample e-commerce CSVs, then upload to DBFS.
Output: data/customers.csv, data/products.csv, data/orders.csv, data/order_items.csv

Upload command (Databricks CLI):
  databricks fs cp -r data/ dbfs:/FileStore/ecommerce/raw/
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 2_000
NUM_PRODUCTS  = 200
NUM_ORDERS    = 15_000
START_DATE    = datetime(2024, 1, 1)
END_DATE      = datetime(2024, 12, 31)

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen", "Sports", "Beauty", "Toys", "Automotive"]
STATUSES   = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
STATUS_WEIGHTS = [0.05, 0.10, 0.15, 0.55, 0.10, 0.05]

CITIES = [
    ("New York", "NY", "USA"), ("Los Angeles", "CA", "USA"), ("Chicago", "IL", "USA"),
    ("Houston", "TX", "USA"), ("Phoenix", "AZ", "USA"), ("Philadelphia", "PA", "USA"),
    ("San Antonio", "TX", "USA"), ("San Diego", "CA", "USA"), ("Dallas", "TX", "USA"),
    ("San Jose", "CA", "USA"), ("Austin", "TX", "USA"), ("Seattle", "WA", "USA"),
    ("Denver", "CO", "USA"), ("Boston", "MA", "USA"), ("Nashville", "TN", "USA"),
]

FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
               "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
               "Thomas","Karen","Charles","Lisa","Emma","Noah","Olivia","Liam","Ava"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
               "Wilson","Anderson","Taylor","Thomas","Jackson","White","Harris","Martin"]


def rand_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                             hours=random.randint(0, 23),
                             minutes=random.randint(0, 59))


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Customers ─────────────────────────────────────────────────────────────────
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    city, state, country = random.choice(CITIES)
    first = random.choice(FIRST_NAMES)
    last  = random.choice(LAST_NAMES)
    customers.append({
        "customer_id":  f"CUST{i:05d}",
        "first_name":   first,
        "last_name":    last,
        "email":        f"{first.lower()}.{last.lower()}{i}@email.com",
        "city":         city,
        "state":        state,
        "country":      country,
        "signup_date":  fmt(rand_date(datetime(2020, 1, 1), START_DATE)),
        "is_premium":   random.choice([True, False]),
    })

with open(os.path.join(OUTPUT_DIR, "customers.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)
print(f"customers.csv   → {len(customers):,} rows")


# ── Products ──────────────────────────────────────────────────────────────────
products = []
for i in range(1, NUM_PRODUCTS + 1):
    cat   = random.choice(CATEGORIES)
    price = round(random.uniform(5.99, 499.99), 2)
    products.append({
        "product_id":   f"PROD{i:04d}",
        "product_name": f"{cat} Item {i}",
        "category":     cat,
        "unit_price":   price,
        "cost_price":   round(price * random.uniform(0.35, 0.65), 2),
        "stock_qty":    random.randint(0, 500),
        "is_active":    random.choices([True, False], weights=[0.9, 0.1])[0],
    })

with open(os.path.join(OUTPUT_DIR, "products.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=products[0].keys())
    writer.writeheader()
    writer.writerows(products)
print(f"products.csv    → {len(products):,} rows")


# ── Orders + Order Items ──────────────────────────────────────────────────────
orders        = []
order_items   = []
item_seq      = 1
active_prods  = [p for p in products if p["is_active"]]

for i in range(1, NUM_ORDERS + 1):
    order_date = rand_date(START_DATE, END_DATE)
    customer   = random.choice(customers)
    status     = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    num_items  = random.randint(1, 6)

    lines = []
    for _ in range(num_items):
        prod     = random.choice(active_prods)
        qty      = random.randint(1, 4)
        price    = prod["unit_price"]
        discount = round(random.uniform(0, 0.20), 2) if random.random() < 0.3 else 0.0
        lines.append({
            "item_id":      f"ITEM{item_seq:07d}",
            "order_id":     f"ORD{i:06d}",
            "product_id":   prod["product_id"],
            "quantity":     qty,
            "unit_price":   price,
            "discount_pct": discount,
            "line_total":   round(qty * price * (1 - discount), 2),
        })
        item_seq += 1

    subtotal = round(sum(l["line_total"] for l in lines), 2)
    shipping = round(random.uniform(0, 15.99), 2) if subtotal < 50 else 0.0
    tax      = round(subtotal * 0.08, 2)

    orders.append({
        "order_id":       f"ORD{i:06d}",
        "customer_id":    customer["customer_id"],
        "order_date":     fmt(order_date),
        "status":         status,
        "subtotal":       subtotal,
        "shipping_cost":  shipping,
        "tax_amount":     tax,
        "order_total":    round(subtotal + shipping + tax, 2),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
        "channel":        random.choice(["web", "mobile", "app"]),
    })
    order_items.extend(lines)

with open(os.path.join(OUTPUT_DIR, "orders.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)
print(f"orders.csv      → {len(orders):,} rows")

with open(os.path.join(OUTPUT_DIR, "order_items.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=order_items[0].keys())
    writer.writeheader()
    writer.writerows(order_items)
print(f"order_items.csv → {len(order_items):,} rows")

print("\nDone. Upload with:")
print("  databricks fs cp -r data/ dbfs:/FileStore/ecommerce/raw/")
