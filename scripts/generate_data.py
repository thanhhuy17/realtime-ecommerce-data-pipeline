
import csv
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

SEED = 42
NUM_CUSTOMERS = 1_000
NUM_PRODUCTS = 100
NUM_ORDERS = 10_000

random.seed(SEED)

ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT_DIR / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def money(value):
    """Return a monetary value rounded to 2 decimal places."""
    return Decimal(str(value)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )

def write_csv(filename, rows):
    if not rows:
        raise ValueError(f"{filename} has no rows")

    path = OUTPUT_DIR / filename

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created {path.name}: {len(rows):,} rows")

def random_timestamp(start, end):
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))

def generate_data():
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365)

    # 1. Customers
    cities = [
        "Ho Chi Minh City",
        "Hanoi",
        "Da Nang",
        "Can Tho",
        "Dong Nai",
        "Hai Phong",
    ]

    customers = []

    for customer_id in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id": customer_id,
            "full_name": f"Customer {customer_id:04d}",
            "email": f"customer{customer_id:04d}@example.com",
            "city": random.choice(cities),
            "created_at": random_timestamp(
                start - timedelta(days=365), now
            ).isoformat(),
        })

    # 2. Products
    categories = [
        "Electronics",
        "Fashion",
        "Home",
        "Beauty",
        "Sports",
    ]

    products = []

    for product_id in range(1, NUM_PRODUCTS + 1):
        products.append({
            "product_id": product_id,
            "product_name": f"Product {product_id:03d}",
            "category": random.choice(categories),
            "price": str(money(random.uniform(10, 2000))),
            "is_active": random.choice([True, True, True, False]),
            "created_at": random_timestamp(start, now).isoformat(),
        })

    # 3. Orders and order items
    orders = []
    order_items = []
    payments = []

    order_statuses = [
        "pending",
        "confirmed",
        "shipped",
        "delivered",
        "cancelled",
    ]

    payment_methods = [
        "credit_card",
        "debit_card",
        "e_wallet",
        "bank_transfer",
    ]

    payment_statuses = [
        "pending",
        "success",
        "failed",
        "refunded",
    ]

    item_id = 1

    for order_id in range(1, NUM_ORDERS + 1):
        customer = random.choice(customers)
        order_date = random_timestamp(start, now)
        order_status = random.choice(order_statuses)

        # Each order contains 1-5 distinct products.
        item_count = random.randint(1, 5)
        selected_products = random.sample(products, item_count)

        order_total = Decimal("0.00")

        for product in selected_products:
            quantity = random.randint(1, 4)
            unit_price = money(product["price"])

            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": str(unit_price),
            })

            order_total += unit_price * quantity
            item_id += 1

        order_total = money(order_total)

        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(),
            "status": order_status,
            "total_amount": str(order_total),
        })

        # One payment record per order for this project version.
        if order_status == "cancelled":
            payment_status = "refunded"
        else:
            payment_status = random.choice(payment_statuses)

        payments.append({
            "payment_id": order_id,
            "order_id": order_id,
            "payment_method": random.choice(payment_methods),
            "amount": str(order_total),
            "status": payment_status,
            "payment_date": random_timestamp(
                order_date, now
            ).isoformat(),
        })

    # 4. Export CSV files
    write_csv("customers.csv", customers)
    write_csv("products.csv", products)
    write_csv("orders.csv", orders)
    write_csv("order_items.csv", order_items)
    write_csv("payments.csv", payments)

    print("\nData generation completed!")

if __name__ == "__main__":
    generate_data()
