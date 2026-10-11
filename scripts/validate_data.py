
import csv
import sys
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data" / "raw"

EXPECTED_COUNTS = {
    "customers": 1_000,
    "products": 100,
    "orders": 10_000,
    "payments": 10_000,
}

REQUIRED_COLUMNS = {
    "customers": [
        "customer_id", "full_name", "email", "city", "created_at"
    ],
    "products": [
        "product_id", "product_name", "category", "price",
        "is_active", "created_at"
    ],
    "orders": [
        "order_id", "customer_id", "order_date",
        "status", "total_amount"
    ],
    "order_items": [
        "order_item_id", "order_id", "product_id",
        "quantity", "unit_price"
    ],
    "payments": [
        "payment_id", "order_id", "payment_method",
        "amount", "status", "payment_date"
    ],
}

ORDER_STATUSES = {
    "pending", "confirmed", "shipped", "delivered", "cancelled"
}

PAYMENT_STATUSES = {
    "pending", "success", "failed", "refunded"
}

PAYMENT_METHODS = {
    "credit_card", "debit_card", "e_wallet", "bank_transfer"
}

errors = []
warnings = []


def check(condition, message):
    """Record a failed data quality check."""
    if condition:
        print(f"  PASS: {message}")
    else:
        print(f"  FAIL: {message}")
        errors.append(message)


def load_csv(table_name):
    path = DATA_DIR / f"{table_name}.csv"

    if not path.exists():
        errors.append(f"{table_name}.csv does not exist")
        print(f"  FAIL: Missing file {path}")
        return []

    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            errors.append(f"{table_name}.csv has no header")
            return []

        missing_columns = (
            set(REQUIRED_COLUMNS[table_name]) - set(reader.fieldnames)
        )

        if missing_columns:
            errors.append(
                f"{table_name}.csv missing columns: "
                f"{sorted(missing_columns)}"
            )
            print(f"  FAIL: Missing columns {sorted(missing_columns)}")
            return []

        rows = list(reader)

    print(f"Loaded {table_name}: {len(rows):,} rows")
    return rows


def parse_decimal(value, description):
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        errors.append(f"Invalid numeric value: {description}={value!r}")
        return None


def check_required_fields(table_name, rows):
    required = REQUIRED_COLUMNS[table_name]
    missing = []

    for row_number, row in enumerate(rows, start=2):
        for column in required:
            if not row.get(column) or not row[column].strip():
                missing.append(f"row {row_number}, column {column}")

    check(
        not missing,
        f"{table_name}: required fields are populated"
        + (f" ({len(missing)} missing values)" if missing else "")
    )


def check_unique(rows, column, table_name):
    values = [row[column] for row in rows]
    duplicates = [
        value for value, count in Counter(values).items() if count > 1
    ]

    check(
        not duplicates,
        f"{table_name}.{column} is unique"
        + (f" ({len(duplicates)} duplicate values)" if duplicates else "")
    )


def check_foreign_key(child_rows, child_column, parent_ids, description):
    invalid = [
        row[child_column]
        for row in child_rows
        if row[child_column] not in parent_ids
    ]

    check(
        not invalid,
        f"{description}: all foreign keys exist"
        + (f" ({len(invalid)} invalid references)" if invalid else "")
    )


def main():
    print("\n=== 1. Load CSV files ===")

    data = {
        table: load_csv(table)
        for table in REQUIRED_COLUMNS
    }

    if errors:
        print("\nValidation stopped because required files or columns are missing.")
        return False

    customers = data["customers"]
    products = data["products"]
    orders = data["orders"]
    items = data["order_items"]
    payments = data["payments"]

    print("\n=== 2. Row counts ===")

    for table, expected in EXPECTED_COUNTS.items():
        check(
            len(data[table]) == expected,
            f"{table}: expected {expected:,}, got {len(data[table]):,}"
        )

    check(
        len(items) > 0,
        f"order_items: contains records ({len(items):,})"
    )

    print("\n=== 3. Required fields and uniqueness ===")

    for table, rows in data.items():
        check_required_fields(table, rows)

    for table, column in [
        ("customers", "customer_id"),
        ("customers", "email"),
        ("products", "product_id"),
        ("orders", "order_id"),
        ("order_items", "order_item_id"),
        ("payments", "payment_id"),
    ]:
        check_unique(data[table], column, table)

    print("\n=== 4. Foreign key integrity ===")

    customer_ids = {row["customer_id"] for row in customers}
    product_ids = {row["product_id"] for row in products}
    order_ids = {row["order_id"] for row in orders}

    check_foreign_key(
        orders, "customer_id", customer_ids, "orders.customer_id"
    )
    check_foreign_key(
        items, "order_id", order_ids, "order_items.order_id"
    )
    check_foreign_key(
        items, "product_id", product_ids, "order_items.product_id"
    )
    check_foreign_key(
        payments, "order_id", order_ids, "payments.order_id"
    )

    print("\n=== 5. Numeric values and business rules ===")

    numeric_errors = []

    for row in products:
        price = parse_decimal(row["price"], "products.price")
        if price is not None and price < 0:
            numeric_errors.append("Negative product price")

    for row in items:
        quantity = row["quantity"]
        unit_price = parse_decimal(
            row["unit_price"], "order_items.unit_price"
        )

        try:
            if int(quantity) <= 0 or str(int(quantity)) != quantity:
                numeric_errors.append("Invalid item quantity")
        except ValueError:
            numeric_errors.append("Non-integer item quantity")

        if unit_price is not None and unit_price < 0:
            numeric_errors.append("Negative item price")

    for row in orders:
        amount = parse_decimal(
            row["total_amount"], "orders.total_amount"
        )

        if amount is not None and amount < 0:
            numeric_errors.append("Negative order total")

        if row["status"] not in ORDER_STATUSES:
            numeric_errors.append("Invalid order status")

    for row in payments:
        amount = parse_decimal(row["amount"], "payments.amount")

        if amount is not None and amount <= 0:
            numeric_errors.append("Non-positive payment amount")

        if row["status"] not in PAYMENT_STATUSES:
            numeric_errors.append("Invalid payment status")

        if row["payment_method"] not in PAYMENT_METHODS:
            numeric_errors.append("Invalid payment method")

    check(
        not numeric_errors,
        "Prices, quantities, order statuses and payment values are valid"
        + (
            f" ({len(numeric_errors)} violations)"
            if numeric_errors else ""
        )
    )

    print("\n=== 6. Order totals ===")

    item_totals = defaultdict(lambda: Decimal("0.00"))

    for row in items:
        try:
            quantity = int(row["quantity"])
            unit_price = Decimal(row["unit_price"])
            item_totals[row["order_id"]] += quantity * unit_price
        except (ValueError, InvalidOperation):
            continue

    total_mismatches = []

    for row in orders:
        try:
            declared_total = Decimal(row["total_amount"])
            calculated_total = item_totals[row["order_id"]]

            if declared_total != calculated_total:
                total_mismatches.append(row["order_id"])
        except (InvalidOperation, KeyError):
            total_mismatches.append(row["order_id"])

    check(
        not total_mismatches,
        "Order totals match the sum of their order items"
        + (
            f" ({len(total_mismatches)} mismatches)"
            if total_mismatches else ""
        )
    )

    orders_without_items = order_ids - set(item_totals.keys())

    check(
        not orders_without_items,
        "Every order contains at least one order item"
    )

    print("\n=== 7. Payment consistency ===")

    payment_counts = Counter(row["order_id"] for row in payments)

    incorrect_payment_counts = [
        order_id for order_id in order_ids
        if payment_counts[order_id] != 1
    ]

    check(
        not incorrect_payment_counts,
        "Every order has exactly one payment"
        + (
            f" ({len(incorrect_payment_counts)} violations)"
            if incorrect_payment_counts else ""
        )
    )

    payment_by_order = {
        row["order_id"]: row for row in payments
    }

    payment_mismatches = []

    for order in orders:
        payment = payment_by_order.get(order["order_id"])

        if payment is None:
            continue

        try:
            if Decimal(payment["amount"]) != Decimal(order["total_amount"]):
                payment_mismatches.append(order["order_id"])
        except InvalidOperation:
            payment_mismatches.append(order["order_id"])

    check(
        not payment_mismatches,
        "Payment amounts match order totals"
        + (
            f" ({len(payment_mismatches)} mismatches)"
            if payment_mismatches else ""
        )
    )

    print("\n=== Final result ===")

    if errors:
        print(f"VALIDATION FAILED: {len(errors)} issue(s) found.")
        return False

    print("VALIDATION PASSED: All checks passed!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
