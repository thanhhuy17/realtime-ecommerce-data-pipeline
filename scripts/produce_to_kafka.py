import csv
import json
import sys
from pathlib import Path

from kafka import KafkaProducer


# Resolve paths relative to the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# CSV file, Kafka topic, primary key.
DATASETS = [
    ("customers.csv", "customers", "customer_id"),
    ("products.csv", "products", "product_id"),
    ("orders.csv", "orders", "order_id"),
    ("order_items.csv", "order_items", "order_item_id"),
    ("payments.csv", "payments", "payment_id"),
]


def convert_row(topic, row):
    """Convert selected CSV fields to appropriate JSON types."""

    if topic == "order_items":
        row["quantity"] = int(row["quantity"])

    elif topic == "products":
        value = row["is_active"].strip().lower()

        if value in ("true", "1", "yes"):
            row["is_active"] = True
        elif value in ("false", "0", "no"):
            row["is_active"] = False
        else:
            raise ValueError(f"Invalid is_active value: {value!r}")

    # Keep monetary values as decimal strings to avoid float precision loss.
    return row


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        acks="all",
        retries=5,
        linger_ms=10,
        max_block_ms=30000,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
    )

    total_sent = 0
    total_failed = 0

    try:
        for filename, topic, primary_key in DATASETS:
            file_path = DATA_DIR / filename

            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {file_path}")

            print(f"\nProcessing {filename} -> topic '{topic}'")

            futures = []
            rows_read = 0
            send_errors = 0

            with file_path.open("r", encoding="utf-8", newline="") as csv_file:
                reader = csv.DictReader(csv_file)

                if primary_key not in (reader.fieldnames or []):
                    raise ValueError(
                        f"Missing primary key '{primary_key}' in {filename}"
                    )

                for line_number, row in enumerate(reader, start=2):
                    rows_read += 1

                    try:
                        row = convert_row(topic, row)
                        message_key = row[primary_key]

                        if not message_key:
                            raise ValueError(
                                f"Empty primary key at line {line_number}"
                            )

                        future = producer.send(
                            topic,
                            key=message_key,
                            value=row,
                        )
                        futures.append(future)

                    except Exception as exc:
                        send_errors += 1
                        print(
                            f"ERROR: {filename}, line {line_number}: {exc}",
                            file=sys.stderr,
                        )

            # Wait until Kafka has processed the queued messages.
            producer.flush(timeout=60)

            sent = 0
            failed = send_errors

            for future in futures:
                try:
                    future.get(timeout=10)
                    sent += 1
                except Exception as exc:
                    failed += 1
                    print(
                        f"Kafka delivery failed for topic '{topic}': {exc}",
                        file=sys.stderr,
                    )

            total_sent += sent
            total_failed += failed

            print(
                f"Rows read: {rows_read} | "
                f"Sent: {sent} | Failed: {failed}"
            )

    finally:
        producer.close(timeout=30)

    print("\n========== PRODUCER SUMMARY ==========")
    print(f"Total messages sent: {total_sent}")
    print(f"Total messages failed: {total_failed}")

    if total_failed:
        sys.exit(1)

    print("PRODUCER COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
