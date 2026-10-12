# Real-Time E-commerce Data Pipeline

A hands-on Data Engineering portfolio project built with Python, Apache Kafka, Apache Spark Structured Streaming, PostgreSQL, and Docker.

The project simulates an e-commerce data platform and demonstrates how to ingest streaming events, validate and transform data, load records into PostgreSQL, and perform SQL analytics.

## Overview

This project implements a Kafka-based data pipeline for five e-commerce entities: customers, products, orders, order items, and payments.

A Python producer reads synthetic datasets from CSV files and publishes JSON messages to Kafka topics. Spark Structured Streaming consumes the messages, parses JSON payloads, validates data, and writes micro-batches into PostgreSQL staging tables. PostgreSQL UPSERT operations then insert new records or update existing records.

SQL queries are used to verify referential integrity, reconcile order totals with order-item details, and generate basic business analytics.

## Architecture

```text
Synthetic CSV Data
        |
        v
Python Kafka Producer
        |
        v
Apache Kafka Topics
  |      |      |      |       |
  v      v      v      v       v
Customers Products Orders Order Items Payments
  |      |      |      |       |
  +------+------+------+-------+
                 |
                 v
     Spark Structured Streaming
                 |
                 v
       JSON Parsing & Validation
                 |
                 v
      PostgreSQL Staging Tables
                 |
                 v
          SQL UPSERT Operations
                 |
                 v
       PostgreSQL Target Tables
                 |
                 v
       SQL Analytics & Data Quality

Docker: Local development infrastructure
Apache Airflow: Planned workflow orchestration
Dashboard: Planned analytics visualization
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | CSV ingestion and Kafka producer |
| Apache Kafka | Event streaming and message transport |
| Apache Spark Structured Streaming | Micro-batch stream processing |
| PostgreSQL | Relational storage and SQL analytics |
| JDBC | Spark-to-PostgreSQL connectivity |
| Docker | Local service deployment |
| SQL | UPSERT operations, analytics, and data quality checks |
| Git and GitHub | Version control and project documentation |
| Apache Airflow | Planned workflow orchestration |

## Data Model

The pipeline processes five core entities:

| Entity | Description | Verified row count |
|---|---|---:|
| Customers | Customer information | 1,000 |
| Products | Product catalog and pricing | 100 |
| Orders | Customer purchases and order status | 10,000 |
| Order Items | Products and quantities within each order | 29,934 |
| Payments | Payment methods, amounts, and statuses | 10,000 |

These counts represent a successful local test run and may vary when the source data changes.

## Pipeline Workflow

1. **Data source:** Read synthetic e-commerce records from CSV files.
2. **Kafka producer:** Serialize records as JSON and publish them to entity-specific Kafka topics.
3. **Stream ingestion:** Consume Kafka messages using Spark Structured Streaming.
4. **Transformation:** Parse JSON payloads, enforce data types, and prepare structured records.
5. **Data validation:** Check required fields, data types, and business constraints.
6. **Staging:** Write each micro-batch to a PostgreSQL staging table through JDBC.
7. **UPSERT:** Apply PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` to the target table.
8. **Data quality:** Validate foreign-key relationships and reconcile order totals with order-item values.
9. **Analytics:** Run SQL queries for order-status summaries, top-selling products, and payment-status metrics.

## Data Quality Validation

The local test run includes the following checks:

- Orders reference existing customers.
- Order items reference existing orders and products.
- Payments reference existing orders.
- Order totals match the sum of `quantity × unit_price` for orders with item records.
- Payment statuses and payment counts can be summarized using SQL.

**Verified results from the test dataset:**

- No missing customer references were found in orders.
- No missing order or product references were found in order items.
- No missing order references were found in payments.
- No order-total mismatches were found within a tolerance of 0.01 for orders with item records.
- Each order had exactly one payment record in the tested dataset.

These results describe the tested dataset, not a guarantee about all future input data.

## Example Analytics

The project includes SQL queries for:

- Order counts and order values by status.
- Top-selling products by units sold.
- Payment success rate.
- Payment status distribution.

The sample dataset contains 10,000 payment records, of which 2,000 have a `success` status, resulting in a payment-record success rate of 20%.

This is a dataset-level metric and should not be interpreted as a production conversion rate without additional business definitions.

## Repository Structure

```text
realtime-ecommerce-data-pipeline/
├── data/
│   └── raw/
├── scripts/
│   ├── produce_to_kafka.py
│   ├── spark_customers_consumer.py
│   ├── spark_products_consumer.py
│   ├── spark_orders_consumer.py
│   ├── spark_order_items_consumer.py
│   └── spark_payments_consumer.py
├── sql/
│   ├── analytics/
│   └── data_quality/
├── docs/
├── README.md
├── requirements.txt
└── .gitignore
```

Adjust this structure to match the files actually committed to the repository.

## Getting Started

### Prerequisites

- Python and a virtual environment
- Java 17
- Apache Spark 3.5.9
- Docker
- Apache Kafka
- PostgreSQL

### Local Setup

1. Start Kafka and PostgreSQL using the project's local Docker configuration.
2. Create and activate a Python virtual environment.
3. Install the required Python dependencies.
4. Ensure the PostgreSQL database, target tables, and constraints are initialized.
5. Start the appropriate Spark consumer before running the Kafka producer.
6. Publish the CSV records to Kafka.
7. Verify the loaded records and run the SQL analytics queries.

See the project scripts and SQL files for implementation details.

**Note:** The consumers use checkpointing and Kafka offsets. When starting a new query with `startingOffsets=latest`, start the consumer before publishing the messages you want it to process.

## Project Status

- [x] Repository and project structure initialized
- [x] Synthetic CSV datasets available
- [x] Python Kafka producer implemented
- [x] Kafka topics and Spark consumers implemented for the five entities
- [x] Spark Structured Streaming processing and validation implemented
- [x] PostgreSQL integration and UPSERT operations tested
- [x] Referential-integrity and order-total checks executed
- [x] SQL analytics queries tested
- [ ] Automated pipeline orchestration with Apache Airflow
- [ ] Automated data quality test suite
- [ ] Analytics dashboard
- [ ] Complete setup and troubleshooting documentation

## Future Improvements

- Add Apache Airflow DAGs for orchestration, scheduling, retries, and monitoring.
- Automate data quality checks as repeatable tests.
- Move database credentials into environment variables.
- Add automated integration tests.
- Build an analytics dashboard.
- Improve observability, error handling, and deployment configuration.

## Author

A hands-on Data Engineering portfolio project focused on event streaming, structured data processing, relational data modeling, SQL analytics, and data quality validation.
