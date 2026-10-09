# realtime-ecommerce-data-pipeline
A real-time e-commerce data pipeline using Python, Kafka, PySpark, PostgreSQL, and Airflow.
# Real-Time E-commerce Data Pipeline

## Overview

This project builds a real-time data pipeline for a simulated e-commerce platform.

It generates synthetic customer, product, order, and payment data, streams business events through Apache Kafka, processes them with PySpark, and stores analytics-ready data in PostgreSQL.

Apache Airflow will orchestrate scheduled batch workflows and data quality checks.

## Architecture

```text
Synthetic Data
      |
      v
Python Data Generator
      |
      v
Apache Kafka
      |
      v
PySpark Processing
      |
      v
PostgreSQL
      |
      v
Analytics Dashboard

Apache Airflow: Workflow orchestration
Docker: Local development environment
```

## Technology Stack

* Python — data generation and pipeline logic
* Apache Kafka — event streaming
* PySpark — distributed data processing
* PostgreSQL — analytical data storage
* Apache Airflow — workflow orchestration
* Docker — service deployment
* Git and GitHub — version control and documentation

## Data Model

The project includes four core entities:

* **Customer:** customer information
* **Product:** product catalog and pricing
* **Order:** customer purchases and order status
* **Payment:** payment transactions and payment status

## Project Goals

* Generate reproducible synthetic e-commerce data.
* Build a Kafka-based event streaming pipeline.
* Process and validate data using PySpark.
* Store clean data in PostgreSQL.
* Orchestrate batch workflows with Airflow.
* Build an analytics dashboard.
* Apply data engineering practices such as testing, logging, and data quality validation.

## Project Status

* [x] Repository initialized
* [ ] Data model designed
* [ ] Synthetic data generator implemented
* [ ] Data validation implemented
* [ ] Kafka producer and consumer implemented
* [ ] PySpark processing implemented
* [ ] PostgreSQL integration completed
* [ ] Airflow orchestration completed
* [ ] Dashboard and documentation completed

## Getting Started

Setup instructions will be added as each pipeline component is implemented.

## Author

Data Engineering Portfolio Project

