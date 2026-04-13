# Tech Stack and Skills Demonstrated

This project demonstrates a full batch data engineering stack rather than a single tool or isolated script. Each technology was chosen to reflect a realistic role in an end-to-end analytics platform.

## Python

Python is the primary implementation language across the project. It is used for synthetic data generation, Spark job orchestration, warehouse loading, configuration management, logging, validation logic, and monitoring utilities.

## PySpark / Apache Spark

PySpark is the main data processing engine. It is used to ingest raw CSV files into the bronze layer, standardize and validate silver datasets, build gold fact and dimension tables, and load warehouse-ready data.

## Apache Airflow

Airflow is used as the orchestration layer for scheduling and coordinating the batch workflow. The DAG defines the order of execution across ingestion, bronze loading, silver transformation, gold transformation, validation, warehouse loading, and completion logging.

## PostgreSQL

PostgreSQL is used as the local analytics warehouse. Gold datasets are loaded into relational fact and dimension tables for SQL-based reporting and business analysis. PostgreSQL also stores the audit table.

## Parquet

Parquet is the storage format for the bronze, silver, and gold layers. It makes the local data lake more efficient for analytics workloads than CSV.

## Medallion Architecture

The project uses a bronze-silver-gold structure to separate raw ingestion, cleaned business-ready data, and analytics-ready outputs.

## Dimensional Modeling

The gold and warehouse layers use fact and dimension tables such as `fact_orders`, `fact_payments`, `dim_customer`, `dim_product`, and `dim_date`.

## Incremental Batch Processing

The platform is designed around `batch_date`, which allows daily processing, partition-based outputs, and rerunnable batch workflows.

## Data Quality Engineering

Reusable quality checks validate identifiers, duplicates, prices, quantities, timestamps, reference integrity, payment values, order totals, and location mappings.

## Logging and Monitoring

The platform uses centralized logging utilities and a warehouse-backed audit table to track pipeline status, row counts, success/failure events, and timestamps.

## SQL

SQL is used for warehouse schema creation and business-facing analytics queries.

## dotenv and Environment-Based Configuration

Configuration values such as storage paths, Spark settings, warehouse credentials, and logging directories are managed through `.env` and Python configuration utilities.

## pytest

The project includes basic automated tests for generator behavior and reusable validation logic.

## WSL-Based Runtime Engineering

In practice, the project was executed using Ubuntu in WSL for Spark, while PostgreSQL ran on Windows and was accessed through network configuration.
