# Myanmar E-commerce Batch Data Platform at Scale

Portfolio-grade batch data engineering project for a Myanmar-focused e-commerce business. The platform simulates realistic raw batch ingestion, Spark-based bronze/silver/gold processing, PostgreSQL warehouse loading, and Airflow orchestration for daily analytics workloads.

## 1. Final Folder Structure

```text
myanmar-ecommerce-batch-platform/
├── airflow/
│   ├── dags/
│   │   └── myanmar_ecommerce_batch_dag.py
│   └── plugins/
├── configs/
│   └── pipeline_design.yaml
├── data/
│   ├── raw/
│   └── sample_generation/
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker/
├── logs/
├── notebooks/
├── sql/
│   ├── analytics/
│   │   └── business_queries.sql
│   └── schema/
│       ├── 01_create_schemas.sql
│       ├── 02_create_audit_table.sql
│       ├── 03_create_dimensions.sql
│       └── 04_create_facts.sql
├── src/
│   ├── ingestion/
│   │   └── bronze_loader.py
│   ├── monitoring/
│   │   └── audit.py
│   ├── spark_jobs/
│   │   ├── common.py
│   │   ├── gold_transform.py
│   │   └── silver_transform.py
│   ├── utils/
│   │   ├── config.py
│   │   ├── date_utils.py
│   │   └── logger.py
│   ├── validation/
│   │   └── quality_checks.py
│   └── warehouse/
│       ├── postgres_loader.py
│       ├── run_full_pipeline.py
│       └── schema_manager.py
├── tests/
│   ├── test_data_generator.py
│   └── test_quality_checks.py
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 2. Architecture Overview

The platform uses a local S3-style medallion architecture:

1. Raw CSV files land in `data/raw/`.
2. Bronze ingestion converts raw files to append-oriented Parquet under `data_lake/bronze/`.
3. Silver Spark jobs clean data, cast types, deduplicate rows, and run reusable data quality checks before writing validated Parquet to `data_lake/silver/`.
4. Gold Spark jobs build analytics-ready facts and dimensions in `data_lake/gold/`.
5. Warehouse loaders push gold data into PostgreSQL for BI and SQL analytics.
6. Airflow orchestrates daily batch execution, retries, validation, and logging.

This structure demonstrates how a real batch platform evolves from raw landing to analytics consumption without depending on cloud services.

### README-Quality Architecture Summary

Myanmar E-commerce Batch Data Platform at Scale is a local, portfolio-grade batch data engineering project that simulates how a Myanmar-focused e-commerce company would collect, process, validate, and serve analytics data. The platform is built around realistic business entities such as customers, sellers, products, orders, payments, deliveries, and inventory snapshots, with Myanmar-specific geography and payment behavior included in the source data.

The architecture follows a medallion-style batch design with three core layers: bronze, silver, and gold. Raw batch CSV files arrive daily under a local S3-style folder structure in `data/raw/batches/batch_date=YYYY-MM-DD/`, while master datasets such as customers, sellers, and products are stored under `data/raw/master/`. This simulates how operational systems deliver daily extracts and reference data to a batch platform.

In the bronze layer, raw CSV files are ingested into Parquet using PySpark and stored under `data_lake/bronze/`. This layer preserves source fidelity while making the data more efficient for downstream Spark processing. Bronze datasets are partitioned by `batch_date` where appropriate so the platform can support incremental daily processing instead of full historical reloads.

The silver layer is responsible for cleaning, standardization, and data quality enforcement. Spark jobs cast data types, normalize business fields, deduplicate records, validate timestamps, verify entity references, and reconcile order totals. This is the layer where raw operational records become trusted analytical datasets. Reusable validation utilities help catch issues such as missing identifiers, duplicate orders, invalid payment amounts, negative quantities, inconsistent totals, and invalid city-region mappings.

The gold layer contains analytics-ready fact and dimension datasets designed for reporting and business analysis. Fact tables include orders, order items, payments, inventory snapshots, and delivery performance. Dimension tables include customer, product, seller, region, and date. These curated datasets are written to `data_lake/gold/` in Parquet format and shaped for downstream warehouse loading and SQL analytics.

For local warehousing, the project loads gold datasets into PostgreSQL, which acts as the analytics serving layer. This creates a realistic separation between the batch processing engine and the query-serving warehouse. The database is organized into `analytics` and `monitoring` schemas, where business tables support reporting and an audit table tracks pipeline stages, row counts, statuses, and timestamps.

Pipeline execution is built around incremental daily batches keyed by `batch_date`. Each run can process a single day independently, which makes the platform easier to rerun, debug, and scale. This mirrors real-world batch processing patterns where daily arrivals are handled incrementally and fact partitions can be safely reloaded without rebuilding the entire platform.

The project also includes Airflow orchestration for local development. The DAG defines a realistic flow of tasks: ingest raw batch data, load bronze datasets, transform silver data, build gold outputs, validate processed results, load the warehouse, and log completion. Retry behavior, task dependencies, and simple failure handling are included so the workflow feels production-minded rather than notebook-driven.

Overall, this architecture demonstrates how a Data Engineer can design a scalable, business-oriented batch platform using PySpark, a layered data lake, PostgreSQL, data quality controls, and orchestration patterns. Even though it runs locally, the project reflects the structure and engineering decisions commonly used in real data platforms.

## 3. Dataset Design

### Myanmar Business Geography

- Yangon Region / Yangon
- Mandalay Region / Mandalay
- Naypyidaw Union Territory / Naypyidaw
- Shan State / Taunggyi
- Mon State / Mawlamyine
- Bago Region / Bago
- Ayeyarwady Region / Pathein

### Core Datasets

- `customers`: customer master with township, city, region, segment, phone, email.
- `sellers`: seller master including marketplace sellers, brand stores, and wholesale partners.
- `products`: category, brand, MMK pricing, seller ownership, active flag.
- `orders`: order header with geography, payment method, subtotal, shipping fee, total, status.
- `order_items`: transactional grain by line item with quantities, unit price, discount, net value.
- `payments`: payment status and amount across local payment methods like `KBZPay`, `Wave Pay`, `AYA Pay`, `cash_on_delivery`, and `bank_transfer`.
- `deliveries`: local courier behavior, promised versus actual delivery dates, status, and delay.
- `inventory_snapshots`: daily stock state for out-of-stock and replenishment analysis.

### Scale Design

Synthetic data generation supports scaling by `row_scale`, allowing local demonstration at thousands of rows and extension toward hundreds of thousands or more.

## 4. Warehouse Schema

### Dimension Tables

- `analytics.dim_customer`
- `analytics.dim_product`
- `analytics.dim_seller`
- `analytics.dim_region`
- `analytics.dim_date`

### Fact Tables

- `analytics.fact_orders`
- `analytics.fact_order_items`
- `analytics.fact_payments`
- `analytics.fact_inventory_snapshot`
- `analytics.fact_delivery_performance`

### Monitoring Table

- `monitoring.pipeline_run_audit`

These tables support GMV, seller, category, customer value, payment mix, delivery performance, and stock health analytics.

## 5. Airflow DAG Design

The DAG `myanmar_ecommerce_batch_pipeline` runs daily and includes:

1. `ingest_raw_batch`: generate the synthetic batch files for the execution date.
2. `load_bronze`: convert raw batch CSVs into bronze Parquet.
3. `transform_silver`: apply cleaning, typing, deduplication, joins, and data quality validation.
4. `transform_gold`: create analytics-ready fact and dimension datasets.
5. `validate_outputs`: verify that gold outputs exist before warehouse loading.
6. `load_warehouse`: push gold tables into PostgreSQL.
7. `log_completion`: emit final pipeline completion log.

Production-minded touches:

- retry count of `2`
- `5` minute retry delay
- explicit dependencies
- simple failure surfacing through PythonOperator exceptions
- daily scheduling with local-friendly defaults

## 6. Implementation Roadmap

### Phase 1

- Final folder structure
- Architecture overview
- Dataset design
- Batch processing design
- Warehouse schema
- Airflow DAG plan

### Phase 2

- Myanmar-focused synthetic generator
- Environment-driven config
- Centralized logging
- Reusable Spark validation utilities

### Phase 3

- Bronze CSV to Parquet ingestion
- Silver business-standardized transformations
- Gold marts and dimensions
- Incremental processing by `batch_date`
- Partitioned Parquet outputs

### Phase 4

- PostgreSQL schemas
- Warehouse loading scripts
- Pipeline audit logging
- Portfolio analytics SQL

### Phase 5

- Airflow DAG implementation
- Retry and dependency handling
- End-to-end orchestration flow

### Phase 6

- Tests
- README and runbook
- Architecture explanation
- Future improvements

## Batch Processing and Incremental Design

- Each incoming batch is keyed by `batch_date`.
- Raw files arrive under `data/raw/batches/batch_date=YYYY-MM-DD/`.
- Bronze persists raw history in Parquet partitioned by `batch_date`.
- Silver and gold process the requested `batch_date` rather than full history.
- Warehouse fact tables delete and reload the current `batch_date` partition for idempotent local reruns.
- Master entities are refreshed from current source extracts to keep the example practical for local development.

## How It Works End-to-End

The platform runs as a daily batch pipeline that moves e-commerce data from raw source files to analytics-ready warehouse tables. Each run is organized around a single `batch_date`, which keeps the workflow incremental, rerunnable, and easy to debug.

### 1. Synthetic Source Data Is Generated

The pipeline begins by generating realistic Myanmar e-commerce datasets for customers, sellers, products, orders, order items, payments, deliveries, and inventory snapshots. These files are written as CSVs into the raw landing zone:

- `data/raw/master/`
- `data/raw/batches/batch_date=YYYY-MM-DD/`

This simulates how daily source extracts arrive from operational systems.

### 2. Raw Data Is Ingested into the Bronze Layer

The bronze ingestion job reads the raw CSV files and writes them to Parquet under `data_lake/bronze/`. Minimal transformation is applied at this stage because the main purpose of bronze is to preserve the landed batch while making the data efficient for Spark processing.

Batch-oriented datasets such as orders, payments, deliveries, and inventory snapshots are partitioned by `batch_date`, which allows the platform to process data incrementally.

### 3. Bronze Data Is Cleaned and Validated in the Silver Layer

The silver transformation job reads bronze data and applies core engineering rules:

- standardize columns and formats
- cast correct data types
- remove duplicate records
- normalize timestamps and business fields
- validate foreign key relationships
- check for invalid quantities, prices, and payment amounts
- reconcile order totals against item-level amounts

The silver layer is where raw operational data becomes trusted analytical data.

### 4. Analytics-Ready Tables Are Built in the Gold Layer

The gold transformation job creates fact and dimension datasets designed for reporting and business analysis. These include:

- `fact_orders`
- `fact_order_items`
- `fact_payments`
- `fact_inventory_snapshot`
- `fact_delivery_performance`
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_region`
- `dim_date`

These datasets are written to `data_lake/gold/` in Parquet format and represent the curated business layer of the platform.

### 5. Gold Data Is Loaded into PostgreSQL

Once the gold datasets are ready, the warehouse loading job reads them and inserts them into PostgreSQL analytics tables. Fact tables are reloaded by `batch_date`, which makes reruns safer and more practical for local development. Dimension tables are refreshed so that the warehouse always reflects the latest current-state reference data.

At this point, the warehouse is ready for SQL-based analytics, dashboarding, and ad hoc reporting.

### 6. Audit Logging Tracks Pipeline Execution

Each major stage writes audit information into `monitoring.pipeline_run_audit`, including:

- batch date
- pipeline stage
- status
- row count
- message
- timestamp

This provides basic observability and makes it easier to trace what happened during a pipeline run.

### 7. Analytics Queries Are Run on the Warehouse

After loading completes, business users or analysts can query PostgreSQL to answer questions such as:

- which cities generate the most GMV
- which sellers drive the most revenue
- which products frequently go out of stock
- how payment method usage varies by region
- how delivery performance differs across locations

The SQL examples in `sql/analytics/business_queries.sql` are designed to demonstrate those use cases directly.

### End-to-End Outcome

By the end of a successful run, the pipeline has:

- generated realistic raw batch data
- landed source files in a raw zone
- persisted bronze, silver, and gold Parquet datasets
- enforced core data quality checks
- loaded a PostgreSQL analytics warehouse
- written audit records for operational visibility

This gives the project a complete batch data lifecycle from ingestion to analytics consumption, which is exactly what makes it portfolio-grade.

## Data Quality Checks

Reusable validations cover:

- missing primary identifiers
- duplicate order records
- invalid or non-positive prices
- negative quantities
- missing customer references
- missing product references
- invalid payment amounts
- inconsistent order totals against item-level recalculation

## Tech Stack and Skills Demonstrated

This project demonstrates a full batch data engineering stack rather than a single tool or isolated script. Each technology was chosen to reflect a realistic role in an end-to-end analytics platform.

### Python

Python is the primary implementation language across the project. It is used for synthetic data generation, Spark job orchestration, warehouse loading, configuration management, logging, validation logic, and monitoring utilities. Python acts as the control layer that ties the entire platform together.

This demonstrates practical software engineering for data systems, including modular code organization, reusable utilities, and environment-based configuration.

### PySpark / Apache Spark

PySpark is the main data processing engine. It is used to ingest raw CSV files into the bronze layer, standardize and validate silver datasets, build gold fact and dimension tables, and load warehouse-ready data. Spark is also responsible for partition-aware batch processing and scalable file-based transformations.

This demonstrates core data engineering skills in distributed-style processing, data lake transformation patterns, and production-minded batch pipeline design.

### Apache Airflow

Airflow is used as the orchestration layer for scheduling and coordinating the batch workflow. The DAG defines the order of execution across ingestion, bronze loading, silver transformation, gold transformation, validation, warehouse loading, and pipeline completion logging.

This demonstrates workflow orchestration, dependency management, retry strategy design, and the ability to turn multiple jobs into a single managed pipeline.

### PostgreSQL

PostgreSQL is used as the local analytics warehouse. Gold datasets are loaded into relational fact and dimension tables for SQL-based reporting and business analysis. PostgreSQL also stores the audit table used to track pipeline runs and stage-level monitoring events.

This demonstrates warehouse design, SQL-based analytics enablement, and the separation of batch processing from warehouse serving.

### Parquet

Parquet is the storage format for the bronze, silver, and gold layers. Using Parquet makes the local data lake more efficient for analytics workloads than CSV and better reflects modern batch platform design.

This demonstrates good storage format choices for analytical processing and file-based lake design.

### Medallion Architecture

The project uses a bronze-silver-gold structure to separate raw ingestion, cleaned business-ready data, and analytics-ready outputs. Bronze preserves source data, silver enforces trust and consistency, and gold serves reporting use cases.

This demonstrates layered architecture thinking, maintainability, traceability, and a strong understanding of modern batch platform organization.

### Dimensional Modeling

The gold and warehouse layers use fact and dimension tables such as `fact_orders`, `fact_payments`, `dim_customer`, `dim_product`, and `dim_date`. This makes the final data model easier to query for trend analysis, customer metrics, seller reporting, and operational insights.

This demonstrates warehouse modeling skills and the ability to translate operational data into an analytics-friendly schema.

### Incremental Batch Processing

The platform is designed around `batch_date`, which allows daily processing, partition-based outputs, and rerunnable batch workflows. Fact loads are handled by date instead of reprocessing the entire dataset every time.

This demonstrates real-world batch engineering design, where incremental processing is critical for performance, reliability, and maintainability.

### Data Quality Engineering

Reusable quality checks validate identifiers, duplicates, prices, quantities, timestamps, reference integrity, payment values, order totals, and location mappings. These checks run as part of transformation instead of being treated as an afterthought.

This demonstrates that the platform prioritizes trust in the data, not just successful movement of data.

### Logging and Monitoring

The platform uses centralized logging utilities and a warehouse-backed audit table to track pipeline status, row counts, success/failure events, and timestamps. This gives visibility into the operational health of each pipeline stage.

This demonstrates observability thinking and basic production operations awareness.

### SQL

SQL is used for warehouse schema creation and business-facing analytics queries. It defines the relational model in PostgreSQL and provides direct examples of how stakeholders would consume the data.

This demonstrates fluency in both engineering-oriented SQL and analytics-oriented SQL.

### dotenv and Environment-Based Configuration

Configuration values such as storage paths, Spark settings, warehouse credentials, and logging directories are managed through `.env` and Python configuration utilities rather than hardcoded values.

This demonstrates environment-aware engineering practices and cleaner separation of code from runtime configuration.

### pytest

The project includes basic automated tests for generator behavior and reusable validation logic. While lightweight, these tests help ensure that important building blocks behave as expected.

This demonstrates software engineering discipline inside a data engineering project.

### WSL-Based Runtime Engineering

In practice, the project was executed using Ubuntu in WSL for Spark, while PostgreSQL ran on Windows and was accessed through network configuration. This setup reflects a realistic engineering lesson: local environment architecture can matter just as much as pipeline code.

This demonstrates practical troubleshooting, platform awareness, and the ability to adapt runtime strategy to match tool stability.

### Overall Skill Signal

Taken together, the project demonstrates not just familiarity with tools, but the ability to combine them into a cohesive batch data platform. The strongest skills shown here are data pipeline design, Spark transformation engineering, warehouse modeling, validation, orchestration, operational monitoring, and environment-aware problem solving.

## Run Instructions

### Local Environment

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
copy .env.example .env
```

### Generate Raw Data

```bash
python data/sample_generation/generate_myanmar_ecommerce_data.py --batch-start-date 2026-04-10 --days 3 --row-scale 5000
```

### Start PostgreSQL and Initialize Schema

```bash
docker compose up -d postgres
python -m src.warehouse.schema_manager
```

### Run Pipeline Step by Step

```bash
python -m src.ingestion.bronze_loader --batch-date 2026-04-10
python -m src.spark_jobs.silver_transform --batch-date 2026-04-10
python -m src.spark_jobs.gold_transform --batch-date 2026-04-10
python -m src.warehouse.postgres_loader --batch-date 2026-04-10
```

### Run Full Pipeline

```bash
python -m src.warehouse.run_full_pipeline --batch-date 2026-04-10
```

### Run Tests

```bash
pytest
```

### Airflow

```bash
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Open Airflow locally on `http://localhost:8080`.

## Future Improvements

- Add late-arriving data handling with watermarks.
- Add upsert logic for warehouse dimensions.
- Add SCD Type 2 support for sellers and products.
- Add richer batch metrics and SLA monitoring.
- Add data contracts and schema drift detection.
- Add BI dashboards on top of the warehouse.

## Challenges and Lessons Learned

Building this project surfaced several practical lessons that are common in real data engineering work. The biggest challenge was not the business logic itself, but the surrounding environment and platform setup needed to run Spark and PostgreSQL reliably.

### Native Windows Spark Can Be Fragile

Running PySpark directly on Windows introduced Hadoop filesystem permission issues during Parquet writes. Even after configuring `winutils.exe`, native Windows Spark remained unreliable for this workflow. This reinforced an important lesson: local operating system choice can significantly affect data engineering productivity.

### WSL Is a Better Local Runtime for Spark

Switching Spark execution to Ubuntu in WSL made the pipeline much more stable. The project code stayed on the Windows filesystem, while the Python virtual environment and Spark runtime ran inside Linux. This hybrid setup proved to be much more reliable than native Windows for PySpark development.

### Local Networking Between WSL and PostgreSQL Matters

Once Spark was running inside WSL, PostgreSQL connectivity became the next challenge. `localhost` did not work because the warehouse was running on Windows while the Spark jobs ran inside Ubuntu. The solution was to connect through `host.docker.internal` and adjust PostgreSQL access rules so the WSL environment could authenticate successfully.

### PostgreSQL Configuration Is Part of the Platform

The warehouse setup required more than just creating tables. PostgreSQL had to be configured to accept network connections from WSL through `postgresql.conf` and `pg_hba.conf`. This highlighted an important production lesson: database connectivity, authentication, and host-level access policies are part of platform engineering, not just database administration.

### Environment Management Is a Real Engineering Skill

This project required fixing dependency conflicts, managing Python environments across Windows and Linux, setting Java correctly for Spark, and troubleshooting system-level issues. That experience reflects real-world data engineering, where success often depends as much on environment reliability as on transformation logic.

### Incremental Design Paid Off

Because the project was built around `batch_date`, it was possible to rerun a single day repeatedly while debugging setup issues. This made troubleshooting much more practical than if the entire dataset had to be regenerated and reprocessed on every attempt. Incremental processing is not only efficient for production workloads, but also extremely helpful during development.

### Key Takeaway

One of the most important lessons from this project is that data engineering is not just about writing transformations. It also involves operating systems, package management, runtime configuration, connectivity, validation, and observability. Building the project end to end made those hidden parts visible and turned the implementation into a much more realistic engineering exercise.
