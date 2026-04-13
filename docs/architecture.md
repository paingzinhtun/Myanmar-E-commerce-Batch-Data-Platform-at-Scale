# Architecture

## Overview

Myanmar E-commerce Batch Data Platform at Scale is a local, portfolio-grade batch data engineering project that simulates how a Myanmar-focused e-commerce company would collect, process, validate, and serve analytics data. The platform is built around realistic business entities such as customers, sellers, products, orders, payments, deliveries, and inventory snapshots, with Myanmar-specific geography and payment behavior included in the source data.

The architecture follows a medallion-style batch design with three core layers: bronze, silver, and gold. Raw batch CSV files arrive daily under a local S3-style folder structure in `data/raw/batches/batch_date=YYYY-MM-DD/`, while master datasets such as customers, sellers, and products are stored under `data/raw/master/`. This simulates how operational systems deliver daily extracts and reference data to a batch platform.

In the bronze layer, raw CSV files are ingested into Parquet using PySpark and stored under `data_lake/bronze/`. This layer preserves source fidelity while making the data more efficient for downstream Spark processing. Bronze datasets are partitioned by `batch_date` where appropriate so the platform can support incremental daily processing instead of full historical reloads.

The silver layer is responsible for cleaning, standardization, and data quality enforcement. Spark jobs cast data types, normalize business fields, deduplicate records, validate timestamps, verify entity references, and reconcile order totals. This is the layer where raw operational records become trusted analytical datasets. Reusable validation utilities help catch issues such as missing identifiers, duplicate orders, invalid payment amounts, negative quantities, inconsistent totals, and invalid city-region mappings.

The gold layer contains analytics-ready fact and dimension datasets designed for reporting and business analysis. Fact tables include orders, order items, payments, inventory snapshots, and delivery performance. Dimension tables include customer, product, seller, region, and date. These curated datasets are written to `data_lake/gold/` in Parquet format and shaped for downstream warehouse loading and SQL analytics.

For local warehousing, the project loads gold datasets into PostgreSQL, which acts as the analytics serving layer. This creates a realistic separation between the batch processing engine and the query-serving warehouse. The database is organized into `analytics` and `monitoring` schemas, where business tables support reporting and an audit table tracks pipeline stages, row counts, statuses, and timestamps.

Pipeline execution is built around incremental daily batches keyed by `batch_date`. Each run can process a single day independently, which makes the platform easier to rerun, debug, and scale. This mirrors real-world batch processing patterns where daily arrivals are handled incrementally and fact partitions can be safely reloaded without rebuilding the entire platform.

The project also includes Airflow orchestration for local development. The DAG defines a realistic flow of tasks: ingest raw batch data, load bronze datasets, transform silver data, build gold outputs, validate processed results, load the warehouse, and log completion. Retry behavior, task dependencies, and simple failure handling are included so the workflow feels production-minded rather than notebook-driven.

Overall, this architecture demonstrates how a Data Engineer can design a scalable, business-oriented batch platform using PySpark, a layered data lake, PostgreSQL, data quality controls, and orchestration patterns. Even though it runs locally, the project reflects the structure and engineering decisions commonly used in real data platforms.

## Folder Structure

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
├── docs/
├── logs/
├── sql/
│   ├── analytics/
│   └── schema/
├── src/
│   ├── ingestion/
│   ├── monitoring/
│   ├── spark_jobs/
│   ├── utils/
│   ├── validation/
│   └── warehouse/
└── tests/
```

## Dataset Design

### Myanmar Business Geography

- Yangon Region / Yangon
- Mandalay Region / Mandalay
- Naypyidaw Union Territory / Naypyidaw
- Shan State / Taunggyi
- Mon State / Mawlamyine
- Bago Region / Bago
- Ayeyarwady Region / Pathein

### Core Datasets

- `customers`: customer master with township, city, region, segment, phone, email
- `sellers`: marketplace sellers, brand stores, and wholesale partners
- `products`: category, brand, MMK pricing, seller ownership, active flag
- `orders`: order header with geography, payment method, subtotal, shipping fee, total, status
- `order_items`: line-level quantity, price, discount, and revenue
- `payments`: local payment methods including `KBZPay`, `Wave Pay`, `AYA Pay`, `cash_on_delivery`, and `bank_transfer`
- `deliveries`: promised versus actual delivery dates, status, and delay
- `inventory_snapshots`: stock state for replenishment and out-of-stock analysis

## Warehouse Schema

### Dimensions

- `analytics.dim_customer`
- `analytics.dim_product`
- `analytics.dim_seller`
- `analytics.dim_region`
- `analytics.dim_date`

### Facts

- `analytics.fact_orders`
- `analytics.fact_order_items`
- `analytics.fact_payments`
- `analytics.fact_inventory_snapshot`
- `analytics.fact_delivery_performance`

### Monitoring

- `monitoring.pipeline_run_audit`

## Batch and Incremental Design

- Each incoming batch is keyed by `batch_date`
- Raw files arrive under `data/raw/batches/batch_date=YYYY-MM-DD/`
- Bronze persists raw history in Parquet partitioned by `batch_date`
- Silver and gold process the requested `batch_date` rather than full history
- Warehouse fact tables delete and reload the current `batch_date` partition for idempotent reruns
- Master entities are refreshed from current source extracts

## Airflow DAG Design

The DAG `myanmar_ecommerce_batch_pipeline` includes:

1. `ingest_raw_batch`
2. `load_bronze`
3. `transform_silver`
4. `transform_gold`
5. `validate_outputs`
6. `load_warehouse`
7. `log_completion`

Production-minded touches:

- retry count of `2`
- `5` minute retry delay
- explicit dependencies
- daily scheduling
