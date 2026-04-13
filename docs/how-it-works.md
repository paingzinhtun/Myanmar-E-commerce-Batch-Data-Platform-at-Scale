# How It Works End-to-End

The platform runs as a daily batch pipeline that moves e-commerce data from raw source files to analytics-ready warehouse tables. Each run is organized around a single `batch_date`, which keeps the workflow incremental, rerunnable, and easy to debug.

## 1. Synthetic Source Data Is Generated

The pipeline begins by generating realistic Myanmar e-commerce datasets for customers, sellers, products, orders, order items, payments, deliveries, and inventory snapshots. These files are written as CSVs into the raw landing zone:

- `data/raw/master/`
- `data/raw/batches/batch_date=YYYY-MM-DD/`

## 2. Raw Data Is Ingested into the Bronze Layer

The bronze ingestion job reads the raw CSV files and writes them to Parquet under `data_lake/bronze/`. Minimal transformation is applied at this stage because the main purpose of bronze is to preserve the landed batch while making the data efficient for Spark processing.

## 3. Bronze Data Is Cleaned and Validated in the Silver Layer

The silver transformation job reads bronze data and applies core engineering rules:

- standardize columns and formats
- cast correct data types
- remove duplicate records
- normalize timestamps and business fields
- validate foreign key relationships
- check for invalid quantities, prices, and payment amounts
- reconcile order totals against item-level amounts

## 4. Analytics-Ready Tables Are Built in the Gold Layer

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

## 5. Gold Data Is Loaded into PostgreSQL

Once the gold datasets are ready, the warehouse loading job reads them and inserts them into PostgreSQL analytics tables. Fact tables are reloaded by `batch_date`, which makes reruns safer and more practical for local development. Dimension tables are refreshed so that the warehouse always reflects the latest current-state reference data.

## 6. Audit Logging Tracks Pipeline Execution

Each major stage writes audit information into `monitoring.pipeline_run_audit`, including:

- batch date
- pipeline stage
- status
- row count
- message
- timestamp

## 7. Analytics Queries Are Run on the Warehouse

After loading completes, business users or analysts can query PostgreSQL to answer questions such as:

- which cities generate the most GMV
- which sellers drive the most revenue
- which products frequently go out of stock
- how payment method usage varies by region
- how delivery performance differs across locations

## End-to-End Outcome

By the end of a successful run, the pipeline has:

- generated realistic raw batch data
- landed source files in a raw zone
- persisted bronze, silver, and gold Parquet datasets
- enforced core data quality checks
- loaded a PostgreSQL analytics warehouse
- written audit records for operational visibility
