# Myanmar E-commerce Batch Data Platform at Scale

Portfolio-grade batch data engineering project for a Myanmar-focused e-commerce business. The platform simulates realistic raw batch ingestion, Spark-based bronze/silver/gold processing, PostgreSQL warehouse loading, and Airflow orchestration for daily analytics workloads.

## မြန်မာဘာသာဖြင့် အကျဉ်းချုပ်

ဤ project သည် မြန်မာနိုင်ငံအခြေပြု e-commerce လုပ်ငန်းတစ်ခုအတွက် batch data engineering platform ကို portfolio အဆင့်ဖြင့် တည်ဆောက်ထားသော project ဖြစ်ပါသည်။ Raw data များကို ingest လုပ်ခြင်း၊ Spark ဖြင့် bronze/silver/gold layer များအဖြစ် transform လုပ်ခြင်း၊ PostgreSQL warehouse ထဲသို့ analytics-ready table များ load လုပ်ခြင်းနှင့် Airflow ဖြင့် workflow orchestration လုပ်ခြင်းတို့ကို end-to-end ပြသထားပါသည်။

အဓိကအားဖြင့် ဤ project မှာ အောက်ပါအရာများကို ဖော်ပြထားပါသည်။

- မြန်မာဈေးကွက်နဲ့ ကိုက်ညီသော synthetic e-commerce dataset များ
- `bronze` ၊ `silver` ၊ `gold` အဆင့်ခွဲထားသော medallion-style data lake design
- PySpark ဖြင့် daily batch processing နှင့် `batch_date` အလိုက် incremental run များ
- PostgreSQL analytics warehouse နှင့် fact / dimension table design
- Airflow DAG ဖြင့် pipeline orchestration design
- data quality validation, audit logging, business SQL query use cases

ဒီ project ကို အသုံးပြုခြင်းဖြင့် Data Engineer တစ်ယောက်အနေနဲ့ raw source data ကနေ analytics-ready warehouse အထိ ဘယ်လို pipeline တည်ဆောက်ရမလဲ၊ data quality ကို ဘယ်လိုထိန်းမလဲ၊ incremental batch processing ကို ဘယ်လို design လုပ်မလဲ ဆိုတာများကို လက်တွေ့နီးပါး နားလည်နိုင်ပါသည်။

## What This Project Covers

- Myanmar-focused synthetic e-commerce datasets
- Medallion-style data lake design with `bronze`, `silver`, and `gold`
- PySpark batch transformations and incremental processing by `batch_date`
- PostgreSQL analytics warehouse with fact and dimension tables
- Airflow DAG orchestration design
- Data quality validation, audit logging, and business SQL queries

## Core Architecture

1. Raw CSV files land in `data/raw/`
2. Bronze ingestion converts raw files to Parquet in `data_lake/bronze/`
3. Silver Spark jobs clean, standardize, deduplicate, and validate data in `data_lake/silver/`
4. Gold Spark jobs build analytics-ready facts and dimensions in `data_lake/gold/`
5. Warehouse loaders push final datasets into PostgreSQL
6. Airflow coordinates the end-to-end batch workflow

## Key Business Questions Supported

- Which cities and regions generate the most GMV?
- Which products and categories perform best?
- Which sellers contribute the most sales?
- What are daily and monthly order trends?
- How does delivery performance vary by region?
- Which products are frequently out of stock?
- Which customers are most valuable?
- How does payment method usage vary across locations?

## Project Structure

```text
myanmar-ecommerce-batch-platform/
|-- airflow/
|-- configs/
|-- data/
|-- data_lake/
|-- docs/
|-- logs/
|-- sql/
|-- src/
`-- tests/
```

## Documentation

- [Architecture](docs/architecture.md)
- [How It Works End-to-End](docs/how-it-works.md)
- [Tech Stack and Skills Demonstrated](docs/tech-stack.md)
- [Runbook](docs/runbook.md)
- [Challenges and Lessons Learned](docs/challenges-and-lessons.md)

## Main Components

- Synthetic data generator: [data/sample_generation/generate_myanmar_ecommerce_data.py](data/sample_generation/generate_myanmar_ecommerce_data.py)
- Bronze ingestion: [src/ingestion/bronze_loader.py](src/ingestion/bronze_loader.py)
- Silver transformation: [src/spark_jobs/silver_transform.py](src/spark_jobs/silver_transform.py)
- Gold transformation: [src/spark_jobs/gold_transform.py](src/spark_jobs/gold_transform.py)
- Warehouse loading: [src/warehouse/postgres_loader.py](src/warehouse/postgres_loader.py)
- Airflow DAG: [airflow/dags/myanmar_ecommerce_batch_dag.py](airflow/dags/myanmar_ecommerce_batch_dag.py)
- Business SQL: [sql/analytics/business_queries.sql](sql/analytics/business_queries.sql)

## Quick Start

Recommended local runtime:

- Ubuntu in WSL for Spark and Python
- PostgreSQL running on Windows
- `host.docker.internal` as the warehouse host from WSL

Basic flow:

```bash
python data/sample_generation/generate_myanmar_ecommerce_data.py --batch-start-date 2026-04-10 --days 1 --row-scale 1000
python -m src.warehouse.schema_manager
python -m src.ingestion.bronze_loader --batch-date 2026-04-10
python -m src.spark_jobs.silver_transform --batch-date 2026-04-10
python -m src.spark_jobs.gold_transform --batch-date 2026-04-10
python -m src.warehouse.postgres_loader --batch-date 2026-04-10
```

For the full setup and environment details, see the [Runbook](docs/runbook.md).

## Warehouse Outputs

### Dimensions

- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_region`
- `dim_date`

### Facts

- `fact_orders`
- `fact_order_items`
- `fact_payments`
- `fact_inventory_snapshot`
- `fact_delivery_performance`

## Future Improvements

- Add late-arriving data handling with watermarks
- Add warehouse upsert logic for dimensions
- Add SCD Type 2 support for sellers and products
- Add richer SLA monitoring and pipeline metrics
- Add dashboards on top of PostgreSQL with Superset or Metabase
