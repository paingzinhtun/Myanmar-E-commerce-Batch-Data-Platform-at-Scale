# Runbook

## Recommended Local Setup

This project ran most reliably with:

- Ubuntu in WSL for Spark and Python
- PostgreSQL running on Windows
- `host.docker.internal` used as the warehouse host from WSL

## Local Environment

### Windows

- install PostgreSQL
- keep the PostgreSQL service running

### WSL Ubuntu

- install Python
- install `python3-venv`
- install Java 17

Example setup:

```bash
sudo apt update
sudo apt install -y python3-venv openjdk-17-jdk
python3 -m venv ~/venvs/ecommerce-spark
source ~/venvs/ecommerce-spark/bin/activate
cd /mnt/c/Users/HP/Desktop/e_commerce_spark
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Environment Configuration

Create `.env` from `.env.example` and update:

```env
WAREHOUSE_HOST=host.docker.internal
WAREHOUSE_PORT=5432
WAREHOUSE_DB=myanmar_commerce
WAREHOUSE_USER=postgres
WAREHOUSE_PASSWORD=your_password
```

## Generate Raw Data

```bash
python data/sample_generation/generate_myanmar_ecommerce_data.py --batch-start-date 2026-04-10 --days 1 --row-scale 1000
```

## Initialize Warehouse

```bash
python -m src.warehouse.schema_manager
```

## Run Pipeline Step by Step

```bash
python -m src.ingestion.bronze_loader --batch-date 2026-04-10
python -m src.spark_jobs.silver_transform --batch-date 2026-04-10
python -m src.spark_jobs.gold_transform --batch-date 2026-04-10
python -m src.warehouse.postgres_loader --batch-date 2026-04-10
```

## Run Full Pipeline

```bash
python -m src.warehouse.run_full_pipeline --batch-date 2026-04-10
```

## Run Tests

```bash
pytest
```

## Airflow

Airflow is included in the repo as the orchestration layer. The DAG is located at:

- `airflow/dags/myanmar_ecommerce_batch_dag.py`

For local experimentation with containers:

```bash
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```
