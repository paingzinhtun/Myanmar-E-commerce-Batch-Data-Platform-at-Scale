from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from data.sample_generation.generate_myanmar_ecommerce_data import generate_dataset
from src.ingestion.bronze_loader import load_daily_batch_to_bronze
from src.spark_jobs.gold_transform import build_gold_layer
from src.spark_jobs.silver_transform import transform_daily_batch
from src.utils.logger import get_logger
from src.warehouse.postgres_loader import load_gold_to_postgres


logger = get_logger(__name__)


def resolve_batch_date(**context) -> str:
    return os.environ.get("AIRFLOW_BATCH_DATE", context["ds"])


def generate_batch_files(**context) -> None:
    batch_date = resolve_batch_date(**context)
    generate_dataset(batch_date, days=1, row_scale=3000)
    logger.info("Synthetic raw files generated for %s", batch_date)


def validate_gold_output(**context) -> None:
    batch_date = resolve_batch_date(**context)
    from src.spark_jobs.common import get_spark_session
    from src.utils.config import get_settings

    spark = get_spark_session("gold-validator")
    settings = get_settings()
    fact_orders = spark.read.parquet(str(settings.gold_dir / "fact_orders")).filter(f"batch_date = '{batch_date}'")
    if fact_orders.count() == 0:
        raise ValueError(f"No gold fact_orders rows produced for batch_date={batch_date}")
    spark.stop()


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="myanmar_ecommerce_batch_pipeline",
    description="Daily batch platform for Myanmar e-commerce analytics",
    default_args=default_args,
    start_date=datetime(2026, 4, 10),
    schedule="@daily",
    catchup=False,
    tags=["spark", "batch", "myanmar", "ecommerce"],
) as dag:
    ingest_raw_batch = PythonOperator(task_id="ingest_raw_batch", python_callable=generate_batch_files)
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=lambda **context: load_daily_batch_to_bronze(resolve_batch_date(**context)),
    )
    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=lambda **context: transform_daily_batch(resolve_batch_date(**context)),
    )
    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=lambda **context: build_gold_layer(resolve_batch_date(**context)),
    )
    validate_outputs = PythonOperator(task_id="validate_outputs", python_callable=validate_gold_output)
    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=lambda **context: load_gold_to_postgres(resolve_batch_date(**context)),
    )
    log_completion = PythonOperator(
        task_id="log_completion",
        python_callable=lambda **context: logger.info(
            "Pipeline completed successfully for batch_date=%s", resolve_batch_date(**context)
        ),
    )

    ingest_raw_batch >> load_bronze >> transform_silver >> transform_gold >> validate_outputs >> load_warehouse >> log_completion
