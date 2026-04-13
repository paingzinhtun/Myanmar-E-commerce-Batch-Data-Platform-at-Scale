from __future__ import annotations

import argparse

from sqlalchemy import create_engine, text

from src.monitoring.audit import write_audit_log
from src.spark_jobs.common import get_spark_session
from src.utils.config import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)

TABLES = [
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_region",
    "dim_date",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_inventory_snapshot",
    "fact_delivery_performance",
]


def truncate_and_prepare_tables(engine, batch_date: str) -> None:
    settings = get_settings()
    target_date_key = int(batch_date.replace("-", ""))
    dimension_tables = [
        "dim_customer",
        "dim_seller",
        "dim_product",
        "dim_region",
    ]
    fact_tables = [
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_inventory_snapshot",
        "fact_delivery_performance",
    ]
    with engine.begin() as connection:
        for table_name in dimension_tables:
            connection.execute(text(f"TRUNCATE TABLE {settings.warehouse_schema}.{table_name}"))
        connection.execute(
            text(f"DELETE FROM {settings.warehouse_schema}.dim_date WHERE date_key = :date_key"),
            {"date_key": target_date_key},
        )
        for table_name in fact_tables:
            connection.execute(
                text(f"DELETE FROM {settings.warehouse_schema}.{table_name} WHERE batch_date = :batch_date"),
                {"batch_date": batch_date},
            )


def load_gold_to_postgres(batch_date: str) -> None:
    settings = get_settings()
    engine = create_engine(settings.sqlalchemy_url)
    truncate_and_prepare_tables(engine, batch_date)

    spark = get_spark_session("warehouse-loader")
    for table_name in TABLES:
        df = spark.read.parquet(str(settings.gold_dir / table_name))
        if "batch_date" in df.columns:
            df = df.filter(df.batch_date == batch_date)
        row_count = df.count()
        (
            df.write.format("jdbc")
            .option("url", settings.jdbc_url)
            .option("dbtable", f"{settings.warehouse_schema}.{table_name}")
            .option("user", settings.warehouse_user)
            .option("password", settings.warehouse_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Loaded %s rows=%s batch_date=%s", table_name, row_count, batch_date)

    spark.stop()
    try:
        write_audit_log(batch_date, "warehouse_load", "SUCCESS", None, "Gold datasets loaded into PostgreSQL")
    except Exception as exc:  # pragma: no cover
        logger.warning("Audit log skipped for warehouse_load: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load gold datasets into PostgreSQL.")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    load_gold_to_postgres(args.batch_date)


if __name__ == "__main__":
    main()
