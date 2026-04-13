from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from src.monitoring.audit import write_audit_log
from src.spark_jobs.common import get_spark_session
from src.utils.config import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _read_csv(spark, path: Path):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


def load_master_data_to_bronze(spark) -> None:
    settings = get_settings()
    for table_name in ["customers", "sellers", "products"]:
        source_path = settings.raw_data_dir / "master" / f"{table_name}.csv"
        df = _read_csv(spark, source_path).withColumn("ingested_at", F.current_timestamp())
        df.write.mode("overwrite").parquet(str(settings.bronze_dir / table_name))
        logger.info("Bronze master load complete for %s rows=%s", table_name, df.count())


def load_daily_batch_to_bronze(batch_date: str) -> None:
    settings = get_settings()
    spark = get_spark_session("bronze-loader")
    load_master_data_to_bronze(spark)

    batch_dir = settings.raw_data_dir / "batches" / f"batch_date={batch_date}"
    for table_name in ["orders", "order_items", "payments", "deliveries", "inventory_snapshots"]:
        source_path = batch_dir / f"{table_name}.csv"
        df = _read_csv(spark, source_path).withColumn("batch_date", F.lit(batch_date)).withColumn(
            "ingested_at", F.current_timestamp()
        )
        (
            df.write.mode("append")
            .partitionBy("batch_date")
            .parquet(str(settings.bronze_dir / table_name))
        )
        row_count = df.count()
        logger.info("Bronze batch load complete for %s batch_date=%s rows=%s", table_name, batch_date, row_count)
        try:
            write_audit_log(batch_date, f"bronze_{table_name}", "SUCCESS", row_count)
        except Exception as exc:  # pragma: no cover
            logger.warning("Audit log skipped for %s: %s", table_name, exc)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw batch data to bronze Parquet.")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    load_daily_batch_to_bronze(args.batch_date)


if __name__ == "__main__":
    main()
