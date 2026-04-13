from __future__ import annotations

import argparse

from src.ingestion.bronze_loader import load_daily_batch_to_bronze
from src.spark_jobs.gold_transform import build_gold_layer
from src.spark_jobs.silver_transform import transform_daily_batch
from src.utils.logger import get_logger
from src.warehouse.postgres_loader import load_gold_to_postgres


logger = get_logger(__name__)


def run_pipeline(batch_date: str) -> None:
    logger.info("Starting end-to-end batch pipeline for %s", batch_date)
    load_daily_batch_to_bronze(batch_date)
    transform_daily_batch(batch_date)
    build_gold_layer(batch_date)
    load_gold_to_postgres(batch_date)
    logger.info("Completed end-to-end batch pipeline for %s", batch_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Myanmar e-commerce batch pipeline.")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    run_pipeline(args.batch_date)


if __name__ == "__main__":
    main()
