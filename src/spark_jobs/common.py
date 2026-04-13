from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from src.utils.config import get_settings


def get_spark_session(app_name: str | None = None) -> SparkSession:
    settings = get_settings()
    builder = (
        SparkSession.builder.appName(app_name or settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "Asia/Yangon")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.warehouse.dir", str(Path(settings.spark_warehouse_dir).resolve()))
    )
    return builder.getOrCreate()
