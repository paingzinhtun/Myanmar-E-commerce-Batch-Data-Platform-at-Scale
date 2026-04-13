from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, functions as F

from src.monitoring.audit import write_audit_log
from src.spark_jobs.common import get_spark_session
from src.utils.config import get_settings
from src.utils.logger import get_logger
from src.validation.quality_checks import (
    raise_for_failed_checks,
    require_amount_match,
    require_allowed_pairs,
    require_no_duplicates,
    require_non_negative,
    require_non_null,
    require_references,
    require_valid_timestamp,
)


logger = get_logger(__name__)
VALID_REGION_CITY_PAIRS = [
    ("Yangon", "Yangon Region"),
    ("Mandalay", "Mandalay Region"),
    ("Naypyidaw", "Naypyidaw Union Territory"),
    ("Taunggyi", "Shan State"),
    ("Mawlamyine", "Mon State"),
    ("Bago", "Bago Region"),
    ("Pathein", "Ayeyarwady Region"),
]


def _bronze(spark, table_name: str, batch_date: str | None = None) -> DataFrame:
    settings = get_settings()
    df = spark.read.parquet(str(settings.bronze_dir / table_name))
    if batch_date and "batch_date" in df.columns:
        df = df.filter(F.col("batch_date") == batch_date)
    return df


def _write_silver(df: DataFrame, table_name: str, partition_column: str | None = None) -> None:
    settings = get_settings()
    writer = df.write.mode("overwrite")
    if partition_column:
        writer = writer.partitionBy(partition_column)
    writer.parquet(str(settings.silver_dir / table_name))


def transform_dimensions(spark) -> None:
    customers = (
        _bronze(spark, "customers")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_name", F.initcap("customer_name"))
        .withColumn("city", F.initcap("city"))
        .withColumn("region", F.initcap("region"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )
    sellers = (
        _bronze(spark, "sellers")
        .dropDuplicates(["seller_id"])
        .withColumn("seller_name", F.initcap("seller_name"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )
    products = (
        _bronze(spark, "products")
        .dropDuplicates(["product_id"])
        .withColumn("unit_price_mmk", F.col("unit_price_mmk").cast("double"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    _write_silver(customers, "customers")
    _write_silver(sellers, "sellers")
    _write_silver(products, "products")


def transform_daily_batch(batch_date: str) -> None:
    settings = get_settings()
    spark = get_spark_session("silver-transform")
    transform_dimensions(spark)

    orders = (
        _bronze(spark, "orders", batch_date)
        .dropDuplicates(["order_id"])
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("shipping_fee_mmk", F.col("shipping_fee_mmk").cast("double"))
        .withColumn("subtotal_amount_mmk", F.col("subtotal_amount_mmk").cast("double"))
        .withColumn("order_total_mmk", F.col("order_total_mmk").cast("double"))
        .withColumn("item_count", F.col("item_count").cast("int"))
        .withColumn("total_quantity", F.col("total_quantity").cast("int"))
        .fillna({"order_status": "placed", "payment_method": "cash_on_delivery"})
    )
    order_items = (
        _bronze(spark, "order_items", batch_date)
        .dropDuplicates(["order_item_id"])
        .withColumn("line_number", F.col("line_number").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price_mmk", F.col("unit_price_mmk").cast("double"))
        .withColumn("gross_line_amount_mmk", F.col("gross_line_amount_mmk").cast("double"))
        .withColumn("discount_amount_mmk", F.col("discount_amount_mmk").cast("double"))
        .withColumn("net_line_amount_mmk", F.col("net_line_amount_mmk").cast("double"))
    )
    payments = (
        _bronze(spark, "payments", batch_date)
        .dropDuplicates(["payment_id"])
        .withColumn("payment_amount_mmk", F.col("payment_amount_mmk").cast("double"))
        .withColumn("paid_at", F.to_timestamp("paid_at"))
    )
    deliveries = (
        _bronze(spark, "deliveries", batch_date)
        .dropDuplicates(["delivery_id"])
        .withColumn("promised_delivery_date", F.to_date("promised_delivery_date"))
        .withColumn("actual_delivery_date", F.to_date("actual_delivery_date"))
        .withColumn("delivery_fee_mmk", F.col("delivery_fee_mmk").cast("double"))
        .withColumn("delivery_delay_days", F.datediff(F.col("actual_delivery_date"), F.col("promised_delivery_date")))
    )
    inventory = (
        _bronze(spark, "inventory_snapshots", batch_date)
        .dropDuplicates(["inventory_snapshot_id"])
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("available_quantity", F.col("available_quantity").cast("int"))
        .withColumn("reserved_quantity", F.col("reserved_quantity").cast("int"))
        .withColumn("reorder_threshold", F.col("reorder_threshold").cast("int"))
    )

    silver_customers = spark.read.parquet(str(settings.silver_dir / "customers"))
    silver_products = spark.read.parquet(str(settings.silver_dir / "products"))
    order_reconciliation = (
        order_items.groupBy("order_id")
        .agg(F.round(F.sum("net_line_amount_mmk"), 2).alias("recalc_subtotal_mmk"))
        .join(orders.select("order_id", "subtotal_amount_mmk"), "order_id", "inner")
    )

    checks = [
        require_non_null(orders, ["order_id", "customer_id"], "orders_primary_keys"),
        require_no_duplicates(orders, ["order_id"], "orders_duplicates"),
        require_valid_timestamp(orders, "order_timestamp", "orders_timestamp_valid"),
        require_valid_timestamp(payments, "paid_at", "payments_timestamp_valid"),
        require_allowed_pairs(orders, "city", "region", VALID_REGION_CITY_PAIRS, "orders_region_city_mapping"),
        require_allowed_pairs(deliveries, "city", "region", VALID_REGION_CITY_PAIRS, "deliveries_region_city_mapping"),
        require_non_null(order_items, ["product_id"], "order_items_product_id_present"),
        require_non_negative(order_items, "unit_price_mmk", "order_item_price_non_negative"),
        require_non_negative(order_items, "quantity", "order_item_quantity_non_negative"),
        require_references(orders, silver_customers, "customer_id", "customer_id", "orders_customer_reference"),
        require_references(order_items, silver_products, "product_id", "product_id", "order_items_product_reference"),
        require_non_negative(payments, "payment_amount_mmk", "payment_amount_non_negative"),
        require_amount_match(order_reconciliation, "recalc_subtotal_mmk", "subtotal_amount_mmk", 1.0, "order_subtotal_reconciliation"),
    ]
    raise_for_failed_checks(checks)

    _write_silver(orders, "orders", "batch_date")
    _write_silver(order_items, "order_items", "batch_date")
    _write_silver(payments, "payments", "batch_date")
    _write_silver(deliveries, "deliveries", "batch_date")
    _write_silver(inventory, "inventory_snapshots", "batch_date")

    try:
        write_audit_log(batch_date, "silver_transform", "SUCCESS", orders.count())
    except Exception as exc:  # pragma: no cover
        logger.warning("Audit log skipped for silver_transform: %s", exc)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run silver layer transformations.")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    transform_daily_batch(args.batch_date)


if __name__ == "__main__":
    main()
