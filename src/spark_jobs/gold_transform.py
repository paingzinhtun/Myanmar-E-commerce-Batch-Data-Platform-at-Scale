from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from src.monitoring.audit import write_audit_log
from src.spark_jobs.common import get_spark_session
from src.utils.config import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _silver(spark, table_name: str):
    settings = get_settings()
    return spark.read.parquet(str(settings.silver_dir / table_name))


def _write_gold(df, table_name: str, partition_column: str | None = None) -> None:
    settings = get_settings()
    writer = df.write.mode("overwrite")
    if partition_column:
        writer = writer.partitionBy(partition_column)
    writer.parquet(str(settings.gold_dir / table_name))


def build_gold_layer(batch_date: str) -> None:
    spark = get_spark_session("gold-transform")

    customers = _silver(spark, "customers")
    sellers = _silver(spark, "sellers")
    products = _silver(spark, "products")
    orders = _silver(spark, "orders").filter(F.col("batch_date") == batch_date)
    order_items = _silver(spark, "order_items").filter(F.col("batch_date") == batch_date)
    payments = _silver(spark, "payments").filter(F.col("batch_date") == batch_date)
    deliveries = _silver(spark, "deliveries").filter(F.col("batch_date") == batch_date)
    inventory = _silver(spark, "inventory_snapshots").filter(F.col("batch_date") == batch_date)

    dim_customer = customers.select("customer_id", "customer_name", "phone_number", "email", "city", "region", "township", "segment", "created_at")
    dim_seller = sellers.select("seller_id", "seller_name", "city", "region", "seller_type", "rating", "created_at")
    dim_product = products.select("product_id", "seller_id", "product_name", "category", "brand", "unit_price_mmk", "is_active", "created_at")
    dim_region = customers.select("region", "city").unionByName(sellers.select("region", "city")).dropDuplicates().withColumn(
        "region_key", F.abs(F.hash(F.concat_ws("|", "region", "city")))
    )

    target_date = datetime.strptime(batch_date, "%Y-%m-%d").date().isoformat()
    dim_date = (
        spark.createDataFrame([(target_date,)], ["calendar_date"])
        .withColumn("calendar_date", F.to_date("calendar_date"))
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("day", F.dayofmonth("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
    )

    fact_orders = orders.join(dim_region, ["region", "city"], "left").select(
        "order_id",
        "customer_id",
        "region_key",
        F.to_date("order_timestamp").alias("order_date"),
        "order_timestamp",
        "order_status",
        "payment_method",
        "shipping_fee_mmk",
        "subtotal_amount_mmk",
        "order_total_mmk",
        "item_count",
        "total_quantity",
        "batch_date",
    )
    fact_order_items = order_items.join(products.select("product_id", "category", "brand"), "product_id", "left").select(
        "order_item_id",
        "order_id",
        "product_id",
        "seller_id",
        "category",
        "brand",
        "line_number",
        "quantity",
        "unit_price_mmk",
        "gross_line_amount_mmk",
        "discount_amount_mmk",
        "net_line_amount_mmk",
        "batch_date",
    )
    fact_payments = payments.select(
        "payment_id",
        "order_id",
        "customer_id",
        "payment_method",
        "payment_status",
        "payment_amount_mmk",
        F.to_date("paid_at").alias("payment_date"),
        "paid_at",
        "batch_date",
    )
    fact_inventory_snapshot = inventory.select(
        "inventory_snapshot_id",
        "product_id",
        "seller_id",
        "snapshot_date",
        "available_quantity",
        "reserved_quantity",
        "inventory_status",
        "reorder_threshold",
        "batch_date",
    )
    fact_delivery_performance = deliveries.select(
        "delivery_id",
        "order_id",
        "delivery_provider",
        "region",
        "city",
        "promised_delivery_date",
        "actual_delivery_date",
        "delivery_status",
        "delivery_fee_mmk",
        "delivery_delay_days",
        "batch_date",
    )

    _write_gold(dim_customer, "dim_customer")
    _write_gold(dim_seller, "dim_seller")
    _write_gold(dim_product, "dim_product")
    _write_gold(dim_region, "dim_region")
    _write_gold(dim_date, "dim_date")
    _write_gold(fact_orders, "fact_orders", "batch_date")
    _write_gold(fact_order_items, "fact_order_items", "batch_date")
    _write_gold(fact_payments, "fact_payments", "batch_date")
    _write_gold(fact_inventory_snapshot, "fact_inventory_snapshot", "batch_date")
    _write_gold(fact_delivery_performance, "fact_delivery_performance", "batch_date")

    try:
        write_audit_log(batch_date, "gold_transform", "SUCCESS", fact_orders.count())
    except Exception as exc:  # pragma: no cover
        logger.warning("Audit log skipped for gold_transform: %s", exc)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build gold layer outputs.")
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()
    build_gold_layer(args.batch_date)


if __name__ == "__main__":
    main()
