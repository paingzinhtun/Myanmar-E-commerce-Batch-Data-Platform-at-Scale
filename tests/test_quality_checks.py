from pyspark.sql import SparkSession

from src.validation.quality_checks import require_no_duplicates, require_positive


def test_require_no_duplicates_detects_duplicate_rows():
    spark = SparkSession.builder.master("local[1]").appName("test-quality").getOrCreate()
    df = spark.createDataFrame([("ORD1", "C1"), ("ORD1", "C1")], ["order_id", "customer_id"])
    result = require_no_duplicates(df, ["order_id"], "duplicate_orders")
    assert result.passed is False
    assert result.failed_rows == 1
    spark.stop()


def test_require_positive_passes_for_valid_amounts():
    spark = SparkSession.builder.master("local[1]").appName("test-quality").getOrCreate()
    df = spark.createDataFrame([(1000.0,), (2500.0,)], ["payment_amount_mmk"])
    result = require_positive(df, "payment_amount_mmk", "payments_positive")
    assert result.passed is True
    spark.stop()
