from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame, functions as F


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    failed_rows: int
    message: str


def require_non_null(df: DataFrame, columns: Iterable[str], check_name: str) -> ValidationResult:
    condition = None
    for column in columns:
        expr = F.col(column).isNull()
        condition = expr if condition is None else (condition | expr)
    failed_rows = df.filter(condition).count() if condition is not None else 0
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Null check on {list(columns)}")


def require_no_duplicates(df: DataFrame, columns: Iterable[str], check_name: str) -> ValidationResult:
    failed_rows = df.groupBy(*columns).count().filter(F.col("count") > 1).count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Duplicate check on {list(columns)}")


def require_positive(df: DataFrame, column: str, check_name: str) -> ValidationResult:
    failed_rows = df.filter(F.col(column) <= 0).count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Positive check on {column}")


def require_non_negative(df: DataFrame, column: str, check_name: str) -> ValidationResult:
    failed_rows = df.filter(F.col(column) < 0).count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Non-negative check on {column}")


def require_valid_timestamp(df: DataFrame, column: str, check_name: str) -> ValidationResult:
    failed_rows = df.filter(F.col(column).isNull()).count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Timestamp validity check on {column}")


def require_references(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str,
    dim_key: str,
    check_name: str,
) -> ValidationResult:
    failed_rows = fact_df.join(dim_df, fact_df[fact_key] == dim_df[dim_key], "left_anti").count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Reference check for {fact_key}")


def require_amount_match(df: DataFrame, left_col: str, right_col: str, tolerance: float, check_name: str) -> ValidationResult:
    failed_rows = df.filter(F.abs(F.col(left_col) - F.col(right_col)) > tolerance).count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Amount reconciliation for {left_col} and {right_col}")


def require_allowed_pairs(
    df: DataFrame,
    left_col: str,
    right_col: str,
    allowed_pairs: list[tuple[str, str]],
    check_name: str,
) -> ValidationResult:
    allowed_df = df.sparkSession.createDataFrame(allowed_pairs, [left_col, right_col])
    failed_rows = df.select(left_col, right_col).dropDuplicates().join(allowed_df, [left_col, right_col], "left_anti").count()
    return ValidationResult(check_name, failed_rows == 0, failed_rows, f"Allowed pair validation for {left_col} and {right_col}")


def raise_for_failed_checks(results: list[ValidationResult]) -> None:
    failed = [result for result in results if not result.passed]
    if failed:
        formatted = ", ".join(f"{item.check_name}({item.failed_rows})" for item in failed)
        raise ValueError(f"Data quality checks failed: {formatted}")
