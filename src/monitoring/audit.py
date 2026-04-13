from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine, text

from src.utils.config import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


def write_audit_log(
    batch_date: str,
    pipeline_stage: str,
    status: str,
    row_count: int | None = None,
    message: str | None = None,
) -> None:
    settings = get_settings()
    engine = create_engine(settings.sqlalchemy_url)
    sql = text(
        f"""
        INSERT INTO {settings.warehouse_audit_schema}.pipeline_run_audit
        (batch_date, pipeline_stage, status, row_count, message, created_at)
        VALUES (:batch_date, :pipeline_stage, :status, :row_count, :message, :created_at)
        """
    )
    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "batch_date": batch_date,
                "pipeline_stage": pipeline_stage,
                "status": status,
                "row_count": row_count,
                "message": message,
                "created_at": datetime.utcnow(),
            },
        )
    logger.info("Audit logged for stage=%s batch_date=%s status=%s", pipeline_stage, batch_date, status)
