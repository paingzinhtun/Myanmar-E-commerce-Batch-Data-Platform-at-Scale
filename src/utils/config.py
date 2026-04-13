from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


class Settings(BaseSettings):
    project_name: str = Field(default="myanmar-ecommerce-batch-platform", alias="PROJECT_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    data_root: Path = Field(default=ROOT_DIR / "data", alias="DATA_ROOT")
    raw_data_dir: Path = Field(default=ROOT_DIR / "data" / "raw", alias="RAW_DATA_DIR")
    sample_generation_dir: Path = Field(default=ROOT_DIR / "data" / "sample_generation", alias="SAMPLE_GENERATION_DIR")
    data_lake_root: Path = Field(default=ROOT_DIR / "data_lake", alias="DATA_LAKE_ROOT")
    bronze_dir: Path = Field(default=ROOT_DIR / "data_lake" / "bronze", alias="BRONZE_DIR")
    silver_dir: Path = Field(default=ROOT_DIR / "data_lake" / "silver", alias="SILVER_DIR")
    gold_dir: Path = Field(default=ROOT_DIR / "data_lake" / "gold", alias="GOLD_DIR")
    log_dir: Path = Field(default=ROOT_DIR / "logs", alias="LOG_DIR")

    spark_app_name: str = Field(default="MyanmarEcommerceBatchPlatform", alias="SPARK_APP_NAME")
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_warehouse_dir: Path = Field(default=ROOT_DIR / "spark-warehouse", alias="SPARK_WAREHOUSE_DIR")
    default_batch_date: str = Field(default="2026-04-12", alias="DEFAULT_BATCH_DATE")
    default_row_scale: int = Field(default=5000, alias="DEFAULT_ROW_SCALE")

    warehouse_host: str = Field(default="localhost", alias="WAREHOUSE_HOST")
    warehouse_port: int = Field(default=5432, alias="WAREHOUSE_PORT")
    warehouse_db: str = Field(default="myanmar_commerce", alias="WAREHOUSE_DB")
    warehouse_user: str = Field(default="postgres", alias="WAREHOUSE_USER")
    warehouse_password: str = Field(default="postgres", alias="WAREHOUSE_PASSWORD")
    warehouse_schema: str = Field(default="analytics", alias="WAREHOUSE_SCHEMA")
    warehouse_audit_schema: str = Field(default="monitoring", alias="WAREHOUSE_AUDIT_SCHEMA")

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.warehouse_user}:{self.warehouse_password}"
            f"@{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    for path in (
        settings.data_root,
        settings.raw_data_dir,
        settings.sample_generation_dir,
        settings.data_lake_root,
        settings.bronze_dir,
        settings.silver_dir,
        settings.gold_dir,
        settings.log_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return settings
