from __future__ import annotations

from sqlalchemy import create_engine, text

from src.utils.config import ROOT_DIR, get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


def initialize_warehouse() -> None:
    settings = get_settings()
    engine = create_engine(settings.sqlalchemy_url)
    schema_dir = ROOT_DIR / "sql" / "schema"
    with engine.begin() as connection:
        for path in sorted(schema_dir.glob("*.sql")):
            logger.info("Applying schema file %s", path.name)
            connection.execute(text(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    initialize_warehouse()
