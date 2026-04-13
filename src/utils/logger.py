from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.utils.config import get_settings


def get_logger(name: str) -> logging.Logger:
    settings = get_settings()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(settings.log_level.upper())
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    log_file = Path(settings.log_dir) / "platform.log"
    file_handler = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=5)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger
