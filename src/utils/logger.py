import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler

from src.shared.observability.structured_logging import JsonLineFormatter


LOG_DIR = Path("logs")


def setup_logging(name: str) -> logging.Logger:
    """Set up idempotent JSON console and rotating-file logging for one logger."""
    logger = logging.getLogger(name)
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO"))
    logger.setLevel(level)
    if any(
        getattr(handler, "_adventureworks_json_handler", False)
        for handler in logger.handlers
    ):
        return logger

    formatter = JsonLineFormatter()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    setattr(console_handler, "_adventureworks_json_handler", True)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        LOG_DIR / "adventureworks.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    setattr(file_handler, "_adventureworks_json_handler", True)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    return logger
