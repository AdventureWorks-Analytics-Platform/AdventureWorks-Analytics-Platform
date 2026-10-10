"""Structured JSON logging for pipeline and adapter events."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from src.shared.security.log_redaction import redact_log_message


_EVENT_FIELDS = frozenset(
    {
        "run_id",
        "load_id",
        "batch_id",
        "stage",
        "source_table",
        "target_table",
        "attempt",
        "status",
        "duration_ms",
        "retry_delay_ms",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "rows_deduplicated",
        "table_count",
        "pipeline_name",
        "mode",
        "failed_stage",
        "error_type",
        "snapshot_id",
        "source_snapshot_id",
        "gold_run_id",
        "gold_version",
        "adapter",
        "operation",
    }
)


def emit_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    """Emit one JSON event containing only approved operational context."""
    unknown_fields = set(fields).difference(_EVENT_FIELDS)
    if unknown_fields:
        raise ValueError(
            "Unsupported structured log fields: " + ", ".join(sorted(unknown_fields))
        )

    payload: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": logging.getLevelName(level),
        "event": event,
    }
    for name, value in fields.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = redact_log_message(value)
        elif not isinstance(value, (bool, int, float)):
            raise TypeError(f"Structured log field {name!r} must be scalar")
        payload[name] = value
    logger.log(level, json.dumps(payload, sort_keys=True, separators=(",", ":")))


class JsonLineFormatter(logging.Formatter):
    """Render every record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            payload = {
                "timestamp": datetime.fromtimestamp(
                    record.created, timezone.utc
                ).isoformat(),
                "event": "log.message",
                "logger": record.name,
                "message": redact_log_message(message),
            }
        if not isinstance(payload, dict):
            payload = {
                "timestamp": datetime.fromtimestamp(
                    record.created, timezone.utc
                ).isoformat(),
                "event": "log.message",
                "logger": record.name,
                "message": redact_log_message(message),
            }
        payload["level"] = record.levelname
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def configure_logging(
    level: int = logging.INFO,
    log_directory: str | Path = "logs",
) -> None:
    """Configure JSON console and rotating-file handlers for command entrypoints."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    owned_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, "_adventureworks_json_handler", False)
    ]
    if owned_handlers:
        for handler in owned_handlers:
            handler.setLevel(level)
        return
    if root_logger.handlers:
        formatter = JsonLineFormatter()
        for handler in root_logger.handlers:
            handler.setLevel(level)
            handler.setFormatter(formatter)
        return

    formatter = JsonLineFormatter()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    setattr(console_handler, "_adventureworks_json_handler", True)

    directory = Path(log_directory)
    directory.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        directory / "adventureworks.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    setattr(file_handler, "_adventureworks_json_handler", True)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
