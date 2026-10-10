from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Callable

from src.app.app import App
from src.app.reporter import PipelineReporter
from src.shared.observability.structured_logging import configure_logging


EXIT_CODES = {
    "SUCCESS": 0,
    "SUCCESS_WITH_REJECTIONS": 0,
    "PARTIAL_SUCCESS": 2,
    "FAILED": 1,
}
REPORT_SCHEMA_VERSION = "1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the AdventureWorks analytics pipeline"
    )
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument(
        "--stage", choices=("full", "bronze", "silver", "gold"), default="full"
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="logging verbosity",
    )
    parser.add_argument("--recovery-snapshot", type=Path)
    parser.add_argument("--source-snapshot-id")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--report-format", choices=("json", "markdown"), default="json")
    return parser


def main(
    argv: list[str] | None = None,
    app_factory: Callable[[], App] = App,
    reporter_factory: Callable[[], PipelineReporter] = PipelineReporter,
) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(level=getattr(logging, args.log_level))
    recovery = None
    if args.recovery_snapshot:
        try:
            recovery = json.loads(args.recovery_snapshot.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return _delivery_failure_code(
                "FAILED", f"invalid recovery snapshot: {type(exc).__name__}"
            )
    result = app_factory().pipeline_runner.run(
        mode=args.mode,
        stage=args.stage,
        recovery_snapshot=recovery,
        source_snapshot_id=args.source_snapshot_id,
    )
    if args.report:
        report_paths = result.get("report_paths")
        if report_paths is None:
            report_paths = []
            result["report_paths"] = report_paths
        if not isinstance(report_paths, list):
            return _delivery_failure_code(
                "FAILED", "report delivery failed: report_paths is not a list"
            )
        report_paths.append(str(args.report))
        result["report_schema_version"] = REPORT_SCHEMA_VERSION
        result["report_format"] = args.report_format
        try:
            reporter_factory().write(result, args.report, args.report_format)
        except (OSError, ValueError, TypeError) as exc:
            detail = f": {exc}" if isinstance(exc, TypeError) else ""
            return _delivery_failure_code(
                _result_status(result),
                f"report delivery failed: {type(exc).__name__}{detail}",
            )
    return EXIT_CODES.get(_result_status(result), 1)


def _result_status(result: dict[str, object]) -> str:
    status = result.get("status", "FAILED")
    return status if isinstance(status, str) else "FAILED"


def _delivery_failure_code(status: str, error: str) -> int:
    logging.error(error)
    return EXIT_CODES.get(status, 1) or 1


if __name__ == "__main__":
    raise SystemExit(main())
