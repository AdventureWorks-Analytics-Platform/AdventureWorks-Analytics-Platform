from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np


class PipelineReporter:
    """Persist runner results at the delivery boundary."""

    formats = {"json", "markdown"}

    @classmethod
    def render(cls, result: dict[str, Any], fmt: str) -> str:
        if fmt == "json":
            return json.dumps(result, indent=2, sort_keys=True, default=_json_default)
        if fmt == "markdown":
            return cls.to_markdown(result)
        raise ValueError(f"unsupported report format: {fmt}")

    def write(
        self, result: dict[str, Any], output: str | Path, fmt: str = "json"
    ) -> str:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(result, fmt), encoding="utf-8")
        return str(path)

    @staticmethod
    def to_markdown(result: dict[str, Any]) -> str:
        lines = [
            "# AdventureWorks Pipeline Report",
            "",
            f"- Run ID: `{result.get('run_id', '')}`",
            f"- Status: **{result.get('status', 'UNKNOWN')}**",
            f"- Mode: `{result.get('mode', '')}`",
            f"- Stages: `{', '.join(result.get('requested_stages', []))}`",
            f"- Failed stage: `{result.get('failed_stage') or 'none'}`",
            f"- Duration: `{result.get('duration_ms', 0)} ms`",
            "",
        ]
        counts = result.get("counts") or {}
        lines.extend(
            [
                "## Counts",
                "",
                f"- Rows read: `{counts.get('rows_read', 0)}`",
                f"- Rows written: `{counts.get('rows_written', 0)}`",
                f"- Rows rejected: `{counts.get('rows_rejected', 0)}`",
                "",
            ]
        )
        if result.get("gold"):
            gold = result["gold"]
            lines.extend(
                [
                    "## Gold",
                    "",
                    f"- Gold status: **{gold.get('status', 'UNKNOWN')}**",
                    f"- Gold version: `{gold.get('gold_version', '')}`",
                    f"- Published: `{gold.get('published', '')}`",
                    "",
                ]
            )
        if result.get("error"):
            lines.extend([f"Error: {result['error']}", ""])
        return "\n".join(lines)


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
