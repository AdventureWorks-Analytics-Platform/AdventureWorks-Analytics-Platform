from __future__ import annotations

from datetime import datetime, timezone
import inspect
from time import perf_counter
from typing import Any
from uuid import uuid4


class PipelineRunner:
    """Coordinate the canonical Bronze-to-Gold pipeline stages and gates."""

    stage_order = ("settings", "health", "bootstrap", "bronze", "silver", "gold")

    def __init__(
        self,
        health_service: Any,
        bootstrap_job: Any,
        bronze_to_silver_pipeline: Any,
        gold_pipeline: Any | None = None,
        settings: Any | None = None,
        reporter: Any | None = None,
        clock=perf_counter,
    ):
        self.health_service = health_service
        self.bootstrap_job = bootstrap_job
        self.bronze_to_silver_pipeline = bronze_to_silver_pipeline
        self.gold_pipeline = gold_pipeline
        self.settings = settings
        self.reporter = reporter
        self.clock = clock

    valid_modes = {"full", "incremental"}
    valid_stages = {"full", "bronze", "silver", "gold"}
    success_statuses = {"SUCCESS", "SUCCESS_WITH_REJECTIONS"}

    def run(
        self,
        mode: str = "full",
        stage: str = "full",
        recovery_snapshot: dict[str, object] | None = None,
        source_snapshot_id: str | None = None,
    ) -> dict[str, object]:
        started_at = datetime.now(timezone.utc)
        started_clock = self.clock()
        run_id = str(uuid4())
        requested_stages = ["bronze", "silver"] if stage == "full" else [stage]
        if stage == "full" and self.gold_pipeline is not None:
            requested_stages.append("gold")
        if mode not in self.valid_modes or stage not in self.valid_stages:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                    health={"status": "ok"}, bootstrap=None, pipeline=None, gold=None,
                failed_stage="configuration", status="FAILED",
                error=f"unsupported mode/stage: mode={mode}, stage={stage}",
            )
        if stage == "silver" and not recovery_snapshot:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health={"status": "ok"}, bootstrap=None, pipeline=None, gold=None,
                failed_stage="configuration", status="FAILED",
                error="standalone silver requires recovery_snapshot",
            )
        if stage == "gold" and not source_snapshot_id:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health={"status": "ok"}, bootstrap=None, pipeline=None, gold=None,
                failed_stage="configuration", status="FAILED",
                error="standalone gold requires source_snapshot_id",
            )
        if stage == "gold" and self.gold_pipeline is None:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health={"status": "ok"}, bootstrap=None, pipeline=None, gold=None,
                failed_stage="configuration", status="FAILED",
                error="gold stage is not configured",
            )
        settings_check = None
        validate_settings = getattr(self.health_service, "validate_settings", None)
        if callable(validate_settings):
            settings_check = validate_settings()
            if settings_check.get("status") != "ok":
                return self._result(
                    run_id, mode, requested_stages, started_at, started_clock,
                    health={"status": "not_checked", "phase": "pre_bootstrap"},
                    bootstrap=None, pipeline=None, gold=None,
                    failed_stage="settings", status="FAILED",
                    error=self._readiness_error(settings_check),
                    settings=settings_check,
                )
        health = self.health_service.check_all()
        if health.get("status") != "ok":
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=None, pipeline=None,
                gold=None,
                failed_stage="health", status="FAILED", settings=settings_check,
            )

        bootstrap = self.bootstrap_job.run()
        readiness = bootstrap.get("readiness")
        post_bootstrap_failed = isinstance(readiness, dict) and readiness.get("status") != "ready"
        if bootstrap.get("status") != "ok" or post_bootstrap_failed:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=bootstrap, pipeline=None,
                gold=None,
                failed_stage="bootstrap", status="FAILED", settings=settings_check,
            )

        pipeline = None
        if stage in {"full", "bronze", "silver"}:
            pipeline = self._run_stage(
                mode, stage, recovery_snapshot, self.bronze_to_silver_pipeline
            )
        if pipeline is not None and pipeline.get("status") not in self.success_statuses:
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=bootstrap, pipeline=pipeline,
                gold=None,
                failed_stage="silver" if stage == "full" else stage,
                status="FAILED", settings=settings_check,
            )

        gold = None
        if stage in {"full", "gold"} and self.gold_pipeline is not None:
            gold = self._run_gold(
                self.gold_pipeline,
                source_snapshot_id or run_id,
                pipeline or {"snapshot_id": source_snapshot_id},
            )
            if gold.get("status") not in self.success_statuses:
                return self._result(
                    run_id, mode, requested_stages, started_at, started_clock,
                    health=health, bootstrap=bootstrap, pipeline=pipeline,
                    gold=gold,
                    failed_stage="gold", status="FAILED", settings=settings_check,
                )

        return self._result(
            run_id, mode, requested_stages, started_at, started_clock,
            health=health, bootstrap=bootstrap, pipeline=pipeline,
            gold=gold,
            failed_stage=None, status=self._overall_status(pipeline, gold), settings=settings_check,
        )

    @staticmethod
    def _readiness_error(report):
        failures = [
            check.get("reason", check.get("name", "readiness check failed"))
            for check in report.get("checks", [])
            if check.get("status") != "ok"
        ]
        return "; ".join(failures) or report.get("reason", "readiness check failed")

    @staticmethod
    def _run_stage(mode, stage, recovery_snapshot, pipeline):
        parameters = inspect.signature(pipeline.run).parameters
        accepts_any = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        kwargs = {"mode": mode}
        if "stage" in parameters or accepts_any:
            kwargs["stage"] = stage
        if "recovery_snapshot" in parameters or accepts_any:
            kwargs["recovery_snapshot"] = recovery_snapshot
        return pipeline.run(**kwargs)

    @classmethod
    def _overall_status(cls, pipeline, gold):
        statuses = []
        for result in (pipeline, gold):
            if isinstance(result, dict):
                statuses.append(result.get("status"))
        return "SUCCESS_WITH_REJECTIONS" if "SUCCESS_WITH_REJECTIONS" in statuses else "SUCCESS"

    @staticmethod
    def _run_gold(gold_pipeline, pipeline_snapshot_id, pipeline):
        run = gold_pipeline.run
        parameters = inspect.signature(run).parameters
        kwargs = {}
        accepts_any = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        if "pipeline_snapshot_id" in parameters or accepts_any:
            kwargs["pipeline_snapshot_id"] = pipeline_snapshot_id
        if "silver_result" in parameters or accepts_any:
            kwargs["silver_result"] = {
                "snapshot_id": pipeline.get("snapshot_id"),
                "silver": pipeline.get("silver", {}),
            }
        return run(**kwargs) if kwargs else run()

    def _result(
        self,
        run_id,
        mode,
        requested_stages,
        started_at,
        started_clock,
        *,
        health,
        bootstrap,
        pipeline,
        gold,
        failed_stage,
        status=None,
        error=None,
        settings=None,
    ) -> dict[str, object]:
        finished_at = datetime.now(timezone.utc)
        pipeline = pipeline or {}
        status = status or ("SUCCESS" if failed_stage is None else "FAILED")
        result = {
            "run_id": run_id,
            "pipeline_name": "adventureworks",
            "mode": mode,
            "requested_stages": requested_stages,
            "status": status,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": max(0, int((self.clock() - started_clock) * 1000)),
            "failed_stage": failed_stage,
            "error": error,
            "error_message": error,
            "error_type": self._error_type(error, failed_stage),
            "settings": settings,
            "health": health,
            "bootstrap": bootstrap,
            "bronze": pipeline.get("bronze"),
            "bronze_gate": pipeline.get("bronze_gate"),
            "snapshot_id": pipeline.get("snapshot_id"),
            "silver": pipeline.get("silver"),
            "gold": gold if gold is not None else {"status": "NOT_REQUESTED"},
            "stages": self._stage_results(
                settings, health, bootstrap, pipeline, gold, failed_stage
            ),
            "counts": self._aggregate_counts(pipeline, gold),
            "report_paths": [],
        }
        result.update(result["counts"])
        if self.reporter is not None and hasattr(self.reporter, "attach"):
            self.reporter.attach(result)
        return result

    @classmethod
    def _stage_results(cls, settings, health, bootstrap, pipeline, gold, failed_stage):
        results = []
        for stage, value in (("settings", settings), ("health", health), ("bootstrap", bootstrap)):
            if value is None:
                continue
            status = value.get("status") if isinstance(value, dict) else None
            if stage == "settings" and status == "ok":
                status = "SUCCESS"
            elif stage in {"health", "bootstrap"} and status == "ok":
                status = "SUCCESS"
            results.append(cls._stage_entry(stage, status, value, failed_stage))
        if isinstance(pipeline, dict):
            results.append(
                cls._stage_entry(
                    "bronze",
                    cls._nested_status(pipeline.get("bronze")),
                    pipeline.get("bronze"),
                    failed_stage,
                )
            )
            results.append(
                cls._stage_entry(
                    "silver",
                    cls._nested_status(pipeline.get("silver"))
                    if pipeline.get("silver") is not None
                    else pipeline.get("status"),
                    pipeline.get("silver"),
                    failed_stage,
                )
            )
        if gold is not None:
            results.append(cls._stage_entry("gold", gold.get("status"), gold, failed_stage))
        return results

    @staticmethod
    def _stage_entry(stage, status, value, failed_stage):
        if status == "ok":
            status = "SUCCESS"
        return {
            "stage": stage,
            "status": status or "NOT_REQUESTED",
            "failed": stage == failed_stage,
            "result": value,
        }

    @classmethod
    def _nested_status(cls, value):
        if not isinstance(value, dict):
            return None
        if "status" in value:
            return value["status"]
        statuses = [
            item.get("status")
            for item in value.values()
            if isinstance(item, dict) and item.get("status") is not None
        ]
        if not statuses:
            return None
        if any(status not in cls.success_statuses for status in statuses):
            return "FAILED"
        if "SUCCESS_WITH_REJECTIONS" in statuses:
            return "SUCCESS_WITH_REJECTIONS"
        return "SUCCESS"

    @classmethod
    def _aggregate_counts(cls, pipeline, gold):
        counts = {"rows_read": 0, "rows_written": 0, "rows_rejected": 0}
        for result in (pipeline, gold):
            cls._sum_counts(result, counts)
        return counts

    @classmethod
    def _sum_counts(cls, value, counts):
        if isinstance(value, dict):
            for key in counts:
                numeric = value.get(key)
                if isinstance(numeric, (int, float)):
                    counts[key] += numeric
            for nested in value.values():
                if isinstance(nested, (dict, list, tuple)):
                    cls._sum_counts(nested, counts)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                cls._sum_counts(nested, counts)

    @staticmethod
    def _error_type(error, failed_stage):
        if not error:
            return None
        return f"{failed_stage.title()}Error" if failed_stage else "PipelineError"
