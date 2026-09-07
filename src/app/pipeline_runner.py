from __future__ import annotations

from datetime import datetime, timezone
import inspect
from time import perf_counter
from typing import Any
from uuid import uuid4


class PipelineRunner:
    """Coordinate the canonical Bronze-to-Gold pipeline stages and gates."""

    def __init__(
        self,
        health_service: Any,
        bootstrap_job: Any,
        bronze_to_silver_pipeline: Any,
        gold_pipeline: Any | None = None,
        clock=perf_counter,
    ):
        self.health_service = health_service
        self.bootstrap_job = bootstrap_job
        self.bronze_to_silver_pipeline = bronze_to_silver_pipeline
        self.gold_pipeline = gold_pipeline
        self.clock = clock

    def run(self, mode: str = "full") -> dict[str, object]:
        started_at = datetime.now(timezone.utc)
        started_clock = self.clock()
        run_id = str(uuid4())
        requested_stages = ["bronze", "silver"]
        if self.gold_pipeline is not None:
            requested_stages.append("gold")
        health = self.health_service.check_all()
        if health.get("status") != "ok":
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=None, pipeline=None,
                gold=None,
                failed_stage="health",
            )

        bootstrap = self.bootstrap_job.run()
        if bootstrap.get("status") != "ok":
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=bootstrap, pipeline=None,
                gold=None,
                failed_stage="bootstrap",
            )

        pipeline = self.bronze_to_silver_pipeline.run(mode=mode)
        if pipeline.get("status") != "SUCCESS":
            return self._result(
                run_id, mode, requested_stages, started_at, started_clock,
                health=health, bootstrap=bootstrap, pipeline=pipeline,
                gold=None,
                failed_stage="silver",
            )

        gold = None
        if self.gold_pipeline is not None:
            gold = self._run_gold(self.gold_pipeline, run_id, pipeline)
            if gold.get("status") != "SUCCESS":
                return self._result(
                    run_id, mode, requested_stages, started_at, started_clock,
                    health=health, bootstrap=bootstrap, pipeline=pipeline,
                    gold=gold,
                    failed_stage="gold",
                )

        return self._result(
            run_id, mode, requested_stages, started_at, started_clock,
            health=health, bootstrap=bootstrap, pipeline=pipeline,
            gold=gold,
            failed_stage=None,
        )

    @staticmethod
    def _run_gold(gold_pipeline, pipeline_snapshot_id, pipeline):
        run = gold_pipeline.run
        parameters = inspect.signature(run).parameters
        kwargs = {}
        if "pipeline_snapshot_id" in parameters:
            kwargs["pipeline_snapshot_id"] = pipeline_snapshot_id
        if "silver_result" in parameters:
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
    ) -> dict[str, object]:
        finished_at = datetime.now(timezone.utc)
        pipeline = pipeline or {}
        status = "SUCCESS" if failed_stage is None else "FAILED"
        return {
            "run_id": run_id,
            "pipeline_name": "adventureworks",
            "mode": mode,
            "requested_stages": requested_stages,
            "status": status,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": max(0, int((self.clock() - started_clock) * 1000)),
            "failed_stage": failed_stage,
            "health": health,
            "bootstrap": bootstrap,
            "bronze": pipeline.get("bronze"),
            "bronze_gate": pipeline.get("bronze_gate"),
            "snapshot_id": pipeline.get("snapshot_id"),
            "silver": pipeline.get("silver"),
            "gold": gold if gold is not None else {"status": "NOT_REQUESTED"},
            "report_paths": [],
        }
