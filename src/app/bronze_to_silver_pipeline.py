from __future__ import annotations

import inspect
from datetime import datetime
from typing import Any
from uuid import uuid4

from src.app.bronze_snapshot_gate import BronzeSnapshotGate
from src.core.settings import Settings, get_settings
from src.features.Person.jobs.person_bronze_job import PersonBronzeJob
from src.features.Production.jobs.production_bronze_job import ProductionBronzeJob
from src.features.Sales_Performance.jobs.sales_bronze_ingestion_job import (
    SalesBronzeIngestionJob,
)
from src.features.Sales_Performance.jobs.sales_silver_job import SalesSilverJob


class BronzeToSilverPipeline:
    """Run the complete Bronze dependency set, then Silver on one snapshot."""

    allowed_silver_statuses = {"SUCCESS", "SUCCESS_WITH_REJECTIONS"}

    def __init__(
        self,
        bronze_jobs: tuple[Any, ...] | None = None,
        silver_job: Any | None = None,
        snapshot_gate: BronzeSnapshotGate | None = None,
        settings: Settings | None = None,
    ):
        self.settings = settings or get_settings()
        self.bronze_jobs = bronze_jobs or (
            SalesBronzeIngestionJob(self.settings),
            PersonBronzeJob(self.settings),
            ProductionBronzeJob(self.settings),
        )
        self.silver_job = silver_job or SalesSilverJob(settings=self.settings)
        self.snapshot_gate = snapshot_gate or BronzeSnapshotGate()

    def run(
        self,
        mode: str = "full",
        stage: str = "full",
        load_date: datetime | None = None,
        resume_load_ids: dict[str, str] | None = None,
        recovery_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        snapshot_id = str(uuid4())
        recovery = recovery_snapshot is not None
        if recovery:
            snapshot_id = str(recovery_snapshot.get("snapshot_id", ""))
            bronze_result = recovery_snapshot.get("bronze", {})
            if not snapshot_id or not isinstance(bronze_result, dict):
                return self._failed_result(
                    snapshot_id,
                    "Invalid recovery snapshot: snapshot_id and bronze results are required",
                )
        else:
            bronze_result = self._run_bronze_jobs(mode, load_date, resume_load_ids)

        bronze_result = self._annotate_snapshot(bronze_result, snapshot_id)
        gate_result = self.snapshot_gate.validate(bronze_result, snapshot_id)
        if gate_result["status"] != "SUCCESS":
            return {
                "status": "FAILED",
                "snapshot_id": snapshot_id,
                "recovery": recovery,
                "bronze": bronze_result,
                "bronze_gate": gate_result,
                "silver": None,
            }

        if stage == "bronze":
            return {
                "status": self._bronze_status(bronze_result),
                "snapshot_id": snapshot_id,
                "recovery": recovery,
                "bronze": bronze_result,
                "bronze_gate": gate_result,
                "silver": None,
            }

        silver_result = self._run_silver(snapshot_id)
        silver_identity = self._validate_silver_identity(silver_result, snapshot_id)
        silver_ok = self._silver_succeeded(silver_result) and silver_identity["status"] == "SUCCESS"
        return {
            "status": self._silver_status(silver_result) if silver_ok else "FAILED",
            "snapshot_id": snapshot_id,
            "recovery": recovery,
            "bronze": bronze_result,
            "bronze_gate": gate_result,
            "silver_identity": silver_identity,
            "silver": silver_result,
        }

    @staticmethod
    def _bronze_status(bronze_result):
        statuses = [
            result.get("status")
            for result in bronze_result.values()
            if isinstance(result, dict)
        ]
        if any(status not in {"SUCCESS", "SUCCESS_WITH_REJECTIONS"} for status in statuses):
            return "FAILED"
        return "SUCCESS_WITH_REJECTIONS" if "SUCCESS_WITH_REJECTIONS" in statuses else "SUCCESS"

    @staticmethod
    def _silver_status(silver_result):
        results = (
            [silver_result]
            if "status" in silver_result
            else [item for item in silver_result.values() if isinstance(item, dict)]
        )
        return (
            "SUCCESS_WITH_REJECTIONS"
            if any(item.get("status") == "SUCCESS_WITH_REJECTIONS" for item in results)
            else "SUCCESS"
        )

    def _run_bronze_jobs(self, mode, load_date, resume_load_ids):
        combined: dict[str, dict] = {}
        for job in self.bronze_jobs:
            result = self._invoke_bronze(job, mode, load_date, resume_load_ids)
            if isinstance(result, dict):
                combined.update(result)
        return combined

    @staticmethod
    def _invoke_bronze(job, mode, load_date, resume_load_ids):
        if resume_load_ids and hasattr(job, "resume"):
            return job.resume(resume_load_ids, mode=mode, load_date=load_date)
        run = job.run
        parameters = inspect.signature(run).parameters
        kwargs = {"mode": mode, "load_date": load_date}
        if "resume_load_ids" in parameters:
            kwargs["resume_load_ids"] = resume_load_ids
        return run(**kwargs)

    def _run_silver(self, snapshot_id):
        run = self.silver_job.run
        parameters = inspect.signature(run).parameters
        if "run_id" in parameters and "load_id" in parameters:
            return run(run_id=snapshot_id, load_id=snapshot_id)
        return run()

    @staticmethod
    def _annotate_snapshot(bronze_result, snapshot_id):
        annotated = {}
        for target, result in bronze_result.items():
            if isinstance(result, dict):
                annotated[target] = {
                    **result,
                    "snapshot_id": result.get("snapshot_id", snapshot_id),
                    "source_run_id": result.get("run_id"),
                    "source_load_id": result.get("load_id"),
                }
            else:
                annotated[target] = result
        return annotated

    @staticmethod
    def _validate_silver_identity(silver_result, snapshot_id):
        failures = []
        if not isinstance(silver_result, dict):
            failures.append("Silver result is not a mapping")
            return {"status": "FAILED", "snapshot_id": snapshot_id, "failures": failures}
        results = (
            [silver_result]
            if "status" in silver_result
            else [item for item in silver_result.values() if isinstance(item, dict)]
        )
        if not results:
            failures.append("Silver returned no table results")
        for result in results:
            if result.get("run_id") != snapshot_id:
                failures.append(
                    f"Silver run_id mismatch: expected={snapshot_id}, "
                    f"found={result.get('run_id')}"
                )
            if result.get("load_id") != snapshot_id:
                failures.append(
                    f"Silver load_id mismatch: expected={snapshot_id}, "
                    f"found={result.get('load_id')}"
                )
        return {
            "status": "SUCCESS" if not failures else "FAILED",
            "snapshot_id": snapshot_id,
            "failures": failures,
            "table_count": len(results),
        }

    def _silver_succeeded(self, silver_result) -> bool:
        if not isinstance(silver_result, dict):
            return False
        results = (
            [silver_result]
            if "status" in silver_result
            else [item for item in silver_result.values() if isinstance(item, dict)]
        )
        return bool(results) and all(
            item.get("status") in self.allowed_silver_statuses for item in results
        )

    @staticmethod
    def _failed_result(snapshot_id: str, message: str) -> dict[str, object]:
        return {
            "status": "FAILED",
            "snapshot_id": snapshot_id,
            "recovery": True,
            "bronze": {},
            "bronze_gate": {"status": "FAILED", "failures": [message]},
            "silver_identity": None,
            "silver": None,
        }
