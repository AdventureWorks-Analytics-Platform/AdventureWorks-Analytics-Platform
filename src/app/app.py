import json
import logging

from src.jobs.platform_bootstrap import PlatformBootstrapJob
from src.features.Sales_Performance.jobs.sales_bronze_ingestion_job import SalesBronzeIngestionJob
from src.features.Person.jobs.person_bronze_job import PersonBronzeJob
from src.features.Production.jobs.production_bronze_job import ProductionBronzeJob
from src.app.bronze_to_silver_pipeline import BronzeToSilverPipeline
from src.app.pipeline_runner import PipelineRunner
from src.shared.services.connection_health_service import ConnectionHealthService
from src.shared.security.log_redaction import redact_log_message
from src.core.settings import Settings, get_settings


class SilverGoldPipeline:
    """Run Gold only after every Silver table passes its publication gate."""

    _allowed_silver_statuses = {"SUCCESS", "SUCCESS_WITH_REJECTIONS"}

    def __init__(self, silver_job, gold_job, logger=None):
        self.silver_job = silver_job
        self.gold_job = gold_job
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def _table_results(silver_result):
        if not isinstance(silver_result, dict):
            return ()
        if "status" in silver_result:
            return (silver_result,)
        return tuple(
            result for result in silver_result.values() if isinstance(result, dict)
        )

    def _summary(self, silver_result, status, gold_called):
        table_results = self._table_results(silver_result)
        return {
            "event": "silver_gold_pipeline",
            "stage": "silver_gate",
            "status": status,
            "table_count": len(table_results),
            "rows_read": sum(result.get("rows_read", 0) for result in table_results),
            "rows_written": sum(result.get("rows_written", 0) for result in table_results),
            "rows_rejected": sum(result.get("rows_rejected", 0) for result in table_results),
            "rows_deduplicated": sum(
                result.get("rows_deduplicated", 0) for result in table_results
            ),
            "gold_called": gold_called,
        }

    def run(self):
        silver_result = self.silver_job.run()
        table_results = self._table_results(silver_result)
        silver_ok = bool(table_results) and all(
            result.get("status") in self._allowed_silver_statuses
            for result in table_results
        )
        gate_status = "SUCCESS" if silver_ok else "FAILED"
        summary = self._summary(silver_result, gate_status, False)
        self.logger.info(redact_log_message(json.dumps(summary, sort_keys=True)))

        if not silver_ok:
            return {
                "status": "FAILED",
                "silver": silver_result,
                "gold": None,
                "gold_called": False,
                "summary": summary,
            }

        gold_result = self.gold_job.run()
        summary = self._summary(silver_result, "SUCCESS", True)
        self.logger.info(redact_log_message(json.dumps(summary, sort_keys=True)))
        return {
            "status": "SUCCESS",
            "silver": silver_result,
            "gold": gold_result,
            "gold_called": True,
            "summary": summary,
        }


class App:
    """Application bootstrap and orchestration entry point."""

    def __init__(
        self,
        bootstrap_job=None,
        health_service=None,
        bronze_job=None,
        settings: Settings | None = None,
    ):
        self.settings = settings or get_settings()
        self.bootstrap_job = bootstrap_job or PlatformBootstrapJob()
        self.health_service = health_service or ConnectionHealthService(self.settings)
        self.bronze_job = bronze_job or SalesBronzeIngestionJob(self.settings)
        self.bronze_to_silver_pipeline = BronzeToSilverPipeline(
            bronze_jobs=(
                self.bronze_job,
                PersonBronzeJob(self.settings),
                ProductionBronzeJob(self.settings),
            ),
            settings=self.settings,
        )
        self.pipeline_runner = PipelineRunner(
            health_service=self.health_service,
            bootstrap_job=self.bootstrap_job,
            bronze_to_silver_pipeline=self.bronze_to_silver_pipeline,
        )
        self._running = False

    def run(self):
        """Run the application workflow using job and service classes."""
        self._running = True
        runner = getattr(self, "pipeline_runner", None)
        if runner is None:
            runner = PipelineRunner(
                health_service=self.health_service,
                bootstrap_job=self.bootstrap_job,
                bronze_to_silver_pipeline=self.bronze_to_silver_pipeline,
            )
        result = runner.run(mode="full")
        return {
            **result,
            "status": "ok" if result["status"] == "SUCCESS" else "degraded",
        }
