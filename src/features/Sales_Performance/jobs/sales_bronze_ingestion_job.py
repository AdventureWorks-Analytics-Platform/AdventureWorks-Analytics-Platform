from datetime import datetime
from typing import Dict, Optional

from src.core.settings import Settings
from src.features.Sales_Performance.jobs.sales_bronze_job import (
    SALES_TABLE_SPECS,
    SalesBronzeJob,
)


class SalesBronzeIngestionJob:
    """Compatibility wrapper for the canonical Sales Bronze job."""

    def __init__(self, settings: Settings | None = None):
        self._job = SalesBronzeJob(settings=settings)

    def run(
        self, mode: str = "full", load_date: Optional[datetime] = None
    ) -> Dict[str, Dict]:
        return self._job.run(mode=mode, load_date=load_date)

    def resume(
        self,
        resume_load_ids: dict[str, str],
        mode: str = "full",
        load_date: Optional[datetime] = None,
    ) -> Dict[str, Dict]:
        return self._job.run(
            mode=mode,
            load_date=load_date,
            resume_load_ids=resume_load_ids,
        )


__all__ = ["SALES_TABLE_SPECS", "SalesBronzeIngestionJob"]
