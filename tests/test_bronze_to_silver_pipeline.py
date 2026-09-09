from src.app.bronze_snapshot_gate import BronzeSnapshotGate, REQUIRED_BRONZE_TARGETS
from src.app.bronze_to_silver_pipeline import BronzeToSilverPipeline
from src.core.settings import Settings
from src.features.Person.jobs.person_bronze_job import PersonBronzeJob
from src.shared.ingestion.postgres_publish_service import (
    PostgresSilverDedupService,
    PostgresSilverStagingValidator,
)


class _FakeBronze:
    def __init__(self, result, name=None, events=None):
        self.result = result
        self.name = name
        self.events = events
        self.called = False

    def run(self, mode="full", load_date=None):
        self.called = True
        if self.events is not None:
            self.events.append(self.name)
        return self.result


class _FakeSilver:
    def __init__(self, result, events=None):
        self.result = result
        self.events = events
        self.called = False
        self.arguments = None

    def run(self, run_id=None, load_id=None):
        self.called = True
        self.arguments = (run_id, load_id)
        if self.events is not None:
            self.events.append("silver")
        return self.result(run_id) if callable(self.result) else self.result


def _bronze_result(
    targets=REQUIRED_BRONZE_TARGETS, status="SUCCESS", published=True
):
    return {
        target: {
            "run_id": f"run-{target}",
            "load_id": f"load-{target}",
            "status": status,
            "published": published,
            "rows_read": 1,
            "rows_written": 1,
        }
        for target in targets
    }


def _silver_result(snapshot_id, targets=REQUIRED_BRONZE_TARGETS):
    return {
        target.replace("bronze.", "silver_"): {
            "status": "SUCCESS",
            "run_id": snapshot_id,
            "load_id": snapshot_id,
        }
        for target in targets
    }


def test_snapshot_gate_requires_all_published_bronze_targets():
    gate = BronzeSnapshotGate()
    result = _bronze_result()
    result.pop("bronze.person")
    result["bronze.product"]["published"] = False

    gate_result = gate.validate(result, "snapshot-1")

    assert gate_result["status"] == "FAILED"
    assert any("bronze.person" in failure for failure in gate_result["failures"])
    assert any("bronze.product" in failure for failure in gate_result["failures"])


def test_bronze_failure_blocks_silver():
    failed_result = _bronze_result()
    failed_result["bronze.customer"]["status"] = "FAILED"
    bronze = _FakeBronze(failed_result)
    silver = _FakeSilver({"customer_clean": {"status": "SUCCESS"}})
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(bronze,),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run()

    assert result["status"] == "FAILED"
    assert silver.called is False
    assert bronze.called is True
    assert any(
        "bronze.customer" in failure
        for failure in result["bronze_gate"]["failures"]
    )


def test_successful_snapshot_calls_silver_with_snapshot_identity():
    bronze = _FakeBronze(_bronze_result())
    silver = _FakeSilver(lambda snapshot_id: _silver_result(snapshot_id))
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(bronze,),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run()

    assert result["status"] == "SUCCESS"
    assert silver.called is True
    assert silver.arguments == (result["snapshot_id"], result["snapshot_id"])


def test_recovery_snapshot_skips_bronze_and_runs_silver():
    bronze = _FakeBronze(_bronze_result())
    silver = _FakeSilver(lambda snapshot_id: _silver_result(snapshot_id))
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(bronze,),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run(
        recovery_snapshot={
            "snapshot_id": "snapshot-previous",
            "bronze": _bronze_result(),
        }
    )

    assert result["status"] == "SUCCESS"
    assert result["recovery"] is True
    assert bronze.called is False
    assert silver.arguments == ("snapshot-previous", "snapshot-previous")


def test_snapshot_gate_rejects_missing_real_source_identity():
    result = _bronze_result()
    result["bronze.customer"].pop("load_id")

    gate_result = BronzeSnapshotGate().validate(result, "snapshot-1")

    assert gate_result["status"] == "FAILED"
    assert any("source identity" in failure for failure in gate_result["failures"])


def test_silver_identity_mismatch_blocks_pipeline():
    bronze = _FakeBronze(_bronze_result())
    silver = _FakeSilver({
        "customer_clean": {
            "status": "SUCCESS",
            "run_id": "wrong-run",
            "load_id": "wrong-load",
        }
    })
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(bronze,), silver_job=silver, snapshot_gate=BronzeSnapshotGate()
    )

    result = pipeline.run()

    assert result["status"] == "FAILED"
    assert result["silver_identity"]["status"] == "FAILED"


def test_bronze_snapshot_mismatch_blocks_silver():
    bronze_result = _bronze_result()
    bronze_result["bronze.customer"]["snapshot_id"] = "snapshot-old"
    bronze = _FakeBronze(bronze_result)
    silver = _FakeSilver(lambda snapshot_id: _silver_result(snapshot_id))
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(bronze,),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run()

    assert result["status"] == "FAILED"
    assert silver.called is False
    assert any(
        "Bronze snapshot identity mismatch" in failure
        for failure in result["bronze_gate"]["failures"]
    )


def test_default_pipeline_includes_person_bronze_dependency():
    pipeline = BronzeToSilverPipeline()

    assert any(isinstance(job, PersonBronzeJob) for job in pipeline.bronze_jobs)


def _settings(**overrides):
    values = {
        "sql_server_host": "sql-host",
        "postgres_username": "warehouse-user",
        "postgres_password": "warehouse-secret",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


def test_default_silver_job_has_no_sql_dedup_wiring_when_flag_disabled():
    pipeline = BronzeToSilverPipeline(settings=_settings())

    assert pipeline.silver_job.dedup_service is None
    assert pipeline.silver_job.sql_validator is None


def test_default_silver_job_wires_sql_dedup_when_flag_enabled():
    pipeline = BronzeToSilverPipeline(settings=_settings(silver_sql_dedup_enabled=True))

    assert isinstance(pipeline.silver_job.dedup_service, PostgresSilverDedupService)
    assert isinstance(pipeline.silver_job.sql_validator, PostgresSilverStagingValidator)


def test_sales_and_person_bronze_complete_before_silver():
    events = []
    bronze_result = _bronze_result()
    sales = _FakeBronze(bronze_result, "sales", events)
    person = _FakeBronze({}, "person", events)
    production = _FakeBronze({}, "production", events)
    silver = _FakeSilver(lambda snapshot_id: _silver_result(snapshot_id), events)
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(sales, person, production),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run()

    assert result["status"] == "SUCCESS"
    assert events == ["sales", "person", "production", "silver"]


def test_full_bronze_to_silver_uses_one_snapshot_across_all_domains():
    sales_targets = REQUIRED_BRONZE_TARGETS[:5]
    production_targets = ("bronze.product",)
    person_targets = ("bronze.person",)
    sales = _FakeBronze(_bronze_result(sales_targets), "sales")
    production = _FakeBronze(_bronze_result(production_targets), "production")
    person = _FakeBronze(_bronze_result(person_targets), "person")
    silver = _FakeSilver(lambda snapshot_id: _silver_result(snapshot_id))
    pipeline = BronzeToSilverPipeline(
        bronze_jobs=(sales, person, production),
        silver_job=silver,
        snapshot_gate=BronzeSnapshotGate(),
    )

    result = pipeline.run(mode="full")

    assert result["status"] == "SUCCESS"
    assert result["bronze_gate"]["status"] == "SUCCESS"
    assert set(result["bronze"]) == set(REQUIRED_BRONZE_TARGETS)
    assert {
        item["snapshot_id"] for item in result["bronze"].values()
    } == {result["snapshot_id"]}
    assert silver.arguments == (result["snapshot_id"], result["snapshot_id"])
