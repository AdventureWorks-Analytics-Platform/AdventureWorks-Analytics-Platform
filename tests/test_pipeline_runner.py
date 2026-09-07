from src.app.pipeline_runner import PipelineRunner


class _Health:
    def __init__(self, status="ok"):
        self.status = status
        self.called = False

    def check_all(self):
        self.called = True
        return {"status": self.status}


class _Bootstrap:
    def __init__(self, status="ok"):
        self.status = status
        self.called = False

    def run(self):
        self.called = True
        return {"status": self.status}


class _Stage:
    def __init__(self, status="SUCCESS"):
        self.status = status
        self.called = False

    def run(self, mode="full"):
        self.called = True
        return {"status": self.status, "bronze": {}, "silver": {}}


class _OrderedHealth:
    def __init__(self, events):
        self.events = events

    def check_all(self):
        self.events.append("health")
        return {"status": "ok"}


class _OrderedBootstrap:
    def __init__(self, events):
        self.events = events

    def run(self):
        self.events.append("bootstrap")
        return {"status": "ok"}


class _OrderedPipeline:
    def __init__(self, events, status="SUCCESS"):
        self.events = events
        self.status = status

    def run(self, mode="full"):
        self.events.append("bronze_silver")
        return {
            "status": self.status,
            "snapshot_id": "snapshot-1",
            "bronze": {"customer": {"status": "SUCCESS", "rows_written": 3}},
            "silver": {"customer": {"status": "SUCCESS", "rows_written": 2}},
        }


class _OrderedGold:
    def __init__(self, events, status="SUCCESS"):
        self.events = events
        self.status = status

    def run(self, **kwargs):
        self.events.append("gold")
        return {
            "status": self.status,
            "pipeline_snapshot_id": kwargs["pipeline_snapshot_id"],
            "source_snapshot_id": "snapshot-1",
            "gold_run_id": "gold-run-1",
            "gold_load_id": "gold-load-1",
            "gold_version": "v001",
            "current_pointer": {
                "gold_version": "v001",
                "candidate_schema": "gold_v001",
            },
            "kpi_passed": True,
            "constraints_verified": True,
            "published": True,
            "rows_written": 4,
        }


def test_runner_success_keeps_gold_registered_but_not_requested():
    health = _Health()
    bootstrap = _Bootstrap()
    stage = _Stage()
    runner = PipelineRunner(health, bootstrap, stage)

    result = runner.run()

    assert result["status"] == "SUCCESS"
    assert result["requested_stages"] == ["bronze", "silver"]
    assert result["gold"]["status"] == "NOT_REQUESTED"
    assert result["failed_stage"] is None
    assert result["duration_ms"] >= 0


def test_runner_uses_documented_error_message_field():
    result = PipelineRunner(_Health(), _Bootstrap(), _Stage()).run(mode="unsupported")

    assert result["status"] == "FAILED"
    assert result["error_message"] == result["error"]
    assert result["error_type"] == "ConfigurationError"


def test_runner_health_failure_stops_before_bootstrap_and_data():
    health = _Health("degraded")
    bootstrap = _Bootstrap()
    stage = _Stage()
    result = PipelineRunner(health, bootstrap, stage).run()

    assert result["status"] == "FAILED"
    assert result["failed_stage"] == "health"
    assert bootstrap.called is False
    assert stage.called is False


def test_runner_bootstrap_failure_stops_before_data():
    health = _Health()
    bootstrap = _Bootstrap("failed")
    stage = _Stage()
    result = PipelineRunner(health, bootstrap, stage).run()

    assert result["status"] == "FAILED"
    assert result["failed_stage"] == "bootstrap"
    assert stage.called is False


def test_runner_stage_failure_records_silver_failure():
    stage = _Stage("FAILED")
    result = PipelineRunner(_Health(), _Bootstrap(), stage).run()

    assert result["status"] == "FAILED"
    assert result["failed_stage"] == "silver"
    assert result["gold"]["status"] == "NOT_REQUESTED"


def test_runner_full_stage_order_and_structured_stage_results():
    events = []
    result = PipelineRunner(
        _OrderedHealth(events),
        _OrderedBootstrap(events),
        _OrderedPipeline(events),
        gold_pipeline=_OrderedGold(events),
    ).run()

    assert events == ["health", "bootstrap", "bronze_silver", "gold"]
    assert result["status"] == "SUCCESS"
    assert [item["stage"] for item in result["stages"]] == [
        "health", "bootstrap", "bronze", "silver", "gold"
    ]
    assert result["counts"]["rows_written"] == 9
    assert result["rows_written"] == 9
    assert result["gold"]["current_pointer"]["gold_version"] == "v001"
    assert result["gold"]["pipeline_snapshot_id"] == result["run_id"]
    assert PipelineRunner.stage_order == (
        "settings", "health", "bootstrap", "bronze", "silver", "gold"
    )


def test_partial_stage_result_blocks_gold_and_preserves_context():
    events = []
    result = PipelineRunner(
        _OrderedHealth(events),
        _OrderedBootstrap(events),
        _OrderedPipeline(events, status="PARTIAL_SUCCESS"),
        gold_pipeline=_OrderedGold(events),
    ).run()

    assert result["status"] == "FAILED"
    assert result["failed_stage"] == "silver"
    assert events == ["health", "bootstrap", "bronze_silver"]
    assert result["stages"][-1]["stage"] == "silver"
    assert result["stages"][-1]["failed"] is True


def test_gold_failure_is_failed_and_keeps_upstream_results():
    events = []
    result = PipelineRunner(
        _OrderedHealth(events),
        _OrderedBootstrap(events),
        _OrderedPipeline(events),
        gold_pipeline=_OrderedGold(events, status="FAILED"),
    ).run()

    assert result["status"] == "FAILED"
    assert result["failed_stage"] == "gold"
    assert result["gold"]["status"] == "FAILED"
    assert result["error_message"] is None
    assert result["silver"] is not None
    assert events == ["health", "bootstrap", "bronze_silver", "gold"]
