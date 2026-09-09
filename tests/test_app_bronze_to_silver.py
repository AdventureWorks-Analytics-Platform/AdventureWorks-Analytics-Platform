from src.app.app import App


class _Health:
    def __init__(self, status):
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


class _Pipeline:
    def __init__(self, result):
        self.result = result
        self.called = False

    def run(self, mode="full"):
        self.called = True
        return self.result


def _app(health, bootstrap, pipeline):
    app = App.__new__(App)
    app.health_service = health
    app.bootstrap_job = bootstrap
    app.bronze_to_silver_pipeline = pipeline
    app._running = False
    return app


def test_app_runs_bronze_to_silver_after_health_and_bootstrap():
    health = _Health("ok")
    bootstrap = _Bootstrap()
    pipeline = _Pipeline(
        {
            "status": "SUCCESS",
            "snapshot_id": "snapshot-1",
            "bronze": {"customer": {"status": "SUCCESS"}},
            "bronze_gate": {"status": "SUCCESS"},
            "silver": {"customer_clean": {"status": "SUCCESS"}},
        }
    )

    result = _app(health, bootstrap, pipeline).run()

    assert result["status"] == "ok"
    assert health.called is True
    assert bootstrap.called is True
    assert pipeline.called is True
    assert result["snapshot_id"] == "snapshot-1"
    assert result["silver"]["customer_clean"]["status"] == "SUCCESS"


def test_app_health_failure_blocks_bootstrap_and_pipeline():
    health = _Health("degraded")
    bootstrap = _Bootstrap()
    pipeline = _Pipeline({"status": "SUCCESS"})

    result = _app(health, bootstrap, pipeline).run()

    assert result["status"] == "degraded"
    assert bootstrap.called is False
    assert pipeline.called is False
    assert result["silver"] is None


def test_app_pipeline_failure_is_degraded():
    health = _Health("ok")
    bootstrap = _Bootstrap()
    pipeline = _Pipeline(
        {
            "status": "FAILED",
            "bronze_gate": {"status": "FAILED"},
            "silver": None,
        }
    )

    result = _app(health, bootstrap, pipeline).run()

    assert result["status"] == "degraded"
    assert result["bronze_gate"]["status"] == "FAILED"
    assert result["silver"] is None


class _Runner:
    def __init__(self, result):
        self.result = result

    def run(self, mode="full"):
        return self.result


def test_app_partial_success_is_degraded_not_ok():
    app = App.__new__(App)
    app.pipeline_runner = _Runner({"status": "PARTIAL_SUCCESS", "failed_stage": "silver"})

    result = app.run()

    assert result["status"] == "degraded"
    assert result["failed_stage"] == "silver"

