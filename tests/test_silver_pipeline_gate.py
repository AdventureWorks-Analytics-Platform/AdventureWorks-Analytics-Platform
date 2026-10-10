import logging

from src.app.app import SilverGoldPipeline


class _FakeSilver:
    def __init__(self, result):
        self.result = result

    def run(self):
        return self.result


class _FakeGold:
    def __init__(self):
        self.called = False

    def run(self):
        self.called = True
        return {"status": "SUCCESS"}


def test_silver_failure_blocks_gold_execution():
    gold = _FakeGold()
    pipeline = SilverGoldPipeline(
        _FakeSilver({"sales": {"status": "FAILED", "rows_rejected": 1}}), gold
    )

    result = pipeline.run()

    assert result["status"] == "FAILED"
    assert result["gold_called"] is False
    assert gold.called is False
    assert result["summary"]["rows_rejected"] == 1


def test_success_with_rejections_allows_gold_and_logs_redacted_summary(caplog):
    gold = _FakeGold()
    pipeline = SilverGoldPipeline(
        _FakeSilver(
            {
                "sales": {
                    "status": "SUCCESS_WITH_REJECTIONS",
                    "rows_read": 10,
                    "rows_written": 9,
                    "rows_rejected": 1,
                    "rows_deduplicated": 0,
                    "error_message": "password=secret full raw payload",
                }
            }
        ),
        gold,
    )

    with caplog.at_level(logging.INFO):
        result = pipeline.run()

    assert result["status"] == "SUCCESS"
    assert result["gold_called"] is True
    assert gold.called is True
    assert result["summary"]["rows_read"] == 10
    assert result["summary"]["rows_rejected"] == 1
    assert "secret" not in caplog.text
    assert "full raw payload" not in caplog.text
