import pytest

from src.shared.ingestion.ingestion_models import IngestionStatus
from src.shared.ingestion.retry_policy import (
    ErrorClass,
    RetryPolicy,
    classify_error,
    execute_with_retry,
)


def test_error_classifier_retries_transient_only():
    assert classify_error(TimeoutError("read timeout")) == ErrorClass.TRANSIENT
    assert classify_error(ValueError("invalid schema")) == ErrorClass.DETERMINISTIC


def test_retry_reuses_operation_and_returns_attempt_count():
    attempts = []
    delays = []

    def operation():
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise TimeoutError("temporary timeout")
        return "ok"

    result, attempt_count, status = execute_with_retry(
        operation,
        RetryPolicy(initial_delay_seconds=0.01, max_delay_seconds=1),
        delays.append,
    )

    assert result == "ok"
    assert attempts == [1, 2, 3]
    assert attempt_count == 3
    assert status is IngestionStatus.SUCCESS
    assert len(delays) == 2


def test_retry_does_not_retry_deterministic_error():
    attempts = []

    def operation():
        attempts.append(1)
        raise ValueError("invalid contract")

    with pytest.raises(ValueError):
        execute_with_retry(operation, RetryPolicy(), lambda _: None)

    assert len(attempts) == 1


def test_retry_exhaustion_raises_after_max_attempts():
    attempts = []

    def operation():
        attempts.append(1)
        raise ConnectionError("connection reset")

    with pytest.raises(ConnectionError):
        execute_with_retry(operation, RetryPolicy(max_attempts=3), lambda _: None)

    assert len(attempts) == 3


@pytest.mark.parametrize("max_attempts", [0, 4, 10])
def test_retry_policy_rejects_values_outside_one_to_three(max_attempts):
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=max_attempts)


def test_retry_max_attempts_counts_initial_attempt_and_retries():
    attempts = []
    retry_events = []

    def operation():
        attempts.append(len(attempts) + 1)
        raise TimeoutError("temporary timeout")

    with pytest.raises(TimeoutError):
        execute_with_retry(
            operation,
            RetryPolicy(max_attempts=2, initial_delay_seconds=0.01),
            lambda _: None,
            on_retry=lambda attempt, delay, error: retry_events.append(attempt),
        )

    assert attempts == [1, 2]
    assert retry_events == [1]
