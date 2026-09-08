from dataclasses import dataclass
import random
from typing import Callable, TypeVar

from src.shared.ingestion.ingestion_models import IngestionStatus

T = TypeVar("T")


class ErrorClass(str):
    TRANSIENT = "TRANSIENT"
    DETERMINISTIC = "DETERMINISTIC"


def classify_error(error: BaseException) -> str:
    if isinstance(error, (TimeoutError, ConnectionError)):
        return ErrorClass.TRANSIENT

    message = str(error).lower()
    transient_markers = (
        "timeout",
        "connection reset",
        "temporarily unavailable",
        "deadlock",
        "could not connect",
    )
    return (
        ErrorClass.TRANSIENT
        if any(marker in message for marker in transient_markers)
        else ErrorClass.DETERMINISTIC
    )


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    jitter_ratio: float = 0.1

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.max_attempts > 3:
            raise ValueError("max_attempts cannot exceed 3 total attempts")
        if self.initial_delay_seconds <= 0 or self.max_delay_seconds <= 0:
            raise ValueError("retry delays must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        if self.jitter_ratio < 0:
            raise ValueError("jitter_ratio cannot be negative")

    def delay_seconds(self, attempt: int, random_value: float | None = None) -> float:
        base = min(
            self.max_delay_seconds,
            self.initial_delay_seconds * (2 ** max(attempt - 1, 0)),
        )
        jitter = random.uniform(0, self.jitter_ratio) if random_value is None else random_value
        return min(self.max_delay_seconds, base * (1 + jitter))


def execute_with_retry(
    operation: Callable[[], T],
    policy: RetryPolicy,
    sleeper: Callable[[float], None],
    on_retry: Callable[[int, float, BaseException], None] | None = None,
) -> tuple[T, int, IngestionStatus]:
    last_error = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation(), attempt, IngestionStatus.SUCCESS
        except Exception as error:  # noqa: BLE001 - classifier decides retryability
            last_error = error
            if classify_error(error) != ErrorClass.TRANSIENT or attempt == policy.max_attempts:
                raise
            delay = policy.delay_seconds(attempt)
            if on_retry:
                on_retry(attempt, delay, error)
            sleeper(delay)

    raise last_error