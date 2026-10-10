import re
from collections.abc import Iterable


_CREDENTIAL_PATTERN = re.compile(
    r"(?i)(password|passwd|pwd|secret)(\s*[=:]\s*)([^\s;,]+)"
)


def redact_log_message(message: object, secrets: Iterable[str] = ()) -> str:
    redacted = str(message)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return _CREDENTIAL_PATTERN.sub(r"\1\2[REDACTED]", redacted)
