"""Logging configuration with basic secret redaction."""

from __future__ import annotations

import logging
import re


REDACTION_PATTERNS = [
    re.compile(r"(Authorization['\"]?\s*[:=]\s*['\"]?Bearer\s+)([^'\",\s]+)", re.IGNORECASE),
    re.compile(r"(token=)([^&\s]+)", re.IGNORECASE),
    re.compile(r"(webhook[s]?/)([^/\s]+/[^?\s]+)", re.IGNORECASE),
]


class RedactingFilter(logging.Filter):
    """Redact obvious credentials from log output."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for pattern in REDACTION_PATTERNS:
            message = pattern.sub(r"\1[REDACTED]", message)
        record.msg = message
        record.args = ()
        return True


def configure_logging() -> None:
    """Install a conservative root logger with redaction."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.addFilter(RedactingFilter())
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            handler.addFilter(RedactingFilter())
    root.setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
