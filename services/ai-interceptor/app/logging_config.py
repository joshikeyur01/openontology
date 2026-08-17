"""Structured JSON logging with request correlation.

Industrial operators aggregate logs centrally; free-text lines are unusable at
that scale, so every record is emitted as a single JSON object carrying the
request id bound by the licensing middleware.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any

_request_id: ContextVar[str] = ContextVar("request_id", default="-")
_tenant: ContextVar[str] = ContextVar("tenant", default="-")

_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "asctime",
    "message",
    "taskName",
}


def bind_request_context(request_id: str, tenant: str = "-") -> tuple[Token[str], Token[str]]:
    """Bind correlation identifiers for the duration of one request."""
    return _request_id.set(request_id), _tenant.set(tenant)


def reset_request_context(tokens: tuple[Token[str], Token[str]]) -> None:
    """Restore the previous correlation identifiers."""
    request_token, tenant_token = tokens
    _request_id.reset(request_token)
    _tenant.reset(tenant_token)


def current_request_id() -> str:
    return _request_id.get()


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": _request_id.get(),
            "tenant": _tenant.get(),
        }

        # Anything passed via logger.info(..., extra={...}) becomes a field.
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Install the JSON formatter on the root logger, once."""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # uvicorn ships its own handlers; route them through ours so a deployment
    # emits exactly one log format.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
