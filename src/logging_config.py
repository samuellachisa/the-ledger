"""
Structured JSON logging configuration.

Usage:
    from src.logging_config import configure_logging
    configure_logging()

Produces log lines like:
    {"time": "2026-03-21T10:00:00Z", "level": "INFO", "logger": "src.event_store",
     "message": "Application submitted: ...", "trace_id": "...", "span_id": "..."}

Set LOG_FORMAT=text for human-readable output during local dev.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone


class _TraceContextFilter(logging.Filter):
    """Injects trace_id and span_id from the current span into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from src.observability.tracing import get_trace_context
            ctx = get_trace_context()
            if ctx:
                record.trace_id = ctx.trace_id  # type: ignore[attr-defined]
                record.span_id = ctx.span_id    # type: ignore[attr-defined]
        except Exception:
            pass
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        # Thread any extra fields through
        for key in ("correlation_id", "tenant_id", "agent_id", "tool_name", "trace_id", "span_id"):
            if hasattr(record, key):
                log[key] = getattr(record, key)
        return json.dumps(log)


def configure_logging(level: str | None = None) -> None:
    """
    Configure root logger. Call once at process startup.

    Args:
        level: Override LOG_LEVEL env var. Defaults to INFO.
    """
    log_level_str = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_format = os.environ.get("LOG_FORMAT", "json").lower()

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(_TraceContextFilter())
    if log_format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
        ))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    # Quiet noisy third-party loggers
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("mcp").setLevel(logging.WARNING)
