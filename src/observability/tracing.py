"""
Lightweight distributed tracing without an OpenTelemetry SDK dependency.

Generates W3C traceparent-compatible trace/span IDs and propagates them
through tool calls and event store operations via contextvars.

Usage:
    from src.observability.tracing import start_span, get_trace_context

    async with start_span("tool.submit_application") as span:
        span.set_attribute("tool_name", "submit_application")
        result = await handler.handle_submit_application(cmd)

    # In any nested call:
    ctx = get_trace_context()
    logger.info("...", extra={"trace_id": ctx.trace_id, "span_id": ctx.span_id})
"""
from __future__ import annotations

import secrets
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Optional

from src.observability.metrics import get_metrics


@dataclass
class TraceContext:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    attributes: dict = field(default_factory=dict)
    start_time: float = field(default_factory=time.monotonic)

    def traceparent(self) -> str:
        """W3C traceparent header value."""
        return f"00-{self.trace_id}-{self.span_id}-01"

    def set_attribute(self, key: str, value) -> None:
        self.attributes[key] = value

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000


_current_span: ContextVar[Optional[TraceContext]] = ContextVar("current_span", default=None)


def get_trace_context() -> Optional[TraceContext]:
    return _current_span.get()


def new_trace_id() -> str:
    return secrets.token_hex(16)


def new_span_id() -> str:
    return secrets.token_hex(8)


@asynccontextmanager
async def start_span(name: str, trace_id: Optional[str] = None):
    """
    Async context manager that creates a new span.
    Inherits trace_id from parent span if present.
    """
    parent = _current_span.get()
    span = TraceContext(
        trace_id=trace_id or (parent.trace_id if parent else new_trace_id()),
        span_id=new_span_id(),
        parent_span_id=parent.span_id if parent else None,
    )
    span.set_attribute("span.name", name)
    token = _current_span.set(span)
    try:
        yield span
    finally:
        elapsed = span.elapsed_ms()
        get_metrics().histogram("span_duration_ms", elapsed, labels={"span_name": name})
        _current_span.reset(token)


def span_from_traceparent(header: str) -> Optional[TraceContext]:
    """Parse a W3C traceparent header into a TraceContext."""
    try:
        parts = header.split("-")
        if len(parts) != 4 or parts[0] != "00":
            return None
        return TraceContext(trace_id=parts[1], span_id=new_span_id(), parent_span_id=parts[2])
    except Exception:
        return None
