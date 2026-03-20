"""
Structured in-memory metrics for the Apex Financial Services Event Store.

Pre-defined metric names
------------------------
events_appended_total           counter  labels: aggregate_type
occ_conflicts_total             counter  labels: aggregate_type
projection_lag_ms               gauge    labels: projection_name
projection_events_processed_total counter labels: projection_name
dead_letter_queue_depth         gauge    (no labels)
saga_steps_total                counter  labels: step, outcome (success/failure)
outbox_published_total          counter
outbox_failed_total             counter
tool_calls_total                counter  labels: tool_name, outcome (success/error)
auth_failures_total             counter  labels: reason
rate_limit_hits_total           counter  labels: agent_id, action
snapshot_hits_total             counter  labels: aggregate_type
snapshot_misses_total           counter  labels: aggregate_type
"""
from __future__ import annotations

from typing import Optional

_instance: Optional["MetricsCollector"] = None


def _encode_name(name: str, labels: Optional[dict]) -> str:
    """Encode labels into metric name as name{k=v,...}."""
    if not labels:
        return name
    pairs = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return f"{name}{{{pairs}}}"


class MetricsCollector:
    """In-memory metrics collector (counters, gauges, histograms)."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}

    def increment(self, name: str, value: int = 1, labels: Optional[dict] = None) -> None:
        """Increment a counter by value (default 1)."""
        key = _encode_name(name, labels)
        self._counters[key] = self._counters.get(key, 0) + value

    def gauge(self, name: str, value: float, labels: Optional[dict] = None) -> None:
        """Set a gauge to value."""
        key = _encode_name(name, labels)
        self._gauges[key] = value

    def histogram(self, name: str, value: float, labels: Optional[dict] = None) -> None:
        """Record a histogram observation."""
        key = _encode_name(name, labels)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)

    def snapshot(self) -> dict:
        """Return all current metrics as a JSON-serialisable dict."""
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {k: list(v) for k, v in self._histograms.items()},
        }

    def reset(self) -> None:
        """Clear all metrics (for tests)."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()


def get_metrics() -> MetricsCollector:
    """Return the module-level singleton MetricsCollector."""
    global _instance
    if _instance is None:
        _instance = MetricsCollector()
    return _instance
