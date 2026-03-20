"""Prometheus text-format exporter for MetricsCollector snapshots."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.observability.metrics import MetricsCollector

_BUCKETS = [10, 50, 100, 250, 500, 1000]
_LABEL_RE = re.compile(r"\{.*\}$")


def _bare_name(key: str) -> str:
    return _LABEL_RE.sub("", key)


def _labels_str(key: str) -> str:
    m = re.search(r"(\{.*\})$", key)
    return m.group(1) if m else ""


class PrometheusExporter:
    """Formats a MetricsCollector snapshot as Prometheus text exposition format."""

    def __init__(self, collector: "MetricsCollector") -> None:
        self._collector = collector

    def export(self) -> str:
        snap = self._collector.snapshot()
        lines: list[str] = []

        seen: set[str] = set()
        for key, value in snap["counters"].items():
            bare = _bare_name(key)
            if bare not in seen:
                lines.append(f"# TYPE {bare} counter")
                seen.add(bare)
            lines.append(f"{key} {value}")

        seen = set()
        for key, value in snap["gauges"].items():
            bare = _bare_name(key)
            if bare not in seen:
                lines.append(f"# TYPE {bare} gauge")
                seen.add(bare)
            lines.append(f"{key} {value}")

        seen = set()
        for key, observations in snap["histograms"].items():
            bare = _bare_name(key)
            label_suffix = _labels_str(key)
            if bare not in seen:
                lines.append(f"# TYPE {bare} histogram")
                seen.add(bare)
            count = len(observations)
            total = sum(observations)
            for bucket in _BUCKETS:
                le_count = sum(1 for v in observations if v <= bucket)
                lines.append(f"{bare}_bucket{_inject_le(label_suffix, bucket)} {le_count}")
            lines.append(f"{bare}_bucket{_inject_le(label_suffix, '+Inf')} {count}")
            lines.append(f"{bare}_sum{label_suffix} {total}")
            lines.append(f"{bare}_count{label_suffix} {count}")

        return "\n".join(lines)


def _inject_le(label_suffix: str, le_value: object) -> str:
    le_str = f'le="{le_value}"'
    if not label_suffix:
        return "{" + le_str + "}"
    return label_suffix[:-1] + "," + le_str + "}"
