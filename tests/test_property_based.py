"""
Property-based tests using Hypothesis.

Tests invariants that must hold for ALL valid inputs, not just specific examples.
"""
from __future__ import annotations

import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from src.mcp.validation import ValidationError, validate_arguments
from src.mcp.tools import get_tool_definitions
from src.observability.metrics import MetricsCollector
from src.observability.exporters import PrometheusExporter
from src.observability.tracing import new_trace_id, new_span_id, TraceContext


# =============================================================================
# Validation layer properties
# =============================================================================

_tool_schemas = {t.name: t.inputSchema for t in get_tool_definitions()}


@given(
    credit_score=st.integers(min_value=300, max_value=850),
    dti=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_credit_analysis_valid_range_never_raises(credit_score, dti):
    """Any credit_score in [300,850] and DTI in [0,1] must pass validation."""
    args = {
        "application_id": "00000000-0000-0000-0000-000000000001",
        "credit_score": credit_score,
        "debt_to_income_ratio": dti,
    }
    # Should not raise
    validate_arguments("record_credit_analysis", args, _tool_schemas["record_credit_analysis"])


@given(credit_score=st.integers().filter(lambda x: x < 300 or x > 850))
@settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_credit_analysis_out_of_range_raises(credit_score):
    """credit_score outside [300,850] must fail validation when jsonschema is available."""
    try:
        import jsonschema
    except ImportError:
        pytest.skip("jsonschema not installed")

    args = {
        "application_id": "00000000-0000-0000-0000-000000000001",
        "credit_score": credit_score,
        "debt_to_income_ratio": 0.3,
    }
    with pytest.raises(ValidationError):
        validate_arguments("record_credit_analysis", args, _tool_schemas["record_credit_analysis"])


@given(
    fraud_score=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    passed=st.booleans(),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_fraud_check_valid_range_never_raises(fraud_score, passed):
    args = {
        "application_id": "00000000-0000-0000-0000-000000000001",
        "fraud_risk_score": fraud_score,
        "passed": passed,
    }
    validate_arguments("record_fraud_check", args, _tool_schemas["record_fraud_check"])


# =============================================================================
# Metrics collector properties
# =============================================================================

@given(
    name=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_")),
    value=st.integers(min_value=1, max_value=1000),
    n=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=50)
def test_counter_increment_is_additive(name, value, n):
    """Incrementing a counter n times by value must equal n*value."""
    m = MetricsCollector()
    for _ in range(n):
        m.increment(name, value)
    snap = m.snapshot()
    assert snap["counters"][name] == value * n


@given(
    values=st.lists(st.floats(min_value=0.0, max_value=10000.0, allow_nan=False), min_size=1, max_size=100),
)
@settings(max_examples=50)
def test_histogram_count_matches_observations(values):
    """Histogram count must equal number of observations recorded."""
    m = MetricsCollector()
    for v in values:
        m.histogram("latency_ms", v)
    snap = m.snapshot()
    assert len(snap["histograms"]["latency_ms"]) == len(values)


@given(
    value=st.floats(min_value=-1e9, max_value=1e9, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=50)
def test_gauge_last_write_wins(value):
    """Setting a gauge multiple times — last write wins."""
    m = MetricsCollector()
    m.gauge("test_gauge", 999.0)
    m.gauge("test_gauge", value)
    assert m.snapshot()["gauges"]["test_gauge"] == value


# =============================================================================
# Prometheus exporter properties
# =============================================================================

@given(
    observations=st.lists(
        st.floats(min_value=0.0, max_value=5000.0, allow_nan=False),
        min_size=1, max_size=50,
    )
)
@settings(max_examples=30)
def test_prometheus_histogram_count_consistent(observations):
    """Prometheus export: _count must equal number of observations."""
    m = MetricsCollector()
    for v in observations:
        m.histogram("tool_latency_ms", v)
    text = PrometheusExporter(m).export()
    for line in text.splitlines():
        if line.startswith("tool_latency_ms_count"):
            count = int(line.split()[-1])
            assert count == len(observations)
            break


# =============================================================================
# Tracing properties
# =============================================================================

@given(st.integers())  # seed — just run it many times
@settings(max_examples=20)
def test_trace_ids_are_unique(_):
    """Each call to new_trace_id() must produce a unique 32-char hex string."""
    ids = {new_trace_id() for _ in range(10)}
    assert len(ids) == 10
    for tid in ids:
        assert len(tid) == 32
        int(tid, 16)  # must be valid hex


@given(st.integers())
@settings(max_examples=20)
def test_span_ids_are_unique(_):
    ids = {new_span_id() for _ in range(10)}
    assert len(ids) == 10
    for sid in ids:
        assert len(sid) == 16
        int(sid, 16)


def test_traceparent_format():
    ctx = TraceContext(trace_id="a" * 32, span_id="b" * 16)
    tp = ctx.traceparent()
    parts = tp.split("-")
    assert parts[0] == "00"
    assert parts[1] == "a" * 32
    assert parts[2] == "b" * 16
    assert parts[3] == "01"
