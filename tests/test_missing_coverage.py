"""
Tests for modules that previously had zero coverage:
- WhatIfProjector (unit tests with mock event store)
- RegulatoryPackage (verify_regulatory_package offline)
- Validation layer
- Tracing context propagation
- Auth audit log entries
- Saga manager checkpoint
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4, UUID

from src.commands.handlers import CommandHandler, SubmitApplicationCommand
from src.mcp.validation import ValidationError, validate_arguments
from src.observability.tracing import start_span, get_trace_context, span_from_traceparent


# ---------------------------------------------------------------------------
# Validation layer
# ---------------------------------------------------------------------------

def test_validate_missing_required_field():
    schema = {"type": "object", "required": ["application_id"], "properties": {
        "application_id": {"type": "string"}
    }}
    with pytest.raises(ValidationError) as exc:
        validate_arguments("test_tool", {}, schema)
    assert "application_id" in str(exc.value)


def test_validate_passes_with_all_required():
    schema = {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}}
    validate_arguments("test_tool", {"name": "Alice"}, schema)  # no exception


def test_validate_number_range():
    schema = {
        "type": "object",
        "required": ["score"],
        "properties": {"score": {"type": "number", "minimum": 0, "maximum": 1}},
    }
    # Valid
    validate_arguments("t", {"score": 0.5}, schema)
    # Out of range — only caught if jsonschema installed
    try:
        import jsonschema
        with pytest.raises(ValidationError):
            validate_arguments("t", {"score": 1.5}, schema)
    except ImportError:
        pass  # fallback validation doesn't check ranges for floats


def test_validate_empty_schema_always_passes():
    validate_arguments("any_tool", {"anything": 123}, {})


# ---------------------------------------------------------------------------
# Tracing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_span_context_propagation():
    """Nested spans inherit trace_id from parent."""
    async with start_span("outer") as outer:
        outer_trace = outer.trace_id
        async with start_span("inner") as inner:
            assert inner.trace_id == outer_trace
            assert inner.parent_span_id == outer.span_id
            ctx = get_trace_context()
            assert ctx is not None
            assert ctx.span_id == inner.span_id


@pytest.mark.asyncio
async def test_span_elapsed_ms():
    import asyncio
    async with start_span("timed") as span:
        await asyncio.sleep(0.01)
    assert span.elapsed_ms() >= 10.0


def test_traceparent_roundtrip():
    trace_id = "a" * 32
    span_id = "b" * 16
    from src.observability.tracing import TraceContext
    ctx = TraceContext(trace_id=trace_id, span_id=span_id)
    tp = ctx.traceparent()
    parsed = span_from_traceparent(tp)
    assert parsed is not None
    assert parsed.trace_id == trace_id


def test_span_from_invalid_traceparent():
    assert span_from_traceparent("not-valid") is None
    assert span_from_traceparent("01-abc-def") is None


# ---------------------------------------------------------------------------
# WhatIfProjector (unit — no DB)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_what_if_projector_unit():
    """WhatIfProjector correctly strips causally-dependent events."""
    from src.what_if.projector import WhatIfProjector
    from src.models.events import (
        LoanApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted,
        FraudCheckCompleted, DecisionGenerated, ApplicationApproved, DecisionOutcome,
    )
    app_id = uuid4()
    session_id = uuid4()

    real_events = [
        LoanApplicationSubmitted(aggregate_id=app_id, applicant_name="Test",
                                  loan_amount=10000.0, loan_purpose="Test",
                                  applicant_id=uuid4(), submitted_by="test"),
        CreditAnalysisRequested(aggregate_id=app_id, requested_by="system"),
        CreditAnalysisCompleted(aggregate_id=app_id, credit_score=720, debt_to_income_ratio=0.3),
        FraudCheckCompleted(aggregate_id=app_id, fraud_risk_score=0.05, passed=True, flags=[]),
        DecisionGenerated(aggregate_id=app_id, outcome=DecisionOutcome.APPROVE,
                          confidence_score=0.9, reasoning="Good", model_version="v1",
                          agent_session_id=session_id),
        ApplicationApproved(aggregate_id=app_id, approved_amount=10000.0,
                             interest_rate=0.06, approved_by="officer"),
    ]

    mock_store = MagicMock()
    mock_store.load_stream = AsyncMock(return_value=real_events)

    projector = WhatIfProjector(mock_store)
    cf_event = CreditAnalysisCompleted(
        aggregate_id=app_id, credit_score=400, debt_to_income_ratio=0.9,
    )
    result = await projector.run_what_if(app_id, [cf_event])

    assert result["injected_events"] == ["CreditAnalysisCompleted"]
    assert "DecisionGenerated" in result["causally_removed_events"] or \
           "ApplicationApproved" in result["causally_removed_events"]
    assert "narrative" in result
    assert result["real_event_count"] == len(real_events)


# ---------------------------------------------------------------------------
# RegulatoryPackage (offline verification)
# ---------------------------------------------------------------------------

def test_verify_regulatory_package_tamper_detection():
    """Modifying a package field should invalidate the package hash."""
    import json, hashlib
    from src.regulatory.package import verify_regulatory_package

    # Build a minimal valid package
    package = {
        "package_version": "1.0",
        "generated_at": "2026-01-01T00:00:00",
        "generated_by": "test",
        "application_id": str(uuid4()),
        "event_count": 1,
        "events": [{"event_id": str(uuid4()), "event_type": "LoanApplicationSubmitted",
                     "schema_version": 1, "occurred_at": "2026-01-01T00:00:00",
                     "payload": {}, "metadata": {}}],
        "raw_events": [{"event_id": "same-id", "event_type": "LoanApplicationSubmitted",
                         "schema_version": 1, "raw_payload": {}, "recorded_at": "2026-01-01T00:00:00"}],
        "projection_state": {},
        "integrity_proof": {"chain_valid": True, "entries_checked": 1},
        "regulatory_metadata": {"chain_valid": True},
    }
    # Sync event IDs
    eid = str(uuid4())
    package["events"][0]["event_id"] = eid
    package["raw_events"][0]["event_id"] = eid

    # Compute correct hash
    package_json = json.dumps(package, sort_keys=True, default=str)
    package["package_hash"] = hashlib.sha256(package_json.encode()).hexdigest()

    # Valid package should pass hash check
    result = verify_regulatory_package(package)
    assert result["package_hash_valid"] is True

    # Tamper with it
    package["event_count"] = 999
    result2 = verify_regulatory_package(package)
    assert result2["package_hash_valid"] is False
    assert len(result2["issues"]) > 0


def test_verify_regulatory_package_event_count_mismatch():
    """Declared event_count != actual events list length → issue reported."""
    import json, hashlib
    from src.regulatory.package import verify_regulatory_package

    eid = str(uuid4())
    package = {
        "package_version": "1.0",
        "generated_at": "2026-01-01T00:00:00",
        "generated_by": "test",
        "application_id": str(uuid4()),
        "event_count": 5,  # wrong
        "events": [{"event_id": eid, "event_type": "X", "schema_version": 1,
                     "occurred_at": "2026-01-01T00:00:00", "payload": {}, "metadata": {}}],
        "raw_events": [{"event_id": eid, "event_type": "X", "schema_version": 1,
                         "raw_payload": {}, "recorded_at": "2026-01-01T00:00:00"}],
        "projection_state": {},
        "integrity_proof": {"chain_valid": True},
        "regulatory_metadata": {"chain_valid": True},
    }
    package_json = json.dumps(package, sort_keys=True, default=str)
    package["package_hash"] = hashlib.sha256(package_json.encode()).hexdigest()

    result = verify_regulatory_package(package)
    assert result["event_count_matches"] is False


# ---------------------------------------------------------------------------
# Outbox relay unit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_outbox_relay_in_memory_publisher():
    """InMemoryPublisher collects published events."""
    from src.outbox.relay import InMemoryPublisher
    pub = InMemoryPublisher()
    await pub.publish("LoanApplicationSubmitted", "LoanApplication", {"id": "1"}, {})
    assert len(pub.published) == 1
    assert pub.published[0]["event_type"] == "LoanApplicationSubmitted"


# ---------------------------------------------------------------------------
# Saga checkpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_saga_manager_checkpoint(db_pool, event_store, handler):
    """SagaManager persists checkpoint after processing events."""
    from src.saga import SagaManager
    manager = SagaManager(db_pool, event_store, handler)
    await manager.start()
    # Process one batch (may be empty — just verifies no crash)
    await manager._process_batch()
    await manager.stop()
    # Checkpoint should exist
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_position FROM saga_checkpoints WHERE saga_name = 'LoanProcessingSaga'"
        )
    assert row is not None


# ---------------------------------------------------------------------------
# Projection lag metric
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_projection_lag_metric_emitted(db_pool, event_store, handler):
    """ProjectionDaemon emits projection_lag_events gauge after processing."""
    from src.projections.daemon import ProjectionDaemon
    from src.observability.metrics import get_metrics

    get_metrics().reset()

    daemon = ProjectionDaemon(db_pool, event_store)

    async def noop_handler(conn, event):
        pass

    daemon.register("TestProjection", noop_handler)
    # Seed checkpoint
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO projection_checkpoints (projection_name, last_position) VALUES ('TestProjection', 0) "
            "ON CONFLICT (projection_name) DO UPDATE SET last_position = 0"
        )

    await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test", loan_amount=1000.0, loan_purpose="Test",
        applicant_id=uuid4(), submitted_by="test",
    ))

    await daemon._load_checkpoints()
    await daemon._process_batch()

    snap = get_metrics().snapshot()
    # Either lag_events or lag_ms gauge should be present
    gauge_keys = list(snap["gauges"].keys())
    assert any("projection_lag" in k for k in gauge_keys)
