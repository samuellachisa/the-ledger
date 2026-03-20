"""
Projection Tests: Lag SLO verification and rebuild correctness.

SLO: ApplicationSummary projection lag < 500ms.
"""
from __future__ import annotations

import asyncio
import time
from uuid import uuid4

import pytest
import pytest_asyncio

from src.projections.application_summary import handle_application_summary, get_application_summary
from src.projections.agent_performance import handle_agent_performance, get_agent_performance
from src.projections.compliance_audit import handle_compliance_audit, get_state_at
from src.projections.daemon import ProjectionDaemon


@pytest_asyncio.fixture
async def daemon(db_pool, event_store):
    d = ProjectionDaemon(db_pool, event_store)
    d.register("ApplicationSummary", handle_application_summary)
    d.register("AgentPerformanceLedger", handle_agent_performance)
    d.register("ComplianceAuditView", handle_compliance_audit)
    await d.start()
    yield d
    await d.stop()


@pytest.mark.asyncio
async def test_application_summary_lag_slo(event_store, daemon, db_pool):
    """
    ApplicationSummary projection must reflect a submitted application
    within 500ms of the event being written.
    """
    from src.models.events import LoanApplicationSubmitted

    application_id = uuid4()
    event = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="SLO Test",
        loan_amount=75000.0,
        loan_purpose="Home purchase",
        applicant_id=uuid4(),
        submitted_by="slo_test",
    )

    # Record time of write
    write_time = time.monotonic()
    await event_store.append("LoanApplication", application_id, [event], expected_version=0)

    # Poll until projection is updated or timeout
    deadline = write_time + 0.5  # 500ms SLO
    summary = None
    while time.monotonic() < deadline:
        async with db_pool.acquire() as conn:
            summary = await get_application_summary(conn, application_id)
        if summary:
            break
        await asyncio.sleep(0.01)

    lag_ms = (time.monotonic() - write_time) * 1000
    assert summary is not None, f"Projection not updated within 500ms (lag: {lag_ms:.0f}ms)"
    assert summary["status"] == "Submitted"
    assert summary["applicant_name"] == "SLO Test"
    assert lag_ms < 500, f"Lag {lag_ms:.0f}ms exceeds 500ms SLO"


@pytest.mark.asyncio
async def test_projection_rebuild_truncates_stale_data(event_store, db_pool):
    """
    rebuild_projection() must truncate the projection table AND reset checkpoint.
    After rebuild, the daemon replays all events from position 0.
    Verifies no stale data remains after truncation.
    """
    from src.models.events import LoanApplicationSubmitted

    # Write and manually project an application
    application_id = uuid4()
    event = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="Rebuild Truncate Test",
        loan_amount=55000.0,
        loan_purpose="Car",
        applicant_id=uuid4(),
        submitted_by="test",
    )
    await event_store.append("LoanApplication", application_id, [event], expected_version=0)

    async with db_pool.acquire() as conn:
        from src.event_store import StoredEvent
        rows = await conn.fetch(
            "SELECT * FROM events WHERE aggregate_type = 'LoanApplication' AND aggregate_id = $1",
            application_id,
        )
        for row in rows:
            stored = StoredEvent(row)
            await handle_application_summary(conn, stored)

    # Verify projection exists
    async with db_pool.acquire() as conn:
        summary = await get_application_summary(conn, application_id)
    assert summary is not None

    # Trigger rebuild — must truncate
    daemon_instance = ProjectionDaemon(db_pool, event_store)
    daemon_instance.register("ApplicationSummary", handle_application_summary)
    await daemon_instance._load_checkpoints()
    await daemon_instance.rebuild_projection("ApplicationSummary")

    # After rebuild, projection table must be empty (truncated)
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM application_summary_projection")
    assert count == 0, f"Expected 0 rows after truncation, got {count}"

    # Checkpoint must be reset to 0
    assert daemon_instance._last_positions.get("ApplicationSummary", -1) == 0


@pytest.mark.asyncio
async def test_agent_performance_projection(event_store, db_pool):
    """AgentPerformanceLedger correctly aggregates decision metrics."""
    from src.models.events import DecisionGenerated, DecisionOutcome

    application_id = uuid4()

    decisions = [
        DecisionGenerated(
            aggregate_id=application_id,
            outcome=DecisionOutcome.APPROVE,
            confidence_score=0.92,
            reasoning="Strong credit profile",
            model_version="gpt-4-turbo",
            agent_session_id=uuid4(),
        ),
        DecisionGenerated(
            aggregate_id=application_id,
            outcome=DecisionOutcome.DENY,
            confidence_score=0.78,
            reasoning="High DTI ratio",
            model_version="gpt-4-turbo",
            agent_session_id=uuid4(),
        ),
    ]

    async with db_pool.acquire() as conn:
        for d in decisions:
            from src.event_store import StoredEvent
            import asyncpg
            # Simulate stored event
            class MockStoredEvent:
                event_id = d.event_id
                aggregate_id = d.aggregate_id
                aggregate_type = "LoanApplication"
                event_type = d.event_type
                schema_version = 1
                stream_position = 1
                global_position = 1
                payload = d.to_payload()
                metadata = {}
                causation_id = None
                correlation_id = None
                recorded_at = d.occurred_at

            await handle_agent_performance(conn, MockStoredEvent())

        perf = await get_agent_performance(conn, "gpt-4-turbo")

    assert perf is not None
    assert perf["total_decisions"] == 2
    assert perf["approved_count"] == 1
    assert perf["denied_count"] == 1
    assert perf["referred_count"] == 0
    assert 0.84 < perf["avg_confidence"] < 0.86  # (0.92 + 0.78) / 2


@pytest.mark.asyncio
async def test_compliance_temporal_query(event_store, db_pool):
    """ComplianceAuditView supports point-in-time queries."""
    from src.models.events import (
        ComplianceRecordCreated, ComplianceCheckPassed, ComplianceRecordFinalized,
        ComplianceStatus
    )
    from datetime import datetime, timedelta

    record_id = uuid4()
    application_id = uuid4()

    t0 = datetime(2025, 1, 1, 10, 0, 0)
    t1 = datetime(2025, 1, 1, 10, 5, 0)
    t2 = datetime(2025, 1, 1, 10, 10, 0)

    class MockEvent:
        def __init__(self, event_type, aggregate_id, payload, ts):
            self.event_type = event_type
            self.aggregate_type = "ComplianceRecord"
            self.aggregate_id = aggregate_id
            self.payload = payload
            self.metadata = {"occurred_at": ts.isoformat()}
            self.recorded_at = ts

    events = [
        MockEvent("ComplianceRecordCreated", record_id, {
            "application_id": str(application_id),
            "officer_id": "officer_1",
            "required_checks": ["AML", "KYC"],
        }, t0),
        MockEvent("ComplianceCheckPassed", record_id, {
            "check_name": "AML",
            "officer_id": "officer_1",
        }, t1),
        MockEvent("ComplianceRecordFinalized", record_id, {
            "overall_status": "Passed",
            "officer_id": "officer_1",
            "notes": "All clear",
        }, t2),
    ]

    async with db_pool.acquire() as conn:
        for e in events:
            await handle_compliance_audit(conn, e)

        # Query at t1 (after AML passed, before finalization)
        state_at_t1 = await get_state_at(conn, application_id, t1)
        assert state_at_t1 is not None
        assert "AML" in state_at_t1["checks_passed"]

        # Query at t0 (just created)
        state_at_t0 = await get_state_at(conn, application_id, t0)
        assert state_at_t0 is not None
        assert state_at_t0["status"] == "InProgress"

        # Query at t2 (finalized)
        state_at_t2 = await get_state_at(conn, application_id, t2)
        assert state_at_t2 is not None
        assert state_at_t2["status"] == "Passed"


@pytest.mark.asyncio
async def test_daemon_get_lag(event_store, daemon, db_pool):
    """get_lag() returns correct lag metrics including time-based lag."""
    from src.models.events import LoanApplicationSubmitted

    # Write 3 events without processing
    for _ in range(3):
        app_id = uuid4()
        event = LoanApplicationSubmitted(
            aggregate_id=app_id,
            applicant_name="Lag Test",
            loan_amount=10000.0,
            loan_purpose="Test",
            applicant_id=uuid4(),
            submitted_by="test",
        )
        await event_store.append("LoanApplication", app_id, [event], expected_version=0)

    # Give daemon time to process
    await asyncio.sleep(0.3)

    lag = await daemon.get_lag("ApplicationSummary")
    assert isinstance(lag, dict)
    assert "event_lag" in lag
    assert "time_lag_ms" in lag
    assert "checkpoint" in lag
    assert "head_position" in lag
    assert lag["event_lag"] >= 0
    assert lag["time_lag_ms"] >= 0.0


@pytest.mark.asyncio
async def test_projection_lag_under_50_concurrent_handlers(event_store, daemon, db_pool):
    """
    50 concurrent command handlers submit applications simultaneously.
    After all writes complete, the ApplicationSummary projection must
    reflect all 50 applications within the 500ms SLO.
    """
    from src.models.events import LoanApplicationSubmitted

    N = 50
    app_ids = [uuid4() for _ in range(N)]

    # Fire 50 concurrent appends
    async def submit_one(app_id):
        event = LoanApplicationSubmitted(
            aggregate_id=app_id,
            applicant_name=f"Load Test {app_id}",
            loan_amount=10000.0,
            loan_purpose="Load test",
            applicant_id=uuid4(),
            submitted_by="load_test",
        )
        await event_store.append("LoanApplication", app_id, [event], expected_version=0)

    write_start = time.monotonic()
    await asyncio.gather(*[submit_one(aid) for aid in app_ids])
    write_end = time.monotonic()

    # Poll until all 50 are projected or 500ms SLO expires
    deadline = write_end + 0.5  # 500ms SLO from last write
    projected = set()
    while time.monotonic() < deadline and len(projected) < N:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT application_id FROM application_summary_projection "
                "WHERE application_id = ANY($1::uuid[])",
                app_ids,
            )
            projected = {r["application_id"] for r in rows}
        if len(projected) < N:
            await asyncio.sleep(0.01)

    lag_ms = (time.monotonic() - write_end) * 1000
    assert len(projected) == N, (
        f"Only {len(projected)}/{N} applications projected within 500ms SLO "
        f"(lag: {lag_ms:.0f}ms)"
    )
    assert lag_ms < 500, f"Projection lag {lag_ms:.0f}ms exceeds 500ms SLO under 50 concurrent handlers"
