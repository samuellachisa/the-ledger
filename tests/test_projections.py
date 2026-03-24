"""
tests/phase3/test_projections.py
Test framework hitting the SLOs requested in Phase 3 metrics.
"""
import pytest
import pytest_asyncio
import asyncpg
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from ledger.event_store import EventStore
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditView
from ledger.schema.events import LoanApplicationSubmitted, ComplianceRecordCreated, ComplianceCheckPassed

@pytest_asyncio.fixture
async def db_pool():
    pool = await asyncpg.create_pool("postgresql://localhost/apex_ledger")
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS events, event_streams, projection_checkpoints, outbox CASCADE;")
        await conn.execute("""
            CREATE TABLE event_streams (stream_id TEXT PRIMARY KEY, aggregate_type TEXT NOT NULL, current_version INT NOT NULL, created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), archived_at TIMESTAMP WITH TIME ZONE);
            CREATE TABLE events (event_id UUID PRIMARY KEY, stream_id TEXT NOT NULL REFERENCES event_streams(stream_id), stream_position INT NOT NULL, global_position SERIAL NOT NULL, event_type TEXT NOT NULL, event_version INT NOT NULL, payload JSONB NOT NULL, metadata JSONB NOT NULL DEFAULT '{}', recorded_at TIMESTAMP WITH TIME ZONE NOT NULL, UNIQUE(stream_id, stream_position));
            CREATE TABLE outbox (id SERIAL PRIMARY KEY, event_id UUID NOT NULL, destination TEXT NOT NULL, payload JSONB NOT NULL, published_at TIMESTAMP WITH TIME ZONE, attempts INT DEFAULT 0);
        """)
    yield pool
    await pool.close()

@pytest.fixture
def store(db_pool):
    return EventStore(db_pool)

@pytest.mark.asyncio
async def test_projection_daemon_lag_slo_under_load(store):
    summary_proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store, [summary_proj.handle_event])
    daemon.start()

    # Load heavily
    tasks = []
    for i in range(15):
        app_id = uuid4()
        events = [LoanApplicationSubmitted(applicant_name="A", loan_amount=100.0, loan_purpose="B", applicant_id=uuid4(), submitted_by="C")]
        tasks.append(store.append(f"loan-{app_id}", events, 0))
    await asyncio.gather(*tasks)

    # Allow daemon a quick window to poll (SLO < 500ms)
    await asyncio.sleep(0.6)
    
    lag = daemon.get_lag()
    assert lag == 0, f"Lag SLO Failed, remaining lag was {lag} after 500ms"
    await daemon.stop()

@pytest.mark.asyncio
async def test_temporal_query_slo(store):
    audit_proj = ComplianceAuditView(event_store=store)
    
    app_id = str(uuid4())
    ts1 = datetime.now(timezone.utc)
    ev1 = ComplianceRecordCreated(application_id=UUID(app_id), officer_id="u1", required_checks=["A", "B"])
    ev1.recorded_at = ts1
    
    ts2 = ts1 + timedelta(seconds=1)
    ev2 = ComplianceCheckPassed(application_id=UUID(app_id), officer_id="u1", check_name="A")
    ev2.recorded_at = ts2

    await store.append(f"compliance-{app_id}", [ev1], 0)
    await store.append(f"compliance-{app_id}", [ev2], 1)
    
    await audit_proj.rebuild_from_scratch()
    
    # Temporal Queries
    state_t1 = audit_proj.get_state_at(app_id, ts1 + timedelta(milliseconds=500))
    assert state_t1["status"] == "Created"
    assert "A" not in state_t1["passed_checks"]

    state_t2 = audit_proj.get_state_at(app_id, ts2 + timedelta(milliseconds=500))
    assert "A" in state_t2["passed_checks"]
