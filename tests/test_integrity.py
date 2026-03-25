"""
tests/phase4/test_integrity.py
Test framework hitting the constraints for Upcasters, Crash contexts, and Cryptography.
"""
import pytest
import pytest_asyncio
import asyncpg
import asyncio
import json
import hashlib
from uuid import uuid4
from ledger.event_store import EventStore
from ledger.schema.events import DomainEvent, CreditAnalysisCompleted
from ledger.integrity.audit_chain import run_integrity_check
from ledger.integrity.gas_town import reconstruct_agent_context

@pytest_asyncio.fixture
async def db_pool():
    pool = await asyncpg.create_pool("postgresql://postgres:12345@localhost:5432/apex_ledger")
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
async def test_upcaster_does_not_write_to_events_table(store, db_pool):
    # Base event V1 simulation
    class OldCreditAnalysisCompleted(DomainEvent):
        event_type: str = "CreditAnalysisCompleted"
        credit_score: int
        debt_to_income_ratio: float
        
    ev = OldCreditAnalysisCompleted(aggregate_id=uuid4(), credit_score=750, debt_to_income_ratio=0.25)
    await store.append("credit-1", [ev], 0)
    
    # Load and check memory Upcast matches V2 automatically (adding 'model_version')
    loaded = await store.load_stream("credit-1")
    upcasted_payload = loaded[0].payload
    assert "model_version" in upcasted_payload
    
    # Ensure raw database was not updated
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT payload, event_version FROM events WHERE stream_id = 'credit-1'")
        raw_payload = row["payload"]
        # asyncpg JSONB decoding can vary depending on type codecs.
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)
        elif isinstance(raw_payload, list):
            raw_payload = dict(raw_payload)
        raw_db_payload = raw_payload
        assert "model_version" not in raw_db_payload
        assert row["event_version"] == 1

@pytest.mark.asyncio
async def test_audit_chain_is_independently_verifiable(store):
    from ledger.schema.events import AgentNodeExecuted
    ev1 = AgentNodeExecuted(node_name="test1", node_sequence=1, llm_tokens_input=10, llm_tokens_output=10, llm_cost_usd=0.1)
    
    # Normally chain_hash is supplied, we force it manually
    ev1.metadata["chain_hash"] = "invalid_hash"
    
    await store.append("audit-test-1", [ev1], 0)
    
    res = await run_integrity_check(store, "audit", "test-1")
    assert res.events_verified == 0
    assert res.tamper_detected is True
    assert res.chain_valid is False

@pytest.mark.asyncio
async def test_gas_town_recovery_resumes_from_correct_node(store):
    from ledger.schema.events import AgentSessionStarted, AgentNodeExecuted, AgentSessionFailed
    
    ev1 = AgentSessionStarted(application_id=uuid4(), agent_model="m", agent_version="v")
    ev2 = AgentNodeExecuted(node_name="validate_inputs", node_sequence=1, llm_tokens_input=1, llm_tokens_output=1, llm_cost_usd=0.0)
    ev3 = AgentNodeExecuted(node_name="load_facts", node_sequence=2, llm_tokens_input=1, llm_tokens_output=1, llm_cost_usd=0.0)
    ev4 = AgentSessionFailed(application_id=uuid4(), error_type="Crash", error_message="Memory full")
    
    await store.append("agent-sys-s1", [ev1, ev2, ev3, ev4], 0)
    
    ctx = await reconstruct_agent_context(store, "sys", "s1", 8000)
    assert ctx["status"] == "IN_PROGRESS"
    assert ctx["needs_reconciliation"] is True
    
    # Summarized node validate_inputs
    assert "validate_inputs completed" in ctx["summary"]
    
    # Check verbatims explicitly
    events = ctx["recent_events"]
    assert len(events) >= 1
    # Check that failed session error is preserved verbatim to pick up accurately
    assert any(e.event_type == "AgentSessionFailed" for e in events)
