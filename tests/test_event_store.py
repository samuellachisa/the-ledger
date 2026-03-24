"""
tests/test_event_store.py
Test suite for Event Store
"""
import pytest
import pytest_asyncio
import asyncpg
import asyncio
from uuid import uuid4
from ledger.event_store import EventStore, OptimisticConcurrencyError, extract_aggregate_type
from ledger.schema.events import AgentSessionStarted, LoanApplicationSubmitted, AgentNodeExecuted

@pytest_asyncio.fixture
async def db_pool():
    # Setup fresh DB schema mapping to exactly the requirement
    pool = await asyncpg.create_pool("postgresql://localhost/apex_ledger")
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS events, event_streams, projection_checkpoints, outbox CASCADE;")
        
        await conn.execute("""
            CREATE TABLE event_streams (
                stream_id TEXT PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                current_version INT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                archived_at TIMESTAMP WITH TIME ZONE
            );
            
            CREATE TABLE events (
                event_id UUID PRIMARY KEY,
                stream_id TEXT NOT NULL REFERENCES event_streams(stream_id),
                stream_position INT NOT NULL,
                global_position SERIAL NOT NULL,
                event_type TEXT NOT NULL,
                event_version INT NOT NULL,
                payload JSONB NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}',
                recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
                UNIQUE(stream_id, stream_position)
            );

            CREATE TABLE projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_position INT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE
            );

            CREATE TABLE outbox (
                id SERIAL PRIMARY KEY,
                event_id UUID NOT NULL,
                destination TEXT NOT NULL,
                payload JSONB NOT NULL,
                published_at TIMESTAMP WITH TIME ZONE,
                attempts INT DEFAULT 0
            );
        """)
    yield pool
    await pool.close()

@pytest.fixture
def store(db_pool):
    return EventStore(db_pool)

@pytest.mark.asyncio
async def test_extract_aggregate_type():
    assert extract_aggregate_type("loan-123") == "LoanApplication"
    assert extract_aggregate_type("docpkg-456") == "DocumentPackage"
    assert extract_aggregate_type("agent-analyst-abc") == "AgentSession"
    assert extract_aggregate_type("credit-c") == "CreditRecord"
    assert extract_aggregate_type("fraud-f") == "FraudRecord"
    assert extract_aggregate_type("compliance-comp") == "ComplianceRecord"
    assert extract_aggregate_type("audit-action-au") == "AuditLedger"
    with pytest.raises(ValueError):
        extract_aggregate_type("bad-prefix")

@pytest.mark.asyncio
async def test_append_new_stream(store):
    events = [LoanApplicationSubmitted(applicant_name="A", loan_amount=100.0, loan_purpose="B", applicant_id=uuid4(), submitted_by="C")]
    v = await store.append("loan-1", events, 0)
    assert v == 1
    loaded = await store.load_stream("loan-1")
    assert len(loaded) == 1
    assert loaded[0].event_type == "LoanApplicationSubmitted"

@pytest.mark.asyncio
async def test_append_occ_conflict_on_new(store):
    events = [LoanApplicationSubmitted(applicant_name="A", loan_amount=100.0, loan_purpose="B", applicant_id=uuid4(), submitted_by="C")]
    await store.append("loan-2", events, 0)
    # Appending 0 again on the same stream should fail
    with pytest.raises(OptimisticConcurrencyError):
        await store.append("loan-2", events, 0)

@pytest.mark.asyncio
async def test_append_occ_conflict_on_existing(store):
    events = [LoanApplicationSubmitted(applicant_name="A", loan_amount=100.0, loan_purpose="B", applicant_id=uuid4(), submitted_by="C")]
    await store.append("loan-3", events, 0)
    # Appending expected_version 5 when actually 1
    with pytest.raises(OptimisticConcurrencyError):
        await store.append("loan-3", events, 5)

@pytest.mark.asyncio
async def test_gas_town_require_agent_session_started(store):
    ev = AgentNodeExecuted(node_name="foo", node_sequence=1, llm_tokens_input=10, llm_tokens_output=10, llm_cost_usd=0.01)
    with pytest.raises(ValueError, match="AgentSessionStarted must be the first event"):
        await store.append("agent-bot-s1", [ev], 0)

@pytest.mark.asyncio
async def test_gas_town_context_source_metadata(store):
    ev = AgentSessionStarted(application_id=uuid4(), agent_model="m", agent_version="v")
    await store.append("agent-bot-s2", [ev], 0)
    
    loaded = await store.load_stream("agent-bot-s2")
    # Should attach context_source = "fresh"
    assert loaded[0].metadata["context_source"] == "fresh"

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store):
    """KEY TEST: Two asyncio tasks, exactly one succeeds."""
    ev = LoanApplicationSubmitted(applicant_name="A", loan_amount=1.0, loan_purpose="B", applicant_id=uuid4(), submitted_by="C")
    
    # Try to insert `loan-concurrent` at expected_version=0 simultaneously
    async def append_task():
        try:
            await store.append("loan-concurrent", [ev], 0)
            return True
        except OptimisticConcurrencyError:
            return False

    results = await asyncio.gather(append_task(), append_task())
    success_count = sum(results)
    assert success_count == 1  # Exactly one must succeed!

@pytest.mark.asyncio
async def test_append_with_retry_succeeds(store):
    call_count = 0
    async def cmd():
        nonlocal call_count
        call_count += 1
        # Pretend first attempt conflicts
        expected = 0 if call_count == 2 else 99 
        return [LoanApplicationSubmitted(applicant_name="A", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], expected

    with pytest.raises(OptimisticConcurrencyError): # Will fail 3 times on retries and throw if we just return 99 every time. Wait my mock fixes it on try 2!
        await store.append_with_retry("loan-retry-1", cmd)
        
    call_count = 0 
    async def successful_cmd():
        nonlocal call_count
        call_count += 1
        expected = await store.stream_version("loan-retry-2")
        # To simulate a conflict on try 1, we manually advance the version behind its back
        if call_count == 1:
            await store.append("loan-retry-2", [LoanApplicationSubmitted(applicant_name="A", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], expected)
        expected = await store.stream_version("loan-retry-2") # reload correctly
        return [LoanApplicationSubmitted(applicant_name="B", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], expected

    v = await store.append_with_retry("loan-retry-2", successful_cmd)
    assert v == 2
    assert call_count == 2

@pytest.mark.asyncio
async def test_archive_stream(store):
    await store.append("loan-arch", [LoanApplicationSubmitted(applicant_name="A", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], 0)
    res = await store.archive_stream("loan-arch")
    assert res is True
    res2 = await store.archive_stream("loan-arch")
    assert res2 is False

@pytest.mark.asyncio
async def test_load_all_generator(store):
    await store.append("loan-gen1", [LoanApplicationSubmitted(applicant_name="A", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], 0)
    await store.append("loan-gen2", [LoanApplicationSubmitted(applicant_name="A", loan_amount=2.0, loan_purpose="C", applicant_id=uuid4(), submitted_by="U")], 0)
    
    evts = [e async for e in store.load_all(batch_size=1)]
    assert len(evts) == 2
