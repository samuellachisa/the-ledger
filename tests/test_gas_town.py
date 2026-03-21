"""
Gas Town Tests: Agent crash recovery and context reconstruction.

Verifies that:
1. AgentContextLoaded checkpoints are preserved after crash
2. Agent can resume from last checkpoint
3. Context sources are not re-fetched after recovery
4. Decision cannot be made without context (Gas Town rule)
"""
from __future__ import annotations

from uuid import uuid4

import pytest

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.aggregates.agent_session import AgentSessionAggregate
from src.integrity.gas_town import AgentContextReconstructor
from src.models.events import AgentSessionStatus, BusinessRuleViolationError


@pytest.mark.asyncio
async def test_context_preserved_after_crash(event_store):
    """Crash recovery preserves loaded context sources and stream positions."""
    session_id = uuid4()
    application_id = uuid4()
    reconstructor = AgentContextReconstructor(event_store)

    # Start session
    session = AgentSessionAggregate(session_id)
    session.start(
        application_id=application_id,
        agent_model="gpt-4-turbo",
        agent_version="1.0",
    )
    await event_store.append("AgentSession", session_id, session.pending_events, expected_version=0)

    # Load context (Gas Town checkpoint)
    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)
    session.load_context(
        application_id=application_id,
        context_sources=["credit_bureau", "fraud_db", "compliance_rules"],
        context_snapshot={"credit_score": 720, "fraud_risk": 0.1},
        loaded_stream_positions={"LoanApplication": 3, "ComplianceRecord": 2},
    )
    await event_store.append(
        "AgentSession", session_id, session.pending_events,
        expected_version=session.version - len(session.pending_events)
    )

    # Simulate crash
    result = await reconstructor.simulate_crash_and_recover(
        session_id=session_id,
        event_store=event_store,
        application_id=application_id,
    )

    assert result["checkpoints_preserved"], "Stream positions must be preserved after crash"
    assert result["context_preserved"], "Context sources must be preserved after crash"
    assert result["after_crash"]["crash_detected"] is True
    assert result["after_recovery"]["status"] == AgentSessionStatus.PROCESSING


@pytest.mark.asyncio
async def test_cannot_decide_without_context(event_store):
    """Gas Town rule: decision cannot be made without AgentContextLoaded."""
    session_id = uuid4()
    application_id = uuid4()

    # Start session but DON'T load context
    session = AgentSessionAggregate(session_id)
    session.start(
        application_id=application_id,
        agent_model="gpt-4-turbo",
        agent_version="1.0",
    )
    await event_store.append("AgentSession", session_id, session.pending_events, expected_version=0)

    # Attempt decision without context
    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)

    from src.models.events import DecisionOutcome
    with pytest.raises(BusinessRuleViolationError) as exc_info:
        session.record_decision(
            application_id=application_id,
            outcome=DecisionOutcome.APPROVE,
            confidence_score=0.9,
            reasoning="Test",
            processing_duration_ms=100,
        )

    assert "GasTown" in str(exc_info.value)
    assert "AgentContextLoaded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_reconstruct_returns_correct_checkpoint(event_store):
    """reconstruct_agent_context returns the last loaded stream positions."""
    session_id = uuid4()
    application_id = uuid4()
    reconstructor = AgentContextReconstructor(event_store)

    session = AgentSessionAggregate(session_id)
    session.start(application_id=application_id, agent_model="gpt-4", agent_version="1.0")
    await event_store.append("AgentSession", session_id, session.pending_events, expected_version=0)

    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)
    session.load_context(
        application_id=application_id,
        context_sources=["credit_bureau"],
        context_snapshot={"credit_score": 680},
        loaded_stream_positions={"LoanApplication": 5},
    )
    await event_store.append(
        "AgentSession", session_id, session.pending_events,
        expected_version=session.version - len(session.pending_events)
    )

    context = await reconstructor.reconstruct_agent_context(session_id)

    assert context["context_loaded"] is True
    assert context["loaded_stream_positions"] == {"LoanApplication": 5}
    assert context["context_sources"] == ["credit_bureau"]
    assert context["can_make_decision"] is True
    assert context["crash_detected"] is False


@pytest.mark.asyncio
async def test_missing_context_sources_after_partial_load(event_store):
    """get_missing_context_sources returns only unloaded sources."""
    session_id = uuid4()
    application_id = uuid4()
    reconstructor = AgentContextReconstructor(event_store)

    session = AgentSessionAggregate(session_id)
    session.start(application_id=application_id, agent_model="gpt-4", agent_version="1.0")
    await event_store.append("AgentSession", session_id, session.pending_events, expected_version=0)

    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)
    session.load_context(
        application_id=application_id,
        context_sources=["credit_bureau", "fraud_db"],  # Only 2 of 3 loaded
        context_snapshot={},
        loaded_stream_positions={},
    )
    await event_store.append(
        "AgentSession", session_id, session.pending_events,
        expected_version=session.version - len(session.pending_events)
    )

    required = ["credit_bureau", "fraud_db", "compliance_rules"]
    missing = await reconstructor.get_missing_context_sources(session_id, required)

    assert missing == ["compliance_rules"]


@pytest.mark.asyncio
async def test_session_not_found_returns_error(event_store):
    """reconstruct_agent_context handles missing sessions gracefully."""
    reconstructor = AgentContextReconstructor(event_store)
    result = await reconstructor.reconstruct_agent_context(uuid4())

    assert result["context_loaded"] is False
    assert "error" in result


@pytest.mark.asyncio
async def test_resume_requires_crashed_status(event_store):
    """resume() can only be called on a crashed session."""
    session_id = uuid4()
    application_id = uuid4()

    session = AgentSessionAggregate(session_id)
    session.start(application_id=application_id, agent_model="gpt-4", agent_version="1.0")
    await event_store.append("AgentSession", session_id, session.pending_events, expected_version=0)

    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)

    from src.models.events import InvalidStateTransitionError
    with pytest.raises(InvalidStateTransitionError):
        session.resume(application_id=application_id)
