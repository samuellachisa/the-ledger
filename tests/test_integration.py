"""
End-to-end integration tests: full happy path and key cross-cutting scenarios.

Covers the complete loan processing workflow:
  submit → credit → fraud → compliance → decision → finalize

Also tests: withdraw, agent session, what-if, regulatory package, outbox relay,
replay engine, and auth token admin tools.
"""
from __future__ import annotations

import pytest
from uuid import uuid4, UUID

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.commands.handlers import (
    CommandHandler,
    FinalizeApplicationCommand,
    FinalizeComplianceCommand,
    GenerateDecisionCommand,
    LoadAgentContextCommand,
    RecordComplianceCheckCommand,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    RequestComplianceCheckCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
)
from src.models.events import DecisionOutcome, LoanStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _full_workflow(handler: CommandHandler, compliance_checks=("AML", "KYC")) -> dict:
    """Run the complete happy-path workflow. Returns IDs for assertions."""
    applicant_id = uuid4()

    # 1. Submit
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Jane Smith",
        loan_amount=50_000.0,
        loan_purpose="Home renovation",
        applicant_id=applicant_id,
        submitted_by="test-agent",
    ))

    # 2. Credit analysis
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id,
        credit_score=720,
        debt_to_income_ratio=0.28,
        model_version="v2.1",
    ))

    # 3. Fraud check
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id,
        fraud_risk_score=0.05,
        flags=[],
        passed=True,
    ))

    # 4. Compliance
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id,
            officer_id="officer-1",
            required_checks=list(compliance_checks),
        )
    )
    for check in compliance_checks:
        await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
            compliance_record_id=compliance_id,
            check_name=check,
            passed=True,
            check_result={"result": "pass"},
            failure_reason="",
            officer_id="officer-1",
        ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id,
        application_id=app_id,
        officer_id="officer-1",
    ))

    # 5. Agent session
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id,
        agent_model="gpt-4o",
        agent_version="2024-11",
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id,
        application_id=app_id,
        context_sources=["credit_bureau", "fraud_db"],
        context_snapshot={"credit_score": 720},
        loaded_stream_positions={},
    ))

    # 6. Decision
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.APPROVE,
        confidence_score=0.92,
        reasoning="Strong credit profile, low DTI, no fraud flags.",
        model_version="gpt-4o-2024-11",
    ))

    # 7. Finalize
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id,
        approved=True,
        approved_amount=50_000.0,
        interest_rate=0.065,
        approved_by="officer-1",
    ))

    return {"app_id": app_id, "compliance_id": compliance_id, "session_id": session_id}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_happy_path(handler, event_store):
    """Complete workflow produces FinalApproved state."""
    ids = await _full_workflow(handler)
    events = await event_store.load_stream("LoanApplication", ids["app_id"])
    from src.aggregates.loan_application import LoanApplicationAggregate
    app = LoanApplicationAggregate.load(ids["app_id"], events)
    assert app.status == LoanStatus.FINAL_APPROVED
    assert app.credit_score == 720
    assert app.compliance_passed is True


@pytest.mark.asyncio
async def test_full_deny_path(handler, event_store):
    """Low credit score + compliance failure → Denied."""
    applicant_id = uuid4()
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Bob Jones",
        loan_amount=100_000.0,
        loan_purpose="Speculative investment",
        applicant_id=applicant_id,
        submitted_by="test-agent",
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=580, debt_to_income_ratio=0.55,
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.3, flags=["velocity"], passed=True,
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(application_id=app_id, officer_id="o1", required_checks=["AML"])
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=False,
        check_result={}, failure_reason="Suspicious pattern", officer_id="o1",
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="o1",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4o", agent_version="2024-11",
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={}, loaded_stream_positions={},
    ))
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id, agent_session_id=session_id,
        outcome=DecisionOutcome.DENY, confidence_score=0.88,
        reasoning="Failed AML check.", model_version="gpt-4o",
    ))
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id, approved=False,
        denial_reasons=["Failed AML"], denied_by="o1",
    ))
    events = await event_store.load_stream("LoanApplication", app_id)
    from src.aggregates.loan_application import LoanApplicationAggregate
    app = LoanApplicationAggregate.load(app_id, events)
    assert app.status == LoanStatus.DENIED


# ---------------------------------------------------------------------------
# Withdraw
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_withdraw_after_submission(handler, event_store):
    """Application can be withdrawn after submission."""
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Alice", loan_amount=10_000.0, loan_purpose="Car",
        applicant_id=uuid4(), submitted_by="agent",
    ))
    events = await event_store.load_stream("LoanApplication", app_id)
    from src.aggregates.loan_application import LoanApplicationAggregate
    app = LoanApplicationAggregate.load(app_id, events)
    app.withdraw(withdrawn_by="applicant", reason="Changed mind")
    await event_store.append(
        aggregate_type="LoanApplication", aggregate_id=app_id,
        events=app.pending_events,
        expected_version=app.version - len(app.pending_events),
    )
    events2 = await event_store.load_stream("LoanApplication", app_id)
    app2 = LoanApplicationAggregate.load(app_id, events2)
    assert app2.status == LoanStatus.WITHDRAWN


@pytest.mark.asyncio
async def test_cannot_withdraw_approved(handler, event_store):
    """Cannot withdraw a FinalApproved application."""
    from src.models.events import InvalidStateTransitionError
    ids = await _full_workflow(handler)
    events = await event_store.load_stream("LoanApplication", ids["app_id"])
    from src.aggregates.loan_application import LoanApplicationAggregate
    app = LoanApplicationAggregate.load(ids["app_id"], events)
    with pytest.raises(InvalidStateTransitionError):
        app.withdraw(withdrawn_by="applicant", reason="Too late")


# ---------------------------------------------------------------------------
# Agent session
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_session_lifecycle(handler, event_store):
    """start_agent_session → load_agent_context produces ContextLoaded status."""
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test", loan_amount=5000.0, loan_purpose="Test",
        applicant_id=uuid4(), submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4o", agent_version="v1",
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"],
        context_snapshot={"score": 700},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    from src.aggregates.agent_session import AgentSessionAggregate
    from src.models.events import AgentSessionStatus
    events = await event_store.load_stream("AgentSession", session_id)
    session = AgentSessionAggregate.load(session_id, events)
    assert session.status == AgentSessionStatus.CONTEXT_LOADED


# ---------------------------------------------------------------------------
# What-if projector
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_what_if_credit_score(handler, event_store):
    """What-if with higher credit score should not change outcome if already approved."""
    ids = await _full_workflow(handler)
    from src.what_if.projector import WhatIfProjector
    from src.models.events import CreditAnalysisCompleted
    projector = WhatIfProjector(event_store)
    cf_event = CreditAnalysisCompleted(
        aggregate_id=ids["app_id"], credit_score=800, debt_to_income_ratio=0.2,
    )
    result = await projector.run_what_if(ids["app_id"], [cf_event])
    assert "baseline_outcome" in result
    assert "counterfactual_outcome" in result
    assert "narrative" in result
    assert result["injected_events"] == ["CreditAnalysisCompleted"]


@pytest.mark.asyncio
async def test_what_if_removes_causally_dependent_events(handler, event_store):
    """Replacing CreditAnalysisCompleted should strip DecisionGenerated from counterfactual."""
    ids = await _full_workflow(handler)
    from src.what_if.projector import WhatIfProjector
    from src.models.events import CreditAnalysisCompleted
    projector = WhatIfProjector(event_store)
    cf_event = CreditAnalysisCompleted(
        aggregate_id=ids["app_id"], credit_score=400, debt_to_income_ratio=0.9,
    )
    result = await projector.run_what_if(ids["app_id"], [cf_event])
    # DecisionGenerated and ApplicationApproved should be causally removed
    assert "DecisionGenerated" in result["causally_removed_events"] or \
           "ApplicationApproved" in result["causally_removed_events"]


# ---------------------------------------------------------------------------
# Regulatory package
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_regulatory_package_generation(handler, event_store, db_pool):
    """Regulatory package contains all events and valid integrity proof."""
    ids = await _full_workflow(handler)
    from src.regulatory.package import generate_regulatory_package, verify_regulatory_package
    package = await generate_regulatory_package(
        pool=db_pool,
        event_store=event_store,
        application_id=ids["app_id"],
        generated_by="test",
    )
    assert package["event_count"] > 0
    assert "package_hash" in package
    assert "integrity_proof" in package

    verification = verify_regulatory_package(package)
    assert verification["package_hash_valid"] is True
    assert verification["event_count_matches"] is True
    assert verification["raw_upcasted_consistent"] is True


# ---------------------------------------------------------------------------
# Outbox relay
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_outbox_relay_publishes_events(handler, db_pool):
    """OutboxRelay drains pending outbox entries."""
    from src.outbox.relay import OutboxRelay, InMemoryPublisher
    publisher = InMemoryPublisher()
    relay = OutboxRelay(db_pool, publisher)

    # Submit creates outbox entries
    await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test", loan_amount=1000.0, loan_purpose="Test",
        applicant_id=uuid4(), submitted_by="test",
    ))

    processed = await relay._process_batch()
    assert processed > 0
    assert len(publisher.published) > 0
    assert publisher.published[0]["event_type"] == "LoanApplicationSubmitted"

    # Verify outbox entries are now published
    async with db_pool.acquire() as conn:
        pending = await conn.fetchval("SELECT COUNT(*) FROM outbox WHERE status = 'pending'")
    assert pending == 0


# ---------------------------------------------------------------------------
# Replay engine
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_range(handler, event_store, db_pool, dead_letter):
    """ReplayEngine can replay events in a position range."""
    from src.replay.engine import ReplayEngine, ReplayedEvent
    import asyncpg

    await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test", loan_amount=1000.0, loan_purpose="Test",
        applicant_id=uuid4(), submitted_by="test",
    ))

    replayed_events = []

    async def capture_handler(conn: asyncpg.Connection, event: ReplayedEvent) -> None:
        replayed_events.append(event.event_type)

    engine = ReplayEngine(db_pool, event_store, dead_letter)
    result = await engine.replay_range(
        after_position=0,
        before_position=999999,
        processor_name="test",
        handler_fn=capture_handler,
    )
    assert result["replayed"] >= 1
    assert "LoanApplicationSubmitted" in replayed_events


# ---------------------------------------------------------------------------
# Auth token admin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_issue_verify_revoke(token_store):
    """Full token lifecycle: issue → verify → revoke → verify fails."""
    from src.auth.tokens import Role, AuthError
    raw = await token_store.issue_token("agent-test", [Role.SUBMITTER])
    identity = await token_store.verify(raw)
    assert identity.agent_id == "agent-test"
    assert Role.SUBMITTER in identity.roles

    revoked = await token_store.revoke(identity.token_id, revoked_by="admin")
    assert revoked is True

    with pytest.raises(AuthError) as exc_info:
        await token_store.verify(raw)
    assert exc_info.value.code == "TokenRevoked"


@pytest.mark.asyncio
async def test_token_refresh(token_store):
    """Token refresh produces new token and revokes old one."""
    from src.auth.tokens import Role, AuthError
    raw = await token_store.issue_token("agent-refresh", [Role.DECISION_AGENT])
    new_raw = await token_store.refresh(raw)
    assert new_raw != raw

    # Old token should be revoked
    with pytest.raises(AuthError):
        await token_store.verify(raw)

    # New token should work
    identity = await token_store.verify(new_raw)
    assert identity.agent_id == "agent-refresh"


@pytest.mark.asyncio
async def test_auth_audit_log_written(token_store, db_pool):
    """Auth operations write to auth_audit_log."""
    from src.auth.tokens import Role
    await token_store.issue_token("audit-test", [Role.READ_ONLY])
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM auth_audit_log WHERE agent_id = 'audit-test'"
        )
    assert count >= 1


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idempotent_submit(handler, idempotency_store):
    """Submitting with the same idempotency_key twice returns the same result."""
    from src.mcp.tools import dispatch_tool
    key = f"idem-{uuid4()}"
    args = {
        "applicant_name": "Idem Test",
        "loan_amount": 5000.0,
        "loan_purpose": "Test",
        "applicant_id": str(uuid4()),
        "submitted_by": "test",
        "idempotency_key": key,
    }
    r1 = await dispatch_tool("submit_application", args, handler, idempotency=idempotency_store)
    r2 = await dispatch_tool("submit_application", args, handler, idempotency=idempotency_store)
    import json
    d1 = json.loads(r1.content[0].text)
    d2 = json.loads(r2.content[0].text)
    assert d1["application_id"] == d2["application_id"]
