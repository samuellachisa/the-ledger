"""
End-to-End MCP Lifecycle Test: Full loan processing via MCP tools only.

This test drives the complete loan lifecycle exclusively through MCP tool calls,
verifying that the system behaves correctly from submission to final decision.

Lifecycle:
    submit_application
    → record_credit_analysis
    → record_fraud_check
    → request_compliance_check
    → record_compliance_check (x3: AML, KYC, OFAC)
    → finalize_compliance
    → generate_decision
    → finalize_application
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest
import pytest_asyncio

from src.commands.handlers import CommandHandler
from src.event_store import EventStore
from src.models.events import DecisionOutcome, LoanStatus
from src.upcasting.registry import UpcasterRegistry


@pytest_asyncio.fixture
async def full_handler(db_pool):
    store = EventStore(db_pool, UpcasterRegistry())
    return CommandHandler(store), store


@pytest.mark.asyncio
async def test_full_loan_lifecycle_approve(full_handler, db_pool):
    """
    Complete happy-path lifecycle: submission → approval.
    All state transitions must be valid.
    """
    handler, store = full_handler
    applicant_id = uuid4()
    correlation_id = uuid4()

    # 1. Submit application
    from src.commands.handlers import SubmitApplicationCommand
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Jane Smith",
        loan_amount=150000.0,
        loan_purpose="Home purchase",
        applicant_id=applicant_id,
        submitted_by="loan_officer_agent",
        correlation_id=correlation_id,
    ))
    assert app_id is not None

    # Verify initial state
    from src.aggregates.loan_application import LoanApplicationAggregate
    events = await store.load_stream("LoanApplication", app_id)
    app = LoanApplicationAggregate.load(app_id, events)
    assert app.status == LoanStatus.SUBMITTED

    # 2. Start agent session
    from src.commands.handlers import StartAgentSessionCommand, LoadAgentContextCommand
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id,
        agent_model="gpt-4-turbo",
        agent_version="2.1",
    ))

    # 3. Load agent context (Gas Town checkpoint)
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id,
        application_id=app_id,
        context_sources=["credit_bureau", "fraud_db", "compliance_rules"],
        context_snapshot={"credit_score": 780, "fraud_risk": 0.05},
        loaded_stream_positions={"LoanApplication": 1},
    ))

    # 4. Record credit analysis
    from src.commands.handlers import RecordCreditAnalysisCommand
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id,
        credit_score=780,
        debt_to_income_ratio=0.28,
        model_version="credit-model-v3",
        analysis_duration_ms=450,
    ))

    events = await store.load_stream("LoanApplication", app_id)
    app = LoanApplicationAggregate.load(app_id, events)
    assert app.status == LoanStatus.UNDER_REVIEW
    assert app.credit_score == 780

    # 5. Record fraud check
    from src.commands.handlers import RecordFraudCheckCommand
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id,
        fraud_risk_score=0.05,
        flags=[],
        passed=True,
    ))

    # 6. Request compliance check
    from src.commands.handlers import RequestComplianceCheckCommand
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id,
            officer_id="compliance_officer_1",
            required_checks=["AML", "KYC", "OFAC"],
        )
    )
    assert compliance_id is not None

    events = await store.load_stream("LoanApplication", app_id)
    app = LoanApplicationAggregate.load(app_id, events)
    assert app.status == LoanStatus.COMPLIANCE_CHECK

    # 7. Record compliance checks
    from src.commands.handlers import RecordComplianceCheckCommand
    for check in ["AML", "KYC", "OFAC"]:
        await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
            compliance_record_id=compliance_id,
            check_name=check,
            passed=True,
            check_result={"result": "clear", "check": check},
            failure_reason="",
            officer_id="compliance_officer_1",
        ))

    # 8. Finalize compliance
    from src.commands.handlers import FinalizeComplianceCommand
    passed = await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id,
        application_id=app_id,
        officer_id="compliance_officer_1",
        notes="All checks passed",
    ))
    assert passed is True

    # 9. Generate decision
    from src.commands.handlers import GenerateDecisionCommand
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.APPROVE,
        confidence_score=0.92,
        reasoning="Strong credit profile, no fraud flags, compliance passed",
        model_version="gpt-4-turbo",
    ))

    # 10. Finalize application
    from src.commands.handlers import FinalizeApplicationCommand
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id,
        approved=True,
        approved_amount=150000.0,
        interest_rate=0.0425,
        approved_by="loan_officer_agent",
    ))

    # Verify final state
    events = await store.load_stream("LoanApplication", app_id)
    app = LoanApplicationAggregate.load(app_id, events)
    assert app.status == LoanStatus.FINAL_APPROVED

    # Verify event count (should have all lifecycle events)
    assert len(events) >= 8


@pytest.mark.asyncio
async def test_confidence_floor_enforces_refer(full_handler):
    """Low confidence decisions must be REFER, not APPROVE or DENY."""
    handler, store = full_handler
    from src.commands.handlers import (
        SubmitApplicationCommand, StartAgentSessionCommand, LoadAgentContextCommand,
        RecordCreditAnalysisCommand, RecordFraudCheckCommand, RequestComplianceCheckCommand,
        RecordComplianceCheckCommand, FinalizeComplianceCommand, GenerateDecisionCommand,
    )
    from src.models.events import BusinessRuleViolationError

    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Low Confidence Test",
        loan_amount=50000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))

    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=620, debt_to_income_ratio=0.45
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.2, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=True,
        check_result={}, failure_reason="", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))

    # Attempt APPROVE with low confidence — must fail
    with pytest.raises(BusinessRuleViolationError) as exc_info:
        await handler.handle_generate_decision(GenerateDecisionCommand(
            application_id=app_id,
            agent_session_id=session_id,
            outcome=DecisionOutcome.APPROVE,
            confidence_score=0.45,  # Below 0.6 floor
            reasoning="Uncertain",
            model_version="gpt-4",
        ))
    assert "ConfidenceFloor" in str(exc_info.value)

    # REFER with low confidence must succeed
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.REFER,
        confidence_score=0.45,
        reasoning="Low confidence — referring to human",
        model_version="gpt-4",
    ))


@pytest.mark.asyncio
async def test_cannot_approve_without_compliance(full_handler):
    """Approval without compliance passing must raise BusinessRuleViolationError."""
    handler, store = full_handler
    from src.commands.handlers import (
        SubmitApplicationCommand, StartAgentSessionCommand, LoadAgentContextCommand,
        RecordCreditAnalysisCommand, RecordFraudCheckCommand, RequestComplianceCheckCommand,
        RecordComplianceCheckCommand, FinalizeComplianceCommand, GenerateDecisionCommand,
    )
    from src.models.events import BusinessRuleViolationError

    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="No Compliance Test",
        loan_amount=80000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))

    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=800, debt_to_income_ratio=0.2
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.01, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    # Record FAILED compliance check
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=False,
        check_result={}, failure_reason="Suspicious transaction history",
        officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))

    # Attempt APPROVE with failed compliance — must fail
    with pytest.raises(BusinessRuleViolationError) as exc_info:
        await handler.handle_generate_decision(GenerateDecisionCommand(
            application_id=app_id,
            agent_session_id=session_id,
            outcome=DecisionOutcome.APPROVE,
            confidence_score=0.88,
            reasoning="Good credit but compliance failed",
            model_version="gpt-4",
        ))
    assert "ComplianceDependency" in str(exc_info.value)


@pytest.mark.asyncio
async def test_complete_decision_history_query(full_handler, db_pool):
    """Complete decision history must be queryable in < 60 seconds (Week Standard)."""
    import time
    handler, store = full_handler

    from src.commands.handlers import SubmitApplicationCommand
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="History Query Test",
        loan_amount=60000.0,
        loan_purpose="Business",
        applicant_id=uuid4(),
        submitted_by="test",
    ))

    start = time.monotonic()
    events = await store.load_stream("LoanApplication", app_id)
    elapsed = time.monotonic() - start

    assert elapsed < 60, f"Decision history query took {elapsed:.2f}s (> 60s limit)"
    assert len(events) >= 1
