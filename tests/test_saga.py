"""
Tests for LoanProcessingSaga — durable process manager.

Verifies:
1. FraudCheckCompleted automatically triggers request_compliance_check
2. ComplianceRecordFinalized automatically triggers finalize_compliance
3. Saga state is persisted and idempotent (re-processing same event is a no-op)
4. Saga gracefully handles manual path (application already advanced)
5. Full automated path: fraud → compliance requested → compliance finalized → PendingDecision
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.commands.handlers import (
    CommandHandler,
    RecordComplianceCheckCommand,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
)
from src.models.events import LoanStatus
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.saga.loan_processing_saga import SagaManager, SagaStep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _submit_and_credit(handler: CommandHandler, **kwargs) -> tuple:
    """Submit an application and record credit analysis. Returns (app_id,)."""
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test Applicant",
        loan_amount=50_000,
        loan_purpose="home improvement",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id,
        credit_score=720,
        debt_to_income_ratio=0.3,
        model_version="credit-v3",
    ))
    return app_id


async def _load_app(event_store, app_id):
    events = await event_store.load_stream(LoanApplicationAggregate.aggregate_type, app_id)
    return LoanApplicationAggregate.load(app_id, events)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fraud_check_triggers_compliance_request(db_pool, event_store, handler, saga_manager):
    """FraudCheckCompleted → saga auto-requests compliance check."""
    app_id = await _submit_and_credit(handler)

    # Record fraud check (app is now UnderReview)
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id,
        fraud_risk_score=0.05,
        flags=[],
        passed=True,
    ))

    # Run one saga batch
    await saga_manager._process_batch()

    # Application should now be in ComplianceCheck
    app = await _load_app(event_store, app_id)
    assert app.status == LoanStatus.COMPLIANCE_CHECK
    assert app.compliance_record_id is not None

    # Saga state should be COMPLIANCE_REQUESTED
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT step, compliance_record_id FROM saga_instances WHERE application_id = $1",
            app_id,
        )
    assert row is not None
    assert row["step"] == SagaStep.COMPLIANCE_REQUESTED.value
    assert row["compliance_record_id"] is not None


@pytest.mark.asyncio
async def test_compliance_finalized_triggers_pending_decision(db_pool, event_store, handler, saga_manager):
    """ComplianceRecordFinalized → saga auto-finalizes → application PendingDecision."""
    app_id = await _submit_and_credit(handler)

    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.02, flags=[], passed=True,
    ))

    # Saga step 1: request compliance
    await saga_manager._process_batch()

    app = await _load_app(event_store, app_id)
    compliance_id = app.compliance_record_id
    assert compliance_id is not None

    # Record all required checks on the compliance record
    for check in ["AML", "KYC", "OFAC"]:
        await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
            compliance_record_id=compliance_id,
            check_name=check,
            passed=True,
            check_result={"result": "clear"},
            failure_reason="",
            officer_id="officer-1",
        ))

    # Finalize the compliance record aggregate directly
    # (in production an officer does this via MCP tool)
    from src.aggregates.compliance_record import ComplianceRecordAggregate
    comp_events = await event_store.load_stream(
        ComplianceRecordAggregate.aggregate_type, compliance_id
    )
    comp = ComplianceRecordAggregate.load(compliance_id, comp_events)
    comp.finalize(officer_id="officer-1", notes="all clear")
    await event_store.append(
        aggregate_type=ComplianceRecordAggregate.aggregate_type,
        aggregate_id=compliance_id,
        events=comp.pending_events,
        expected_version=comp.version - len(comp.pending_events),
    )

    # Saga step 2: finalize compliance on the loan application
    await saga_manager._process_batch()

    app = await _load_app(event_store, app_id)
    assert app.status == LoanStatus.PENDING_DECISION
    assert app.compliance_passed is True

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT step FROM saga_instances WHERE application_id = $1", app_id
        )
    assert row["step"] == SagaStep.COMPLIANCE_FINALIZED.value


@pytest.mark.asyncio
async def test_saga_idempotent_on_replay(db_pool, event_store, handler, saga_manager):
    """Processing the same FraudCheckCompleted event twice does not double-issue commands."""
    app_id = await _submit_and_credit(handler)

    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.05, flags=[], passed=True,
    ))

    # First batch — issues compliance request
    await saga_manager._process_batch()
    app_after_first = await _load_app(event_store, app_id)
    assert app_after_first.status == LoanStatus.COMPLIANCE_CHECK

    # Reset checkpoint to force replay
    saga_manager._last_position = 0

    # Second batch — should be a no-op (saga already at COMPLIANCE_REQUESTED)
    await saga_manager._process_batch()

    app_after_second = await _load_app(event_store, app_id)
    # Status unchanged — no duplicate compliance record created
    assert app_after_second.status == LoanStatus.COMPLIANCE_CHECK
    assert app_after_second.compliance_record_id == app_after_first.compliance_record_id


@pytest.mark.asyncio
async def test_saga_handles_manual_compliance_request(db_pool, event_store, handler, saga_manager):
    """
    Manual request_compliance_check before saga runs → saga marks itself completed.
    """
    from src.commands.handlers import RequestComplianceCheckCommand

    app_id = await _submit_and_credit(handler)

    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.05, flags=[], passed=True,
    ))

    # Manual call — app moves to ComplianceCheck
    await handler.handle_request_compliance_check(RequestComplianceCheckCommand(
        application_id=app_id,
        officer_id="manual-officer",
        required_checks=["AML", "KYC"],
    ))

    # Saga processes FraudCheckCompleted — app is already past UnderReview
    await saga_manager._process_batch()

    # Saga should be COMPLETED (graceful no-op), not FAILED
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT step FROM saga_instances WHERE application_id = $1", app_id
        )
    assert row is not None
    assert row["step"] == SagaStep.COMPLETED.value


@pytest.mark.asyncio
async def test_saga_checkpoint_persisted(db_pool, event_store, handler, saga_manager):
    """Saga checkpoint advances after processing events."""
    app_id = await _submit_and_credit(handler)
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.05, flags=[], passed=True,
    ))

    assert saga_manager._last_position == 0
    await saga_manager._process_batch()
    assert saga_manager._last_position > 0

    # Checkpoint is persisted to DB
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_position FROM saga_checkpoints WHERE saga_name = 'LoanProcessingSaga'"
        )
    assert row["last_position"] == saga_manager._last_position
