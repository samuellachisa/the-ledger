"""
Command Handlers: Load Aggregate → Validate Rules → Determine Events → Append.

Each handler follows the strict pattern:
    1. Load aggregate from event store
    2. Execute command (validates business rules, raises events)
    3. Append pending events to event store with OCC

Handlers are the ONLY place that writes to the event store.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore
from src.models.events import DecisionOutcome

logger = logging.getLogger(__name__)


# =============================================================================
# Command DTOs
# =============================================================================

@dataclass
class SubmitApplicationCommand:
    applicant_name: str
    loan_amount: float
    loan_purpose: str
    applicant_id: UUID
    submitted_by: str
    application_id: UUID | None = None
    correlation_id: UUID | None = None


@dataclass
class RecordCreditAnalysisCommand:
    application_id: UUID
    credit_score: int
    debt_to_income_ratio: float
    model_version: str | None = None
    analysis_duration_ms: int | None = None


@dataclass
class RecordFraudCheckCommand:
    application_id: UUID
    fraud_risk_score: float
    flags: list[str]
    passed: bool


@dataclass
class RequestComplianceCheckCommand:
    application_id: UUID
    officer_id: str
    required_checks: list[str]


@dataclass
class RecordComplianceCheckCommand:
    compliance_record_id: UUID
    check_name: str
    passed: bool
    check_result: dict
    failure_reason: str
    officer_id: str


@dataclass
class FinalizeComplianceCommand:
    compliance_record_id: UUID
    application_id: UUID
    officer_id: str
    notes: str = ""


@dataclass
class GenerateDecisionCommand:
    application_id: UUID
    agent_session_id: UUID
    outcome: DecisionOutcome
    confidence_score: float
    reasoning: str
    model_version: str


@dataclass
class FinalizeApplicationCommand:
    application_id: UUID
    approved: bool
    approved_amount: float | None = None
    interest_rate: float | None = None
    approved_by: str = ""
    denial_reasons: list[str] | None = None
    denied_by: str = ""
    referral_reason: str | None = None
    referred_to: str | None = None


@dataclass
class StartAgentSessionCommand:
    application_id: UUID
    agent_model: str
    agent_version: str
    session_config: dict | None = None
    session_id: UUID | None = None


@dataclass
class LoadAgentContextCommand:
    session_id: UUID
    application_id: UUID
    context_sources: list[str]
    context_snapshot: dict
    loaded_stream_positions: dict[str, int]


# =============================================================================
# Command Handler
# =============================================================================

class CommandHandler:
    def __init__(self, event_store: EventStore):
        self._store = event_store

    # -------------------------------------------------------------------------
    # Loan Application Commands
    # -------------------------------------------------------------------------

    async def handle_submit_application(self, cmd: SubmitApplicationCommand) -> UUID:
        """Submit a new loan application. Returns the application_id."""
        application_id = cmd.application_id or uuid4()
        aggregate = LoanApplicationAggregate(application_id)

        aggregate.submit(
            applicant_name=cmd.applicant_name,
            loan_amount=cmd.loan_amount,
            loan_purpose=cmd.loan_purpose,
            applicant_id=cmd.applicant_id,
            submitted_by=cmd.submitted_by,
            correlation_id=cmd.correlation_id,
        )

        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=application_id,
            events=aggregate.pending_events,
            expected_version=0,
        )
        logger.info("Application submitted: %s", application_id)
        return application_id

    async def handle_record_credit_analysis(self, cmd: RecordCreditAnalysisCommand) -> None:
        events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        aggregate = LoanApplicationAggregate.load(cmd.application_id, events)

        # First request analysis (Submitted → AwaitingAnalysis)
        aggregate.request_credit_analysis(requested_by="system")
        # Then record results (AwaitingAnalysis → UnderReview)
        aggregate.record_credit_analysis(
            credit_score=cmd.credit_score,
            debt_to_income_ratio=cmd.debt_to_income_ratio,
            model_version=cmd.model_version,
            analysis_duration_ms=cmd.analysis_duration_ms,
        )

        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=cmd.application_id,
            events=aggregate.pending_events,
            expected_version=aggregate.version - len(aggregate.pending_events),
        )

    async def handle_record_fraud_check(self, cmd: RecordFraudCheckCommand) -> None:
        events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        aggregate = LoanApplicationAggregate.load(cmd.application_id, events)
        aggregate.record_fraud_check(
            fraud_risk_score=cmd.fraud_risk_score,
            flags=cmd.flags,
            passed=cmd.passed,
        )
        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=cmd.application_id,
            events=aggregate.pending_events,
            expected_version=aggregate.version - len(aggregate.pending_events),
        )

    async def handle_request_compliance_check(self, cmd: RequestComplianceCheckCommand) -> UUID:
        """Create compliance record and link to application. Returns compliance_record_id."""
        compliance_id = uuid4()

        # Create compliance record
        compliance = ComplianceRecordAggregate(compliance_id)
        compliance.create(
            application_id=cmd.application_id,
            officer_id=cmd.officer_id,
            required_checks=cmd.required_checks,
        )
        await self._store.append(
            aggregate_type=ComplianceRecordAggregate.aggregate_type,
            aggregate_id=compliance_id,
            events=compliance.pending_events,
            expected_version=0,
        )

        # Link to loan application
        app_events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        app = LoanApplicationAggregate.load(cmd.application_id, app_events)
        app.request_compliance_check(compliance_record_id=compliance_id)
        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=cmd.application_id,
            events=app.pending_events,
            expected_version=app.version - len(app.pending_events),
        )

        return compliance_id

    async def handle_record_compliance_check(self, cmd: RecordComplianceCheckCommand) -> None:
        events = await self._store.load_stream(
            ComplianceRecordAggregate.aggregate_type, cmd.compliance_record_id
        )
        aggregate = ComplianceRecordAggregate.load(cmd.compliance_record_id, events)

        if cmd.passed:
            aggregate.pass_check(
                check_name=cmd.check_name,
                check_result=cmd.check_result,
                officer_id=cmd.officer_id,
            )
        else:
            aggregate.fail_check(
                check_name=cmd.check_name,
                failure_reason=cmd.failure_reason,
                officer_id=cmd.officer_id,
            )

        await self._store.append(
            aggregate_type=ComplianceRecordAggregate.aggregate_type,
            aggregate_id=cmd.compliance_record_id,
            events=aggregate.pending_events,
            expected_version=aggregate.version - len(aggregate.pending_events),
        )

    async def handle_finalize_compliance(self, cmd: FinalizeComplianceCommand) -> bool:
        """Finalize compliance and update loan application. Returns overall_passed."""
        comp_events = await self._store.load_stream(
            ComplianceRecordAggregate.aggregate_type, cmd.compliance_record_id
        )
        compliance = ComplianceRecordAggregate.load(cmd.compliance_record_id, comp_events)
        compliance.finalize(officer_id=cmd.officer_id, notes=cmd.notes)

        await self._store.append(
            aggregate_type=ComplianceRecordAggregate.aggregate_type,
            aggregate_id=cmd.compliance_record_id,
            events=compliance.pending_events,
            expected_version=compliance.version - len(compliance.pending_events),
        )

        # Update loan application with compliance result
        app_events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        app = LoanApplicationAggregate.load(cmd.application_id, app_events)
        app.set_compliance_result(compliance.overall_passed)

        # Move to PendingDecision
        from src.models.events import ComplianceCheckRequested
        # The compliance result is communicated via the compliance aggregate's finalization
        # The loan app moves to PendingDecision — this is handled by the decision command

        return compliance.overall_passed

    async def handle_generate_decision(self, cmd: GenerateDecisionCommand) -> None:
        app_events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        app = LoanApplicationAggregate.load(cmd.application_id, app_events)

        # Load compliance result from compliance record
        if app.compliance_record_id:
            comp_events = await self._store.load_stream(
                ComplianceRecordAggregate.aggregate_type, app.compliance_record_id
            )
            compliance = ComplianceRecordAggregate.load(app.compliance_record_id, comp_events)
            app.set_compliance_result(compliance.overall_passed)

        # Move to PendingDecision if in ComplianceCheck
        from src.models.events import LoanStatus
        if app.status == LoanStatus.COMPLIANCE_CHECK:
            # Transition via internal state — compliance check is done
            app.status = LoanStatus.PENDING_DECISION

        app.generate_decision(
            outcome=cmd.outcome,
            confidence_score=cmd.confidence_score,
            reasoning=cmd.reasoning,
            model_version=cmd.model_version,
            agent_session_id=cmd.agent_session_id,
        )

        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=cmd.application_id,
            events=app.pending_events,
            expected_version=app.version - len(app.pending_events),
        )

        # Also record in agent session
        sess_events = await self._store.load_stream(
            AgentSessionAggregate.aggregate_type, cmd.agent_session_id
        )
        session = AgentSessionAggregate.load(cmd.agent_session_id, sess_events)
        session.record_decision(
            application_id=cmd.application_id,
            outcome=cmd.outcome,
            confidence_score=cmd.confidence_score,
            reasoning=cmd.reasoning,
            processing_duration_ms=0,
        )
        await self._store.append(
            aggregate_type=AgentSessionAggregate.aggregate_type,
            aggregate_id=cmd.agent_session_id,
            events=session.pending_events,
            expected_version=session.version - len(session.pending_events),
        )

    async def handle_finalize_application(self, cmd: FinalizeApplicationCommand) -> None:
        app_events = await self._store.load_stream(
            LoanApplicationAggregate.aggregate_type, cmd.application_id
        )
        app = LoanApplicationAggregate.load(cmd.application_id, app_events)

        if app.compliance_record_id:
            comp_events = await self._store.load_stream(
                ComplianceRecordAggregate.aggregate_type, app.compliance_record_id
            )
            compliance = ComplianceRecordAggregate.load(app.compliance_record_id, comp_events)
            app.set_compliance_result(compliance.overall_passed)

        if cmd.referral_reason:
            app.refer(referral_reason=cmd.referral_reason, referred_to=cmd.referred_to or "")
        elif cmd.approved:
            app.approve(
                approved_amount=cmd.approved_amount or app.loan_amount,
                interest_rate=cmd.interest_rate or 0.0,
                approved_by=cmd.approved_by,
                conditions=[],
            )
        else:
            app.deny(
                denial_reasons=cmd.denial_reasons or ["Decision denied"],
                denied_by=cmd.denied_by,
            )

        await self._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=cmd.application_id,
            events=app.pending_events,
            expected_version=app.version - len(app.pending_events),
        )

    # -------------------------------------------------------------------------
    # Agent Session Commands
    # -------------------------------------------------------------------------

    async def handle_start_agent_session(self, cmd: StartAgentSessionCommand) -> UUID:
        session_id = cmd.session_id or uuid4()
        session = AgentSessionAggregate(session_id)
        session.start(
            application_id=cmd.application_id,
            agent_model=cmd.agent_model,
            agent_version=cmd.agent_version,
            session_config=cmd.session_config,
        )
        await self._store.append(
            aggregate_type=AgentSessionAggregate.aggregate_type,
            aggregate_id=session_id,
            events=session.pending_events,
            expected_version=0,
        )
        return session_id

    async def handle_load_agent_context(self, cmd: LoadAgentContextCommand) -> None:
        events = await self._store.load_stream(
            AgentSessionAggregate.aggregate_type, cmd.session_id
        )
        session = AgentSessionAggregate.load(cmd.session_id, events)
        session.load_context(
            application_id=cmd.application_id,
            context_sources=cmd.context_sources,
            context_snapshot=cmd.context_snapshot,
            loaded_stream_positions=cmd.loaded_stream_positions,
        )
        await self._store.append(
            aggregate_type=AgentSessionAggregate.aggregate_type,
            aggregate_id=cmd.session_id,
            events=session.pending_events,
            expected_version=session.version - len(session.pending_events),
        )
