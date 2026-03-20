"""
LoanApplicationAggregate: Core domain aggregate for loan processing.

State Machine:
    Submitted → AwaitingAnalysis → UnderReview → ComplianceCheck
    → PendingDecision → FinalApproved | Denied | Referred
    Any state → Withdrawn

Business Rules enforced:
    - Cannot approve without compliance passing
    - DecisionGenerated with confidence < 0.6 must be REFER
    - Cannot transition to invalid states
"""
from __future__ import annotations

from uuid import UUID, uuid4

from src.aggregates.base import AggregateRoot
from src.models.events import (
    ApplicationApproved,
    ApplicationDenied,
    ApplicationReferred,
    ApplicationWithdrawn,
    BusinessRuleViolationError,
    ComplianceCheckRequested,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    DecisionOutcome,
    FraudCheckCompleted,
    InvalidStateTransitionError,
    LoanApplicationSubmitted,
    LoanStatus,
)

# Valid state transitions
VALID_TRANSITIONS: dict[LoanStatus, set[LoanStatus]] = {
    LoanStatus.SUBMITTED: {LoanStatus.AWAITING_ANALYSIS, LoanStatus.WITHDRAWN},
    LoanStatus.AWAITING_ANALYSIS: {LoanStatus.UNDER_REVIEW, LoanStatus.WITHDRAWN},
    LoanStatus.UNDER_REVIEW: {LoanStatus.COMPLIANCE_CHECK, LoanStatus.WITHDRAWN},
    LoanStatus.COMPLIANCE_CHECK: {LoanStatus.PENDING_DECISION, LoanStatus.WITHDRAWN},
    LoanStatus.PENDING_DECISION: {
        LoanStatus.CONDITIONALLY_APPROVED,
        LoanStatus.FINAL_APPROVED,
        LoanStatus.DENIED,
        LoanStatus.REFERRED,
        LoanStatus.WITHDRAWN,
    },
    LoanStatus.CONDITIONALLY_APPROVED: {LoanStatus.FINAL_APPROVED, LoanStatus.WITHDRAWN},
    LoanStatus.FINAL_APPROVED: set(),
    LoanStatus.DENIED: set(),
    LoanStatus.REFERRED: {LoanStatus.AWAITING_ANALYSIS, LoanStatus.WITHDRAWN},
    LoanStatus.WITHDRAWN: set(),
}

CONFIDENCE_FLOOR = 0.6


class LoanApplicationAggregate(AggregateRoot):
    aggregate_type = "LoanApplication"

    def __init__(self, aggregate_id: UUID):
        super().__init__(aggregate_id)
        self.status: LoanStatus | None = None
        self.applicant_name: str = ""
        self.loan_amount: float = 0.0
        self.loan_purpose: str = ""
        self.applicant_id: UUID | None = None
        self.credit_score: int | None = None
        self.fraud_passed: bool | None = None
        self.compliance_record_id: UUID | None = None
        self.compliance_passed: bool | None = None
        self.last_decision: DecisionOutcome | None = None
        self.last_confidence: float | None = None
        self.agent_session_id: UUID | None = None

    # -------------------------------------------------------------------------
    # Commands
    # -------------------------------------------------------------------------

    def submit(
        self,
        applicant_name: str,
        loan_amount: float,
        loan_purpose: str,
        applicant_id: UUID,
        submitted_by: str,
        correlation_id: UUID | None = None,
    ) -> None:
        if self.status is not None:
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Submitted",
                "Application already exists"
            )
        self._raise_event(LoanApplicationSubmitted(
            aggregate_id=self.aggregate_id,
            applicant_name=applicant_name,
            loan_amount=loan_amount,
            loan_purpose=loan_purpose,
            applicant_id=applicant_id,
            submitted_by=submitted_by,
            correlation_id=correlation_id,
        ))

    def request_credit_analysis(self, requested_by: str) -> None:
        self._assert_status(LoanStatus.SUBMITTED)
        self._raise_event(CreditAnalysisRequested(
            aggregate_id=self.aggregate_id,
            requested_by=requested_by,
        ))

    def record_credit_analysis(
        self,
        credit_score: int,
        debt_to_income_ratio: float,
        model_version: str | None = None,
        analysis_duration_ms: int | None = None,
    ) -> None:
        self._assert_status(LoanStatus.AWAITING_ANALYSIS)
        self._raise_event(CreditAnalysisCompleted(
            aggregate_id=self.aggregate_id,
            credit_score=credit_score,
            debt_to_income_ratio=debt_to_income_ratio,
            model_version=model_version,
            analysis_duration_ms=analysis_duration_ms,
        ))

    def record_fraud_check(self, fraud_risk_score: float, flags: list[str], passed: bool) -> None:
        self._assert_status(LoanStatus.AWAITING_ANALYSIS)
        self._raise_event(FraudCheckCompleted(
            aggregate_id=self.aggregate_id,
            fraud_risk_score=fraud_risk_score,
            flags=flags,
            passed=passed,
        ))

    def request_compliance_check(self, compliance_record_id: UUID) -> None:
        self._assert_status(LoanStatus.UNDER_REVIEW)
        self._raise_event(ComplianceCheckRequested(
            aggregate_id=self.aggregate_id,
            compliance_record_id=compliance_record_id,
        ))

    def generate_decision(
        self,
        outcome: DecisionOutcome,
        confidence_score: float,
        reasoning: str,
        model_version: str,
        agent_session_id: UUID,
    ) -> None:
        self._assert_status(LoanStatus.PENDING_DECISION)

        # Business Rule: Confidence Floor
        if confidence_score < CONFIDENCE_FLOOR and outcome != DecisionOutcome.REFER:
            raise BusinessRuleViolationError(
                "ConfidenceFloor",
                f"Confidence {confidence_score:.2f} < {CONFIDENCE_FLOOR}. "
                f"Outcome must be REFER, got {outcome}."
            )

        # Business Rule: Cannot approve without compliance
        if outcome == DecisionOutcome.APPROVE and not self.compliance_passed:
            raise BusinessRuleViolationError(
                "ComplianceDependency",
                "Cannot approve application without passing compliance checks."
            )

        self._raise_event(DecisionGenerated(
            aggregate_id=self.aggregate_id,
            outcome=outcome,
            confidence_score=confidence_score,
            reasoning=reasoning,
            model_version=model_version,
            agent_session_id=agent_session_id,
        ))

    def approve(
        self,
        approved_amount: float,
        interest_rate: float,
        approved_by: str,
        conditions: list[str] | None = None,
    ) -> None:
        if self.status not in (LoanStatus.PENDING_DECISION, LoanStatus.CONDITIONALLY_APPROVED):
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "FinalApproved"
            )
        if not self.compliance_passed:
            raise BusinessRuleViolationError(
                "ComplianceDependency",
                "Cannot approve without passing compliance."
            )
        self._raise_event(ApplicationApproved(
            aggregate_id=self.aggregate_id,
            approved_amount=approved_amount,
            interest_rate=interest_rate,
            approved_by=approved_by,
            conditions=conditions or [],
        ))

    def deny(self, denial_reasons: list[str], denied_by: str) -> None:
        self._assert_status(LoanStatus.PENDING_DECISION)
        self._raise_event(ApplicationDenied(
            aggregate_id=self.aggregate_id,
            denial_reasons=denial_reasons,
            denied_by=denied_by,
        ))

    def refer(self, referral_reason: str, referred_to: str) -> None:
        self._assert_status(LoanStatus.PENDING_DECISION)
        self._raise_event(ApplicationReferred(
            aggregate_id=self.aggregate_id,
            referral_reason=referral_reason,
            referred_to=referred_to,
        ))

    def withdraw(self, withdrawn_by: str, reason: str) -> None:
        if self.status in (LoanStatus.FINAL_APPROVED, LoanStatus.DENIED, LoanStatus.WITHDRAWN):
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Withdrawn",
                "Cannot withdraw from terminal state."
            )
        self._raise_event(ApplicationWithdrawn(
            aggregate_id=self.aggregate_id,
            withdrawn_by=withdrawn_by,
            reason=reason,
        ))

    # -------------------------------------------------------------------------
    # Event Handlers (apply state changes)
    # -------------------------------------------------------------------------

    def when_LoanApplicationSubmitted(self, event: LoanApplicationSubmitted) -> None:
        self.status = LoanStatus.SUBMITTED
        self.applicant_name = event.applicant_name
        self.loan_amount = event.loan_amount
        self.loan_purpose = event.loan_purpose
        self.applicant_id = event.applicant_id

    def when_CreditAnalysisRequested(self, event: CreditAnalysisRequested) -> None:
        self.status = LoanStatus.AWAITING_ANALYSIS

    def when_CreditAnalysisCompleted(self, event: CreditAnalysisCompleted) -> None:
        self.credit_score = event.credit_score
        # Transition to UnderReview once credit analysis is done
        self.status = LoanStatus.UNDER_REVIEW

    def when_FraudCheckCompleted(self, event: FraudCheckCompleted) -> None:
        self.fraud_passed = event.passed

    def when_ComplianceCheckRequested(self, event: ComplianceCheckRequested) -> None:
        self.compliance_record_id = event.compliance_record_id
        self.status = LoanStatus.COMPLIANCE_CHECK

    def when_DecisionGenerated(self, event: DecisionGenerated) -> None:
        self.last_decision = event.outcome
        self.last_confidence = event.confidence_score
        self.agent_session_id = event.agent_session_id
        self.status = LoanStatus.PENDING_DECISION

    def when_ApplicationApproved(self, event: ApplicationApproved) -> None:
        self.status = LoanStatus.FINAL_APPROVED

    def when_ApplicationDenied(self, event: ApplicationDenied) -> None:
        self.status = LoanStatus.DENIED

    def when_ApplicationReferred(self, event: ApplicationReferred) -> None:
        self.status = LoanStatus.REFERRED

    def when_ApplicationWithdrawn(self, event: ApplicationWithdrawn) -> None:
        self.status = LoanStatus.WITHDRAWN

    # Compliance result is set by the ComplianceRecord aggregate via event
    # The loan application listens for compliance finalization via projection
    # (aggregates don't call each other — compliance_passed is set by command handler)
    def set_compliance_result(self, passed: bool) -> None:
        """Called by command handler after loading compliance projection."""
        self.compliance_passed = passed

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _assert_status(self, expected: LoanStatus) -> None:
        if self.status != expected:
            raise InvalidStateTransitionError(
                self.aggregate_type,
                str(self.status),
                str(expected),
                f"Expected status {expected}, got {self.status}"
            )
