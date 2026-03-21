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
    ComplianceFinalizedOnApplication,
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
        self._assert_status(LoanStatus.UNDER_REVIEW)
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

    def move_to_pending_decision(self) -> None:
        """Called after compliance is finalized. Transitions ComplianceCheck → PendingDecision."""
        self._assert_status(LoanStatus.COMPLIANCE_CHECK)
        self._raise_event(ComplianceFinalizedOnApplication(
            aggregate_id=self.aggregate_id,
            compliance_record_id=self.compliance_record_id,
            compliance_passed=self.compliance_passed or False,
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

    def finalize(
        self,
        approved: bool,
        approved_amount: float | None = None,
        interest_rate: float | None = None,
        approved_by: str = "",
        conditions: list[str] | None = None,
        denial_reasons: list[str] | None = None,
        denied_by: str = "",
        referral_reason: str | None = None,
        referred_to: str | None = None,
    ) -> None:
        """
        Apply the final disposition of the application.

        Encapsulates the approve / deny / refer branching so handlers
        don't need to make that routing decision themselves.
        """
        if referral_reason:
            self.refer(referral_reason=referral_reason, referred_to=referred_to or "")
        elif approved:
            self.approve(
                approved_amount=approved_amount or self.loan_amount,
                interest_rate=interest_rate or 0.0,
                approved_by=approved_by,
                conditions=conditions or [],
            )
        else:
            self.deny(
                denial_reasons=denial_reasons or ["Decision denied"],
                denied_by=denied_by,
            )

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

    def when_ComplianceFinalizedOnApplication(self, event: ComplianceFinalizedOnApplication) -> None:
        self.compliance_passed = event.compliance_passed
        self.status = LoanStatus.PENDING_DECISION

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

    def to_snapshot(self) -> dict:
        return {
            "status": self.status.value if self.status else None,
            "applicant_name": self.applicant_name,
            "loan_amount": self.loan_amount,
            "loan_purpose": self.loan_purpose,
            "applicant_id": str(self.applicant_id) if self.applicant_id else None,
            "credit_score": self.credit_score,
            "fraud_passed": self.fraud_passed,
            "compliance_record_id": str(self.compliance_record_id) if self.compliance_record_id else None,
            "compliance_passed": self.compliance_passed,
            "last_decision": self.last_decision.value if self.last_decision else None,
            "last_confidence": self.last_confidence,
            "agent_session_id": str(self.agent_session_id) if self.agent_session_id else None,
        }

    @classmethod
    def load(cls, aggregate_id: UUID, events: list) -> "LoanApplicationAggregate":
        """Reconstruct state by replaying *events* in order.

        Accepts the list returned by ``EventStore.load_stream()``.  After
        replay ``instance.version`` equals the number of events applied and
        ``instance.original_version`` is set to that same value so it can be
        passed directly as ``expected_version`` to ``EventStore.append()``.
        """
        instance = cls(aggregate_id)
        for event in events:
            instance.apply(event)
        instance.original_version = instance.version
        instance.pending_events = []
        return instance

    @classmethod
    def from_snapshot(cls, aggregate_id: UUID, data: dict, version: int) -> "LoanApplicationAggregate":
        from uuid import UUID as _UUID
        inst = cls(aggregate_id)
        inst.version = version
        inst.original_version = version
        inst.status = LoanStatus(data["status"]) if data.get("status") else None
        inst.applicant_name = data.get("applicant_name", "")
        inst.loan_amount = data.get("loan_amount", 0.0)
        inst.loan_purpose = data.get("loan_purpose", "")
        inst.applicant_id = _UUID(data["applicant_id"]) if data.get("applicant_id") else None
        inst.credit_score = data.get("credit_score")
        inst.fraud_passed = data.get("fraud_passed")
        inst.compliance_record_id = _UUID(data["compliance_record_id"]) if data.get("compliance_record_id") else None
        inst.compliance_passed = data.get("compliance_passed")
        inst.last_decision = DecisionOutcome(data["last_decision"]) if data.get("last_decision") else None
        inst.last_confidence = data.get("last_confidence")
        inst.agent_session_id = _UUID(data["agent_session_id"]) if data.get("agent_session_id") else None
        return inst
