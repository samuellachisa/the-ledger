"""
Domain event models and exceptions for the Apex Financial Services Event Store.
All events are immutable Pydantic models. Schema versioning is handled via
the schema_version field — upcasters transform old versions at read time.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# =============================================================================
# Exceptions
# =============================================================================

class DomainError(Exception):
    """
    Base class for all domain-layer errors.

    Catching DomainError lets callers handle any domain failure uniformly
    (e.g. MCP error serialisation, saga compensation) without needing to
    enumerate every concrete subclass.  Each subclass still carries its own
    structured attributes for fine-grained handling where needed.
    """

    def to_dict(self) -> dict:
        """Serialise to a JSON-safe dict.  Subclasses should override."""
        return {"error": type(self).__name__, "message": str(self)}


class OptimisticConcurrencyError(DomainError):
    """
    Raised when an append is attempted with an expected_version that does not
    match the current stream version. The caller must reload the aggregate
    and retry the command.
    """
    def __init__(self, stream_id: UUID, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected_version = expected
        self.actual_version = actual
        self.suggested_action = "reload_and_retry"
        super().__init__(
            f"Concurrency conflict on stream {stream_id}: "
            f"expected version {expected}, actual {actual}. "
            f"Suggested action: {self.suggested_action}"
        )

    def to_dict(self) -> dict:
        return {
            "error": "OptimisticConcurrencyError",
            "stream_id": str(self.stream_id),
            "expected_version": self.expected_version,
            "actual_version": self.actual_version,
            "suggested_action": self.suggested_action,
        }


class AggregateNotFoundError(DomainError):
    def __init__(self, aggregate_type: str, aggregate_id: UUID):
        self.aggregate_type = aggregate_type
        self.aggregate_id = aggregate_id
        super().__init__(f"{aggregate_type} {aggregate_id} not found")

    def to_dict(self) -> dict:
        return {
            "error": "AggregateNotFoundError",
            "aggregate_type": self.aggregate_type,
            "aggregate_id": str(self.aggregate_id),
        }


class InvalidStateTransitionError(DomainError):
    def __init__(self, aggregate_type: str, from_state: str, to_state: str, reason: str = ""):
        self.aggregate_type = aggregate_type
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason
        super().__init__(
            f"{aggregate_type}: invalid transition {from_state} → {to_state}. {reason}"
        )

    def to_dict(self) -> dict:
        return {
            "error": "InvalidStateTransitionError",
            "aggregate_type": self.aggregate_type,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "reason": self.reason,
        }


class BusinessRuleViolationError(DomainError):
    def __init__(self, rule: str, detail: str = ""):
        self.rule = rule
        self.detail = detail
        super().__init__(f"Business rule violated: {rule}. {detail}")

    def to_dict(self) -> dict:
        return {
            "error": "BusinessRuleViolationError",
            "rule": self.rule,
            "detail": self.detail,
        }


class IntegrityError(DomainError):
    """Raised when hash chain verification fails."""
    def __init__(self, sequence_number: int, expected_hash: str, actual_hash: str):
        self.sequence_number = sequence_number
        self.expected_hash = expected_hash
        self.actual_hash = actual_hash
        super().__init__(
            f"Hash chain broken at sequence {sequence_number}: "
            f"expected {expected_hash[:16]}..., got {actual_hash[:16]}..."
        )

    def to_dict(self) -> dict:
        return {
            "error": "IntegrityError",
            "sequence_number": self.sequence_number,
            "expected_hash": self.expected_hash,
            "actual_hash": self.actual_hash,
        }


# =============================================================================
# Enums
# =============================================================================

class LoanStatus(str, Enum):
    SUBMITTED = "Submitted"
    AWAITING_ANALYSIS = "AwaitingAnalysis"
    UNDER_REVIEW = "UnderReview"
    COMPLIANCE_CHECK = "ComplianceCheck"
    PENDING_DECISION = "PendingDecision"
    CONDITIONALLY_APPROVED = "ConditionallyApproved"
    FINAL_APPROVED = "FinalApproved"
    DENIED = "Denied"
    REFERRED = "Referred"
    WITHDRAWN = "Withdrawn"


class DecisionOutcome(str, Enum):
    APPROVE = "APPROVE"
    DENY = "DENY"
    REFER = "REFER"


class AgentSessionStatus(str, Enum):
    INITIALIZING = "Initializing"
    CONTEXT_LOADED = "ContextLoaded"
    PROCESSING = "Processing"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CRASHED = "Crashed"


class ComplianceStatus(str, Enum):
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    PASSED = "Passed"
    FAILED = "Failed"
    WAIVED = "Waived"


# =============================================================================
# Stream Metadata
# =============================================================================

class StreamMetadata(BaseModel):
    """
    Typed representation of an event stream's identity and current state.

    Returned by EventStore.get_stream_metadata() so callers get a structured
    object instead of a raw dict — field access is validated and IDE-friendly.

    Fields
    ------
    stream_id       Internal surrogate key (UUID) for the stream row.
    aggregate_type  e.g. 'LoanApplication', 'AgentSession'.
    aggregate_id    Business identity UUID of the aggregate.
    current_version Last appended stream position (0 = no events yet).
    event_count     Total number of events stored in this stream.
    created_at      When the stream was first created.
    updated_at      When the stream was last written to.
    archived_at     Set when the stream has been archived; None otherwise.
    tenant_id       Optional tenant identifier for multi-tenant deployments.
    """

    stream_id: UUID
    aggregate_type: str
    aggregate_id: UUID
    current_version: int
    event_count: int
    created_at: datetime
    updated_at: datetime
    archived_at: Optional[datetime] = None
    tenant_id: Optional[str] = None

    @property
    def is_archived(self) -> bool:
        return self.archived_at is not None


# =============================================================================
# Base Event Model
# =============================================================================

class DomainEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    aggregate_id: UUID
    aggregate_type: str
    event_type: str
    schema_version: int = 1
    occurred_at: datetime = Field(default_factory=datetime.utcnow)
    causation_id: Optional[UUID] = None
    correlation_id: Optional[UUID] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_payload(self) -> dict:
        """Serialize domain-specific fields (excludes base fields stored as columns)."""
        exclude = {
            "event_id", "aggregate_id", "aggregate_type", "event_type",
            "schema_version", "occurred_at", "causation_id", "correlation_id", "metadata"
        }
        return self.model_dump(exclude=exclude)

    def compute_hash(self) -> str:
        """SHA-256 of the canonical payload for integrity chain."""
        canonical = json.dumps(
            {"event_id": str(self.event_id), "payload": self.to_payload()},
            sort_keys=True, default=str
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


# =============================================================================
# LoanApplication Events
# =============================================================================

class LoanApplicationSubmitted(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "LoanApplicationSubmitted"
    applicant_name: str
    loan_amount: float
    loan_purpose: str
    applicant_id: UUID
    submitted_by: str  # agent or user identifier


class CreditAnalysisRequested(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "CreditAnalysisRequested"
    requested_by: str
    priority: str = "normal"


class CreditAnalysisCompleted(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "CreditAnalysisCompleted"
    credit_score: int
    debt_to_income_ratio: float
    model_version: Optional[str] = None  # null for v1 events (upcasted)
    analysis_duration_ms: Optional[int] = None


class FraudCheckCompleted(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "FraudCheckCompleted"
    fraud_risk_score: float  # 0.0 - 1.0
    flags: list[str] = Field(default_factory=list)
    passed: bool


class ComplianceCheckRequested(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "ComplianceCheckRequested"
    compliance_record_id: UUID


class ComplianceFinalizedOnApplication(DomainEvent):
    """Recorded on LoanApplication when compliance is finalized. Transitions to PendingDecision."""
    aggregate_type: str = "LoanApplication"
    event_type: str = "ComplianceFinalizedOnApplication"
    compliance_record_id: UUID
    compliance_passed: bool


class DecisionGenerated(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "DecisionGenerated"
    outcome: DecisionOutcome
    confidence_score: float  # 0.0 - 1.0
    reasoning: str
    model_version: str
    agent_session_id: UUID


class ApplicationApproved(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "ApplicationApproved"
    approved_amount: float
    interest_rate: float
    approved_by: str
    conditions: list[str] = Field(default_factory=list)


class ApplicationDenied(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "ApplicationDenied"
    denial_reasons: list[str]
    denied_by: str


class ApplicationReferred(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "ApplicationReferred"
    referral_reason: str
    referred_to: str


class ApplicationWithdrawn(DomainEvent):
    aggregate_type: str = "LoanApplication"
    event_type: str = "ApplicationWithdrawn"
    withdrawn_by: str
    reason: str


# =============================================================================
# AgentSession Events
# =============================================================================

class AgentSessionStarted(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentSessionStarted"
    application_id: UUID
    agent_model: str
    agent_version: str
    session_config: dict[str, Any] = Field(default_factory=dict)


class AgentContextLoaded(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentContextLoaded"
    application_id: UUID
    context_sources: list[str]  # e.g. ["credit_bureau", "fraud_db", "compliance_rules"]
    context_snapshot: dict[str, Any] = Field(default_factory=dict)
    loaded_stream_positions: dict[str, int] = Field(default_factory=dict)  # Gas Town checkpoints


class AgentDecisionRecorded(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentDecisionRecorded"
    application_id: UUID
    outcome: DecisionOutcome
    confidence_score: float
    reasoning: str
    processing_duration_ms: int


class AgentSessionCompleted(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentSessionCompleted"
    application_id: UUID
    total_duration_ms: int
    events_processed: int


class AgentSessionFailed(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentSessionFailed"
    application_id: UUID
    error_type: str
    error_message: str
    last_checkpoint: Optional[dict[str, int]] = None  # Gas Town: resume point


class AgentSessionResumed(DomainEvent):
    aggregate_type: str = "AgentSession"
    event_type: str = "AgentSessionResumed"
    application_id: UUID
    resumed_from_checkpoint: dict[str, int]
    crash_event_id: Optional[UUID] = None


# =============================================================================
# ComplianceRecord Events
# =============================================================================

class ComplianceRecordCreated(DomainEvent):
    aggregate_type: str = "ComplianceRecord"
    event_type: str = "ComplianceRecordCreated"
    application_id: UUID
    officer_id: str
    required_checks: list[str]


class ComplianceCheckPassed(DomainEvent):
    aggregate_type: str = "ComplianceRecord"
    event_type: str = "ComplianceCheckPassed"
    check_name: str
    check_result: dict[str, Any] = Field(default_factory=dict)
    officer_id: str


class ComplianceCheckFailed(DomainEvent):
    aggregate_type: str = "ComplianceRecord"
    event_type: str = "ComplianceCheckFailed"
    check_name: str
    failure_reason: str
    officer_id: str


class ComplianceRecordFinalized(DomainEvent):
    aggregate_type: str = "ComplianceRecord"
    event_type: str = "ComplianceRecordFinalized"
    overall_status: ComplianceStatus
    officer_id: str
    notes: str = ""


# =============================================================================
# AuditLedger Events
# =============================================================================

class AuditEntryRecorded(DomainEvent):
    aggregate_type: str = "AuditLedger"
    event_type: str = "AuditEntryRecorded"
    application_id: UUID
    action: str
    actor: str
    details: dict[str, Any] = Field(default_factory=dict)
    source_event_id: UUID
    event_hash: str
    chain_hash: str


class IntegrityCheckCompleted(DomainEvent):
    aggregate_type: str = "AuditLedger"
    event_type: str = "IntegrityCheckCompleted"
    application_id: UUID
    chain_valid: bool
    entries_checked: int
    broken_at_sequence: Optional[int] = None
    checked_by: str


# =============================================================================
# GDPR Erasure Events
# =============================================================================

class PersonalDataErased(DomainEvent):
    """
    Tombstone event appended to a stream when a GDPR erasure request is applied.
    Projections react to this event by removing PII fields from read models.
    The original events remain immutable in the log.
    """
    event_type: str = "PersonalDataErased"
    applicant_id: UUID
    erasure_id: UUID
    fields_erased: list[str] = Field(default_factory=list)


# =============================================================================
# Event Registry: maps event_type string → Pydantic class
# =============================================================================

EVENT_REGISTRY: dict[str, type[DomainEvent]] = {
    "LoanApplicationSubmitted": LoanApplicationSubmitted,
    "CreditAnalysisRequested": CreditAnalysisRequested,
    "CreditAnalysisCompleted": CreditAnalysisCompleted,
    "FraudCheckCompleted": FraudCheckCompleted,
    "ComplianceCheckRequested": ComplianceCheckRequested,
    "ComplianceFinalizedOnApplication": ComplianceFinalizedOnApplication,
    "DecisionGenerated": DecisionGenerated,
    "ApplicationApproved": ApplicationApproved,
    "ApplicationDenied": ApplicationDenied,
    "ApplicationReferred": ApplicationReferred,
    "ApplicationWithdrawn": ApplicationWithdrawn,
    "AgentSessionStarted": AgentSessionStarted,
    "AgentContextLoaded": AgentContextLoaded,
    "AgentDecisionRecorded": AgentDecisionRecorded,
    "AgentSessionCompleted": AgentSessionCompleted,
    "AgentSessionFailed": AgentSessionFailed,
    "AgentSessionResumed": AgentSessionResumed,
    "ComplianceRecordCreated": ComplianceRecordCreated,
    "ComplianceCheckPassed": ComplianceCheckPassed,
    "ComplianceCheckFailed": ComplianceCheckFailed,
    "ComplianceRecordFinalized": ComplianceRecordFinalized,
    "AuditEntryRecorded": AuditEntryRecorded,
    "IntegrityCheckCompleted": IntegrityCheckCompleted,
    "PersonalDataErased": PersonalDataErased,
}
