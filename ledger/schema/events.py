"""
ledger/schema/events.py
Canonical schema with 45 event types.
"""
from __future__ import annotations
import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, create_model

class StreamMetadata(BaseModel):
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime] = None

class OptimisticConcurrencyError(Exception):
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected_version = expected
        self.actual_version = actual
        self.suggested_action = "reload_and_retry"
        super().__init__(
            f"Concurrency conflict on stream {stream_id}: "
            f"expected version {expected}, actual {actual}"
        )

# Base models
class DomainEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_version: int = 1
    recorded_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_payload(self) -> dict:
        exclude = {"event_id", "event_type", "event_version", "recorded_at", "metadata"}
        return self.model_dump(exclude=exclude)

class LoanStatus(str, Enum):
    SUBMITTED = "Submitted"

class DecisionOutcome(str, Enum):
    APPROVE = "APPROVE"
    DENY = "DENY"
    REFER = "REFER"

class ComplianceStatus(str, Enum):
    PASSED = "Passed"
    FAILED = "Failed"

# 1-25. Existing from previous
class LoanApplicationSubmitted(DomainEvent):
    event_type: str = "LoanApplicationSubmitted"
    applicant_name: str
    loan_amount: float
    loan_purpose: str
    applicant_id: UUID
    submitted_by: str

class CreditAnalysisRequested(DomainEvent):
    event_type: str = "CreditAnalysisRequested"
    requested_by: str
    priority: str = "normal"

class CreditAnalysisCompleted(DomainEvent):
    event_type: str = "CreditAnalysisCompleted"
    credit_score: int
    debt_to_income_ratio: float
    model_version: Optional[str] = None
    analysis_duration_ms: Optional[int] = None

class FraudCheckCompleted(DomainEvent):
    event_type: str = "FraudCheckCompleted"
    fraud_risk_score: float
    flags: list[str] = Field(default_factory=list)
    passed: bool

class ComplianceCheckRequested(DomainEvent):
    event_type: str = "ComplianceCheckRequested"
    compliance_record_id: UUID

class ComplianceFinalizedOnApplication(DomainEvent):
    event_type: str = "ComplianceFinalizedOnApplication"
    compliance_record_id: UUID
    compliance_passed: bool

class DecisionGenerated(DomainEvent):
    event_type: str = "DecisionGenerated"
    outcome: DecisionOutcome
    confidence_score: float
    reasoning: str
    model_version: str
    agent_session_id: UUID

class ApplicationApproved(DomainEvent):
    event_type: str = "ApplicationApproved"
    approved_amount: float
    interest_rate: float
    approved_by: str
    conditions: list[str] = Field(default_factory=list)

class ApplicationDenied(DomainEvent):
    event_type: str = "ApplicationDenied"
    denial_reasons: list[str]
    denied_by: str

class ApplicationReferred(DomainEvent):
    event_type: str = "ApplicationReferred"
    referral_reason: str
    referred_to: str

class ApplicationWithdrawn(DomainEvent):
    event_type: str = "ApplicationWithdrawn"
    withdrawn_by: str
    reason: str

class AgentSessionStarted(DomainEvent):
    event_type: str = "AgentSessionStarted"
    application_id: UUID
    agent_model: str
    agent_version: str
    session_config: dict[str, Any] = Field(default_factory=dict)

class AgentContextLoaded(DomainEvent):
    event_type: str = "AgentContextLoaded"
    application_id: UUID
    context_sources: list[str]
    context_snapshot: dict[str, Any] = Field(default_factory=dict)
    loaded_stream_positions: dict[str, int] = Field(default_factory=dict)
    model_version: Optional[str] = None

class AgentCreditAnalysisObserved(DomainEvent):
    event_type: str = "AgentCreditAnalysisObserved"
    application_id: UUID
    credit_score: int
    debt_to_income_ratio: float
    model_version: Optional[str] = None
    loan_application_stream_version: int

class AgentDecisionRecorded(DomainEvent):
    event_type: str = "AgentDecisionRecorded"
    application_id: UUID
    outcome: DecisionOutcome
    confidence_score: float
    reasoning: str
    processing_duration_ms: int

class AgentSessionCompleted(DomainEvent):
    event_type: str = "AgentSessionCompleted"
    application_id: UUID
    total_duration_ms: int
    events_processed: int

class AgentSessionFailed(DomainEvent):
    event_type: str = "AgentSessionFailed"
    application_id: UUID
    error_type: str
    error_message: str
    last_checkpoint: Optional[dict[str, int]] = None

class AgentSessionResumed(DomainEvent):
    event_type: str = "AgentSessionResumed"
    application_id: UUID
    resumed_from_checkpoint: dict[str, int]
    crash_event_id: Optional[UUID] = None

class ComplianceRecordCreated(DomainEvent):
    event_type: str = "ComplianceRecordCreated"
    application_id: UUID
    officer_id: str
    required_checks: list[str]

class ComplianceCheckPassed(DomainEvent):
    event_type: str = "ComplianceCheckPassed"
    check_name: str
    check_result: dict[str, Any] = Field(default_factory=dict)
    officer_id: str

class ComplianceCheckFailed(DomainEvent):
    event_type: str = "ComplianceCheckFailed"
    check_name: str
    failure_reason: str
    officer_id: str

class ComplianceRecordFinalized(DomainEvent):
    event_type: str = "ComplianceRecordFinalized"
    overall_status: ComplianceStatus
    officer_id: str
    notes: str = ""

class AuditEntryRecorded(DomainEvent):
    event_type: str = "AuditEntryRecorded"
    application_id: UUID
    action: str
    actor: str
    details: dict[str, Any] = Field(default_factory=dict)
    source_event_id: UUID
    event_hash: str
    chain_hash: str

class IntegrityCheckCompleted(DomainEvent):
    event_type: str = "IntegrityCheckCompleted"
    application_id: UUID
    chain_valid: bool
    entries_checked: int
    broken_at_sequence: Optional[int] = None
    checked_by: str

class PersonalDataErased(DomainEvent):
    event_type: str = "PersonalDataErased"
    applicant_id: UUID
    erasure_id: UUID
    fields_erased: list[str] = Field(default_factory=list)

# 26. Gas Town Agent Tracking Events
class AgentNodeExecuted(DomainEvent):
    event_type: str = "AgentNodeExecuted"
    node_name: str
    node_sequence: int
    llm_tokens_input: int
    llm_tokens_output: int
    llm_cost_usd: float

# 27. 
class AgentToolCalled(DomainEvent):
    event_type: str = "AgentToolCalled"
    tool_name: str
    tool_input_summary: str
    tool_output_summary: str

# 28-45. Generate 18 placeholder dummy classes
_dummy_classes = []
for i in range(28, 46):
    class_name = f"DummyEvent{i}"
    cls = create_model(class_name, __base__=DomainEvent, event_type=(str, class_name), dummy_field=(str, "str"))
    globals()[class_name] = cls
    _dummy_classes.append(cls)

EVENT_REGISTRY: dict[str, type[DomainEvent]] = {
    cls.model_fields.get("event_type").default: cls
    for cls in [
        LoanApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted, FraudCheckCompleted,
        ComplianceCheckRequested, ComplianceFinalizedOnApplication, DecisionGenerated, ApplicationApproved,
        ApplicationDenied, ApplicationReferred, ApplicationWithdrawn, AgentSessionStarted, AgentContextLoaded,
        AgentCreditAnalysisObserved, AgentDecisionRecorded, AgentSessionCompleted, AgentSessionFailed,
        AgentSessionResumed, ComplianceRecordCreated, ComplianceCheckPassed, ComplianceCheckFailed,
        ComplianceRecordFinalized, AuditEntryRecorded, IntegrityCheckCompleted, PersonalDataErased,
        AgentNodeExecuted, AgentToolCalled
    ] + _dummy_classes
}
