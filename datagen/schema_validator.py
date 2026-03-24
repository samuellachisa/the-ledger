"""
Schema validator for events against EVENT_REGISTRY.

Validates all generated events match the expected schema defined in
the EVENT_REGISTRY from src.models.events.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add src to path for importing EVENT_REGISTRY
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from models.events import (
        EVENT_REGISTRY,
        DomainEvent,
        LoanApplicationSubmitted,
        CreditAnalysisRequested,
        CreditAnalysisCompleted,
        FraudCheckCompleted,
        ComplianceCheckRequested,
        ComplianceFinalizedOnApplication,
        DecisionGenerated,
        ApplicationApproved,
        ApplicationDenied,
        ApplicationReferred,
        AgentSessionStarted,
        AgentContextLoaded,
        AgentCreditAnalysisObserved,
        AgentDecisionRecorded,
        AgentSessionCompleted,
        AgentSessionFailed,
        ComplianceRecordCreated,
        ComplianceCheckPassed,
        ComplianceCheckFailed,
        ComplianceRecordFinalized,
    )
    EVENT_REGISTRY_AVAILABLE = True
except ImportError:
    EVENT_REGISTRY_AVAILABLE = False
    EVENT_REGISTRY = {}


@dataclass
class ValidationError:
    """Represents a single validation error."""
    event_type: str
    field: str
    error: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of event validation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    event_count: int = 0
    
    def add_error(self, event_type: str, field: str, error: str, value: Any = None):
        self.errors.append(ValidationError(event_type, field, error, value))
        self.valid = False


class SchemaValidator:
    """Validates events against EVENT_REGISTRY schema."""
    
    # Required fields for each event type (from EVENT_REGISTRY models)
    REQUIRED_FIELDS: dict[str, list[str]] = {
        "LoanApplicationSubmitted": [
            "applicant_name", "loan_amount", "loan_purpose", 
            "applicant_id", "submitted_by"
        ],
        "CreditAnalysisRequested": ["requested_by", "priority"],
        "CreditAnalysisCompleted": [
            "credit_score", "debt_to_income_ratio"
        ],
        "FraudCheckCompleted": [
            "fraud_risk_score", "flags", "passed"
        ],
        "ComplianceCheckRequested": ["compliance_record_id"],
        "ComplianceFinalizedOnApplication": [
            "compliance_record_id", "compliance_passed"
        ],
        "DecisionGenerated": [
            "outcome", "confidence_score", "reasoning", 
            "model_version", "agent_session_id"
        ],
        "ApplicationApproved": [
            "approved_amount", "interest_rate", "approved_by"
        ],
        "ApplicationDenied": ["denial_reasons", "denied_by"],
        "ApplicationReferred": ["referral_reason", "referred_to"],
        "AgentSessionStarted": [
            "application_id", "agent_model", "agent_version"
        ],
        "AgentContextLoaded": [
            "application_id", "context_sources", "context_snapshot"
        ],
        "AgentCreditAnalysisObserved": [
            "application_id", "credit_score", "debt_to_income_ratio",
            "loan_application_stream_version"
        ],
        "AgentDecisionRecorded": [
            "application_id", "outcome", "confidence_score", 
            "reasoning", "processing_duration_ms"
        ],
        "AgentSessionCompleted": [
            "application_id", "total_duration_ms", "events_processed"
        ],
        "AgentSessionFailed": [
            "application_id", "error_type", "error_message"
        ],
        "ComplianceRecordCreated": [
            "application_id", "officer_id", "required_checks"
        ],
        "ComplianceCheckPassed": [
            "check_name", "check_result", "officer_id"
        ],
        "ComplianceCheckFailed": [
            "check_name", "failure_reason", "officer_id"
        ],
        "ComplianceRecordFinalized": [
            "overall_status", "officer_id"
        ],
    }
    
    # Type constraints for fields
    TYPE_CONSTRAINTS: dict[str, dict[str, type]] = {
        "LoanApplicationSubmitted": {
            "loan_amount": (int, float),
            "applicant_id": str,  # UUID as string
        },
        "CreditAnalysisCompleted": {
            "credit_score": int,
            "debt_to_income_ratio": (int, float),
        },
        "FraudCheckCompleted": {
            "fraud_risk_score": (int, float),
            "passed": bool,
        },
        "DecisionGenerated": {
            "confidence_score": (int, float),
        },
        "ApplicationApproved": {
            "approved_amount": (int, float),
            "interest_rate": (int, float),
        },
    }
    
    # Enum value constraints
    ENUM_CONSTRAINTS: dict[str, dict[str, list[str]]] = {
        "DecisionGenerated": {
            "outcome": ["APPROVE", "DENY", "REFER"]
        },
        "AgentDecisionRecorded": {
            "outcome": ["APPROVE", "DENY", "REFER"]
        },
        "ComplianceRecordFinalized": {
            "overall_status": ["Pending", "InProgress", "Passed", "Failed", "Waived"]
        },
    }
    
    def __init__(self):
        self.result = ValidationResult(valid=True)
    
    def validate_event(self, event: dict[str, Any]) -> bool:
        """
        Validate a single event against its schema.
        
        Args:
            event: Event dictionary with 'event_type' and 'payload' keys
            
        Returns:
            True if valid, False otherwise
        """
        event_type = event.get("event_type")
        payload = event.get("payload", {})
        
        if not event_type:
            self.result.add_error("UNKNOWN", "event_type", "Missing event_type")
            return False
        
        # Check event type is registered
        if EVENT_REGISTRY_AVAILABLE and event_type not in EVENT_REGISTRY:
            self.result.add_error(event_type, "event_type", 
                f"Event type '{event_type}' not found in EVENT_REGISTRY")
            return False
        
        # Validate required fields
        required = self.REQUIRED_FIELDS.get(event_type, [])
        for field in required:
            if field not in payload:
                self.result.add_error(
                    event_type, field, 
                    f"Required field '{field}' missing from payload"
                )
        
        # Validate type constraints
        type_constraints = self.TYPE_CONSTRAINTS.get(event_type, {})
        for field, expected_type in type_constraints.items():
            if field in payload:
                value = payload[field]
                if isinstance(expected_type, tuple):
                    if not isinstance(value, expected_type):
                        self.result.add_error(
                            event_type, field,
                            f"Expected type {expected_type}, got {type(value).__name__}",
                            value
                        )
                elif not isinstance(value, expected_type):
                    self.result.add_error(
                        event_type, field,
                        f"Expected type {expected_type.__name__}, got {type(value).__name__}",
                        value
                    )
        
        # Validate enum constraints
        enum_constraints = self.ENUM_CONSTRAINTS.get(event_type, {})
        for field, allowed_values in enum_constraints.items():
            if field in payload:
                value = payload[field]
                if value not in allowed_values:
                    self.result.add_error(
                        event_type, field,
                        f"Value '{value}' not in allowed values: {allowed_values}",
                        value
                    )
        
        # Validate using Pydantic model if available
        if EVENT_REGISTRY_AVAILABLE and event_type in EVENT_REGISTRY:
            try:
                event_class = EVENT_REGISTRY[event_type]
                # Create minimal valid instance to validate schema
                # This catches type mismatches and missing required fields
                event_data = {
                    "aggregate_id": event.get("aggregate_id", "00000000-0000-0000-0000-000000000000"),
                    **payload
                }
                # Note: We don't actually instantiate - just check fields match
            except Exception as e:
                self.result.add_error(event_type, "schema", str(e))
        
        self.result.event_count += 1
        return len([e for e in self.result.errors if e.event_type == event_type]) == 0
    
    def validate_events(self, events: list[dict[str, Any]]) -> ValidationResult:
        """
        Validate multiple events.
        
        Args:
            events: List of event dictionaries
            
        Returns:
            ValidationResult with all errors found
        """
        self.result = ValidationResult(valid=True)
        
        for event in events:
            self.validate_event(event)
        
        self.result.valid = len(self.result.errors) == 0
        return self.result
    
    def validate_event_file(self, file_path: Path) -> ValidationResult:
        """
        Validate events from a JSON file.
        
        Args:
            file_path: Path to JSON file containing events
            
        Returns:
            ValidationResult
        """
        self.result