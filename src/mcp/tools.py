"""
MCP Tool handlers (command side).

Each handle_* function maps one MCP tool call to a CommandHandler method.
Imported and registered by server.py.
"""
from __future__ import annotations

import logging
from uuid import UUID

from mcp.types import CallToolResult, Tool

from src.commands.handlers import (
    CommandHandler,
    FinalizeApplicationCommand,
    FinalizeComplianceCommand,
    GenerateDecisionCommand,
    RecordComplianceCheckCommand,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    RequestComplianceCheckCommand,
    SubmitApplicationCommand,
)
from src.models.events import DecisionOutcome
from src.observability.metrics import get_metrics

logger = logging.getLogger(__name__)


def _ok(data: dict | list | str) -> CallToolResult:
    import json
    from mcp.types import TextContent
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(data, default=str))])


def _err(error_type: str, message: str, extra: dict | None = None) -> CallToolResult:
    import json
    from mcp.types import TextContent
    payload = {"error": error_type, "message": message, **(extra or {})}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, default=str))],
        isError=True,
    )


def get_tool_definitions() -> list[Tool]:
    return [
        Tool(
            name="submit_application",
            description=(
                "Submit a new loan application to the system. "
                "PRECONDITIONS: applicant_name must be non-empty; loan_amount must be > 0. "
                "Returns application_id for use in subsequent commands. "
                "ERRORS: BusinessRuleViolationError if validation fails."
            ),
            inputSchema={
                "type": "object",
                "required": ["applicant_name", "loan_amount", "loan_purpose", "applicant_id", "submitted_by"],
                "properties": {
                    "applicant_name": {"type": "string"},
                    "loan_amount": {"type": "number"},
                    "loan_purpose": {"type": "string"},
                    "applicant_id": {"type": "string", "format": "uuid"},
                    "submitted_by": {"type": "string"},
                    "correlation_id": {"type": "string", "format": "uuid"},
                    "idempotency_key": {"type": "string"},
                },
            },
        ),
        Tool(
            name="record_credit_analysis",
            description=(
                "Record credit analysis results for a loan application. "
                "PRECONDITIONS: Application must be in 'Submitted' status. "
                "Transitions: Submitted → AwaitingAnalysis → UnderReview. "
                "ERRORS: OptimisticConcurrencyError, InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "credit_score", "debt_to_income_ratio"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "credit_score": {"type": "integer", "minimum": 300, "maximum": 850},
                    "debt_to_income_ratio": {"type": "number", "minimum": 0, "maximum": 1},
                    "model_version": {"type": "string"},
                    "analysis_duration_ms": {"type": "integer"},
                },
            },
        ),
        Tool(
            name="record_fraud_check",
            description=(
                "Record fraud detection results for a loan application. "
                "PRECONDITIONS: Application must be in 'UnderReview' status. "
                "ERRORS: OptimisticConcurrencyError, InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "fraud_risk_score", "passed"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "fraud_risk_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "flags": {"type": "array", "items": {"type": "string"}},
                    "passed": {"type": "boolean"},
                },
            },
        ),
        Tool(
            name="request_compliance_check",
            description=(
                "Initiate a compliance check for a loan application. "
                "PRECONDITIONS: Application must be in 'UnderReview' status. "
                "Returns compliance_record_id. "
                "ERRORS: OptimisticConcurrencyError, InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "officer_id", "required_checks"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "officer_id": {"type": "string"},
                    "required_checks": {"type": "array", "items": {"type": "string"}},
                },
            },
        ),
        Tool(
            name="record_compliance_check",
            description=(
                "Record the result of a single compliance check. "
                "PRECONDITIONS: compliance_record_id must exist and be InProgress. "
                "ERRORS: OptimisticConcurrencyError, ValueError."
            ),
            inputSchema={
                "type": "object",
                "required": ["compliance_record_id", "check_name", "passed", "officer_id"],
                "properties": {
                    "compliance_record_id": {"type": "string", "format": "uuid"},
                    "check_name": {"type": "string"},
                    "passed": {"type": "boolean"},
                    "check_result": {"type": "object"},
                    "failure_reason": {"type": "string"},
                    "officer_id": {"type": "string"},
                },
            },
        ),
        Tool(
            name="finalize_compliance",
            description=(
                "Finalize a compliance record after all checks are complete. "
                "PRECONDITIONS: All required_checks must have been recorded. "
                "ERRORS: OptimisticConcurrencyError, InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["compliance_record_id", "application_id", "officer_id"],
                "properties": {
                    "compliance_record_id": {"type": "string", "format": "uuid"},
                    "application_id": {"type": "string", "format": "uuid"},
                    "officer_id": {"type": "string"},
                    "notes": {"type": "string"},
                },
            },
        ),
        Tool(
            name="generate_decision",
            description=(
                "Generate an AI decision for a loan application. "
                "PRECONDITIONS: (1) PendingDecision status. "
                "(2) agent_session_id must have AgentContextLoaded. "
                "(3) confidence_score < 0.6 → outcome must be REFER. "
                "(4) APPROVE requires compliance_passed=true. "
                "ERRORS: OptimisticConcurrencyError, BusinessRuleViolationError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "agent_session_id", "outcome",
                             "confidence_score", "reasoning", "model_version"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "agent_session_id": {"type": "string", "format": "uuid"},
                    "outcome": {"type": "string", "enum": ["APPROVE", "DENY", "REFER"]},
                    "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "reasoning": {"type": "string"},
                    "model_version": {"type": "string"},
                },
            },
        ),
        Tool(
            name="finalize_application",
            description=(
                "Finalize a loan application with APPROVE, DENY, or REFER outcome. "
                "PRECONDITIONS: Application must be in 'PendingDecision' status. "
                "ERRORS: OptimisticConcurrencyError, BusinessRuleViolationError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "approved": {"type": "boolean"},
                    "approved_amount": {"type": "number"},
                    "interest_rate": {"type": "number"},
                    "approved_by": {"type": "string"},
                    "denial_reasons": {"type": "array", "items": {"type": "string"}},
                    "denied_by": {"type": "string"},
                    "referral_reason": {"type": "string"},
                    "referred_to": {"type": "string"},
                },
            },
        ),
        Tool(
            name="refresh_token",
            description=(
                "Exchange a valid auth token for a fresh one before it expires. "
                "ERRORS: TokenExpired, TokenRevoked, InvalidToken."
            ),
            inputSchema={
                "type": "object",
                "required": ["_auth_token"],
                "properties": {
                    "_auth_token": {"type": "string"},
                },
            },
        ),
        Tool(
            name="erase_personal_data",
            description=(
                "Submit a GDPR Right-to-Erasure request for an applicant. "
                "Appends PersonalDataErased tombstone events and removes PII from projections. "
                "ERRORS: AggregateNotFoundError."
            ),
            inputSchema={
                "type": "object",
                "required": ["applicant_id", "requested_by"],
                "properties": {
                    "applicant_id": {"type": "string", "format": "uuid"},
                    "requested_by": {"type": "string"},
                },
            },
        ),
    ]


async def dispatch_tool(
    name: str,
    arguments: dict,
    handler: CommandHandler,
    token_store=None,
    idempotency=None,
    erasure=None,
) -> CallToolResult:
    """Route a tool call to the appropriate handler."""
    if name == "submit_application":
        idem_key = arguments.get("idempotency_key")
        if idem_key and idempotency:
            existing = await idempotency.check_and_reserve(idem_key)
            if existing:
                return _ok(existing.result)

        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name=arguments["applicant_name"],
            loan_amount=float(arguments["loan_amount"]),
            loan_purpose=arguments["loan_purpose"],
            applicant_id=UUID(arguments["applicant_id"]),
            submitted_by=arguments["submitted_by"],
            correlation_id=UUID(arguments["correlation_id"]) if arguments.get("correlation_id") else None,
        ))
        result = {"application_id": str(app_id), "status": "Submitted"}
        if idem_key and idempotency:
            await idempotency.complete(idem_key, result)
        return _ok(result)

    elif name == "record_credit_analysis":
        await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
            application_id=UUID(arguments["application_id"]),
            credit_score=int(arguments["credit_score"]),
            debt_to_income_ratio=float(arguments["debt_to_income_ratio"]),
            model_version=arguments.get("model_version"),
            analysis_duration_ms=arguments.get("analysis_duration_ms"),
        ))
        return _ok({"status": "CreditAnalysisRecorded"})

    elif name == "record_fraud_check":
        await handler.handle_record_fraud_check(RecordFraudCheckCommand(
            application_id=UUID(arguments["application_id"]),
            fraud_risk_score=float(arguments["fraud_risk_score"]),
            flags=arguments.get("flags", []),
            passed=bool(arguments["passed"]),
        ))
        return _ok({"status": "FraudCheckRecorded"})

    elif name == "request_compliance_check":
        compliance_id = await handler.handle_request_compliance_check(
            RequestComplianceCheckCommand(
                application_id=UUID(arguments["application_id"]),
                officer_id=arguments["officer_id"],
                required_checks=arguments["required_checks"],
            )
        )
        return _ok({"compliance_record_id": str(compliance_id), "status": "ComplianceCheckRequested"})

    elif name == "record_compliance_check":
        await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
            compliance_record_id=UUID(arguments["compliance_record_id"]),
            check_name=arguments["check_name"],
            passed=bool(arguments["passed"]),
            check_result=arguments.get("check_result", {}),
            failure_reason=arguments.get("failure_reason", ""),
            officer_id=arguments["officer_id"],
        ))
        return _ok({"status": "ComplianceCheckRecorded"})

    elif name == "finalize_compliance":
        passed = await handler.handle_finalize_compliance(FinalizeComplianceCommand(
            compliance_record_id=UUID(arguments["compliance_record_id"]),
            application_id=UUID(arguments["application_id"]),
            officer_id=arguments["officer_id"],
            notes=arguments.get("notes", ""),
        ))
        return _ok({"status": "ComplianceFinalized", "overall_passed": passed})

    elif name == "generate_decision":
        await handler.handle_generate_decision(GenerateDecisionCommand(
            application_id=UUID(arguments["application_id"]),
            agent_session_id=UUID(arguments["agent_session_id"]),
            outcome=DecisionOutcome(arguments["outcome"]),
            confidence_score=float(arguments["confidence_score"]),
            reasoning=arguments["reasoning"],
            model_version=arguments["model_version"],
        ))
        return _ok({"status": "DecisionGenerated", "outcome": arguments["outcome"]})

    elif name == "finalize_application":
        await handler.handle_finalize_application(FinalizeApplicationCommand(
            application_id=UUID(arguments["application_id"]),
            approved=arguments.get("approved", False),
            approved_amount=arguments.get("approved_amount"),
            interest_rate=arguments.get("interest_rate"),
            approved_by=arguments.get("approved_by", ""),
            denial_reasons=arguments.get("denial_reasons"),
            denied_by=arguments.get("denied_by", ""),
            referral_reason=arguments.get("referral_reason"),
            referred_to=arguments.get("referred_to"),
        ))
        return _ok({"status": "ApplicationFinalized"})

    elif name == "refresh_token":
        if token_store is None:
            return _err("AuthNotEnabled", "Authentication is not enabled on this server")
        new_token = await token_store.refresh(arguments["_auth_token"])
        return _ok({"token": new_token, "status": "TokenRefreshed"})

    elif name == "erase_personal_data":
        if erasure is None:
            return _err("NotConfigured", "Erasure handler not initialized")
        applicant_id = UUID(arguments["applicant_id"])
        erasure_id = await erasure.request_erasure(
            applicant_id=applicant_id,
            requested_by=arguments["requested_by"],
        )
        streams_processed = await erasure.apply_erasure(erasure_id)
        return _ok({
            "erasure_id": str(erasure_id),
            "streams_processed": streams_processed,
            "status": "Erased",
        })

    else:
        return _err("UnknownTool", f"Tool '{name}' not found")
