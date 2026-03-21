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
    LoadAgentContextCommand,
    RecordComplianceCheckCommand,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    RequestComplianceCheckCommand,
    StartAgentSessionCommand,
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
        Tool(
            name="withdraw_application",
            description=(
                "Withdraw a loan application. Can be called from any non-terminal state. "
                "PRECONDITIONS: Application must not be in FinalApproved, Denied, or Withdrawn. "
                "ERRORS: InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "withdrawn_by", "reason"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "withdrawn_by": {"type": "string"},
                    "reason": {"type": "string"},
                },
            },
        ),
        Tool(
            name="start_agent_session",
            description=(
                "Start a new agent session for processing a loan application. "
                "Returns session_id required for load_agent_context and generate_decision. "
                "ERRORS: BusinessRuleViolationError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "agent_model", "agent_version"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "agent_model": {"type": "string"},
                    "agent_version": {"type": "string"},
                    "session_config": {"type": "object"},
                },
            },
        ),
        Tool(
            name="load_agent_context",
            description=(
                "Load application context into an agent session. "
                "PRECONDITIONS: session_id must exist and be in Initializing status. "
                "Must be called before generate_decision. "
                "ERRORS: InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["session_id", "application_id", "context_sources", "context_snapshot"],
                "properties": {
                    "session_id": {"type": "string", "format": "uuid"},
                    "application_id": {"type": "string", "format": "uuid"},
                    "context_sources": {"type": "array", "items": {"type": "string"}},
                    "context_snapshot": {"type": "object"},
                    "loaded_stream_positions": {"type": "object"},
                },
            },
        ),
        Tool(
            name="run_what_if",
            description=(
                "Run a counterfactual what-if analysis on a loan application. "
                "Injects hypothetical events and shows how the outcome would differ. "
                "Read-only — does not modify any state. "
                "ERRORS: AggregateNotFoundError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "counterfactual_event_type"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "counterfactual_event_type": {
                        "type": "string",
                        "enum": ["CreditAnalysisCompleted", "FraudCheckCompleted"],
                    },
                    "credit_score": {"type": "integer", "minimum": 300, "maximum": 850},
                    "debt_to_income_ratio": {"type": "number", "minimum": 0, "maximum": 1},
                    "fraud_risk_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "fraud_passed": {"type": "boolean"},
                },
            },
        ),
        Tool(
            name="generate_regulatory_package",
            description=(
                "Generate a self-contained, tamper-evident regulatory package for an application. "
                "Contains all events, projections, and cryptographic integrity proof. "
                "For ECOA/FCRA audit submissions. "
                "ERRORS: AggregateNotFoundError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "generated_by": {"type": "string"},
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
    # Strip auth token from arguments before any processing to avoid logging it.
    # Exception: refresh_token needs _auth_token as its primary argument.
    if name != "refresh_token":
        arguments = {k: v for k, v in arguments.items() if k != "_auth_token"}
    if name == "submit_application":
        idem_key = arguments.get("idempotency_key")
        if idem_key and idempotency:
            existing = await idempotency.check_and_reserve(idem_key)
            if existing:
                import json as _json
                cached = existing.result
                if isinstance(cached, str):
                    try:
                        cached = _json.loads(cached)
                    except Exception:
                        pass
                return _ok(cached)

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

    elif name == "withdraw_application":
        events = await handler._store.load_stream("LoanApplication", UUID(arguments["application_id"]))
        from src.aggregates.loan_application import LoanApplicationAggregate
        app = LoanApplicationAggregate.load(UUID(arguments["application_id"]), events)
        app.withdraw(withdrawn_by=arguments["withdrawn_by"], reason=arguments["reason"])
        await handler._store.append(
            aggregate_type=LoanApplicationAggregate.aggregate_type,
            aggregate_id=UUID(arguments["application_id"]),
            events=app.pending_events,
            expected_version=app.version - len(app.pending_events),
        )
        return _ok({"status": "Withdrawn"})

    elif name == "start_agent_session":
        session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
            application_id=UUID(arguments["application_id"]),
            agent_model=arguments["agent_model"],
            agent_version=arguments["agent_version"],
            session_config=arguments.get("session_config"),
        ))
        return _ok({"session_id": str(session_id), "status": "Initializing"})

    elif name == "load_agent_context":
        await handler.handle_load_agent_context(LoadAgentContextCommand(
            session_id=UUID(arguments["session_id"]),
            application_id=UUID(arguments["application_id"]),
            context_sources=arguments["context_sources"],
            context_snapshot=arguments["context_snapshot"],
            loaded_stream_positions=arguments.get("loaded_stream_positions", {}),
        ))
        return _ok({"status": "ContextLoaded"})

    elif name == "run_what_if":
        from src.what_if.projector import WhatIfProjector
        from src.models.events import (
            CreditAnalysisCompleted, FraudCheckCompleted,
        )
        from uuid import uuid4
        app_id = UUID(arguments["application_id"])
        cf_type = arguments["counterfactual_event_type"]

        if cf_type == "CreditAnalysisCompleted":
            cf_event = CreditAnalysisCompleted(
                aggregate_id=app_id,
                credit_score=int(arguments.get("credit_score", 700)),
                debt_to_income_ratio=float(arguments.get("debt_to_income_ratio", 0.3)),
            )
        elif cf_type == "FraudCheckCompleted":
            cf_event = FraudCheckCompleted(
                aggregate_id=app_id,
                fraud_risk_score=float(arguments.get("fraud_risk_score", 0.1)),
                passed=bool(arguments.get("fraud_passed", True)),
                flags=[],
            )
        else:
            return _err("InvalidArgument", f"Unsupported counterfactual_event_type: {cf_type}")

        projector = WhatIfProjector(handler._store)
        result = await projector.run_what_if(app_id, [cf_event])
        return _ok(result)

    elif name == "generate_regulatory_package":
        from src.regulatory.package import generate_regulatory_package
        app_id = UUID(arguments["application_id"])
        package = await generate_regulatory_package(
            pool=handler._store._pool,
            event_store=handler._store,
            application_id=app_id,
            generated_by=arguments.get("generated_by", "mcp-tool"),
        )
        return _ok(package)

    else:
        return _err("UnknownTool", f"Tool '{name}' not found")
