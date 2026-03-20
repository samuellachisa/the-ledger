"""
MCP Server: Agent interface for the Apex Financial Services Event Store.

Tools (Commands): submit_application, record_credit_analysis, record_fraud_check,
    request_compliance_check, record_compliance_check, finalize_compliance,
    generate_decision, finalize_application

Resources (Queries): Read from projections ONLY — never from event streams directly.
    ledger://applications/{id}
    ledger://applications (list)
    ledger://applications/{id}/audit-trail
    ledger://applications/{id}/compliance
    ledger://agents/{model_version}/performance
    ledger://integrity/{id}
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolResult,
    GetResourceResult,
    Resource,
    TextContent,
    Tool,
)

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
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.integrity.gas_town import AgentContextReconstructor
from src.models.events import (
    AggregateNotFoundError,
    BusinessRuleViolationError,
    DecisionOutcome,
    InvalidStateTransitionError,
    OptimisticConcurrencyError,
)
from src.projections.agent_performance import get_agent_performance, list_agent_performance
from src.projections.application_summary import get_application_summary, list_applications
from src.projections.compliance_audit import get_compliance_history, get_state_at
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)

app = Server("apex-financial-event-store")

# Global state (initialized in main)
_pool: asyncpg.Pool | None = None
_event_store: EventStore | None = None
_handler: CommandHandler | None = None
_reconstructor: AgentContextReconstructor | None = None


def _get_deps():
    assert _pool and _event_store and _handler and _reconstructor, "Server not initialized"
    return _pool, _event_store, _handler, _reconstructor


def _ok(data: dict | list | str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(data, default=str))])


def _err(error_type: str, message: str, extra: dict | None = None) -> CallToolResult:
    payload = {"error": error_type, "message": message, **(extra or {})}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, default=str))],
        isError=True,
    )


# =============================================================================
# Tools (Commands)
# =============================================================================

@app.list_tools()
async def list_tools() -> list[Tool]:
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
                    "applicant_name": {"type": "string", "description": "Full legal name of applicant"},
                    "loan_amount": {"type": "number", "description": "Requested loan amount in USD"},
                    "loan_purpose": {"type": "string", "description": "Purpose of the loan"},
                    "applicant_id": {"type": "string", "format": "uuid", "description": "Applicant's unique ID"},
                    "submitted_by": {"type": "string", "description": "Agent or user submitting the application"},
                    "correlation_id": {"type": "string", "format": "uuid", "description": "Optional saga correlation ID"},
                },
            },
        ),
        Tool(
            name="record_credit_analysis",
            description=(
                "Record credit analysis results for a loan application. "
                "PRECONDITIONS: Application must be in 'Submitted' status. "
                "Transitions application: Submitted → AwaitingAnalysis → UnderReview. "
                "ERRORS: OptimisticConcurrencyError (reload and retry), InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "credit_score", "debt_to_income_ratio"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "credit_score": {"type": "integer", "minimum": 300, "maximum": 850},
                    "debt_to_income_ratio": {"type": "number", "minimum": 0, "maximum": 1},
                    "model_version": {"type": "string", "description": "ML model version used"},
                    "analysis_duration_ms": {"type": "integer", "description": "Processing time in ms"},
                },
            },
        ),
        Tool(
            name="record_fraud_check",
            description=(
                "Record fraud detection results for a loan application. "
                "PRECONDITIONS: Application must be in 'AwaitingAnalysis' status. "
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
                "Creates a ComplianceRecord aggregate and links it to the application. "
                "Returns compliance_record_id. "
                "ERRORS: OptimisticConcurrencyError, InvalidStateTransitionError."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "officer_id", "required_checks"],
                "properties": {
                    "application_id": {"type": "string", "format": "uuid"},
                    "officer_id": {"type": "string"},
                    "required_checks": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of check names e.g. ['AML', 'KYC', 'OFAC']",
                    },
                },
            },
        ),
        Tool(
            name="record_compliance_check",
            description=(
                "Record the result of a single compliance check. "
                "PRECONDITIONS: compliance_record_id must exist and be InProgress. "
                "check_name must be in the required_checks list for this record. "
                "ERRORS: OptimisticConcurrencyError, ValueError for unknown check names."
            ),
            inputSchema={
                "type": "object",
                "required": ["compliance_record_id", "check_name", "passed", "officer_id"],
                "properties": {
                    "compliance_record_id": {"type": "string", "format": "uuid"},
                    "check_name": {"type": "string"},
                    "passed": {"type": "boolean"},
                    "check_result": {"type": "object", "description": "Check-specific result data"},
                    "failure_reason": {"type": "string", "description": "Required if passed=false"},
                    "officer_id": {"type": "string"},
                },
            },
        ),
        Tool(
            name="finalize_compliance",
            description=(
                "Finalize a compliance record after all checks are complete. "
                "PRECONDITIONS: All required_checks must have been recorded. "
                "Updates the linked loan application's compliance status. "
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
                "PRECONDITIONS: "
                "(1) Application must be in 'ComplianceCheck' or 'PendingDecision' status. "
                "(2) agent_session_id must have AgentContextLoaded recorded (Gas Town rule). "
                "(3) If confidence_score < 0.6, outcome MUST be 'REFER'. "
                "(4) outcome 'APPROVE' requires compliance_passed=true. "
                "ERRORS: OptimisticConcurrencyError (reload and retry with suggested_action), "
                "BusinessRuleViolationError for confidence floor or compliance dependency violations."
            ),
            inputSchema={
                "type": "object",
                "required": ["application_id", "agent_session_id", "outcome", "confidence_score", "reasoning", "model_version"],
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
                "APPROVE requires compliance_passed=true. "
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
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> CallToolResult:
    pool, event_store, handler, reconstructor = _get_deps()

    try:
        if name == "submit_application":
            app_id = await handler.handle_submit_application(SubmitApplicationCommand(
                applicant_name=arguments["applicant_name"],
                loan_amount=float(arguments["loan_amount"]),
                loan_purpose=arguments["loan_purpose"],
                applicant_id=UUID(arguments["applicant_id"]),
                submitted_by=arguments["submitted_by"],
                correlation_id=UUID(arguments["correlation_id"]) if arguments.get("correlation_id") else None,
            ))
            return _ok({"application_id": str(app_id), "status": "Submitted"})

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

        else:
            return _err("UnknownTool", f"Tool '{name}' not found")

    except OptimisticConcurrencyError as e:
        return _err("OptimisticConcurrencyError", str(e), e.to_dict())
    except BusinessRuleViolationError as e:
        return _err("BusinessRuleViolationError", str(e), {"rule": e.rule})
    except InvalidStateTransitionError as e:
        return _err("InvalidStateTransitionError", str(e), {
            "from_state": e.from_state, "to_state": e.to_state
        })
    except AggregateNotFoundError as e:
        return _err("AggregateNotFoundError", str(e))
    except Exception as e:
        logger.exception("Unexpected error in tool %s", name)
        return _err("InternalError", str(e))


# =============================================================================
# Resources (Queries — read from projections only)
# =============================================================================

@app.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(uri="ledger://applications", name="All Applications", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}", name="Application Summary", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}/audit-trail", name="Audit Trail", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}/compliance", name="Compliance History", mimeType="application/json"),
        Resource(uri="ledger://agents/performance", name="Agent Performance", mimeType="application/json"),
        Resource(uri="ledger://integrity/{id}", name="Integrity Check", mimeType="application/json"),
    ]


@app.read_resource()
async def read_resource(uri: str) -> GetResourceResult:
    pool, event_store, handler, reconstructor = _get_deps()

    try:
        async with pool.acquire() as conn:
            # ledger://applications
            if uri == "ledger://applications":
                data = await list_applications(conn)
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(data, default=str))
                ])

            # ledger://applications/{id}
            elif uri.startswith("ledger://applications/") and "/audit-trail" not in uri and "/compliance" not in uri:
                app_id = UUID(uri.split("/")[-1])
                data = await get_application_summary(conn, app_id)
                if not data:
                    return GetResourceResult(contents=[
                        TextContent(type="text", text=json.dumps({"error": "Not found"}))
                    ])
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(data, default=str))
                ])

            # ledger://applications/{id}/audit-trail
            elif uri.endswith("/audit-trail"):
                app_id = UUID(uri.split("/")[-2])
                # Audit trail reads from event stream (exception to projection rule — for auditors)
                events = await event_store.load_stream("LoanApplication", app_id)
                trail = [
                    {
                        "event_type": e.event_type,
                        "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
                        "payload": e.to_payload(),
                    }
                    for e in events
                ]
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(trail, default=str))
                ])

            # ledger://applications/{id}/compliance
            elif uri.endswith("/compliance"):
                app_id = UUID(uri.split("/")[-2])
                data = await get_compliance_history(conn, app_id)
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(data, default=str))
                ])

            # ledger://agents/performance
            elif uri == "ledger://agents/performance":
                data = await list_agent_performance(conn)
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(data, default=str))
                ])

            # ledger://integrity/{id}
            elif uri.startswith("ledger://integrity/"):
                app_id = UUID(uri.split("/")[-1])
                result = await run_integrity_check(pool, event_store, app_id)
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps(result, default=str))
                ])

            else:
                return GetResourceResult(contents=[
                    TextContent(type="text", text=json.dumps({"error": f"Unknown resource: {uri}"}))
                ])

    except Exception as e:
        logger.exception("Error reading resource %s", uri)
        return GetResourceResult(contents=[
            TextContent(type="text", text=json.dumps({"error": str(e)}))
        ])


# =============================================================================
# Server initialization
# =============================================================================

async def create_server(database_url: str) -> None:
    global _pool, _event_store, _handler, _reconstructor

    _pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10)
    _event_store = EventStore(_pool, UpcasterRegistry())
    _handler = CommandHandler(_event_store)
    _reconstructor = AgentContextReconstructor(_event_store)
    logger.info("MCP Server initialized with database: %s", database_url.split("@")[-1])


async def main():
    import asyncio
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/apex_financial"
    )
    await create_server(database_url)
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
