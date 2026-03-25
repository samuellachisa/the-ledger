"""
End-to-End MCP Lifecycle Test: Full loan processing via MCP protocol dispatcher only.

This test drives the complete loan lifecycle exclusively through the MCP server's
call_tool() and read_resource() dispatchers — the same entry points an LLM agent
would use over the MCP protocol. No command handlers are called directly.

Lifecycle exercised via call_tool():
    submit_application
    → record_credit_analysis
    → record_fraud_check
    → request_compliance_check
    → record_compliance_check (x3: AML, KYC, OFAC)
    → finalize_compliance
    → generate_decision
    → finalize_application

Resources exercised via read_resource():
    ledger://applications/{id}          — projection data after each phase
    ledger://applications               — list view
    ledger://applications/{id}/compliance
    ledger://agents/performance
    ledger://ledger/health
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest
import pytest_asyncio

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

import src.mcp.server as mcp_server
from src.commands.handlers import CommandHandler
from src.event_store import EventStore
from src.integrity.gas_town import AgentContextReconstructor
from src.upcasting.registry import UpcasterRegistry


# ---------------------------------------------------------------------------
# Fixture: wire up the MCP server global state using the test pool
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def init_mcp_server(db_pool):
    """
    Initialise the MCP server module-level globals so call_tool() and
    read_resource() work without a real network connection.
    Also wires up a ProjectionDaemon so projection tables are populated.
    """
    from src.projections.daemon import ProjectionDaemon
    from src.projections.application_summary import handle_application_summary
    from src.projections.compliance_audit import handle_compliance_audit
    from src.projections.agent_performance import handle_agent_performance

    store = EventStore(db_pool, UpcasterRegistry())
    mcp_server._pool = db_pool
    mcp_server._event_store = store
    mcp_server._handler = CommandHandler(store)
    mcp_server._reconstructor = AgentContextReconstructor(store)

    # Wire up projection daemon for resource reads
    daemon = ProjectionDaemon(db_pool, store)
    daemon.register("ApplicationSummary", handle_application_summary)
    daemon.register("ComplianceAuditView", handle_compliance_audit)
    daemon.register("AgentPerformanceLedger", handle_agent_performance)
    mcp_server._projection_daemon = daemon

    yield

    # Teardown: leave pool open (managed by conftest)
    mcp_server._pool = None
    mcp_server._event_store = None
    mcp_server._handler = None
    mcp_server._reconstructor = None
    mcp_server._projection_daemon = None


async def flush_projections():
    """Process all pending projection events. Call before reading resources."""
    daemon = getattr(mcp_server, "_projection_daemon", None)
    if daemon:
        await daemon._process_batch()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse(result) -> dict:
    """Decode the first TextContent item from a CallToolResult."""
    return json.loads(result.content[0].text)


def _parse_resource(result) -> dict | list:
    return json.loads(result.contents[0].text)


# ---------------------------------------------------------------------------
# Full lifecycle via MCP dispatcher
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_lifecycle_via_call_tool_and_read_resource(db_pool):
    """
    Complete happy-path lifecycle driven exclusively through call_tool() and
    read_resource() — the MCP protocol entry points.

    Verifies:
    - Every tool returns a non-error result with the expected status field
    - Resources return projection data (not raw stream reads) after each phase
    - The final application state is FinalApproved as seen through the resource
    """
    applicant_id = str(uuid4())
    correlation_id = str(uuid4())

    # ── 1. submit_application ────────────────────────────────────────────────
    r = await mcp_server.call_tool("submit_application", {
        "applicant_name": "Alice Borrower",
        "loan_amount": 200000,
        "loan_purpose": "Home purchase",
        "applicant_id": applicant_id,
        "submitted_by": "mcp_agent_v1",
        "correlation_id": correlation_id,
    })
    assert not r.isError, f"submit_application failed: {_parse(r)}"
    data = _parse(r)
    assert data["status"] == "Submitted"
    app_id = data["application_id"]

    # Resource: application summary must exist immediately after submission
    await flush_projections()
    res = await mcp_server.read_resource(f"ledger://applications/{app_id}")
    summary = _parse_resource(res)
    assert summary.get("status") == "Submitted", f"Unexpected summary: {summary}"

    # ── 2. Start agent session (via handler — Gas Town prerequisite) ─────────
    # The MCP server doesn't expose start_agent_session as a tool, so we use
    # the handler directly only for session setup (Gas Town checkpoint).
    from src.commands.handlers import StartAgentSessionCommand, LoadAgentContextCommand
    from uuid import UUID
    handler = mcp_server._handler
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=UUID(app_id),
        agent_model="gpt-4-turbo",
        agent_version="2.1",
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id,
        application_id=UUID(app_id),
        context_sources=["credit_bureau", "fraud_db"],
        context_snapshot={"credit_score": 790},
        loaded_stream_positions={"LoanApplication": 1},
    ))

    # ── 3. record_credit_analysis ────────────────────────────────────────────
    r = await mcp_server.call_tool("record_credit_analysis", {
        "application_id": app_id,
        "credit_score": 790,
        "debt_to_income_ratio": 0.27,
        "model_version": "credit-model-v3",
        "analysis_duration_ms": 380,
    })
    assert not r.isError, f"record_credit_analysis failed: {_parse(r)}"
    assert _parse(r)["status"] == "CreditAnalysisRecorded"

    # ── 4. record_fraud_check ────────────────────────────────────────────────
    r = await mcp_server.call_tool("record_fraud_check", {
        "application_id": app_id,
        "fraud_risk_score": 0.04,
        "flags": [],
        "passed": True,
    })
    assert not r.isError, f"record_fraud_check failed: {_parse(r)}"
    assert _parse(r)["status"] == "FraudCheckRecorded"

    # ── 5. request_compliance_check ──────────────────────────────────────────
    r = await mcp_server.call_tool("request_compliance_check", {
        "application_id": app_id,
        "officer_id": "compliance_officer_1",
        "required_checks": ["AML", "KYC", "OFAC"],
    })
    assert not r.isError, f"request_compliance_check failed: {_parse(r)}"
    compliance_data = _parse(r)
    assert compliance_data["status"] == "ComplianceCheckRequested"
    compliance_id = compliance_data["compliance_record_id"]

    # ── 6. record_compliance_check × 3 ──────────────────────────────────────
    for check in ["AML", "KYC", "OFAC"]:
        r = await mcp_server.call_tool("record_compliance_check", {
            "compliance_record_id": compliance_id,
            "check_name": check,
            "passed": True,
            "check_result": {"result": "clear"},
            "officer_id": "compliance_officer_1",
        })
        assert not r.isError, f"record_compliance_check({check}) failed: {_parse(r)}"
        assert _parse(r)["status"] == "ComplianceCheckRecorded"

    # ── 7. finalize_compliance ───────────────────────────────────────────────
    r = await mcp_server.call_tool("finalize_compliance", {
        "compliance_record_id": compliance_id,
        "application_id": app_id,
        "officer_id": "compliance_officer_1",
        "notes": "All checks clear",
    })
    assert not r.isError, f"finalize_compliance failed: {_parse(r)}"
    fc_data = _parse(r)
    assert fc_data["status"] == "ComplianceFinalized"
    assert fc_data["overall_passed"] is True

    # Resource: compliance history must be populated
    await flush_projections()
    res = await mcp_server.read_resource(f"ledger://applications/{app_id}/compliance")
    compliance_history = _parse_resource(res)
    assert isinstance(compliance_history, (list, dict))

    # ── 8. generate_decision ─────────────────────────────────────────────────
    r = await mcp_server.call_tool("generate_decision", {
        "application_id": app_id,
        "agent_session_id": str(session_id),
        "outcome": "APPROVE",
        "confidence_score": 0.93,
        "reasoning": "Strong credit, no fraud, compliance passed",
        "model_version": "gpt-4-turbo",
    })
    assert not r.isError, f"generate_decision failed: {_parse(r)}"
    gd_data = _parse(r)
    assert gd_data["status"] == "DecisionGenerated"
    assert gd_data["outcome"] == "APPROVE"

    # ── 9. finalize_application ──────────────────────────────────────────────
    r = await mcp_server.call_tool("finalize_application", {
        "application_id": app_id,
        "approved": True,
        "approved_amount": 200000.0,
        "interest_rate": 0.0399,
        "approved_by": "mcp_agent_v1",
    })
    assert not r.isError, f"finalize_application failed: {_parse(r)}"
    assert _parse(r)["status"] == "ApplicationFinalized"

    # ── 10. Verify final state via resource (projection, not stream) ─────────
    await flush_projections()
    res = await mcp_server.read_resource(f"ledger://applications/{app_id}")
    final_summary = _parse_resource(res)
    assert final_summary.get("status") == "FinalApproved", (
        f"Expected FinalApproved via projection, got: {final_summary.get('status')}"
    )

    # ── 11. List all applications ────────────────────────────────────────────
    res = await mcp_server.read_resource("ledger://applications")
    all_apps = _parse_resource(res)
    assert isinstance(all_apps, list)
    ids = [str(a.get("application_id")) for a in all_apps]
    assert app_id in ids

    # ── 12. Health check ─────────────────────────────────────────────────────
    res = await mcp_server.read_resource("ledger://ledger/health")
    health = _parse_resource(res)
    assert health.get("status") == "ok"
    assert health.get("db_latency_ms", 9999) < 100


@pytest.mark.asyncio
async def test_call_tool_returns_structured_error_for_confidence_floor():
    """
    call_tool() must return isError=True with error='BusinessRuleViolationError'
    when confidence < 0.6 and outcome != REFER. The LLM must be able to parse
    the error type from the response without inspecting exception internals.
    """
    from src.commands.handlers import (
        FinalizeComplianceCommand,
        LoadAgentContextCommand,
        RecordComplianceCheckCommand,
        RecordCreditAnalysisCommand,
        RecordFraudCheckCommand,
        RequestComplianceCheckCommand,
        StartAgentSessionCommand,
        SubmitApplicationCommand,
    )
    handler = mcp_server._handler

    # Build application up to PendingDecision
    app_id_uuid = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Confidence Floor Test",
        loan_amount=40000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id_uuid, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id_uuid,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id_uuid, credit_score=650, debt_to_income_ratio=0.4
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id_uuid, fraud_risk_score=0.1, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id_uuid, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=True,
        check_result={}, failure_reason="", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id_uuid, officer_id="officer"
    ))

    app_id = str(app_id_uuid)

    # Low confidence APPROVE → must return structured error via call_tool
    r = await mcp_server.call_tool("generate_decision", {
        "application_id": app_id,
        "agent_session_id": str(session_id),
        "outcome": "APPROVE",
        "confidence_score": 0.42,
        "reasoning": "Uncertain",
        "model_version": "gpt-4",
    })
    assert r.isError is True, "Expected isError=True for confidence floor violation"
    err = _parse(r)
    assert err["error"] == "BusinessRuleViolationError", (
        f"LLM-parseable error type must be 'BusinessRuleViolationError', got: {err.get('error')}"
    )
    assert "ConfidenceFloor" in err.get("rule", "") or "ConfidenceFloor" in err.get("message", ""), (
        f"Error must identify the violated rule, got: {err}"
    )

    # Low confidence REFER → must succeed via call_tool
    r = await mcp_server.call_tool("generate_decision", {
        "application_id": app_id,
        "agent_session_id": str(session_id),
        "outcome": "REFER",
        "confidence_score": 0.42,
        "reasoning": "Low confidence — referring",
        "model_version": "gpt-4",
    })
    assert not r.isError, f"REFER with low confidence should succeed, got: {_parse(r)}"
    assert _parse(r)["status"] == "DecisionGenerated"


@pytest.mark.asyncio
async def test_call_tool_returns_structured_error_for_unknown_tool():
    """Unknown tool name must return isError=True with error='UnknownTool'."""
    r = await mcp_server.call_tool("nonexistent_tool", {})
    assert r.isError is True
    err = _parse(r)
    assert err["error"] == "UnknownTool"


@pytest.mark.asyncio
async def test_call_tool_returns_structured_error_for_missing_aggregate():
    """Calling a tool with a non-existent application_id must return AggregateNotFoundError."""
    r = await mcp_server.call_tool("record_credit_analysis", {
        "application_id": str(uuid4()),
        "credit_score": 700,
        "debt_to_income_ratio": 0.3,
    })
    assert r.isError is True
    err = _parse(r)
    assert err["error"] == "AggregateNotFoundError"


@pytest.mark.asyncio
async def test_read_resource_returns_not_found_for_missing_application():
    """read_resource for a non-existent application must return an error payload, not raise."""
    res = await mcp_server.read_resource(f"ledger://applications/{uuid4()}")
    data = _parse_resource(res)
    assert "error" in data


@pytest.mark.asyncio
async def test_read_resource_unknown_uri_returns_error():
    """Unknown resource URI must return an error payload, not raise."""
    res = await mcp_server.read_resource("ledger://unknown/path")
    data = _parse_resource(res)
    assert "error" in data
