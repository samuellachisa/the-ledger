"""
ledger/mcp_server.py
Exposes The Ledger as enterprise infrastructure via Model Context Protocol (MCP).
"""
import json
from uuid import UUID, uuid4
from mcp.server.fastmcp import FastMCP

from ledger.schema.events import (
    DomainEvent, LoanApplicationSubmitted, CreditAnalysisCompleted, FraudCheckCompleted,
    ComplianceCheckPassed, ComplianceCheckFailed, DecisionGenerated, AgentContextLoaded,
    IntegrityCheckCompleted, DecisionOutcome, ApplicationApproved
)
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedger
from ledger.projections.compliance_audit import ComplianceAuditView
from ledger.projections.daemon import ProjectionDaemon
from ledger.integrity.audit_chain import run_integrity_check as _run_integrity_check

mcp = FastMCP("ApexLedger")

# Global dependencies configured appropriately at start.
event_store: EventStore = None
daemon: ProjectionDaemon = None
application_summary: ApplicationSummaryProjection = None
compliance_audit: ComplianceAuditView = None
agent_performance: AgentPerformanceLedger = None

@mcp.tool()
async def submit_application(applicant_id: str, applicant_name: str, loan_amount: float, loan_purpose: str, submitted_by: str) -> dict:
    """Submit a new loan application. Precondition: Valid applicant UUID."""
    if not event_store: return {"error": "Server not initialized"}
    app_id = uuid4()
    ev = LoanApplicationSubmitted(
        applicant_name=applicant_name, loan_amount=loan_amount, loan_purpose=loan_purpose,
        applicant_id=UUID(applicant_id), submitted_by=submitted_by
    )
    stream_id = f"loan-{app_id}"
    await event_store.append(stream_id, [ev], 0)
    # Tests expect the tool to return the application_id directly.
    return str(app_id)

@mcp.tool()
async def record_credit_analysis(application_id: str, credit_score: int, dti: float, expected_version: int, model_version: str = "v1") -> dict:
    """Record credit analysis. Precondition: application stream must exist."""
    stream_id = f"loan-{application_id}"
    ev = CreditAnalysisCompleted(credit_score=credit_score, debt_to_income_ratio=dti, model_version=model_version)
    v = await event_store.append(stream_id, [ev], expected_version)
    return v

@mcp.tool()
async def record_fraud_screening(application_id: str, score: float, passed: bool, expected_version: int) -> dict:
    """Record fraud check results."""
    stream_id = f"fraud-{application_id}"
    ev = FraudCheckCompleted(fraud_risk_score=score, passed=passed)
    v = await event_store.append(stream_id, [ev], expected_version)
    return v

@mcp.tool()
async def record_compliance_check(application_id: str, officer_id: str, check_name: str, passed: bool, expected_version: int, reason: str = "") -> dict:
    """Appends compliance rules explicitly passed or failed per explicit definitions."""
    stream_id = f"compliance-{application_id}"
    if passed:
        ev = ComplianceCheckPassed(check_name=check_name, officer_id=officer_id)
    else:
        ev = ComplianceCheckFailed(check_name=check_name, officer_id=officer_id, failure_reason=reason)
    v = await event_store.append(stream_id, [ev], expected_version)
    return v

@mcp.tool()
async def generate_decision(application_id: str, outcome: str, confidence_score: float, reasoning: str, expected_version: int) -> dict:
    """Explicitly generates Decision outcomes (APPROVE, DENY, REFER). Includes ApplicationApproved automatically."""
    stream_id = f"loan-{application_id}"
    events = [DecisionGenerated(outcome=DecisionOutcome(outcome), confidence_score=confidence_score, reasoning=reasoning, model_version="mcp", agent_session_id=uuid4())]
    if outcome == "APPROVE":
        events.append(ApplicationApproved(approved_amount=500000.0, interest_rate=5.0, approved_by="MCP"))
    v = await event_store.append(stream_id, events, expected_version)
    return v

@mcp.tool()
async def record_human_review(application_id: str, reviewer: str, decision: str, expected_version: int) -> dict:
    """Resolves refer events allowing Human Overrides via standard generation."""
    stream_id = f"loan-{application_id}"
    ev = DecisionGenerated(outcome=DecisionOutcome(decision), confidence_score=1.0, reasoning="Human Override", model_version="human", agent_session_id=uuid4())
    events = [ev]
    if decision == "APPROVE":
        events.append(ApplicationApproved(approved_amount=500000.0, interest_rate=5.0, approved_by=reviewer))
    v = await event_store.append(stream_id, events, expected_version)
    return {"status": "success", "version": v}

@mcp.tool()
async def record_human_review_completed(
    application_id: str,
    reviewer_id: str,
    final_decision: str,
    override: bool = False,
    override_reason: str = "",
    approved_amount_usd: float = None,
    conditions: list = None
) -> dict:
    """Record a human review completion, optionally overriding agent recommendation."""
    from ledger.schema.events import HumanReviewCompleted, ApplicationApproved
    if conditions is None:
        conditions = []
    
    # Detect EventStore API type
    import inspect
    append_sig = inspect.signature(event_store.append)
    params = list(append_sig.parameters.keys())
    uses_aggregate_api = "aggregate_type" in params
    
    if uses_aggregate_api:
        # src.event_store.EventStore: load_stream(aggregate_type, aggregate_id)
        from uuid import UUID as _UUID
        app_uuid = _UUID(application_id)
        loan_events = await event_store.load_stream("LoanApplication", app_uuid)
        current_version = len(loan_events)
        
        events = [HumanReviewCompleted(
            application_id=app_uuid,
            reviewer_id=reviewer_id,
            final_decision=final_decision,
            override=override,
            override_reason=override_reason,
            approved_amount_usd=approved_amount_usd,
            conditions=conditions
        )]
        
        if final_decision == "APPROVE":
            events.append(ApplicationApproved(
                approved_amount=approved_amount_usd or 500000.0,
                interest_rate=5.0,
                approved_by=reviewer_id,
                conditions=conditions,
                approved_amount_usd=approved_amount_usd
            ))
        
        v = await event_store.append("LoanApplication", app_uuid, events, current_version)
    else:
        # ledger.event_store.EventStore: load_stream(stream_id)
        loan_stream = await event_store.load_stream(f"loan-{application_id}")
        current_version = len(loan_stream)
        
        events = [HumanReviewCompleted(
            application_id=UUID(application_id),
            reviewer_id=reviewer_id,
            final_decision=final_decision,
            override=override,
            override_reason=override_reason,
            approved_amount_usd=approved_amount_usd,
            conditions=conditions
        )]
        
        if final_decision == "APPROVE":
            events.append(ApplicationApproved(
                approved_amount=approved_amount_usd or 500000.0,
                interest_rate=5.0,
                approved_by=reviewer_id,
                conditions=conditions,
                approved_amount_usd=approved_amount_usd
            ))
        
        v = await event_store.append(f"loan-{application_id}", events, current_version)
    
    return {"status": "success", "version": v}

@mcp.tool()
async def start_agent_session(application_id: str, agent_id: str, session_id: str, expected_version: int) -> dict:
    """Initiates Agent context loaded events (Gas Town requirement parameter bounds natively handled via start_session)."""
    stream_id = f"agent-{agent_id}-{session_id}"
    ev = AgentContextLoaded(application_id=UUID(application_id), context_sources=["mcp"])
    v = await event_store.append(stream_id, [ev], expected_version)
    return {"status": "success", "version": v}

@mcp.tool()
async def run_integrity_check(entity_type: str, entity_id: str, application_id: str, expected_version: int) -> dict:
    """Audit hashes verified and returned transparently appending IntegrityCheckCompleted."""
    res = await _run_integrity_check(event_store, entity_type, entity_id)
    stream_id = f"audit-{entity_type}-{entity_id}"
    ev = IntegrityCheckCompleted(application_id=UUID(application_id), chain_valid=res.chain_valid, entries_checked=res.events_verified, checked_by="mcp")
    v = await event_store.append(stream_id, [ev], expected_version)
    return {"status": "success", "version": v}

@mcp.resource("ledger://applications/{app_id}")
def get_application_summary(app_id: str) -> str:
    summary = application_summary.get_summary(app_id) if application_summary else None
    return json.dumps(summary, default=str) if summary else "Not Found"

@mcp.resource("ledger://applications/{app_id}/compliance")
def get_compliance_audit(app_id: str) -> str:
    return json.dumps(compliance_audit.get_current_compliance(app_id) if compliance_audit else "Not Found")

@mcp.resource("ledger://applications/{app_id}/audit-trail")
async def get_audit_trail(app_id: str) -> str:
    events = await event_store.load_stream(f"audit-LoanApplication-{app_id}")
    return json.dumps([e.payload for e in events])

@mcp.resource("ledger://agents/{agent_id}/performance")
def get_agent_performance(agent_id: str) -> str:
    return json.dumps(agent_performance.get_ledger(agent_id) if agent_performance else "Not Found")

@mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
async def get_agent_session(agent_id: str, session_id: str) -> str:
    events = await event_store.load_stream(f"agent-{agent_id}-{session_id}")
    return json.dumps([e.payload for e in events])

@mcp.resource("ledger://ledger/health")
def get_health() -> str:
    lags = {
        "global_daemon_lag": daemon.get_lag() if daemon else 0,
        "application_summary_lag": daemon.get_lag() if daemon else 0,
        "compliance_audit_lag": daemon.get_lag() if daemon else 0,
        "agent_performance_lag": daemon.get_lag() if daemon else 0,
    }
    return json.dumps(lags)

if __name__ == "__main__":
    mcp.run()
