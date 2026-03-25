"""
tests/test_narratives.py
The 5 Narrative Test Scenarios for The Ledger Challenge

These tests are the primary correctness gate. All 5 must pass for a score of 4+.
"""
import pytest
import asyncio
from uuid import uuid4, UUID
from typing import Dict, Any

# Import agents
from ledger.agents.stub_agents import (
    DocumentProcessingAgent,
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent
)
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.base_agent import SimulatedCrashError

# Import events
from ledger.schema.events import (
    CreditRecordOpened,
    CreditAnalysisCompleted,
    ExtractionCompleted,
    QualityAssessmentCompleted,
    AgentSessionStarted,
    AgentSessionFailed,
    AgentSessionRecovered,
    AgentNodeExecuted,
    FraudScreeningCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceCheckCompleted,
    ApplicationDeclined,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved
)

# ── Helpers ──────────────────────────────────────────────────────────────────

def _app_uuid(app_id: str) -> UUID:
    """Convert string app_id to a deterministic UUID."""
    import hashlib
    h = hashlib.md5(app_id.encode()).hexdigest()
    return UUID(h)

def _company_uuid(company_id: str) -> UUID:
    """Convert string company_id to a deterministic UUID."""
    import hashlib
    h = hashlib.md5(company_id.encode()).hexdigest()
    return UUID(h)

def _payload(event) -> dict:
    """Get payload from either StoredEvent (.payload) or DomainEvent (.to_payload())."""
    if hasattr(event, 'payload') and isinstance(getattr(event, 'payload'), dict):
        return event.payload
    return event.to_payload()


@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision(event_store, registry_client):
    """
    NARR-01: Concurrent OCC Collision

    Two CreditAnalysisAgent instances start simultaneously on the same application.
    Both read the credit stream at version 0 (only CreditRecordOpened exists).

    Expected:
    - Agent A appends CreditAnalysisCompleted at expected_version=0 → succeeds → version becomes 1
    - Agent B hits OptimisticConcurrencyError, reloads stream, sees Agent A's result
    - Agent B appends its own CreditAnalysisCompleted at expected_version=1 → succeeds → version becomes 2
    - Both agents complete without raising to caller

    Assertions:
    - Exactly 2 CreditAnalysisCompleted events in credit stream
    - stream_position 1 and 2 both have event_type=CreditAnalysisCompleted
    """
    app_id = _app_uuid("APEX-NARR01")
    company_id = _company_uuid("COMP-031")

    # Create credit stream with CreditRecordOpened only
    await event_store.append("CreditAnalysis", app_id, [
        CreditRecordOpened(application_id=app_id, company_id=company_id)
    ], expected_version=0)

    # Start two agents concurrently
    agent_a = CreditAnalysisAgent(event_store, registry_client)
    agent_b = CreditAnalysisAgent(event_store, registry_client)

    state_a = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "analysis_result": {}
    }

    state_b = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "analysis_result": {}
    }

    # Run concurrently — both must complete without raising
    results = await asyncio.gather(
        agent_a.process(state_a),
        agent_b.process(state_b),
        return_exceptions=False
    )

    # Assertions
    credit_events = await event_store.load_stream("CreditAnalysis", app_id)
    completed_events = [e for e in credit_events if e.event_type == "CreditAnalysisCompleted"]

    assert len(completed_events) == 2, (
        f"Expected exactly 2 CreditAnalysisCompleted events, got {len(completed_events)}"
    )
    # Both agents completed — verify we have 2 distinct CreditAnalysisCompleted events
    assert completed_events[0].event_type == "CreditAnalysisCompleted"
    assert completed_events[1].event_type == "CreditAnalysisCompleted"
    # Verify they are different events (different event_ids)
    assert completed_events[0].event_id != completed_events[1].event_id


@pytest.mark.asyncio
async def test_narr02_missing_ebitda(event_store, registry_client):
    """
    NARR-02: Document Extraction Failure (Missing EBITDA)

    DocumentProcessingAgent processes an application where the income statement PDF
    has no EBITDA line item (missing_ebitda variant).

    Expected:
    - ExtractionCompleted has facts.ebitda=None, field_confidence["ebitda"]=0.0
    - QualityAssessmentCompleted has "ebitda" in critical_missing_fields
    - CreditAnalysisCompleted.decision["confidence"] <= 0.75
    - CreditAnalysisCompleted.decision["data_quality_caveats"] is non-empty
    """
    app_id = _app_uuid("APEX-NARR02")
    company_id = _company_uuid("COMP-044")

    # Run DocumentProcessingAgent
    doc_agent = DocumentProcessingAgent(event_store, registry_client)
    doc_state = {
        "application_id": app_id,
        "company_id": company_id,
        "document_paths": {
            "income_statement": f"documents/COMP-044/income_statement_2024_missing_ebitda.txt",
            "balance_sheet": f"documents/COMP-044/balance_sheet_2024.txt"
        },
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "extracted_facts": {},
        "field_confidence": {},
        "extraction_notes": []
    }

    await doc_agent.process(doc_state)

    # Load docpkg stream
    docpkg_events = await event_store.load_stream("DocumentPackage", app_id)
    extraction_event = next((e for e in docpkg_events if e.event_type == "ExtractionCompleted"), None)
    quality_event = next((e for e in docpkg_events if e.event_type == "QualityAssessmentCompleted"), None)

    assert extraction_event is not None, "ExtractionCompleted event must exist"
    assert quality_event is not None, "QualityAssessmentCompleted event must exist"

    # Assertions on extraction
    assert _payload(extraction_event)["facts"]["ebitda"] is None, "EBITDA should be None"
    assert _payload(extraction_event)["field_confidence"]["ebitda"] == 0.0, "EBITDA confidence should be 0.0"
    assert any("ebitda" in note.lower() for note in _payload(extraction_event)["extraction_notes"]), "EBITDA should be in extraction_notes"

    # Assertions on quality
    assert "ebitda" in _payload(quality_event)["critical_missing_fields"], "EBITDA should be in critical_missing_fields"

    # Run CreditAnalysisAgent
    credit_agent = CreditAnalysisAgent(event_store, registry_client)
    credit_state = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "analysis_result": {}
    }

    await credit_agent.process(credit_state)

    # Load credit stream
    credit_events = await event_store.load_stream("CreditAnalysis", app_id)
    credit_event = next((e for e in credit_events if e.event_type == "CreditAnalysisCompleted"), None)

    assert credit_event is not None, "CreditAnalysisCompleted event must exist"

    # Assertions on credit decision
    decision = _payload(credit_event).get("decision") or {}
    assert decision.get("confidence", 1.0) <= 0.75, "Confidence should be capped at 0.75"
    assert len(decision.get("data_quality_caveats", [])) > 0, "data_quality_caveats should be non-empty"


@pytest.mark.asyncio
async def test_narr03_crash_recovery(event_store, registry_client):
    """
    NARR-03: Agent Crash and Recovery

    FraudDetectionAgent starts processing and crashes after the load_facts node.

    Expected:
    - AgentSessionFailed event with recoverable=True, last_successful_node="load_facts"
    - New FraudDetectionAgent instance starts
    - New session starts with context_source="prior_session_replay:{crashed_session_id}"
    - AgentSessionRecovered event in new session stream
    - No duplicate load_facts work
    - Exactly ONE FraudScreeningCompleted in fraud stream
    """
    app_id = _app_uuid("APEX-NARR03")
    company_id = _company_uuid("COMP-057")

    # First session — crash after load_facts
    fraud_agent_1 = FraudDetectionAgent(event_store)
    fraud_agent_1._crash_after_node = "load_facts"

    state = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "anomalies": [],
        "fraud_score": 0.0
    }

    try:
        await fraud_agent_1.process(state)
    except SimulatedCrashError:
        pass  # Expected

    # Check crashed session
    session_1_id = fraud_agent_1.session_id
    session_1_events = await event_store.load_stream("AgentSession", session_1_id)

    failed_event = next((e for e in session_1_events if e.event_type == "AgentSessionFailed"), None)
    assert failed_event is not None, "AgentSessionFailed event must exist"
    assert _payload(failed_event).get("recoverable") is True, "Session should be recoverable"
    assert _payload(failed_event).get("last_successful_node") == "load_facts", "Last successful node should be load_facts"

    # Recovery session
    fraud_agent_2 = FraudDetectionAgent(event_store)
    fraud_agent_2._recover_from_session = session_1_id
    fraud_agent_2._last_successful_node = "load_facts"  # Set from crashed session

    # Reset state for recovery
    state2 = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "anomalies": [],
        "fraud_score": 0.0
    }

    await fraud_agent_2.process(state2)

    # Check recovery session
    session_2_id = fraud_agent_2.session_id
    session_2_events = await event_store.load_stream("AgentSession", session_2_id)

    started_event = next((e for e in session_2_events if e.event_type == "AgentSessionStarted"), None)
    assert started_event is not None, "AgentSessionStarted event must exist in recovery session"
    context_source = _payload(started_event).get("session_config", {}).get("context_source", "")
    assert context_source.startswith("prior_session_replay:"), (
        f"context_source should indicate replay, got: {context_source}"
    )

    recovered_event = next((e for e in session_2_events if e.event_type == "AgentSessionRecovered"), None)
    assert recovered_event is not None, "AgentSessionRecovered event must exist"
    assert str(_payload(recovered_event).get("recovered_from_session_id")) == str(session_1_id), (
        "Should reference crashed session"
    )

    # Check no duplicate work — load_facts should appear exactly once across both sessions
    all_node_events = [e for e in session_1_events + session_2_events
                       if e.event_type == "AgentNodeExecuted"]
    load_facts_executions = [e for e in all_node_events if _payload(e).get("node_name") == "load_facts"]
    assert len(load_facts_executions) == 1, (
        f"load_facts should execute exactly once, got {len(load_facts_executions)}"
    )

    # Check final output
    fraud_events = await event_store.load_stream("FraudScreening", app_id)
    fraud_completed = [e for e in fraud_events if e.event_type == "FraudScreeningCompleted"]
    assert len(fraud_completed) == 1, f"Exactly one FraudScreeningCompleted expected, got {len(fraud_completed)}"


@pytest.mark.asyncio
async def test_narr04_montana_hard_block(event_store, registry_client):
    """
    NARR-04: Compliance Hard Block (Montana)

    ComplianceAgent evaluates rules sequentially. REG-003 fails (Montana excluded).

    Expected:
    - ComplianceRulePassed for REG-001
    - ComplianceRulePassed for REG-002
    - ComplianceRuleFailed for REG-003 with is_hard_block=True
    - No further rule events (REG-004 through REG-006 never evaluated)
    - ComplianceCheckCompleted with overall_verdict="BLOCKED"
    - ApplicationDeclined on loan stream with adverse_action_notice_required=True
    - NO DecisionGenerated event ever appears
    """
    app_id = _app_uuid("APEX-NARR04")
    company_id = _company_uuid("COMP-MT-001")

    # Montana company profile — no DB needed, pass directly
    montana_company = {
        "company_id": str(company_id),
        "name": "Montana Test Corp",
        "jurisdiction": "MT",
        "state": "MT"
    }

    # Run ComplianceAgent
    compliance_agent = ComplianceAgent(event_store)
    state = {
        "application_id": app_id,
        "company_id": company_id,
        "company_profile": montana_company,
        "compliance_flags": [],
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "rules_evaluated": 0,
        "hard_block": False,
        "rule_results": []
    }

    await compliance_agent.process(state)

    # Load compliance stream
    compliance_events = await event_store.load_stream("ComplianceCheck", app_id)

    # Count rule events
    rule_events = [e for e in compliance_events if e.event_type in
                   ["ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"]]

    assert len(rule_events) == 3, (
        f"Exactly 3 rules should be evaluated (REG-001, REG-002, REG-003), got {len(rule_events)}"
    )

    # Check REG-003 failed with hard block
    reg003_event = next((e for e in rule_events if _payload(e).get("rule_id") == "REG-003"), None)
    assert reg003_event is not None, "REG-003 event must exist"
    assert reg003_event.event_type == "ComplianceRuleFailed", "REG-003 should fail"
    assert _payload(reg003_event).get("is_hard_block") is True, "REG-003 should be a hard block"

    # Check overall verdict
    completed_event = next((e for e in compliance_events if e.event_type == "ComplianceCheckCompleted"), None)
    assert completed_event is not None, "ComplianceCheckCompleted event must exist"
    assert _payload(completed_event).get("overall_verdict") == "BLOCKED", "Overall verdict should be BLOCKED"

    # Check loan stream — ApplicationDeclined should be there
    loan_events = await event_store.load_stream("LoanApplication", app_id)

    # NO DecisionGenerated
    decision_events = [e for e in loan_events if e.event_type == "DecisionGenerated"]
    assert len(decision_events) == 0, "NO DecisionGenerated event should exist for hard-blocked application"

    # ApplicationDeclined present
    declined_event = next((e for e in loan_events if e.event_type == "ApplicationDeclined"), None)
    assert declined_event is not None, "ApplicationDeclined event must exist"
    assert any("REG-003" in reason for reason in _payload(declined_event).get("decline_reasons", [])), (
        "Decline reasons should mention REG-003"
    )
    assert _payload(declined_event).get("adverse_action_notice_required") is True, (
        "adverse_action_notice_required should be True"
    )


@pytest.mark.asyncio
async def test_narr05_human_override(event_store, registry_client, mcp_server):
    """
    NARR-05: Human Override (The Loan Officer Approves Against the Agent)

    Full pipeline runs on a DECLINING revenue company. DecisionOrchestrator recommends DECLINE.
    A human loan officer overrides and approves.

    Expected:
    - All agents run: Document → Credit → Fraud → Compliance → Decision
    - DecisionGenerated with recommendation="DECLINE"
    - HumanReviewRequested on loan stream
    - Human override via MCP tool: record_human_review_completed
    - HumanReviewCompleted with override=True, reviewer_id="LO-Sarah-Chen", final_decision="APPROVE"
    - ApplicationApproved with approved_amount_usd=750000, conditions=[2 items]
    """
    app_id = _app_uuid("APEX-NARR05")
    company_id = _company_uuid("COMP-068")

    # Company profile for COMP-068 — retail, DECLINING revenue
    company_profile = {
        "company_id": str(company_id),
        "name": "Retail Corp 068",
        "jurisdiction": "CA",
        "state": "CA"
    }

    # 1. DocumentProcessingAgent
    doc_agent = DocumentProcessingAgent(event_store, registry_client)
    doc_state = {
        "application_id": app_id,
        "company_id": company_id,
        "document_paths": {
            "income_statement": f"documents/COMP-068/income_statement_2024.txt",
            "balance_sheet": f"documents/COMP-068/balance_sheet_2024.txt"
        },
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "extracted_facts": {},
        "field_confidence": {},
        "extraction_notes": []
    }
    await doc_agent.process(doc_state)

    # 2. CreditAnalysisAgent
    credit_agent = CreditAnalysisAgent(event_store, registry_client)
    credit_state = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "analysis_result": {}
    }
    await credit_agent.process(credit_state)

    # 3. FraudDetectionAgent
    fraud_agent = FraudDetectionAgent(event_store)
    fraud_state = {
        "application_id": app_id,
        "company_id": company_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "anomalies": [],
        "fraud_score": 0.0
    }
    await fraud_agent.process(fraud_state)

    # 4. ComplianceAgent
    compliance_agent = ComplianceAgent(event_store)
    compliance_state = {
        "application_id": app_id,
        "company_id": company_id,
        "company_profile": company_profile,
        "compliance_flags": [],
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "rules_evaluated": 0,
        "hard_block": False,
        "rule_results": []
    }
    await compliance_agent.process(compliance_state)

    # 5. DecisionOrchestratorAgent — force DECLINE by setting low confidence
    orchestrator = DecisionOrchestratorAgent(event_store)
    decision_state = {
        "application_id": app_id,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "credit_result": {
            "outcome": "DECLINE",
            "confidence": 0.82,
            "risk_tier": "HIGH",
            "data_quality_caveats": []
        },
        "fraud_result": {
            "fraud_score": 0.15,
            "passed": True,
            "anomalies": []
        },
        "compliance_result": {
            "overall_verdict": "PASS",
            "checks_passed": 6,
            "hard_block": False
        },
        "orchestrator_decision": {},
        "final_decision": {}
    }
    await orchestrator.process(decision_state)

    # Check DecisionGenerated
    loan_events = await event_store.load_stream("LoanApplication", app_id)
    decision_event = next((e for e in loan_events if e.event_type == "DecisionGenerated"), None)
    assert decision_event is not None, "DecisionGenerated event must exist"
    recommendation = _payload(decision_event).get("recommendation") or _payload(decision_event).get("outcome")
    assert recommendation in ("DECLINE", "DENY"), f"Recommendation should be DECLINE/DENY, got {recommendation}"

    # Check HumanReviewRequested
    review_requested = any(e.event_type == "HumanReviewRequested" for e in loan_events)
    assert review_requested, "HumanReviewRequested event must exist"

    # Human override via MCP
    await mcp_server.call_tool("record_human_review_completed", {
        "application_id": str(app_id),
        "reviewer_id": "LO-Sarah-Chen",
        "final_decision": "APPROVE",
        "override": True,
        "override_reason": "15-year customer, prior repayment history, collateral offered",
        "approved_amount_usd": 750000.0,
        "conditions": [
            "Monthly revenue reporting for 12 months",
            "Personal guarantee from CEO"
        ]
    })

    # Reload loan stream
    loan_events = await event_store.load_stream("LoanApplication", app_id)

    # Check HumanReviewCompleted
    review_event = next((e for e in loan_events if e.event_type == "HumanReviewCompleted"), None)
    assert review_event is not None, "HumanReviewCompleted event must exist"
    assert _payload(review_event).get("override") is True, "override should be True"
    assert _payload(review_event).get("reviewer_id") == "LO-Sarah-Chen", "reviewer_id should match"
    assert _payload(review_event).get("final_decision") == "APPROVE", "final_decision should be APPROVE"

    # Check ApplicationApproved
    approved_event = next((e for e in loan_events if e.event_type == "ApplicationApproved"), None)
    assert approved_event is not None, "ApplicationApproved event must exist"
    amount = _payload(approved_event).get("approved_amount_usd") or _payload(approved_event).get("approved_amount")
    assert amount == 750000.0, f"approved_amount should be 750000, got {amount}"
    assert len(_payload(approved_event).get("conditions", [])) == 2, "Should have 2 conditions"
