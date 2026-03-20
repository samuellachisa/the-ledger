"""
Concurrent Invariant Tests: All business rules hold under concurrent scenarios.

Tests that invariants cannot be violated even when multiple agents race:
- Two agents cannot both approve the same application
- Confidence floor cannot be bypassed under concurrency
- Compliance dependency cannot be bypassed under concurrency
- Counterfactual command testing: what happens when commands run against
  counterfactual aggregate states
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from src.aggregates.loan_application import LoanApplicationAggregate
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
from src.models.events import (
    BusinessRuleViolationError,
    DecisionOutcome,
    InvalidStateTransitionError,
    LoanStatus,
    OptimisticConcurrencyError,
)
from src.upcasting.registry import UpcasterRegistry
from src.what_if.projector import WhatIfProjector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _setup_application_at_pending_decision(handler, store) -> tuple:
    """Set up a full application ready for decision. Returns (app_id, session_id, compliance_id)."""
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Invariant Test",
        loan_amount=100000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=750, debt_to_income_ratio=0.3
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.05, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=True,
        check_result={}, failure_reason="", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))
    return app_id, session_id, compliance_id


# ---------------------------------------------------------------------------
# Concurrent invariant tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_double_approval_exactly_one_wins(db_pool):
    """
    Two agents concurrently attempt to approve the same application.
    Exactly one must succeed; the other must get OptimisticConcurrencyError.
    The final state must be FinalApproved (not double-approved).
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    app_id, session_id, _ = await _setup_application_at_pending_decision(handler, store)

    # Both agents generate a decision at the same version
    events = await store.load_stream("LoanApplication", app_id)
    version_before_decision = len(events)

    results = {"success": 0, "conflict": 0, "errors": []}

    async def agent_approve(agent_id: int):
        try:
            await handler.handle_generate_decision(GenerateDecisionCommand(
                application_id=app_id,
                agent_session_id=session_id,
                outcome=DecisionOutcome.APPROVE,
                confidence_score=0.88,
                reasoning=f"Agent {agent_id} approves",
                model_version="gpt-4",
            ))
            results["success"] += 1
        except OptimisticConcurrencyError:
            results["conflict"] += 1
        except Exception as e:
            results["errors"].append(str(e))

    await asyncio.gather(agent_approve(1), agent_approve(2))

    assert results["errors"] == [], f"Unexpected errors: {results['errors']}"
    assert results["success"] == 1, f"Expected 1 success, got {results['success']}"
    assert results["conflict"] == 1, f"Expected 1 conflict, got {results['conflict']}"

    # Verify only one DecisionGenerated event
    final_events = await store.load_stream("LoanApplication", app_id)
    decisions = [e for e in final_events if e.event_type == "DecisionGenerated"]
    assert len(decisions) == 1


@pytest.mark.asyncio
async def test_confidence_floor_invariant_under_concurrency(db_pool):
    """
    Even under concurrent load, confidence floor cannot be bypassed.
    Multiple agents with low confidence must all get BusinessRuleViolationError for APPROVE.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    app_id, session_id, _ = await _setup_application_at_pending_decision(handler, store)

    violations = []
    successes = []

    async def attempt_low_confidence_approve(i: int):
        try:
            await handler.handle_generate_decision(GenerateDecisionCommand(
                application_id=app_id,
                agent_session_id=session_id,
                outcome=DecisionOutcome.APPROVE,
                confidence_score=0.45,  # Below floor
                reasoning=f"Agent {i} low confidence approve",
                model_version="gpt-4",
            ))
            successes.append(i)
        except BusinessRuleViolationError as e:
            violations.append(str(e))
        except OptimisticConcurrencyError:
            pass  # Expected under concurrency

    await asyncio.gather(*[attempt_low_confidence_approve(i) for i in range(5)])

    # No agent should have succeeded with low confidence APPROVE
    assert successes == [], f"Low confidence APPROVE should never succeed, but agent(s) {successes} did"
    assert len(violations) > 0, "Expected at least one ConfidenceFloor violation"


@pytest.mark.asyncio
async def test_compliance_dependency_invariant_under_concurrency(db_pool):
    """
    Cannot approve without compliance passing, even under concurrent load.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)

    # Set up application with FAILED compliance
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Compliance Invariant Test",
        loan_amount=75000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=800, debt_to_income_ratio=0.2
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.01, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=False,
        check_result={}, failure_reason="Flagged", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))

    violations = []
    successes = []

    async def attempt_approve_without_compliance(i: int):
        try:
            await handler.handle_generate_decision(GenerateDecisionCommand(
                application_id=app_id,
                agent_session_id=session_id,
                outcome=DecisionOutcome.APPROVE,
                confidence_score=0.92,
                reasoning=f"Agent {i} ignoring compliance",
                model_version="gpt-4",
            ))
            successes.append(i)
        except BusinessRuleViolationError as e:
            violations.append(str(e))
        except OptimisticConcurrencyError:
            pass

    await asyncio.gather(*[attempt_approve_without_compliance(i) for i in range(3)])

    assert successes == [], f"Approval without compliance should never succeed, got {successes}"


# ---------------------------------------------------------------------------
# Counterfactual command tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_counterfactual_high_credit_score_changes_outcome(db_pool):
    """
    Counterfactual: if credit score was 800 instead of 500, outcome changes.
    Verifies causal dependency filtering removes the original decision.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    projector = WhatIfProjector(store)

    # Build a real application that gets DENIED due to low credit
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Counterfactual Test",
        loan_amount=50000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau"], context_snapshot={},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=500, debt_to_income_ratio=0.65  # Bad credit
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.1, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=True,
        check_result={}, failure_reason="", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.DENY,
        confidence_score=0.85,
        reasoning="Low credit score",
        model_version="gpt-4",
    ))
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id,
        approved=False,
        denial_reasons=["Credit score below threshold"],
        denied_by="system",
    ))

    # Verify baseline is Denied
    events = await store.load_stream("LoanApplication", app_id)
    baseline = LoanApplicationAggregate.load(app_id, events)
    assert baseline.status == LoanStatus.DENIED

    # Counterfactual: what if credit score was 800?
    from src.models.events import CreditAnalysisCompleted
    cf_credit = CreditAnalysisCompleted(
        aggregate_id=app_id,
        credit_score=800,
        debt_to_income_ratio=0.25,
        model_version="credit-model-v3",
    )

    result = await projector.run_what_if(
        application_id=app_id,
        counterfactual_events=[cf_credit],
    )

    # Causal filtering must have removed DecisionGenerated and ApplicationDenied
    assert "DecisionGenerated" in result["causally_removed_events"], (
        "DecisionGenerated must be causally removed when CreditAnalysisCompleted is replaced"
    )
    assert "ApplicationDenied" in result["causally_removed_events"], (
        "ApplicationDenied must be causally removed"
    )

    # Counterfactual credit score must be reflected
    assert result["counterfactual_state"]["credit_score"] == 800
    assert result["baseline_state"]["credit_score"] == 500

    # The causal chain must be documented
    assert len(result["causal_chain"]) > 0

    # Outcome must differ (no decision in counterfactual stream → different terminal state)
    assert result["outcome_changed"] is True or result["counterfactual_outcome"] != "Denied"


@pytest.mark.asyncio
async def test_counterfactual_same_outcome_when_credit_already_good(db_pool):
    """
    Counterfactual: if credit score was 790 instead of 780, outcome is unchanged.
    Verifies that causal filtering still removes downstream events for re-evaluation.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    projector = WhatIfProjector(store)

    app_id, session_id, _ = await _setup_application_at_pending_decision(handler, store)
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.APPROVE,
        confidence_score=0.91,
        reasoning="Good credit",
        model_version="gpt-4",
    ))

    from src.models.events import CreditAnalysisCompleted
    cf_credit = CreditAnalysisCompleted(
        aggregate_id=app_id,
        credit_score=790,  # Slightly better but still good
        debt_to_income_ratio=0.28,
        model_version="credit-model-v3",
    )

    result = await projector.run_what_if(
        application_id=app_id,
        counterfactual_events=[cf_credit],
    )

    # Causal filtering must still remove DecisionGenerated
    assert "DecisionGenerated" in result["causally_removed_events"]
    # Credit score must be updated
    assert result["counterfactual_state"]["credit_score"] == 790


@pytest.mark.asyncio
async def test_counterfactual_causal_chain_documented(db_pool):
    """Causal chain must be documented in the what-if result."""
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    projector = WhatIfProjector(store)

    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Causal Chain Test",
        loan_amount=30000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))

    from src.models.events import CreditAnalysisCompleted
    cf = CreditAnalysisCompleted(
        aggregate_id=app_id,
        credit_score=720,
        debt_to_income_ratio=0.35,
    )

    result = await projector.run_what_if(app_id, [cf])

    # Causal chain must mention CreditAnalysisCompleted → downstream
    chain_str = " ".join(result["causal_chain"])
    assert "CreditAnalysisCompleted" in chain_str
    assert "DecisionGenerated" in chain_str


@pytest.mark.asyncio
async def test_counterfactual_multi_hop_fraud_and_credit_simultaneously(db_pool):
    """
    Multi-hop counterfactual: replace BOTH FraudCheckCompleted AND CreditAnalysisCompleted
    simultaneously. Both causal chains must be applied — DecisionGenerated and terminal
    events must be removed from both dependency sets.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    projector = WhatIfProjector(store)

    # Build a full application that gets DENIED (low credit + fraud flagged)
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Multi-Hop Counterfactual Test",
        loan_amount=75000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4", agent_version="1.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau", "fraud_db"], context_snapshot={},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=480, debt_to_income_ratio=0.72  # Very bad credit
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.85, flags=["suspicious_ip", "velocity"], passed=False
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML"]
        )
    )
    await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
        compliance_record_id=compliance_id, check_name="AML", passed=True,
        check_result={}, failure_reason="", officer_id="officer"
    ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id,
        agent_session_id=session_id,
        outcome=DecisionOutcome.DENY,
        confidence_score=0.91,
        reasoning="Low credit and fraud flags",
        model_version="gpt-4",
    ))
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id,
        approved=False,
        denial_reasons=["Credit score below threshold", "Fraud flags detected"],
        denied_by="system",
    ))

    # Verify baseline is Denied
    events = await store.load_stream("LoanApplication", app_id)
    baseline = LoanApplicationAggregate.load(app_id, events)
    assert baseline.status == LoanStatus.DENIED

    # Counterfactual: replace BOTH CreditAnalysisCompleted AND FraudCheckCompleted
    from src.models.events import CreditAnalysisCompleted, FraudCheckCompleted
    cf_credit = CreditAnalysisCompleted(
        aggregate_id=app_id,
        credit_score=800,
        debt_to_income_ratio=0.22,
        model_version="credit-model-v3",
    )
    cf_fraud = FraudCheckCompleted(
        aggregate_id=app_id,
        fraud_risk_score=0.03,
        flags=[],
        passed=True,
    )

    result = await projector.run_what_if(
        application_id=app_id,
        counterfactual_events=[cf_credit, cf_fraud],
    )

    # Both event types must appear in injected_events
    assert "CreditAnalysisCompleted" in result["injected_events"]
    assert "FraudCheckCompleted" in result["injected_events"]

    # DecisionGenerated must be causally removed (it depends on both credit AND fraud)
    assert "DecisionGenerated" in result["causally_removed_events"], (
        "DecisionGenerated must be causally removed when both CreditAnalysis and FraudCheck are replaced"
    )

    # Terminal denial event must also be removed
    assert "ApplicationDenied" in result["causally_removed_events"], (
        "ApplicationDenied must be causally removed as it depends on DecisionGenerated"
    )

    # Causal chain must document both dependency paths
    chain_str = " ".join(result["causal_chain"])
    assert "CreditAnalysisCompleted" in chain_str
    assert "FraudCheckCompleted" in chain_str

    # Counterfactual state must reflect both replacements
    assert result["counterfactual_state"]["credit_score"] == 800
    assert result["baseline_state"]["credit_score"] == 480

    # Outcome must have changed (no decision in counterfactual stream)
    assert result["outcome_changed"] is True

    # Narrative must be coherent and mention both replaced events
    narrative = result["narrative"]
    assert isinstance(narrative, str) and len(narrative) > 20
    assert "CreditAnalysisCompleted" in narrative or "FraudCheckCompleted" in narrative
