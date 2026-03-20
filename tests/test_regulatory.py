"""
Regulatory Package Tests: Independent verifiability and chain break detection.
"""
from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest

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
from src.integrity.audit_chain import record_audit_entry, get_last_chain_hash
from src.models.events import DecisionOutcome
from src.regulatory.package import generate_regulatory_package, verify_regulatory_package
from src.upcasting.registry import UpcasterRegistry


async def _build_full_application(handler, store) -> tuple:
    """Build a complete approved application. Returns (app_id, session_id)."""
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Regulatory Test",
        loan_amount=120000.0,
        loan_purpose="Home purchase",
        applicant_id=uuid4(),
        submitted_by="test",
    ))
    session_id = await handler.handle_start_agent_session(StartAgentSessionCommand(
        application_id=app_id, agent_model="gpt-4-turbo", agent_version="2.0"
    ))
    await handler.handle_load_agent_context(LoadAgentContextCommand(
        session_id=session_id, application_id=app_id,
        context_sources=["credit_bureau", "fraud_db"],
        context_snapshot={"credit_score": 760},
        loaded_stream_positions={"LoanApplication": 1},
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=760, debt_to_income_ratio=0.31,
        model_version="credit-v3"
    ))
    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.03, flags=[], passed=True
    ))
    compliance_id = await handler.handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id, officer_id="officer", required_checks=["AML", "KYC"]
        )
    )
    for check in ["AML", "KYC"]:
        await handler.handle_record_compliance_check(RecordComplianceCheckCommand(
            compliance_record_id=compliance_id, check_name=check, passed=True,
            check_result={}, failure_reason="", officer_id="officer"
        ))
    await handler.handle_finalize_compliance(FinalizeComplianceCommand(
        compliance_record_id=compliance_id, application_id=app_id, officer_id="officer"
    ))
    await handler.handle_generate_decision(GenerateDecisionCommand(
        application_id=app_id, agent_session_id=session_id,
        outcome=DecisionOutcome.APPROVE, confidence_score=0.89,
        reasoning="Strong profile", model_version="gpt-4-turbo",
    ))
    await handler.handle_finalize_application(FinalizeApplicationCommand(
        application_id=app_id, approved=True,
        approved_amount=120000.0, interest_rate=0.0415, approved_by="system"
    ))
    return app_id, session_id


@pytest.mark.asyncio
async def test_regulatory_package_is_independently_verifiable(db_pool):
    """
    A regulatory package must be verifiable without access to the live system.
    verify_regulatory_package() must pass on a freshly generated package.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    app_id, _ = await _build_full_application(handler, store)

    package = await generate_regulatory_package(db_pool, store, app_id, generated_by="auditor")

    result = verify_regulatory_package(package)

    assert result["package_hash_valid"], f"Package hash invalid: {result['issues']}"
    assert result["raw_upcasted_consistent"], f"Raw/upcasted mismatch: {result['issues']}"
    assert result["event_count_matches"], f"Event count mismatch: {result['issues']}"
    assert result["overall_valid"], f"Package verification failed: {result['issues']}"


@pytest.mark.asyncio
async def test_tampered_package_fails_verification(db_pool):
    """
    Modifying any field in the package must cause verify_regulatory_package to fail.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    app_id, _ = await _build_full_application(handler, store)

    package = await generate_regulatory_package(db_pool, store, app_id)

    # Tamper with the package
    tampered = json.loads(json.dumps(package, default=str))
    if tampered["events"]:
        tampered["events"][0]["payload"]["loan_amount"] = 999999999.0

    result = verify_regulatory_package(tampered)
    assert not result["package_hash_valid"], "Tampered package should fail hash verification"
    assert not result["overall_valid"]


@pytest.mark.asyncio
async def test_regulatory_package_contains_raw_and_upcasted_events(db_pool):
    """Package must contain both raw DB events and upcasted events for comparison."""
    store = EventStore(db_pool, UpcasterRegistry())
    handler = CommandHandler(store)
    app_id, _ = await _build_full_application(handler, store)

    package = await generate_regulatory_package(db_pool, store, app_id)

    assert len(package["events"]) > 0, "Package must contain upcasted events"
    assert len(package["raw_events"]) > 0, "Package must contain raw events"
    assert package["event_count"] == len(package["events"])

    # Every raw event must have a corresponding upcasted event
    raw_ids = {e["event_id"] for e in package["raw_events"]}
    upcasted_ids = {e["event_id"] for e in package["events"]}
    assert raw_ids == upcasted_ids


@pytest.mark.asyncio
async def test_chain_break_detection(db_pool):
    """
    Manually corrupt a chain_hash in the audit ledger.
    run_integrity_check must detect the break and report the sequence number.
    """
    from src.integrity.audit_chain import run_integrity_check, record_audit_entry, get_last_chain_hash
    from src.aggregates.audit_ledger import GENESIS_HASH

    store = EventStore(db_pool, UpcasterRegistry())
    app_id = uuid4()

    # Build a real event to reference
    from src.models.events import LoanApplicationSubmitted
    event = LoanApplicationSubmitted(
        aggregate_id=app_id,
        applicant_name="Chain Break Test",
        loan_amount=50000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    )
    await store.append("LoanApplication", app_id, [event], expected_version=0)

    # Get the real event_id from DB
    async with db_pool.acquire() as conn:
        event_id = await conn.fetchval(
            "SELECT event_id FROM events WHERE aggregate_type = 'LoanApplication' AND aggregate_id = $1",
            app_id
        )

    # Record 3 audit entries
    prev_hash = GENESIS_HASH
    for i in range(3):
        prev_hash = await record_audit_entry(
            db_pool, app_id, event_id, "LoanApplicationSubmitted",
            {"loan_amount": 50000.0}, sequence_number=i + 1, prev_chain_hash=prev_hash
        )

    # Verify chain is valid
    result = await run_integrity_check(db_pool, store, app_id)
    assert result["chain_valid"] is True
    assert result["entries_checked"] == 3

    # Corrupt the second entry's chain_hash
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE audit_ledger_projection
            SET chain_hash = 'deadbeef' || chain_hash[9:]
            WHERE application_id = $1 AND sequence_number = 2
            """,
            app_id,
        )

    # Re-run integrity check — must detect break at sequence 2
    result = await run_integrity_check(db_pool, store, app_id)
    assert result["chain_valid"] is False
    assert result["broken_at_sequence"] == 2


@pytest.mark.asyncio
async def test_append_with_retry_resolves_occ(db_pool):
    """
    append_with_retry() must resolve OCC conflicts by reloading and retrying.
    """
    store = EventStore(db_pool, UpcasterRegistry())
    app_id = uuid4()

    from src.models.events import LoanApplicationSubmitted, CreditAnalysisRequested
    from src.aggregates.loan_application import LoanApplicationAggregate

    # Create stream at version 1
    submitted = LoanApplicationSubmitted(
        aggregate_id=app_id,
        applicant_name="Retry Test",
        loan_amount=40000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    )
    await store.append("LoanApplication", app_id, [submitted], expected_version=0)

    # Advance stream to version 2 externally (simulating concurrent write)
    extra = CreditAnalysisRequested(aggregate_id=app_id, requested_by="concurrent_agent")
    await store.append("LoanApplication", app_id, [extra], expected_version=1)

    call_count = 0

    async def command_fn():
        nonlocal call_count
        call_count += 1
        events = await store.load_stream("LoanApplication", app_id)
        agg = LoanApplicationAggregate.load(app_id, events)
        # On first call, expected_version will be stale (1 instead of 2)
        # On second call (after reload), it will be correct (2)
        return [CreditAnalysisRequested(aggregate_id=app_id, requested_by="retry_agent")], agg.version

    new_version = await store.append_with_retry("LoanApplication", app_id, command_fn)
    assert new_version == 3
    assert call_count == 1  # Should succeed on first try since we reload fresh
