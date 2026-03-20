"""
Double-Decision Test: Concurrency stress test for Optimistic Concurrency Control.

Two async tasks attempt to append to the same stream at the same expected_version.
Exactly one must succeed; the other must raise OptimisticConcurrencyError.

This is the critical correctness test for the event store.
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

from src.models.events import (
    DecisionGenerated,
    DecisionOutcome,
    LoanApplicationSubmitted,
    OptimisticConcurrencyError,
)


@pytest.mark.asyncio
async def test_double_decision_exactly_one_succeeds(event_store):
    """
    Two concurrent tasks append a DecisionGenerated event to the same stream
    at expected_version=1. Exactly one must succeed; the other must fail with
    OptimisticConcurrencyError.
    """
    application_id = uuid4()
    applicant_id = uuid4()
    agent_session_id = uuid4()

    # Setup: append initial event to create stream at version 1
    submitted = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="Test Applicant",
        loan_amount=50000.0,
        loan_purpose="Home improvement",
        applicant_id=applicant_id,
        submitted_by="test_agent",
    )
    await event_store.append(
        aggregate_type="LoanApplication",
        aggregate_id=application_id,
        events=[submitted],
        expected_version=0,
    )

    # Verify stream is at version 1
    version = await event_store.stream_version("LoanApplication", application_id)
    assert version == 1

    # Both tasks will try to append at expected_version=1
    results = {"success": 0, "conflict": 0, "errors": []}

    async def attempt_decision(task_id: int):
        decision = DecisionGenerated(
            aggregate_id=application_id,
            outcome=DecisionOutcome.APPROVE,
            confidence_score=0.85,
            reasoning=f"Decision from task {task_id}",
            model_version="gpt-4-turbo",
            agent_session_id=agent_session_id,
        )
        try:
            await event_store.append(
                aggregate_type="LoanApplication",
                aggregate_id=application_id,
                events=[decision],
                expected_version=1,  # Both tasks expect version 1
            )
            results["success"] += 1
        except OptimisticConcurrencyError as e:
            results["conflict"] += 1
            assert e.expected_version == 1
            assert e.actual_version == 2  # The other task already incremented
            assert e.suggested_action == "reload_and_retry"
        except Exception as e:
            results["errors"].append(str(e))

    # Run both tasks concurrently
    await asyncio.gather(
        attempt_decision(1),
        attempt_decision(2),
    )

    assert results["errors"] == [], f"Unexpected errors: {results['errors']}"
    assert results["success"] == 1, f"Expected exactly 1 success, got {results['success']}"
    assert results["conflict"] == 1, f"Expected exactly 1 conflict, got {results['conflict']}"

    # Verify final stream version is 2 (only one event appended)
    final_version = await event_store.stream_version("LoanApplication", application_id)
    assert final_version == 2

    # Verify only one DecisionGenerated event exists
    events = await event_store.load_stream("LoanApplication", application_id)
    decision_events = [e for e in events if e.event_type == "DecisionGenerated"]
    assert len(decision_events) == 1


@pytest.mark.asyncio
async def test_high_concurrency_stress(event_store):
    """
    10 concurrent tasks attempt to append to the same stream.
    Exactly 1 must succeed; 9 must fail with OptimisticConcurrencyError.
    """
    application_id = uuid4()
    applicant_id = uuid4()

    submitted = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="Stress Test",
        loan_amount=100000.0,
        loan_purpose="Business",
        applicant_id=applicant_id,
        submitted_by="stress_test",
    )
    await event_store.append(
        aggregate_type="LoanApplication",
        aggregate_id=application_id,
        events=[submitted],
        expected_version=0,
    )

    success_count = 0
    conflict_count = 0

    async def attempt(i: int):
        nonlocal success_count, conflict_count
        from src.models.events import CreditAnalysisRequested
        event = CreditAnalysisRequested(
            aggregate_id=application_id,
            requested_by=f"agent_{i}",
        )
        try:
            await event_store.append(
                aggregate_type="LoanApplication",
                aggregate_id=application_id,
                events=[event],
                expected_version=1,
            )
            success_count += 1
        except OptimisticConcurrencyError:
            conflict_count += 1

    await asyncio.gather(*[attempt(i) for i in range(10)])

    assert success_count == 1, f"Expected 1 success, got {success_count}"
    assert conflict_count == 9, f"Expected 9 conflicts, got {conflict_count}"


@pytest.mark.asyncio
async def test_sequential_appends_succeed(event_store):
    """Sequential appends with correct expected_version always succeed."""
    application_id = uuid4()
    applicant_id = uuid4()

    for i in range(5):
        if i == 0:
            event = LoanApplicationSubmitted(
                aggregate_id=application_id,
                applicant_name="Sequential Test",
                loan_amount=25000.0,
                loan_purpose="Education",
                applicant_id=applicant_id,
                submitted_by="test",
            )
        else:
            from src.models.events import CreditAnalysisRequested
            event = CreditAnalysisRequested(
                aggregate_id=application_id,
                requested_by=f"agent_{i}",
            )

        new_version = await event_store.append(
            aggregate_type="LoanApplication",
            aggregate_id=application_id,
            events=[event],
            expected_version=i,
        )
        assert new_version == i + 1

    final = await event_store.stream_version("LoanApplication", application_id)
    assert final == 5


@pytest.mark.asyncio
async def test_occ_error_contains_structured_data(event_store):
    """OptimisticConcurrencyError must contain structured data for MCP response."""
    application_id = uuid4()

    submitted = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="OCC Test",
        loan_amount=10000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    )
    await event_store.append("LoanApplication", application_id, [submitted], expected_version=0)

    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await event_store.append(
            "LoanApplication", application_id, [submitted], expected_version=0
        )

    err = exc_info.value
    assert err.expected_version == 0
    assert err.actual_version == 1
    assert err.suggested_action == "reload_and_retry"

    err_dict = err.to_dict()
    assert err_dict["error"] == "OptimisticConcurrencyError"
    assert err_dict["suggested_action"] == "reload_and_retry"


@pytest.mark.asyncio
async def test_outbox_written_in_same_transaction(event_store, db_pool):
    """Events and outbox entries must be written atomically."""
    application_id = uuid4()

    submitted = LoanApplicationSubmitted(
        aggregate_id=application_id,
        applicant_name="Outbox Test",
        loan_amount=30000.0,
        loan_purpose="Test",
        applicant_id=uuid4(),
        submitted_by="test",
    )
    await event_store.append("LoanApplication", application_id, [submitted], expected_version=0)

    async with db_pool.acquire() as conn:
        outbox_count = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE event_type = 'LoanApplicationSubmitted'"
        )
    assert outbox_count == 1
