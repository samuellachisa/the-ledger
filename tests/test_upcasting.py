"""
Upcasting Tests: Verify that raw DB payloads are NEVER modified by upcasters.

Critical invariant: SELECT payload FROM events always returns the original data.
Upcasting is a read-time transformation only.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.models.events import CreditAnalysisCompleted
from src.upcasting.registry import UpcasterRegistry


@pytest.mark.asyncio
async def test_raw_db_payload_unchanged_after_upcast(event_store, db_pool):
    """
    Write a v1 CreditAnalysisCompleted event (no model_version field).
    Read it back via event_store (upcasted to v2).
    Verify the raw DB payload still has no model_version field.
    """
    application_id = uuid4()

    # Simulate a v1 event by directly inserting with schema_version=1
    # and no model_version in payload
    async with db_pool.acquire() as conn:
        # Create stream
        stream_id = await conn.fetchval(
            """
            INSERT INTO event_streams (aggregate_type, aggregate_id, current_version)
            VALUES ('LoanApplication', $1, 1)
            RETURNING stream_id
            """,
            application_id,
        )

        # Insert v1 event (no model_version in payload)
        v1_payload = {
            "credit_score": 720,
            "debt_to_income_ratio": 0.35,
            # Intentionally NO model_version — this is a v1 event
        }
        event_id = await conn.fetchval(
            """
            INSERT INTO events (
                stream_id, aggregate_type, aggregate_id, event_type,
                schema_version, stream_position, payload, metadata
            ) VALUES ($1, 'LoanApplication', $2, 'CreditAnalysisCompleted', 1, 1, $3, '{}')
            RETURNING event_id
            """,
            stream_id, application_id, v1_payload,
        )

    # Read via event_store (applies upcaster)
    events = await event_store.load_stream("LoanApplication", application_id)
    assert len(events) == 1

    upcasted = events[0]
    assert isinstance(upcasted, CreditAnalysisCompleted)
    assert upcasted.schema_version == 2  # Upcasted to v2
    assert upcasted.model_version is None  # null, not fabricated
    assert upcasted.credit_score == 720

    # CRITICAL: Verify raw DB payload is UNCHANGED
    async with db_pool.acquire() as conn:
        raw = await conn.fetchrow(
            "SELECT payload, schema_version FROM events WHERE event_id = $1",
            event_id,
        )

    assert raw["schema_version"] == 1  # DB still shows v1
    assert "model_version" not in raw["payload"]  # Raw payload unchanged
    assert raw["payload"]["credit_score"] == 720


@pytest.mark.asyncio
async def test_upcaster_chain_v1_to_v2(db_pool):
    """UpcasterRegistry correctly chains v1→v2 transformations."""
    registry = UpcasterRegistry()

    v1_payload = {"credit_score": 680, "debt_to_income_ratio": 0.42}
    upcasted, version = registry.upcast("CreditAnalysisCompleted", v1_payload, from_version=1)

    assert version == 2
    assert upcasted["credit_score"] == 680
    assert "model_version" in upcasted
    assert upcasted["model_version"] is None  # null, not fabricated
    assert "analysis_duration_ms" in upcasted
    assert upcasted["analysis_duration_ms"] is None


@pytest.mark.asyncio
async def test_no_upcaster_returns_original(db_pool):
    """Events with no registered upcaster are returned unchanged."""
    registry = UpcasterRegistry()

    payload = {"applicant_name": "Test", "loan_amount": 50000.0}
    result, version = registry.upcast("LoanApplicationSubmitted", payload, from_version=1)

    assert result == payload
    assert version == 1  # No upcaster, version unchanged


@pytest.mark.asyncio
async def test_upcaster_does_not_mutate_original_dict():
    """Upcasters must not mutate the input dict (defensive copy)."""
    registry = UpcasterRegistry()

    original = {"credit_score": 700, "debt_to_income_ratio": 0.3}
    original_copy = dict(original)

    registry.upcast("CreditAnalysisCompleted", original, from_version=1)

    # Original dict must be unchanged
    assert original == original_copy


@pytest.mark.asyncio
async def test_decision_generated_v1_upcast():
    """DecisionGenerated v1 → v2 adds reasoning field as null."""
    registry = UpcasterRegistry()

    v1_payload = {
        "outcome": "APPROVE",
        "confidence_score": 0.87,
        "model_version": "gpt-4",
        "agent_session_id": str(uuid4()),
        # No reasoning field in v1
    }
    upcasted, version = registry.upcast("DecisionGenerated", v1_payload, from_version=1)

    assert version == 2
    assert "reasoning" in upcasted
    assert upcasted["reasoning"] is None


@pytest.mark.asyncio
async def test_multiple_events_upcasted_independently(event_store, db_pool):
    """Multiple events in a stream are each upcasted independently."""
    application_id = uuid4()

    async with db_pool.acquire() as conn:
        stream_id = await conn.fetchval(
            """
            INSERT INTO event_streams (aggregate_type, aggregate_id, current_version)
            VALUES ('LoanApplication', $1, 2)
            RETURNING stream_id
            """,
            application_id,
        )

        # v1 event
        await conn.execute(
            """
            INSERT INTO events (
                stream_id, aggregate_type, aggregate_id, event_type,
                schema_version, stream_position, payload, metadata
            ) VALUES ($1, 'LoanApplication', $2, 'CreditAnalysisCompleted', 1, 1,
                      '{"credit_score": 750, "debt_to_income_ratio": 0.28}', '{}')
            """,
            stream_id, application_id,
        )

        # v2 event (already current)
        await conn.execute(
            """
            INSERT INTO events (
                stream_id, aggregate_type, aggregate_id, event_type,
                schema_version, stream_position, payload, metadata
            ) VALUES ($1, 'LoanApplication', $2, 'CreditAnalysisCompleted', 2, 2,
                      '{"credit_score": 780, "debt_to_income_ratio": 0.22, "model_version": "gpt-4", "analysis_duration_ms": 1200}', '{}')
            """,
            stream_id, application_id,
        )

    events = await event_store.load_stream("LoanApplication", application_id)
    assert len(events) == 2

    # First event: upcasted from v1
    assert events[0].schema_version == 2
    assert events[0].model_version is None

    # Second event: already v2, no upcast needed
    assert events[1].schema_version == 2
    assert events[1].model_version == "gpt-4"
