"""
Tests for the 5 new features:
  1. Snapshot support
  2. Dead letter queue
  3. Agent authentication & authorization
  4. Outbox relay
  5. Withdrawal cleanup saga
  6. Rate limiting
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.commands.handlers import (
    CommandHandler,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    RequestComplianceCheckCommand,
    SubmitApplicationCommand,
)
from src.models.events import LoanStatus, ComplianceStatus
from src.saga.loan_processing_saga import SagaStep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _full_app(handler: CommandHandler):
    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test User", loan_amount=25_000,
        loan_purpose="car", applicant_id=uuid4(), submitted_by="test",
    ))
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=700,
        debt_to_income_ratio=0.35, model_version="v1",
    ))
    return app_id


async def _load_app(event_store, app_id):
    events = await event_store.load_stream(LoanApplicationAggregate.aggregate_type, app_id)
    return LoanApplicationAggregate.load(app_id, events)


# ===========================================================================
# 1. Snapshot support
# ===========================================================================

@pytest.mark.asyncio
async def test_snapshot_saved_at_threshold(db_pool, event_store, handler, snapshot_store):
    """A snapshot is written after SNAPSHOT_THRESHOLD events."""
    from src.snapshots.store import SNAPSHOT_THRESHOLD

    app_id = await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Snap Test", loan_amount=10_000,
        loan_purpose="test", applicant_id=uuid4(), submitted_by="test",
    ))

    # Manually trigger snapshot at version 1 (below threshold — should not save)
    should = await snapshot_store.should_snapshot(
        LoanApplicationAggregate.aggregate_type, app_id, 1
    )
    assert not should

    # Simulate reaching threshold
    should = await snapshot_store.should_snapshot(
        LoanApplicationAggregate.aggregate_type, app_id, SNAPSHOT_THRESHOLD
    )
    assert should

    # Save a snapshot
    app = await _load_app(event_store, app_id)
    await snapshot_store.save(
        LoanApplicationAggregate.aggregate_type, app_id,
        SNAPSHOT_THRESHOLD, app.to_snapshot(),
    )

    # Load it back
    snap = await snapshot_store.load(LoanApplicationAggregate.aggregate_type, app_id)
    assert snap is not None
    assert snap["stream_position"] == SNAPSHOT_THRESHOLD
    assert snap["data"]["status"] == LoanStatus.SUBMITTED.value


@pytest.mark.asyncio
async def test_snapshot_not_duplicated(db_pool, snapshot_store):
    """should_snapshot returns False if snapshot already exists at that version."""
    from src.snapshots.store import SNAPSHOT_THRESHOLD
    agg_id = uuid4()

    await snapshot_store.save("LoanApplication", agg_id, SNAPSHOT_THRESHOLD, {"status": "Submitted"})
    should = await snapshot_store.should_snapshot("LoanApplication", agg_id, SNAPSHOT_THRESHOLD)
    assert not should


@pytest.mark.asyncio
async def test_snapshot_roundtrip(db_pool, event_store, handler, snapshot_store):
    """from_snapshot restores aggregate state correctly."""
    app_id = await _full_app(handler)
    app = await _load_app(event_store, app_id)

    state = app.to_snapshot()
    restored = LoanApplicationAggregate.from_snapshot(app_id, state, app.version)

    assert restored.status == app.status
    assert restored.credit_score == app.credit_score
    assert restored.loan_amount == app.loan_amount


@pytest.mark.asyncio
async def test_load_stream_uses_snapshot(db_pool, event_store, handler, snapshot_store):
    """load_stream skips events before the snapshot position."""
    app_id = await _full_app(handler)
    app = await _load_app(event_store, app_id)

    # Save snapshot at current version
    await snapshot_store.save(
        LoanApplicationAggregate.aggregate_type, app_id,
        app.version, app.to_snapshot(),
    )

    # load_stream should return 0 events (all covered by snapshot)
    events = await event_store.load_stream(
        LoanApplicationAggregate.aggregate_type, app_id
    )
    assert len(events) == 0  # snapshot covers everything


# ===========================================================================
# 2. Dead letter queue
# ===========================================================================

@pytest.mark.asyncio
async def test_dead_letter_write_and_list(db_pool, dead_letter, event_store, handler):
    """Failed events are written to dead letter and listable."""
    app_id = await _full_app(handler)
    events = await event_store.load_all(after_position=0)
    assert events

    fake_error = ValueError("projection exploded")
    await dead_letter.write(
        event=events[0],
        source="projection",
        processor_name="ApplicationSummary",
        retry_count=3,
        error=fake_error,
    )

    unresolved = await dead_letter.list_unresolved()
    assert len(unresolved) == 1
    assert unresolved[0]["error_type"] == "ValueError"
    assert unresolved[0]["processor_name"] == "ApplicationSummary"
    assert unresolved[0]["resolved_at"] is None


@pytest.mark.asyncio
async def test_dead_letter_resolve(db_pool, dead_letter, event_store, handler):
    """Resolving a dead letter entry marks it resolved."""
    events = await event_store.load_all(after_position=0)
    app_id = await _full_app(handler)
    events = await event_store.load_all(after_position=0)

    await dead_letter.write(
        event=events[0], source="saga",
        processor_name="LoanProcessingSaga", retry_count=5,
        error=RuntimeError("saga stuck"),
    )

    unresolved = await dead_letter.list_unresolved()
    dl_id = unresolved[0]["dead_letter_id"]

    resolved = await dead_letter.resolve(dl_id, resolved_by="operator-1")
    assert resolved is True

    unresolved_after = await dead_letter.list_unresolved()
    assert len(unresolved_after) == 0


@pytest.mark.asyncio
async def test_dead_letter_count(db_pool, dead_letter, event_store, handler):
    """count_unresolved groups by processor_name."""
    await _full_app(handler)
    events = await event_store.load_all(after_position=0)

    for i in range(3):
        await dead_letter.write(
            event=events[0], source="projection",
            processor_name="ApplicationSummary", retry_count=3,
            error=ValueError(f"error {i}"),
        )

    counts = await dead_letter.count_unresolved()
    assert counts.get("ApplicationSummary", 0) >= 1  # at least 1 (ON CONFLICT DO NOTHING dedupes by event_id)


# ===========================================================================
# 3. Agent authentication & authorization
# ===========================================================================

@pytest.mark.asyncio
async def test_token_issue_and_verify(db_pool, token_store):
    """Issued token verifies correctly and returns correct identity."""
    from src.auth.tokens import Role
    raw = await token_store.issue_token("agent-001", [Role.DECISION_AGENT])
    identity = await token_store.verify(raw)

    assert identity.agent_id == "agent-001"
    assert Role.DECISION_AGENT in identity.roles


@pytest.mark.asyncio
async def test_token_invalid(db_pool, token_store):
    """Invalid token raises AuthError."""
    from src.auth.tokens import AuthError
    with pytest.raises(AuthError, match="Invalid token"):
        await token_store.verify("not-a-real-token")


@pytest.mark.asyncio
async def test_token_revocation(db_pool, token_store):
    """Revoked token raises AuthError."""
    from src.auth.tokens import Role, AuthError
    raw = await token_store.issue_token("agent-002", [Role.SUBMITTER])
    identity = await token_store.verify(raw)

    await token_store.revoke(identity.token_id, revoked_by="admin")

    with pytest.raises(AuthError, match="revoked"):
        await token_store.verify(raw)


@pytest.mark.asyncio
async def test_role_authorization(db_pool, token_store):
    """Agent without required role is rejected."""
    from src.auth.tokens import Role, AuthError
    raw = await token_store.issue_token("read-only-agent", [Role.READ_ONLY])
    identity = await token_store.verify(raw)

    with pytest.raises(AuthError, match="lacks role"):
        token_store.require(identity, "generate_decision")


@pytest.mark.asyncio
async def test_admin_role_has_all_permissions(db_pool, token_store):
    """Admin role passes all tool authorization checks."""
    from src.auth.tokens import Role, TOOL_ROLES
    raw = await token_store.issue_token("admin-agent", [Role.ADMIN])
    identity = await token_store.verify(raw)

    for tool_name in TOOL_ROLES:
        token_store.require(identity, tool_name)  # should not raise


# ===========================================================================
# 4. Outbox relay
# ===========================================================================

@pytest.mark.asyncio
async def test_outbox_relay_publishes_events(db_pool, event_store, handler):
    """OutboxRelay publishes pending outbox entries via the publisher."""
    from src.outbox.relay import OutboxRelay, InMemoryPublisher

    await _full_app(handler)

    publisher = InMemoryPublisher()
    relay = OutboxRelay(db_pool, publisher)
    processed = await relay._process_batch()

    assert processed > 0
    assert len(publisher.published) > 0
    assert any(e["event_type"] == "LoanApplicationSubmitted" for e in publisher.published)


@pytest.mark.asyncio
async def test_outbox_relay_marks_published(db_pool, event_store, handler):
    """After relay runs, outbox entries are marked 'published'."""
    from src.outbox.relay import OutboxRelay, InMemoryPublisher

    await _full_app(handler)

    relay = OutboxRelay(db_pool, InMemoryPublisher())
    await relay._process_batch()

    async with db_pool.acquire() as conn:
        pending = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE status = 'pending'"
        )
    assert pending == 0


@pytest.mark.asyncio
async def test_outbox_relay_handles_publisher_failure(db_pool, event_store, handler):
    """Failed publish increments retry_count; marks failed after max_retries."""
    from src.outbox.relay import OutboxRelay, MessagePublisher

    class FailingPublisher(MessagePublisher):
        async def publish(self, event_type, aggregate_type, payload, metadata):
            raise RuntimeError("broker down")

    await _full_app(handler)

    # Set max_retries = 1 so it fails immediately
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE outbox SET max_retries = 1")

    relay = OutboxRelay(db_pool, FailingPublisher())
    await relay._process_batch()

    async with db_pool.acquire() as conn:
        failed = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE status = 'failed'"
        )
    assert failed > 0
    assert relay.stats["failed_count"] > 0


# ===========================================================================
# 5. Withdrawal cleanup saga
# ===========================================================================

@pytest.mark.asyncio
async def test_withdrawal_cancels_compliance_record(db_pool, event_store, handler, saga_manager):
    """ApplicationWithdrawn while in ComplianceCheck cancels the compliance record."""
    app_id = await _full_app(handler)

    await handler.handle_record_fraud_check(RecordFraudCheckCommand(
        application_id=app_id, fraud_risk_score=0.05, flags=[], passed=True,
    ))

    # Saga requests compliance
    await saga_manager._process_batch()

    app = await _load_app(event_store, app_id)
    assert app.status == LoanStatus.COMPLIANCE_CHECK
    compliance_id = app.compliance_record_id

    # Withdraw the application
    app_events = await event_store.load_stream(LoanApplicationAggregate.aggregate_type, app_id)
    app_agg = LoanApplicationAggregate.load(app_id, app_events)
    app_agg.withdraw(withdrawn_by="applicant", reason="changed mind")
    await event_store.append(
        aggregate_type=LoanApplicationAggregate.aggregate_type,
        aggregate_id=app_id,
        events=app_agg.pending_events,
        expected_version=app_agg.version - len(app_agg.pending_events),
    )

    # Saga processes ApplicationWithdrawn
    await saga_manager._process_batch()

    # Compliance record should be finalized (cancelled)
    comp_events = await event_store.load_stream(
        ComplianceRecordAggregate.aggregate_type, compliance_id
    )
    comp = ComplianceRecordAggregate.load(compliance_id, comp_events)
    assert comp.status != ComplianceStatus.IN_PROGRESS  # finalized

    # Saga should be COMPLETED
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT step FROM saga_instances WHERE application_id = $1", app_id
        )
    assert row["step"] == SagaStep.COMPLETED.value


# ===========================================================================
# 6. Rate limiting
# ===========================================================================

@pytest.mark.asyncio
async def test_rate_limit_allows_within_limit(db_pool, rate_limiter):
    """Calls within the rate limit succeed."""
    agent_id = f"agent-{uuid4()}"
    # generate_decision has capacity 5
    for _ in range(5):
        await rate_limiter.consume(agent_id=agent_id, action="generate_decision")


@pytest.mark.asyncio
async def test_rate_limit_blocks_over_limit(db_pool, rate_limiter):
    """Calls exceeding the rate limit raise RateLimitExceededError."""
    from src.ratelimit import RateLimitExceededError
    agent_id = f"agent-{uuid4()}"

    # Exhaust the bucket (capacity=5)
    for _ in range(5):
        await rate_limiter.consume(agent_id=agent_id, action="generate_decision")

    with pytest.raises(RateLimitExceededError) as exc_info:
        await rate_limiter.consume(agent_id=agent_id, action="generate_decision")

    assert exc_info.value.retry_after_seconds > 0


@pytest.mark.asyncio
async def test_rate_limit_reset(db_pool, rate_limiter):
    """After reset, bucket is full again."""
    from src.ratelimit import RateLimitExceededError
    agent_id = f"agent-{uuid4()}"

    for _ in range(5):
        await rate_limiter.consume(agent_id=agent_id, action="generate_decision")

    await rate_limiter.reset(agent_id=agent_id, action="generate_decision")

    # Should succeed again
    await rate_limiter.consume(agent_id=agent_id, action="generate_decision")


@pytest.mark.asyncio
async def test_rate_limit_independent_per_agent(db_pool, rate_limiter):
    """Rate limits are per agent — one agent exhausting doesn't affect another."""
    from src.ratelimit import RateLimitExceededError
    agent_a = f"agent-a-{uuid4()}"
    agent_b = f"agent-b-{uuid4()}"

    for _ in range(5):
        await rate_limiter.consume(agent_id=agent_a, action="generate_decision")

    # agent_b should still have full bucket
    await rate_limiter.consume(agent_id=agent_b, action="generate_decision")
