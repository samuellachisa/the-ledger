"""
Tests for the 7 advanced features:
  1. Observability / metrics
  2. Event replay (dead-letter + range)
  3. Correlation / causation chain queries
  4. Application filtering
  5. Token refresh / rotation
  6. Saga compensation (already tested in test_saga.py — withdrawal cleanup)
  7. Prometheus exporter
"""
from __future__ import annotations

import asyncio
from uuid import uuid4, UUID

import pytest

from src.aggregates.loan_application import LoanApplicationAggregate
from src.commands.handlers import (
    CommandHandler,
    RecordCreditAnalysisCommand,
    RecordFraudCheckCommand,
    SubmitApplicationCommand,
)
from src.models.events import LoanStatus
from src.observability.metrics import MetricsCollector, get_metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _submit(handler: CommandHandler, correlation_id=None):
    return await handler.handle_submit_application(SubmitApplicationCommand(
        applicant_name="Test", loan_amount=10_000,
        loan_purpose="test", applicant_id=uuid4(), submitted_by="test",
        correlation_id=correlation_id,
    ))


async def _submit_and_credit(handler: CommandHandler, correlation_id=None):
    app_id = await _submit(handler, correlation_id=correlation_id)
    await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
        application_id=app_id, credit_score=700,
        debt_to_income_ratio=0.3, model_version="v1",
    ))
    return app_id


# ===========================================================================
# 1. Observability / metrics
# ===========================================================================

def test_metrics_counter():
    m = MetricsCollector()
    m.increment("events_appended_total", labels={"aggregate_type": "LoanApplication"})
    m.increment("events_appended_total", labels={"aggregate_type": "LoanApplication"})
    snap = m.snapshot()
    assert snap["counters"]["events_appended_total{aggregate_type=LoanApplication}"] == 2


def test_metrics_gauge():
    m = MetricsCollector()
    m.gauge("projection_lag_ms", 42.5, labels={"projection_name": "ApplicationSummary"})
    snap = m.snapshot()
    assert snap["gauges"]["projection_lag_ms{projection_name=ApplicationSummary}"] == 42.5


def test_metrics_histogram():
    m = MetricsCollector()
    for v in [10.0, 50.0, 200.0]:
        m.histogram("tool_latency_ms", v)
    snap = m.snapshot()
    assert len(snap["histograms"]["tool_latency_ms"]) == 3


def test_metrics_reset():
    m = MetricsCollector()
    m.increment("foo")
    m.reset()
    assert m.snapshot() == {"counters": {}, "gauges": {}, "histograms": {}}


@pytest.mark.asyncio
async def test_events_appended_metric_incremented(db_pool, event_store, handler):
    """events_appended_total increments when events are appended."""
    get_metrics().reset()
    await _submit(handler)
    snap = get_metrics().snapshot()
    total = sum(
        v for k, v in snap["counters"].items()
        if "events_appended_total" in k
    )
    assert total >= 1


@pytest.mark.asyncio
async def test_occ_conflict_metric(db_pool, event_store):
    """occ_conflicts_total increments on OCC conflict."""
    from src.models.events import OptimisticConcurrencyError
    get_metrics().reset()

    app_id = uuid4()
    # Force an OCC by appending with wrong expected_version
    from src.models.events import LoanApplicationSubmitted
    event = LoanApplicationSubmitted(
        aggregate_id=app_id, applicant_name="X", loan_amount=1000,
        loan_purpose="test", applicant_id=uuid4(), submitted_by="test",
    )
    await event_store.append("LoanApplication", app_id, [event], expected_version=0)

    with pytest.raises(OptimisticConcurrencyError):
        await event_store.append("LoanApplication", app_id, [event], expected_version=0)

    snap = get_metrics().snapshot()
    occ_total = sum(v for k, v in snap["counters"].items() if "occ_conflicts_total" in k)
    assert occ_total >= 1


# ===========================================================================
# 2. Prometheus exporter
# ===========================================================================

def test_prometheus_exporter_counters():
    from src.observability.exporters import PrometheusExporter
    m = MetricsCollector()
    m.increment("events_appended_total", labels={"aggregate_type": "LoanApplication"})
    output = PrometheusExporter(m).export()
    assert "# TYPE events_appended_total counter" in output
    assert "events_appended_total{aggregate_type=LoanApplication} 1" in output


def test_prometheus_exporter_histogram():
    from src.observability.exporters import PrometheusExporter
    m = MetricsCollector()
    for v in [10.0, 100.0, 600.0]:
        m.histogram("tool_latency_ms", v)
    output = PrometheusExporter(m).export()
    assert "# TYPE tool_latency_ms histogram" in output
    assert "_count" in output
    assert "_sum" in output
    assert '_bucket{le="500"} 2' in output  # 10 and 100 are <= 500


# ===========================================================================
# 3. Event replay
# ===========================================================================

@pytest.mark.asyncio
async def test_replay_dead_letter(db_pool, event_store, handler, dead_letter):
    """replay_dead_letter calls handler and resolves the entry."""
    from src.replay import ReplayEngine

    await _submit(handler)
    events = await event_store.load_all(after_position=0)
    assert events

    # Write to dead letter
    await dead_letter.write(
        event=events[0], source="projection",
        processor_name="ApplicationSummary", retry_count=3,
        error=ValueError("boom"),
    )
    unresolved = await dead_letter.list_unresolved()
    dl_id = unresolved[0]["dead_letter_id"]

    # Replay it
    replayed_events = []
    async def handler_fn(conn, event):
        replayed_events.append(event.event_type)

    engine = ReplayEngine(db_pool, event_store, dead_letter)
    result = await engine.replay_dead_letter(dl_id, "ApplicationSummary", handler_fn)

    assert result["status"] == "replayed"
    assert len(replayed_events) == 1

    # Should be resolved now
    unresolved_after = await dead_letter.list_unresolved()
    assert len(unresolved_after) == 0


@pytest.mark.asyncio
async def test_replay_range(db_pool, event_store, handler):
    """replay_range calls handler for each event in the position range."""
    from src.replay import ReplayEngine
    from src.dead_letter import DeadLetterQueue

    await _submit_and_credit(handler)
    all_events = await event_store.load_all(after_position=0)
    assert len(all_events) >= 3

    min_pos = all_events[0].global_position
    max_pos = all_events[-1].global_position

    replayed = []
    async def handler_fn(conn, event):
        replayed.append(event.event_type)

    dlq = DeadLetterQueue(db_pool)
    engine = ReplayEngine(db_pool, event_store, dlq)
    result = await engine.replay_range(min_pos - 1, max_pos + 1, "test", handler_fn)

    assert result["replayed"] == len(all_events)
    assert result["failed"] == 0


@pytest.mark.asyncio
async def test_replay_range_partial_failure(db_pool, event_store, handler):
    """replay_range collects failures without aborting the run."""
    from src.replay import ReplayEngine
    from src.dead_letter import DeadLetterQueue

    await _submit_and_credit(handler)
    all_events = await event_store.load_all(after_position=0)
    min_pos = all_events[0].global_position
    max_pos = all_events[-1].global_position

    call_count = [0]
    async def failing_handler(conn, event):
        call_count[0] += 1
        if call_count[0] == 2:
            raise RuntimeError("second event fails")

    dlq = DeadLetterQueue(db_pool)
    engine = ReplayEngine(db_pool, event_store, dlq)
    result = await engine.replay_range(min_pos - 1, max_pos + 1, "test", failing_handler)

    assert result["failed"] == 1
    assert result["replayed"] == len(all_events) - 1
    assert len(result["errors"]) == 1


# ===========================================================================
# 4. Correlation / causation chain queries
# ===========================================================================

@pytest.mark.asyncio
async def test_correlation_chain_query(db_pool, event_store, handler):
    """load_correlation_chain returns all events with the same correlation_id."""
    corr_id = uuid4()
    app_id = await _submit(handler, correlation_id=corr_id)

    chain = await event_store.load_correlation_chain(corr_id)
    assert len(chain) >= 1
    assert all(e.correlation_id == corr_id for e in chain)


@pytest.mark.asyncio
async def test_causation_chain_query(db_pool, event_store, handler):
    """load_causation_chain returns events caused by a root event."""
    await _submit(handler)
    all_events = await event_store.load_all(after_position=0)
    root = all_events[0]

    # Even with no explicit causation links, the root event itself is returned
    chain = await event_store.load_causation_chain(root.event_id)
    assert len(chain) >= 1
    assert chain[0].event_id == root.event_id


# ===========================================================================
# 5. Application filtering
# ===========================================================================

@pytest.mark.asyncio
async def test_list_applications_status_filter(db_pool, event_store, handler):
    """list_applications filters by status correctly."""
    from src.projections.application_summary import list_applications, handle_application_summary
    from src.projections.daemon import ProjectionDaemon

    # Submit two applications
    await _submit(handler)
    await _submit(handler)

    # Run projection
    daemon = ProjectionDaemon(db_pool, event_store)
    daemon.register("ApplicationSummary", handle_application_summary)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    async with db_pool.acquire() as conn:
        submitted = await list_applications(conn, status="Submitted")
        all_apps = await list_applications(conn)

    assert len(submitted) == 2
    assert len(all_apps) >= 2
    assert all(a["status"] == "Submitted" for a in submitted)


@pytest.mark.asyncio
async def test_list_applications_pagination(db_pool, event_store, handler):
    """list_applications respects limit and offset."""
    from src.projections.application_summary import list_applications, handle_application_summary
    from src.projections.daemon import ProjectionDaemon

    for _ in range(5):
        await _submit(handler)

    daemon = ProjectionDaemon(db_pool, event_store)
    daemon.register("ApplicationSummary", handle_application_summary)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    async with db_pool.acquire() as conn:
        page1 = await list_applications(conn, limit=2, offset=0)
        page2 = await list_applications(conn, limit=2, offset=2)

    assert len(page1) == 2
    assert len(page2) == 2
    # Pages should not overlap
    ids1 = {r["application_id"] for r in page1}
    ids2 = {r["application_id"] for r in page2}
    assert ids1.isdisjoint(ids2)


# ===========================================================================
# 6. Token refresh / rotation
# ===========================================================================

@pytest.mark.asyncio
async def test_token_refresh_issues_new_token(db_pool, token_store):
    """refresh() returns a new valid token and revokes the old one."""
    from src.auth.tokens import Role, AuthError

    raw = await token_store.issue_token("agent-refresh", [Role.DECISION_AGENT])
    new_raw = await token_store.refresh(raw)

    # New token is valid
    identity = await token_store.verify(new_raw)
    assert identity.agent_id == "agent-refresh"

    # Old token is revoked
    with pytest.raises(AuthError, match="revoked"):
        await token_store.verify(raw)


@pytest.mark.asyncio
async def test_token_refresh_expired_token_rejected(db_pool, token_store):
    """refresh() rejects an already-expired token."""
    from src.auth.tokens import Role, AuthError
    from datetime import datetime, timezone, timedelta

    # Issue a token that expires immediately
    raw = await token_store.issue_token("agent-exp", [Role.READ_ONLY], ttl_hours=0)

    # Manually expire it in DB
    token_hash = token_store._hash(raw)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_tokens SET expires_at = NOW() - INTERVAL '1 second' WHERE token_hash = $1",
            token_hash,
        )

    with pytest.raises(AuthError, match="expired"):
        await token_store.refresh(raw)


@pytest.mark.asyncio
async def test_token_refresh_preserves_roles(db_pool, token_store):
    """Refreshed token inherits the same roles as the original."""
    from src.auth.tokens import Role

    raw = await token_store.issue_token("agent-roles", [Role.COMPLIANCE_OFFICER, Role.READ_ONLY])
    new_raw = await token_store.refresh(raw)
    identity = await token_store.verify(new_raw)

    assert Role.COMPLIANCE_OFFICER in identity.roles
    assert Role.READ_ONLY in identity.roles
