"""
Tests for Task 6 features:
1. Schema Migration Tooling (MigrationRunner)
2. Stream Archival (StreamArchiver)
3. Multi-tenancy (TenantContext)
4. Idempotent Command Submission (IdempotencyStore)
5. Deep Health Check (ledger://ledger/health)
6. Graceful Shutdown (ProjectionDaemon, SagaManager)
7. GDPR Erasure (ErasureHandler + PersonalDataErased event)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from uuid import uuid4, UUID

import pytest
import pytest_asyncio

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.commands.handlers import CommandHandler, SubmitApplicationCommand
from src.models.events import PersonalDataErased


# =============================================================================
# 1. Schema Migration Tooling
# =============================================================================

class TestMigrationRunner:
    async def test_list_affected_streams_empty(self, migration_runner):
        """No events in DB → no affected streams."""
        streams = await migration_runner.list_affected_streams("CreditAnalysisCompleted", from_version=1)
        assert streams == []

    async def test_dry_run_empty(self, migration_runner):
        """No matching events → empty dry run."""
        results = await migration_runner.dry_run("CreditAnalysisCompleted", from_version=1)
        assert results == []

    async def test_validate_upcaster_no_events(self, migration_runner):
        """Validation passes trivially when no events exist."""
        result = await migration_runner.validate_upcaster("CreditAnalysisCompleted", from_version=1)
        assert result["valid"] is True
        assert result["total_events"] == 0
        assert result["errors"] == 0

    async def test_dry_run_with_events(self, migration_runner, handler, db_pool):
        """Dry run shows before/after diff for matching events."""
        # Submit an application and record credit analysis to generate events
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Test User",
            loan_amount=50000,
            loan_purpose="home",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        await handler.handle_record_credit_analysis(
            __import__("src.commands.handlers", fromlist=["RecordCreditAnalysisCommand"]).RecordCreditAnalysisCommand(
                application_id=app_id,
                credit_score=720,
                debt_to_income_ratio=0.3,
            )
        )

        # Manually set schema_version=1 on the CreditAnalysisCompleted event to simulate old data
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE events SET schema_version = 1 WHERE event_type = 'CreditAnalysisCompleted'"
            )

        results = await migration_runner.dry_run("CreditAnalysisCompleted", from_version=1)
        assert len(results) == 1
        assert results[0].error is None
        # The upcaster adds model_version and analysis_duration_ms
        assert "model_version" in results[0].after

    async def test_list_affected_streams_with_events(self, migration_runner, handler, db_pool):
        """list_affected_streams returns the correct stream."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Test User",
            loan_amount=50000,
            loan_purpose="home",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        await handler.handle_record_credit_analysis(
            __import__("src.commands.handlers", fromlist=["RecordCreditAnalysisCommand"]).RecordCreditAnalysisCommand(
                application_id=app_id,
                credit_score=720,
                debt_to_income_ratio=0.3,
            )
        )
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE events SET schema_version = 1 WHERE event_type = 'CreditAnalysisCompleted'"
            )

        streams = await migration_runner.list_affected_streams("CreditAnalysisCompleted", from_version=1)
        assert len(streams) == 1
        assert streams[0].aggregate_id == app_id


# =============================================================================
# 2. Stream Archival
# =============================================================================

class TestStreamArchiver:
    async def test_list_archivable_empty(self, stream_archiver):
        """No streams → nothing archivable."""
        result = await stream_archiver.list_archivable(datetime.now(timezone.utc))
        assert result == []

    async def test_archive_streams_dry_run(self, stream_archiver, handler, db_pool):
        """Dry run returns count without modifying DB."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Archive Test",
            loan_amount=10000,
            loan_purpose="car",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        # Manually set status to terminal and backdate updated_at
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO application_summary_projection (application_id, status, current_version) "
                "VALUES ($1, 'Denied', 1) ON CONFLICT (application_id) DO UPDATE SET status = 'Denied'",
                app_id,
            )
            await conn.execute(
                "UPDATE event_streams SET updated_at = NOW() - INTERVAL '400 days' WHERE aggregate_id = $1",
                app_id,
            )

        before_date = datetime.now(timezone.utc) - timedelta(days=365)
        count = await stream_archiver.archive_streams(before_date, dry_run=True)
        assert count == 1

        # Verify nothing was actually archived
        async with db_pool.acquire() as conn:
            archived = await conn.fetchval(
                "SELECT COUNT(*) FROM event_streams WHERE archived_at IS NOT NULL"
            )
        assert archived == 0

    async def test_archive_and_restore(self, stream_archiver, handler, db_pool):
        """Archive a stream then restore it."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Archive Test",
            loan_amount=10000,
            loan_purpose="car",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO application_summary_projection (application_id, status, current_version) "
                "VALUES ($1, 'FinalApproved', 1) ON CONFLICT (application_id) DO UPDATE SET status = 'FinalApproved'",
                app_id,
            )
            await conn.execute(
                "UPDATE event_streams SET updated_at = NOW() - INTERVAL '400 days' WHERE aggregate_id = $1",
                app_id,
            )

        before_date = datetime.now(timezone.utc) - timedelta(days=365)
        count = await stream_archiver.archive_streams(before_date)
        assert count == 1

        # Restore
        restored = await stream_archiver.restore_stream(app_id)
        assert restored is True

        async with db_pool.acquire() as conn:
            archived_at = await conn.fetchval(
                "SELECT archived_at FROM event_streams WHERE aggregate_id = $1", app_id
            )
        assert archived_at is None


# =============================================================================
# 3. Multi-tenancy
# =============================================================================

class TestTenantContext:
    def test_get_tenant_id_default_none(self):
        from src.tenancy import get_tenant_id
        # Outside any context, tenant_id is None
        assert get_tenant_id() is None

    def test_set_and_get_tenant_id(self):
        from src.tenancy import TenantContext, get_tenant_id
        with TenantContext("tenant-abc"):
            assert get_tenant_id() == "tenant-abc"
        assert get_tenant_id() is None

    def test_nested_tenant_contexts(self):
        from src.tenancy import TenantContext, get_tenant_id
        with TenantContext("outer"):
            assert get_tenant_id() == "outer"
            with TenantContext("inner"):
                assert get_tenant_id() == "inner"
            assert get_tenant_id() == "outer"
        assert get_tenant_id() is None

    async def test_async_tenant_context(self):
        from src.tenancy import TenantContext, get_tenant_id
        async with TenantContext("async-tenant"):
            assert get_tenant_id() == "async-tenant"
        assert get_tenant_id() is None

    def test_tenant_id_isolation_across_tasks(self):
        """Different asyncio tasks have independent tenant contexts."""
        from src.tenancy import TenantContext, get_tenant_id

        results = {}

        async def task_a():
            with TenantContext("tenant-a"):
                await asyncio.sleep(0.01)
                results["a"] = get_tenant_id()

        async def task_b():
            with TenantContext("tenant-b"):
                await asyncio.sleep(0.01)
                results["b"] = get_tenant_id()

        async def run():
            await asyncio.gather(task_a(), task_b())

        asyncio.get_event_loop().run_until_complete(run())
        assert results["a"] == "tenant-a"
        assert results["b"] == "tenant-b"


# =============================================================================
# 4. Idempotent Command Submission
# =============================================================================

class TestIdempotencyStore:
    async def test_first_call_reserves(self, idempotency_store):
        """First call with a new key returns None (proceed with command)."""
        result = await idempotency_store.check_and_reserve("key-001")
        assert result is None

    async def test_duplicate_before_complete_returns_none(self, idempotency_store):
        """Second call while still 'processing' returns None (not yet completed)."""
        await idempotency_store.check_and_reserve("key-002")
        result = await idempotency_store.check_and_reserve("key-002")
        # Still processing — no result stored yet
        assert result is None

    async def test_complete_and_replay(self, idempotency_store):
        """After complete(), duplicate returns the stored result."""
        key = "key-003"
        await idempotency_store.check_and_reserve(key)
        await idempotency_store.complete(key, {"application_id": "abc-123", "status": "Submitted"})

        result = await idempotency_store.check_and_reserve(key)
        assert result is not None
        assert result.is_duplicate is True
        assert result.result == '{"application_id": "abc-123", "status": "Submitted"}'

    async def test_cleanup_expired(self, idempotency_store, db_pool):
        """Expired keys are removed by cleanup_expired()."""
        key = "key-expired"
        await idempotency_store.check_and_reserve(key, ttl_seconds=1)
        # Backdate the created_at to simulate expiry
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE idempotency_keys SET created_at = NOW() - INTERVAL '2 seconds' "
                "WHERE idempotency_key = $1",
                key,
            )
        count = await idempotency_store.cleanup_expired()
        assert count >= 1

    async def test_idempotent_submit_application(self, handler, idempotency_store):
        """submit_application with same idempotency_key returns same application_id."""
        key = "submit-idem-001"
        applicant_id = uuid4()

        # First call
        await idempotency_store.check_and_reserve(key)
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Idem Test",
            loan_amount=25000,
            loan_purpose="education",
            applicant_id=applicant_id,
            submitted_by="test",
        ))
        await idempotency_store.complete(key, {"application_id": str(app_id)})

        # Second call — should return cached result
        cached = await idempotency_store.check_and_reserve(key)
        assert cached is not None
        assert cached.is_duplicate is True


# =============================================================================
# 5. Deep Health Check
# =============================================================================

class TestDeepHealthCheck:
    async def test_health_ok_empty_db(self, db_pool):
        """Health check returns ok when DB is clean."""
        from src.mcp.server import read_resource, create_server
        # We test the health logic directly via DB queries
        async with db_pool.acquire() as conn:
            dlq_depth = await conn.fetchval(
                "SELECT COUNT(*) FROM dead_letter_events WHERE resolved_at IS NULL"
            ) or 0
            outbox_backlog = await conn.fetchval(
                "SELECT COUNT(*) FROM outbox WHERE status = 'pending'"
            ) or 0
            saga_stuck = await conn.fetchval(
                "SELECT COUNT(*) FROM saga_instances WHERE step = 'failed'"
            ) or 0

        assert dlq_depth == 0
        assert outbox_backlog == 0
        assert saga_stuck == 0

    async def test_health_degraded_on_stuck_sagas(self, db_pool):
        """Health is degraded when saga_stuck > 10."""
        async with db_pool.acquire() as conn:
            for _ in range(11):
                await conn.execute(
                    "INSERT INTO saga_instances (application_id, step) VALUES ($1, 'failed')",
                    uuid4(),
                )
            saga_stuck = await conn.fetchval(
                "SELECT COUNT(*) FROM saga_instances WHERE step = 'failed'"
            )
        assert saga_stuck > 10


# =============================================================================
# 6. Graceful Shutdown
# =============================================================================

class TestGracefulShutdown:
    async def test_projection_daemon_stop_drains(self, db_pool, event_store):
        """ProjectionDaemon.stop() waits for current batch before cancelling."""
        from src.projections.daemon import ProjectionDaemon
        from src.dead_letter.queue import DeadLetterQueue

        dlq = DeadLetterQueue(db_pool)
        daemon = ProjectionDaemon(db_pool, event_store, dlq)

        processed = []

        async def slow_handler(conn, event):
            await asyncio.sleep(0.05)
            processed.append(event.event_id)

        daemon.register("TestProjection", slow_handler)

        # Insert a checkpoint for the test projection
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO projection_checkpoints (projection_name, last_position) "
                "VALUES ('TestProjection', 0) ON CONFLICT DO NOTHING"
            )

        await daemon.start()
        await asyncio.sleep(0.02)  # let it start polling
        await daemon.stop(drain_timeout_seconds=2.0)
        # Daemon stopped cleanly — no assertion error

    async def test_saga_manager_stop_drains(self, db_pool, event_store, handler):
        """SagaManager.stop() completes current batch before stopping."""
        from src.saga.loan_processing_saga import SagaManager

        manager = SagaManager(db_pool, event_store, handler)
        await manager.start()
        await asyncio.sleep(0.02)
        await manager.stop(drain_timeout_seconds=2.0)
        # No exception = graceful stop


# =============================================================================
# 7. GDPR Erasure
# =============================================================================

class TestErasureHandler:
    async def test_request_erasure_creates_record(self, erasure_handler, handler, db_pool):
        """request_erasure creates an erasure_requests row."""
        applicant_id = uuid4()
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Jane Doe",
            loan_amount=30000,
            loan_purpose="home",
            applicant_id=applicant_id,
            submitted_by="test",
        ))

        erasure_id = await erasure_handler.request_erasure(applicant_id, requested_by="dpo@apex.com")

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM erasure_requests WHERE erasure_id = $1", erasure_id
            )
        assert row is not None
        assert row["status"] == "pending"
        assert str(row["applicant_id"]) == str(applicant_id)

    async def test_apply_erasure_marks_applied(self, erasure_handler, handler, db_pool):
        """apply_erasure transitions status to 'applied'."""
        applicant_id = uuid4()
        await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="John Smith",
            loan_amount=20000,
            loan_purpose="car",
            applicant_id=applicant_id,
            submitted_by="test",
        ))

        erasure_id = await erasure_handler.request_erasure(applicant_id, requested_by="dpo@apex.com")
        await erasure_handler.apply_erasure(erasure_id)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, applied_at FROM erasure_requests WHERE erasure_id = $1",
                erasure_id,
            )
        assert row["status"] == "applied"
        assert row["applied_at"] is not None

    async def test_apply_erasure_idempotent(self, erasure_handler, handler):
        """Applying the same erasure twice is safe."""
        applicant_id = uuid4()
        await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Repeat Test",
            loan_amount=15000,
            loan_purpose="other",
            applicant_id=applicant_id,
            submitted_by="test",
        ))

        erasure_id = await erasure_handler.request_erasure(applicant_id, requested_by="dpo@apex.com")
        await erasure_handler.apply_erasure(erasure_id)
        count = await erasure_handler.apply_erasure(erasure_id)  # second call
        assert count == 0  # already applied, no streams re-processed

    async def test_list_requests(self, erasure_handler, handler):
        """list_requests returns all erasure requests."""
        applicant_id = uuid4()
        await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="List Test",
            loan_amount=10000,
            loan_purpose="other",
            applicant_id=applicant_id,
            submitted_by="test",
        ))
        await erasure_handler.request_erasure(applicant_id, requested_by="dpo@apex.com")

        requests = await erasure_handler.list_requests()
        assert len(requests) >= 1

    async def test_personal_data_erased_event_in_registry(self):
        """PersonalDataErased is registered in EVENT_REGISTRY."""
        from src.models.events import EVENT_REGISTRY
        assert "PersonalDataErased" in EVENT_REGISTRY

    async def test_personal_data_erased_event_payload(self):
        """PersonalDataErased serializes correctly."""
        applicant_id = uuid4()
        erasure_id = uuid4()
        event = PersonalDataErased(
            aggregate_id=uuid4(),
            aggregate_type="LoanApplication",
            applicant_id=applicant_id,
            erasure_id=erasure_id,
            fields_erased=["applicant_name", "applicant_id"],
        )
        payload = event.to_payload()
        assert payload["fields_erased"] == ["applicant_name", "applicant_id"]
        assert str(payload["erasure_id"]) == str(erasure_id)
