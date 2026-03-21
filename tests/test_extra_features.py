"""
Tests for the 7 additional features:
1. Circuit Breaker (outbox relay protection)
2. Event Table Partitioning (PartitionManager)
3. Leader Election (pg_advisory_lock)
4. Aggregate Test Harness (Given/When/Then DSL)
5. Schema Compatibility Checker
6. Field-Level Encryption (AES-256-GCM)
7. Integrity Monitor (scheduled hash-chain alerting)
"""
from __future__ import annotations

import asyncio
import base64
import os
from uuid import uuid4, UUID

import pytest
import pytest_asyncio

# All tests in this module require a live database connection.
pytestmark = pytest.mark.usefixtures("require_db")

from src.commands.handlers import SubmitApplicationCommand
from src.models.events import (
    BusinessRuleViolationError,
    CreditAnalysisRequested,
    InvalidStateTransitionError,
    LoanApplicationSubmitted,
    LoanStatus,
)


# =============================================================================
# 1. Circuit Breaker
# =============================================================================

class TestCircuitBreaker:
    async def test_closed_state_passes_calls(self, circuit_breaker):
        from src.circuit_breaker import CircuitState
        assert circuit_breaker.state == CircuitState.CLOSED

        async def ok():
            return 42

        result = await circuit_breaker.call(ok())
        assert result == 42

    async def test_opens_after_threshold_failures(self, circuit_breaker):
        from src.circuit_breaker import CircuitState, CircuitOpenError

        async def fail():
            raise RuntimeError("downstream down")

        for _ in range(3):  # failure_threshold=3
            with pytest.raises(RuntimeError):
                await circuit_breaker.call(fail())

        assert circuit_breaker.state == CircuitState.OPEN

        with pytest.raises(CircuitOpenError):
            await circuit_breaker.call(fail())

    async def test_half_open_after_recovery_timeout(self, circuit_breaker):
        from src.circuit_breaker import CircuitState

        async def fail():
            raise RuntimeError("down")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await circuit_breaker.call(fail())

        assert circuit_breaker.state == CircuitState.OPEN

        # Wait for recovery_timeout (1s in fixture)
        await asyncio.sleep(1.1)

        # Next call should be allowed (HALF_OPEN probe)
        async def ok():
            return "recovered"

        result = await circuit_breaker.call(ok())
        assert result == "recovered"
        assert circuit_breaker.state == CircuitState.CLOSED

    async def test_manual_reset(self, circuit_breaker):
        from src.circuit_breaker import CircuitState

        async def fail():
            raise RuntimeError("down")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await circuit_breaker.call(fail())

        circuit_breaker.reset()
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    async def test_context_manager(self, circuit_breaker):
        from src.circuit_breaker import CircuitState
        async with circuit_breaker:
            pass  # success
        assert circuit_breaker.state == CircuitState.CLOSED

    async def test_outbox_relay_uses_circuit_breaker(self, db_pool):
        """OutboxRelay stats include circuit state."""
        from src.outbox.relay import OutboxRelay, InMemoryPublisher
        publisher = InMemoryPublisher()
        relay = OutboxRelay(db_pool, publisher)
        stats = relay.stats
        assert "circuit_state" in stats
        assert stats["circuit_state"] == "closed"


# =============================================================================
# 2. Event Table Partitioning
# =============================================================================

class TestPartitionManager:
    async def test_list_partitions_no_error(self, db_pool):
        """list_partitions() runs without error (events table may not be partitioned)."""
        from src.partitioning import PartitionManager
        manager = PartitionManager(db_pool)
        # This may return empty list if events is not partitioned — that's fine
        partitions = await manager.list_partitions()
        assert isinstance(partitions, list)

    async def test_get_partition_stats(self, db_pool):
        from src.partitioning import PartitionManager
        manager = PartitionManager(db_pool)
        stats = await manager.get_partition_stats()
        assert "partition_count" in stats
        assert "total_rows" in stats
        assert isinstance(stats["partitions"], list)

    async def test_drop_old_partitions_dry_run(self, db_pool):
        """Dry run returns list without modifying anything."""
        from src.partitioning import PartitionManager
        from datetime import date
        manager = PartitionManager(db_pool)
        to_drop = await manager.drop_old_partitions(
            before_date=date(2020, 1, 1), dry_run=True
        )
        assert isinstance(to_drop, list)

    def test_partition_name_format(self, db_pool):
        from src.partitioning import PartitionManager
        from datetime import date
        manager = PartitionManager(db_pool)
        name = manager._partition_name(date(2025, 3, 1))
        assert name == "events_2025_03"

    def test_next_month_december(self, db_pool):
        from src.partitioning import PartitionManager
        from datetime import date
        manager = PartitionManager(db_pool)
        result = manager._next_month(date(2025, 12, 1))
        assert result == date(2026, 1, 1)


# =============================================================================
# 3. Leader Election
# =============================================================================

class TestLeaderElection:
    async def test_acquire_lock(self, leader_election):
        is_leader = await leader_election.try_acquire()
        assert is_leader is True
        assert leader_election.is_leader is True

    async def test_release_lock(self, leader_election):
        await leader_election.try_acquire()
        await leader_election.release()
        assert leader_election.is_leader is False

    async def test_two_instances_only_one_leader(self, db_pool):
        """Two elections for the same name — only one can be leader."""
        from src.leader_election import LeaderElection
        e1 = LeaderElection(db_pool, "exclusive-component")
        e2 = LeaderElection(db_pool, "exclusive-component")
        try:
            leader1 = await e1.try_acquire()
            leader2 = await e2.try_acquire()
            assert leader1 is True
            assert leader2 is False  # lock already held
        finally:
            await e1.release()
            await e2.release()

    async def test_different_names_both_leaders(self, db_pool):
        """Different component names get independent locks."""
        from src.leader_election import LeaderElection
        e1 = LeaderElection(db_pool, "component-alpha")
        e2 = LeaderElection(db_pool, "component-beta")
        try:
            assert await e1.try_acquire() is True
            assert await e2.try_acquire() is True
        finally:
            await e1.release()
            await e2.release()

    async def test_lock_id_is_stable(self, db_pool):
        """Same name always produces same lock ID."""
        from src.leader_election.lock import _lock_id
        assert _lock_id("ProjectionDaemon") == _lock_id("ProjectionDaemon")
        assert _lock_id("ProjectionDaemon") != _lock_id("SagaManager")


# =============================================================================
# 4. Aggregate Test Harness
# =============================================================================

class TestAggregateTestHarness:
    def _make_submitted_event(self, app_id=None):
        return LoanApplicationSubmitted(
            aggregate_id=app_id or uuid4(),
            applicant_name="Test User",
            loan_amount=50000.0,
            loan_purpose="home",
            applicant_id=uuid4(),
            submitted_by="test",
        )

    def test_given_when_then_events(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = (
            harness
            .given([submitted])
            .when(lambda agg: agg.request_credit_analysis("system"))
        )
        result.then_events([CreditAnalysisRequested])

    def test_then_raises(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = (
            harness
            .given([submitted])
            .when(lambda agg: agg.record_fraud_check(0.1, [], True))
            # Can't record fraud check from Submitted state
        )
        result.then_raises(InvalidStateTransitionError)

    def test_then_state(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = (
            harness
            .given([submitted])
            .when(lambda agg: agg.request_credit_analysis("system"))
        )
        result.then_state(
            lambda agg: agg.status == LoanStatus.AWAITING_ANALYSIS,
            "status should be AwaitingAnalysis"
        )

    def test_then_event_payload(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = (
            harness
            .given([submitted])
            .when(lambda agg: agg.request_credit_analysis("system"))
        )
        result.then_event_payload(0, requested_by="system")

    def test_given_empty_history(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = harness.given([]).when(
            lambda agg: agg.submit(
                applicant_name="New User",
                loan_amount=10000,
                loan_purpose="car",
                applicant_id=uuid4(),
                submitted_by="test",
            )
        )
        result.then_events([LoanApplicationSubmitted])

    def test_then_no_events(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        # set_compliance_result doesn't raise events
        result = harness.given([submitted]).when(
            lambda agg: agg.set_compliance_result(True)
        )
        result.then_no_events()

    def test_chained_assertions(self):
        from src.testing import AggregateTestHarness
        from src.aggregates.loan_application import LoanApplicationAggregate

        app_id = uuid4()
        submitted = self._make_submitted_event(app_id)

        harness = AggregateTestHarness(LoanApplicationAggregate, app_id)
        result = (
            harness
            .given([submitted])
            .when(lambda agg: agg.request_credit_analysis("system"))
        )
        # Chain multiple assertions
        (result
            .then_events([CreditAnalysisRequested])
            .then_state(lambda agg: agg.status == LoanStatus.AWAITING_ANALYSIS)
            .then_event_payload(0, requested_by="system"))


# =============================================================================
# 5. Schema Compatibility Checker
# =============================================================================

class TestSchemaCompatibilityChecker:
    async def test_no_events_returns_warning(self, schema_checker):
        issues = await schema_checker.check_upcaster_compatibility(
            "CreditAnalysisCompleted", from_version=1
        )
        # No events in DB → warning about no sample data
        assert any(i.issue_type == "NoSampleData" for i in issues)

    async def test_missing_upcaster_returns_error(self, schema_checker):
        issues = await schema_checker.check_upcaster_compatibility(
            "NonExistentEvent", from_version=99
        )
        assert len(issues) == 1
        assert issues[0].issue_type == "MissingUpcaster"
        assert issues[0].severity == "error"

    async def test_check_all_upcasters(self, schema_checker):
        results = await schema_checker.check_all_upcasters()
        # Should have entries for all registered upcasters
        assert isinstance(results, dict)
        assert len(results) > 0

    async def test_with_sample_events(self, schema_checker, handler, db_pool):
        """With real events in DB, checker runs without crashing."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Schema Test",
            loan_amount=30000,
            loan_purpose="home",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        from src.commands.handlers import RecordCreditAnalysisCommand
        await handler.handle_record_credit_analysis(RecordCreditAnalysisCommand(
            application_id=app_id,
            credit_score=700,
            debt_to_income_ratio=0.3,
        ))
        # Set schema_version=1 to simulate old events
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE events SET schema_version = 1 WHERE event_type = 'CreditAnalysisCompleted'"
            )

        issues = await schema_checker.check_upcaster_compatibility(
            "CreditAnalysisCompleted", from_version=1
        )
        # Upcaster is valid — no errors expected
        errors = [i for i in issues if i.severity == "error"]
        assert len(errors) == 0


# =============================================================================
# 6. Field-Level Encryption
# =============================================================================

class TestFieldEncryptor:
    def test_encrypt_and_decrypt_roundtrip(self, field_encryptor):
        payload = {"applicant_name": "Jane Doe", "loan_amount": 50000}
        encrypted = field_encryptor.encrypt_fields(payload, ["applicant_name"])
        assert field_encryptor.is_encrypted(encrypted["applicant_name"])
        assert encrypted["loan_amount"] == 50000  # untouched

        decrypted = field_encryptor.decrypt_fields(encrypted, ["applicant_name"])
        assert decrypted["applicant_name"] == "Jane Doe"

    def test_encrypt_prefix(self, field_encryptor):
        payload = {"applicant_name": "John Smith"}
        encrypted = field_encryptor.encrypt_fields(payload, ["applicant_name"])
        assert encrypted["applicant_name"].startswith("enc:v1:")

    def test_is_encrypted_detection(self, field_encryptor):
        assert field_encryptor.is_encrypted("enc:v1:abc123") is True
        assert field_encryptor.is_encrypted("plaintext") is False
        assert field_encryptor.is_encrypted(12345) is False

    def test_already_encrypted_not_double_encrypted(self, field_encryptor):
        payload = {"name": "Alice"}
        once = field_encryptor.encrypt_fields(payload, ["name"])
        twice = field_encryptor.encrypt_fields(once, ["name"])
        # Should not double-encrypt
        assert once["name"] == twice["name"]

    def test_missing_field_skipped(self, field_encryptor):
        payload = {"loan_amount": 10000}
        result = field_encryptor.encrypt_fields(payload, ["applicant_name"])
        assert result == payload  # unchanged

    def test_decrypt_non_encrypted_field_unchanged(self, field_encryptor):
        payload = {"name": "plaintext"}
        result = field_encryptor.decrypt_fields(payload, ["name"])
        assert result["name"] == "plaintext"

    def test_generate_key_is_32_bytes(self):
        from src.encryption import FieldEncryptor
        key_b64 = FieldEncryptor.generate_key()
        key_bytes = base64.b64decode(key_b64)
        assert len(key_bytes) == 32

    def test_wrong_key_raises(self):
        from src.encryption import FieldEncryptor, EncryptionError
        key1 = base64.b64encode(b"a" * 32).decode()
        key2 = base64.b64encode(b"b" * 32).decode()
        enc = FieldEncryptor(base64.b64decode(key1))
        dec = FieldEncryptor(base64.b64decode(key2))
        encrypted = enc.encrypt_fields({"name": "secret"}, ["name"])
        with pytest.raises(EncryptionError):
            dec.decrypt_fields(encrypted, ["name"])

    def test_key_rotation(self):
        from src.encryption import FieldEncryptor
        old_key = base64.b64decode(base64.b64encode(b"a" * 32))
        new_key = base64.b64decode(base64.b64encode(b"b" * 32))

        old_enc = FieldEncryptor(old_key)
        encrypted_with_old = old_enc.encrypt_fields({"name": "Alice"}, ["name"])

        # Rotate: new encryptor knows both keys
        rotated_enc = FieldEncryptor(new_key, old_key=old_key)
        rotated = rotated_enc.rotate_fields(encrypted_with_old, ["name"])

        # Should now decrypt with new key only
        new_enc = FieldEncryptor(new_key)
        result = new_enc.decrypt_fields(rotated, ["name"])
        assert result["name"] == "Alice"


# =============================================================================
# 7. Integrity Monitor
# =============================================================================

class TestIntegrityMonitor:
    async def test_run_once_empty_db(self, integrity_monitor):
        """run_once() on empty DB returns zero counts."""
        summary = await integrity_monitor.run_once()
        assert summary["checked"] == 0
        assert summary["failed"] == 0
        assert summary["alerts_raised"] == 0

    async def test_run_once_valid_chain(self, integrity_monitor, handler, db_pool):
        """Valid chain produces no alerts."""
        await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Monitor Test",
            loan_amount=20000,
            loan_purpose="car",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        summary = await integrity_monitor.run_once()
        assert summary["failed"] == 0

    async def test_get_open_alerts_empty(self, integrity_monitor):
        alerts = await integrity_monitor.get_open_alerts()
        assert alerts == []

    async def test_start_and_stop(self, integrity_monitor):
        """Monitor starts and stops cleanly."""
        await integrity_monitor.start()
        await asyncio.sleep(0.05)
        await integrity_monitor.stop()

    async def test_alert_raised_on_broken_chain(self, integrity_monitor, db_pool, handler):
        """Tampered chain_hash triggers an alert."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Tamper Test",
            loan_amount=15000,
            loan_purpose="other",
            applicant_id=uuid4(),
            submitted_by="test",
        ))

        # Insert a fake audit entry with a broken chain hash
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO audit_ledger_projection
                    (application_id, event_id, event_type, event_hash, chain_hash, sequence_number)
                VALUES ($1, $2, 'LoanApplicationSubmitted', 'deadbeef', 'badhash', 1)
                """,
                app_id, uuid4(),
            )

        summary = await integrity_monitor.run_once()
        assert summary["failed"] >= 1
        assert summary["alerts_raised"] >= 1

        alerts = await integrity_monitor.get_open_alerts()
        assert len(alerts) >= 1
        assert any(str(a["application_id"]) == str(app_id) for a in alerts)

    async def test_acknowledge_alert(self, integrity_monitor, db_pool, handler):
        """Alerts can be acknowledged."""
        app_id = await handler.handle_submit_application(SubmitApplicationCommand(
            applicant_name="Ack Test",
            loan_amount=10000,
            loan_purpose="other",
            applicant_id=uuid4(),
            submitted_by="test",
        ))
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO audit_ledger_projection
                    (application_id, event_id, event_type, event_hash, chain_hash, sequence_number)
                VALUES ($1, $2, 'LoanApplicationSubmitted', 'deadbeef', 'badhash', 1)
                """,
                app_id, uuid4(),
            )

        await integrity_monitor.run_once()
        alerts = await integrity_monitor.get_open_alerts()
        assert len(alerts) >= 1

        alert_id = alerts[0]["alert_id"]
        acked = await integrity_monitor.acknowledge_alert(alert_id, "security-team")
        assert acked is True
