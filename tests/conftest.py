"""
Shared test fixtures for the Apex Financial Services Event Store test suite.

Uses a real PostgreSQL database (test instance). Set TEST_DATABASE_URL env var.
Default: postgresql://postgres:postgres@localhost:5432/apex_financial_test
"""
from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
import pytest_asyncio

from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry

TEST_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/apex_financial_test"
)


@pytest.fixture(scope="session")
def event_loop():
    """Single event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def db_pool():
    """Create a connection pool for the test database. Yields None if DB unavailable."""
    try:
        pool = await asyncpg.create_pool(TEST_DB_URL, min_size=2, max_size=10)
        yield pool
        await pool.close()
    except Exception:
        yield None


@pytest_asyncio.fixture(scope="session")
async def require_db_pool(db_pool):
    """Session-scoped pool that skips if DB is unavailable. Use in DB-dependent tests."""
    if db_pool is None:
        pytest.skip("Database not available")
    return db_pool


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_schema(db_pool):
    """Apply schema to test database once per session."""
    if db_pool is None:
        return
    schema_path = os.path.join(os.path.dirname(__file__), "..", "src", "schema.sql")
    with open(schema_path) as f:
        schema_sql = f.read()
    async with db_pool.acquire() as conn:
        await conn.execute(schema_sql)


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(db_pool):
    """
    Truncate all mutable tables before each test for isolation.
    Uses TRUNCATE ... CASCADE for correctness and resets checkpoints.
    """
    if db_pool is None:
        yield
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE TABLE
                auth_audit_log,
                outbox,
                audit_ledger_projection,
                compliance_audit_projection,
                agent_performance_projection,
                application_summary_projection,
                saga_instances,
                dead_letter_events,
                agent_tokens,
                rate_limit_buckets,
                aggregate_snapshots,
                idempotency_keys,
                erasure_requests,
                schema_migration_runs,
                integrity_alerts,
                events,
                event_streams
            RESTART IDENTITY CASCADE
        """)
        await conn.execute("UPDATE projection_checkpoints SET last_position = 0")
        await conn.execute("UPDATE saga_checkpoints SET last_position = 0")
    yield


@pytest_asyncio.fixture
async def event_store(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    return EventStore(db_pool, UpcasterRegistry())


@pytest_asyncio.fixture
async def require_db(db_pool):
    """Fixture that skips the test if the database is not available."""
    if db_pool is None:
        pytest.skip("Database not available")


@pytest_asyncio.fixture
async def handler(event_store):
    from src.commands.handlers import CommandHandler
    return CommandHandler(event_store)


@pytest_asyncio.fixture
async def saga_manager(db_pool, event_store, handler):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.saga import SagaManager
    return SagaManager(db_pool, event_store, handler)


@pytest_asyncio.fixture
async def dead_letter(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.dead_letter import DeadLetterQueue
    return DeadLetterQueue(db_pool)


@pytest_asyncio.fixture
async def token_store(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.auth import TokenStore
    return TokenStore(db_pool)


@pytest_asyncio.fixture
async def rate_limiter(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.ratelimit import RateLimiter
    return RateLimiter(db_pool)


@pytest_asyncio.fixture
async def snapshot_store(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.snapshots import SnapshotStore
    return SnapshotStore(db_pool)


@pytest_asyncio.fixture
async def migration_runner(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.migrations import MigrationRunner
    from src.upcasting.registry import UpcasterRegistry
    return MigrationRunner(db_pool, UpcasterRegistry())


@pytest_asyncio.fixture
async def stream_archiver(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.archival import StreamArchiver
    return StreamArchiver(db_pool)


@pytest_asyncio.fixture
async def idempotency_store(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.idempotency import IdempotencyStore
    return IdempotencyStore(db_pool)


@pytest_asyncio.fixture
async def erasure_handler(db_pool, event_store):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.erasure import ErasureHandler
    return ErasureHandler(db_pool, event_store)


@pytest_asyncio.fixture
async def circuit_breaker():
    from src.circuit_breaker import CircuitBreaker
    return CircuitBreaker("test-breaker", failure_threshold=3, recovery_timeout_seconds=1.0)


@pytest_asyncio.fixture
async def leader_election(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.leader_election import LeaderElection
    election = LeaderElection(db_pool, "test-component")
    yield election
    await election.release()


@pytest_asyncio.fixture
async def integrity_monitor(db_pool, event_store):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.integrity.monitor import IntegrityMonitor
    return IntegrityMonitor(db_pool, event_store, check_interval_seconds=999)


@pytest_asyncio.fixture
async def schema_checker(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    from src.schema_compat import SchemaCompatibilityChecker
    from src.upcasting.registry import UpcasterRegistry
    return SchemaCompatibilityChecker(db_pool, UpcasterRegistry())


@pytest_asyncio.fixture
def field_encryptor():
    import base64
    key = base64.b64encode(b"a" * 32).decode()
    import os
    os.environ["FIELD_ENCRYPTION_KEY"] = key
    from src.encryption import FieldEncryptor
    return FieldEncryptor.from_env()
