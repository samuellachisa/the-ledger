"""
Shared test fixtures for the Apex Financial Services Event Store test suite.

Uses a real PostgreSQL database (test instance). Set TEST_DATABASE_URL env var.
Default: postgresql://postgres:postgres@localhost:5432/apex_financial_test
"""
from __future__ import annotations

import asyncio
import json
import os

import asyncpg
import pytest
import pytest_asyncio

# Load .env so TEST_DATABASE_URL is available without setting it in the shell
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry

TEST_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/apex_financial_test"
)

# ---------------------------------------------------------------------------
# Check DB availability once at import time (sync) so we can skip early
# ---------------------------------------------------------------------------
_DB_AVAILABLE: bool | None = None


def _check_db_available() -> bool:
    global _DB_AVAILABLE
    if _DB_AVAILABLE is not None:
        return _DB_AVAILABLE
    import socket
    try:
        from urllib.parse import urlparse
        p = urlparse(TEST_DB_URL)
        s = socket.create_connection((p.hostname, p.port or 5432), timeout=2)
        s.close()
        _DB_AVAILABLE = True
    except Exception:
        _DB_AVAILABLE = False
    return _DB_AVAILABLE


# ---------------------------------------------------------------------------
# Schema setup — run once synchronously before any tests
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Apply schema once before the test session starts."""
    if not _check_db_available():
        return
    import asyncio as _asyncio

    async def _apply():
        conn = await asyncpg.connect(TEST_DB_URL)
        schema_path = os.path.join(os.path.dirname(__file__), "..", "src", "schema.sql")
        with open(schema_path) as f:
            sql = f.read()
        await conn.execute(sql)
        await conn.close()

    try:
        _asyncio.run(_apply())
    except Exception as e:
        print(f"\n[conftest] Schema setup failed: {e}")


# ---------------------------------------------------------------------------
# Per-test pool (function-scoped to avoid cross-loop issues)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db_pool():
    """Fresh connection pool per test. Yields None if DB unavailable."""
    if not _check_db_available():
        yield None
        return

    async def _init_conn(conn):
        from uuid import UUID
        from datetime import datetime, date

        def _default(obj):
            if isinstance(obj, UUID):
                return str(obj)
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        def _encoder(val):
            return json.dumps(val, default=_default)

        await conn.set_type_codec("jsonb", encoder=_encoder, decoder=json.loads, schema="pg_catalog")
        await conn.set_type_codec("json", encoder=_encoder, decoder=json.loads, schema="pg_catalog")

    pool = await asyncpg.create_pool(TEST_DB_URL, min_size=2, max_size=5, init=_init_conn)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_tables():
    """Truncate all mutable tables before each test for isolation."""
    if not _check_db_available():
        yield
        return
    conn = await asyncpg.connect(TEST_DB_URL)
    try:
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
    finally:
        await conn.close()
    yield


@pytest_asyncio.fixture
async def require_db(db_pool):
    """Skip the test if the database is not available."""
    if db_pool is None:
        pytest.skip("Database not available")


@pytest_asyncio.fixture
async def event_store(db_pool):
    if db_pool is None:
        pytest.skip("Database not available")
    return EventStore(db_pool, UpcasterRegistry())


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
    return CircuitBreaker("test-breaker", failure_threshold=3, recovery_timeout_seconds=1.0, success_threshold=1)


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
    return SchemaCompatibilityChecker(db_pool, UpcasterRegistry())


@pytest_asyncio.fixture
def field_encryptor():
    import base64
    key = base64.b64encode(b"a" * 32).decode()
    os.environ["FIELD_ENCRYPTION_KEY"] = key
    from src.encryption import FieldEncryptor
    return FieldEncryptor.from_env()
