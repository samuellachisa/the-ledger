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
    """Create a connection pool for the test database."""
    pool = await asyncpg.create_pool(TEST_DB_URL, min_size=2, max_size=10)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_schema(db_pool):
    """Apply schema to test database once per session."""
    schema_path = os.path.join(os.path.dirname(__file__), "..", "src", "schema.sql")
    with open(schema_path) as f:
        schema_sql = f.read()
    async with db_pool.acquire() as conn:
        await conn.execute(schema_sql)


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(db_pool):
    """Truncate all tables before each test for isolation."""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE TABLE
                outbox,
                audit_ledger_projection,
                compliance_audit_projection,
                agent_performance_projection,
                application_summary_projection,
                saga_instances,
                events,
                event_streams
            RESTART IDENTITY CASCADE
        """)
        # Reset checkpoints
        await conn.execute("UPDATE projection_checkpoints SET last_position = 0")
        await conn.execute("UPDATE saga_checkpoints SET last_position = 0")
    yield


@pytest_asyncio.fixture
async def event_store(db_pool):
    return EventStore(db_pool, UpcasterRegistry())


@pytest_asyncio.fixture
async def handler(event_store):
    from src.commands.handlers import CommandHandler
    return CommandHandler(event_store)


@pytest_asyncio.fixture
async def saga_manager(db_pool, event_store, handler):
    from src.saga import SagaManager
    return SagaManager(db_pool, event_store, handler)
