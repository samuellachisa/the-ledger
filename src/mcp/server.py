"""
MCP Server: Apex Financial Services Event Store.

Thin wiring layer — tool and resource logic lives in tools.py / resources.py.
Handles: initialization, auth middleware, error mapping, graceful startup.
"""
from __future__ import annotations

import asyncio
import logging
import os

import asyncpg
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, GetResourceResult, Resource, Tool

from src.auth.tokens import AuthError, TokenStore
from src.commands.handlers import CommandHandler
from src.mcp.validation import ValidationError, validate_arguments
from src.dead_letter.queue import DeadLetterQueue
from src.erasure.handler import ErasureHandler
from src.event_store import EventStore
from src.idempotency.store import IdempotencyStore
from src.integrity.gas_town import AgentContextReconstructor
from src.mcp.resources import dispatch_resource, get_resource_definitions
from src.mcp.tools import dispatch_tool, get_tool_definitions
from src.models.events import (
    AggregateNotFoundError,
    BusinessRuleViolationError,
    InvalidStateTransitionError,
    OptimisticConcurrencyError,
)
from src.observability.metrics import get_metrics
from src.ratelimit.limiter import RateLimitExceededError, RateLimiter
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)

app = Server("apex-financial-event-store")

# Global state (initialized in create_server)
_pool: asyncpg.Pool | None = None
_read_pool: asyncpg.Pool | None = None
_event_store: EventStore | None = None
_handler: CommandHandler | None = None
_reconstructor: AgentContextReconstructor | None = None
_token_store: TokenStore | None = None
_rate_limiter: RateLimiter | None = None
_dead_letter: DeadLetterQueue | None = None
_idempotency: IdempotencyStore | None = None
_erasure: ErasureHandler | None = None


def _get_deps():
    assert _pool and _event_store and _handler, "Server not initialized"
    return _pool, _event_store, _handler


async def _authenticate(arguments: dict, tool_name: str) -> None:
    if _token_store is None:
        return
    raw_token = arguments.get("_auth_token")
    if not raw_token:
        raise AuthError("Missing _auth_token in tool arguments", "MissingToken")
    identity = await _token_store.verify(raw_token)
    _token_store.require(identity, tool_name)
    if _rate_limiter is not None:
        await _rate_limiter.consume(agent_id=identity.agent_id, action=tool_name)


def _err(error_type: str, message: str, extra: dict | None = None) -> CallToolResult:
    import json
    from mcp.types import TextContent
    payload = {"error": error_type, "message": message, **(extra or {})}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, default=str))],
        isError=True,
    )


# =============================================================================
# Tool registration
# =============================================================================

@app.list_tools()
async def list_tools() -> list[Tool]:
    return get_tool_definitions()


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> CallToolResult:
    from src.observability.tracing import start_span
    pool, event_store, handler = _get_deps()
    async with start_span(f"tool.{name}") as span:
        span.set_attribute("tool_name", name)
        try:
            await _authenticate(arguments, name)
            # Validate arguments against inputSchema before dispatch
            tool_defs = {t.name: t for t in get_tool_definitions()}
            if name in tool_defs:
                validate_arguments(name, arguments, tool_defs[name].inputSchema)
            result = await dispatch_tool(
                name=name,
                arguments=arguments,
                handler=handler,
                token_store=_token_store,
                idempotency=_idempotency,
                erasure=_erasure,
            )
            outcome = "error" if result.isError else "success"
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": outcome})
            get_metrics().histogram("tool_latency_ms", span.elapsed_ms(), labels={"tool_name": name})
            return result
        except OptimisticConcurrencyError as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "occ_error"})
            return _err("OptimisticConcurrencyError", str(e), e.to_dict())
        except BusinessRuleViolationError as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "rule_error"})
            return _err("BusinessRuleViolationError", str(e), {"rule": e.rule})
        except InvalidStateTransitionError as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "state_error"})
            return _err("InvalidStateTransitionError", str(e), {"from_state": e.from_state, "to_state": e.to_state})
        except AggregateNotFoundError as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "not_found"})
            return _err("AggregateNotFoundError", str(e))
        except AuthError as e:
            get_metrics().increment("auth_failures_total", labels={"reason": e.code})
            return _err(e.code, str(e))
        except RateLimitExceededError as e:
            get_metrics().increment("rate_limit_hits_total", labels={"agent_id": e.agent_id, "action": e.action})
            return _err("RateLimitExceeded", str(e), {"retry_after_seconds": e.retry_after_seconds})
        except ValidationError as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "validation_error"})
            return _err("ValidationError", str(e), {"field": e.field} if e.field else {})
        except Exception as e:
            get_metrics().increment("tool_calls_total", labels={"tool_name": name, "outcome": "internal_error"})
            logger.exception("Unexpected error in tool %s", name)
            return _err("InternalError", str(e))


# =============================================================================
# Resource registration
# =============================================================================

@app.list_resources()
async def list_resources() -> list[Resource]:
    return get_resource_definitions()


@app.read_resource()
async def read_resource(uri: str) -> GetResourceResult:
    pool, event_store, _ = _get_deps()
    try:
        return await dispatch_resource(
            uri=uri,
            pool=pool,
            event_store=event_store,
            dead_letter=_dead_letter,
        )
    except Exception as e:
        import json
        from mcp.types import TextContent
        logger.exception("Error reading resource %s", uri)
        return GetResourceResult(contents=[
            TextContent(type="text", text=json.dumps({"error": str(e)}))
        ])


# =============================================================================
# Server initialization
# =============================================================================

async def _wait_for_db(url: str, retries: int = 10, delay: float = 2.0) -> None:
    """Wait for postgres to be ready before creating the pool."""
    for attempt in range(1, retries + 1):
        try:
            conn = await asyncpg.connect(url)
            await conn.close()
            logger.info("Database ready.")
            return
        except Exception as e:
            if attempt == retries:
                raise RuntimeError(f"Database not ready after {retries} attempts: {e}") from e
            logger.warning("DB not ready (attempt %d/%d): %s — retrying in %.1fs", attempt, retries, e, delay)
            await asyncio.sleep(delay)


async def create_server(
    database_url: str,
    enable_auth: bool = False,
    pool_min_size: int = 2,
    pool_max_size: int = 10,
    wait_for_db: bool = True,
) -> None:
    global _pool, _read_pool, _event_store, _handler, _reconstructor
    global _token_store, _rate_limiter, _dead_letter, _idempotency, _erasure

    if wait_for_db:
        await _wait_for_db(database_url)

    db_ssl = os.environ.get("DB_SSL", "").lower()
    ssl_param = db_ssl if db_ssl in ("require", "verify-ca", "verify-full") else None

    _pool = await asyncpg.create_pool(
        database_url, min_size=pool_min_size, max_size=pool_max_size,
        ssl=ssl_param,
    )

    # Optional read replica pool
    read_url = os.environ.get("DB_READ_URL")
    _read_pool = None
    if read_url:
        _read_pool = await asyncpg.create_pool(
            read_url, min_size=pool_min_size, max_size=pool_max_size,
            ssl=ssl_param,
        )
        logger.info("Read replica pool initialized from DB_READ_URL")

    _event_store = EventStore(_pool, UpcasterRegistry(), read_pool=_read_pool)
    _handler = CommandHandler(_event_store)
    _reconstructor = AgentContextReconstructor(_event_store)
    _dead_letter = DeadLetterQueue(_pool)
    _idempotency = IdempotencyStore(_pool)
    _erasure = ErasureHandler(_pool, _event_store)

    if enable_auth:
        _token_store = TokenStore(_pool)
        _rate_limiter = RateLimiter(_pool)

    logger.info(
        "MCP Server initialized (auth=%s, pool=%d-%d)",
        enable_auth, pool_min_size, pool_max_size,
    )


async def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    from src.logging_config import configure_logging
    configure_logging()

    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/apex_financial"
    )
    enable_auth = os.environ.get("ENABLE_AUTH", "false").lower() == "true"
    pool_min = int(os.environ.get("DB_POOL_MIN_SIZE", "2"))
    pool_max = int(os.environ.get("DB_POOL_MAX_SIZE", "10"))
    db_retries = int(os.environ.get("DB_STARTUP_RETRIES", "10"))
    db_retry_delay = float(os.environ.get("DB_STARTUP_RETRY_DELAY_SECONDS", "2.0"))

    await create_server(
        database_url,
        enable_auth=enable_auth,
        pool_min_size=pool_min,
        pool_max_size=pool_max,
        wait_for_db=True,
    )

    # Start Prometheus metrics HTTP server if port is configured
    metrics_port = os.environ.get("METRICS_PORT")
    metrics_server = None
    if metrics_port:
        from src.observability.http_server import MetricsHttpServer
        metrics_server = MetricsHttpServer(get_metrics(), pool=_pool)
        await metrics_server.start()

    # Start token cleanup job if auth is enabled
    cleanup_job = None
    if enable_auth and _pool:
        from src.auth.cleanup import TokenCleanupJob
        cleanup_job = TokenCleanupJob(_pool)
        await cleanup_job.start()

    # Start archival scheduler
    archival_scheduler = None
    if os.environ.get("ENABLE_ARCHIVAL", "false").lower() == "true" and _pool:
        from src.archival.scheduler import ArchivalScheduler
        archival_scheduler = ArchivalScheduler(_pool)
        await archival_scheduler.start()

    # Start erasure enforcement scheduler
    erasure_scheduler = None
    if os.environ.get("ENABLE_ERASURE_SCHEDULER", "false").lower() == "true" and _pool and _event_store:
        from src.erasure.scheduler import ErasureScheduler
        erasure_scheduler = ErasureScheduler(_pool, _event_store)
        await erasure_scheduler.start()

    try:
        async with stdio_server() as (read_stream, write_stream):
            await app.run(read_stream, write_stream, app.create_initialization_options())
    finally:
        if cleanup_job:
            await cleanup_job.stop()
        if archival_scheduler:
            await archival_scheduler.stop()
        if erasure_scheduler:
            await erasure_scheduler.stop()
        if metrics_server:
            await metrics_server.stop()
        if _pool:
            await _pool.close()
        if _read_pool:
            await _read_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
