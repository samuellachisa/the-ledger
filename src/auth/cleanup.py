"""
TokenCleanupJob: Periodically removes expired/revoked tokens and stale rate limit buckets.

Runs as a background asyncio task. Safe to run on any instance (idempotent DELETEs).

Schedule:
    - Expired tokens: deleted after TOKEN_CLEANUP_GRACE_SECONDS past expiry (default 1h)
    - Revoked tokens: deleted after same grace period
    - Rate limit buckets: deleted if last_refill is older than BUCKET_TTL_SECONDS (default 24h)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

TOKEN_CLEANUP_INTERVAL_SECONDS = 3600   # run every hour
TOKEN_CLEANUP_GRACE_SECONDS = 3600      # keep expired tokens 1h after expiry
BUCKET_TTL_SECONDS = 86400              # drop buckets idle for 24h


class TokenCleanupJob:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="TokenCleanupJob")
        logger.info("TokenCleanupJob started (interval=%ds)", TOKEN_CLEANUP_INTERVAL_SECONDS)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict:
        """Run a single cleanup pass. Returns counts of deleted rows."""
        async with self._pool.acquire() as conn:
            # Delete expired tokens (past grace period)
            expired = await conn.fetchval(
                """
                DELETE FROM agent_tokens
                WHERE expires_at < NOW() - ($1 || ' seconds')::interval
                RETURNING token_id
                """,
                str(TOKEN_CLEANUP_GRACE_SECONDS),
            )
            tokens_deleted = await conn.execute(
                """
                DELETE FROM agent_tokens
                WHERE expires_at < NOW() - ($1 || ' seconds')::interval
                   OR (revoked_at IS NOT NULL
                       AND revoked_at < NOW() - ($1 || ' seconds')::interval)
                """,
                str(TOKEN_CLEANUP_GRACE_SECONDS),
            )
            buckets_deleted = await conn.execute(
                """
                DELETE FROM rate_limit_buckets
                WHERE last_refill < NOW() - ($1 || ' seconds')::interval
                """,
                str(BUCKET_TTL_SECONDS),
            )

        t_count = int(tokens_deleted.split()[-1])
        b_count = int(buckets_deleted.split()[-1])
        if t_count or b_count:
            logger.info(
                "TokenCleanupJob: deleted %d expired tokens, %d stale buckets",
                t_count, b_count,
            )
        return {"tokens_deleted": t_count, "buckets_deleted": b_count}

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("TokenCleanupJob error: %s", e, exc_info=True)
            await asyncio.sleep(TOKEN_CLEANUP_INTERVAL_SECONDS)
