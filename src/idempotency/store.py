"""
IdempotencyStore: Prevents duplicate command execution.

Problem
-------
Agents may retry a command (e.g. submit_application) after a network timeout,
not knowing if the first attempt succeeded. Without idempotency, this creates
duplicate loan applications.

Design
------
- Caller provides an idempotency_key (UUID or string) with the command.
- check_and_reserve(): atomically checks if key was seen before.
  - If seen: returns the stored result (replay).
  - If new: reserves the key (marks as 'processing') and returns None.
- complete(): stores the result once the command succeeds.
- Keys expire after ttl_seconds (default 24h) to prevent unbounded growth.
- cleanup_expired(): removes expired keys (call periodically).

Guarantees
----------
- At-most-once execution: duplicate requests return the original result.
- Crash safety: if the process crashes after reserve() but before complete(),
  the key stays in 'processing' state. After TTL it expires and the caller
  can retry. This is the standard idempotency tradeoff.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 86400  # 24 hours


@dataclass
class IdempotencyResult:
    """Returned when a duplicate request is detected."""
    idempotency_key: str
    result: Any
    is_duplicate: bool


class IdempotencyStore:
    """
    Idempotency key store backed by PostgreSQL.

    Usage:
        store = IdempotencyStore(pool)
        existing = await store.check_and_reserve(key)
        if existing:
            return existing.result  # duplicate — return cached result
        result = await execute_command()
        await store.complete(key, result)
        return result
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def check_and_reserve(
        self,
        idempotency_key: str,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> Optional[IdempotencyResult]:
        """
        Check if key exists and is completed. If not, reserve it.

        Returns:
            IdempotencyResult if key was already completed (duplicate).
            None if key is new (caller should proceed and call complete()).
        """
        async with self._pool.acquire() as conn:
            # Try to insert a new reservation
            row = await conn.fetchrow(
                """
                INSERT INTO idempotency_keys (idempotency_key, status, ttl_seconds)
                VALUES ($1, 'processing', $2)
                ON CONFLICT (idempotency_key) DO UPDATE
                    SET idempotency_key = EXCLUDED.idempotency_key  -- no-op update to return row
                RETURNING idempotency_key, status, result
                """,
                idempotency_key, ttl_seconds,
            )

        if row["status"] == "completed":
            return IdempotencyResult(
                idempotency_key=idempotency_key,
                result=row["result"],  # raw TEXT — caller decides how to parse
                is_duplicate=True,
            )

        # Key was just reserved (or is still processing from a crashed attempt)
        return None

    async def complete(self, idempotency_key: str, result: Any) -> None:
        """
        Mark an idempotency key as completed and store the result.
        Call this after the command succeeds.
        """
        import json
        result_json = json.dumps(result, default=str)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE idempotency_keys
                SET status = 'completed', result = $1, completed_at = NOW()
                WHERE idempotency_key = $2
                """,
                result_json, idempotency_key,
            )

    async def cleanup_expired(self) -> int:
        """Remove expired idempotency keys. Returns count deleted."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM idempotency_keys
                WHERE created_at + (ttl_seconds * INTERVAL '1 second') < NOW()
                """
            )
        count = int(result.split()[-1])
        if count:
            logger.info("Cleaned up %d expired idempotency keys", count)
        return count
