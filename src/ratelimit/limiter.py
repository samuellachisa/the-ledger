"""
RateLimiter: Token-bucket rate limiting per agent_id + action.

Problem
-------
No throttling on how many decisions an agent session can generate, no
circuit breaker if the DB is slow, no per-agent rate limit. The
AgentSessionAggregate tracks decisions but nothing enforces a cap.

Design
------
Token bucket algorithm:
  - Each (agent_id, action) pair has a bucket with `capacity` tokens
  - Tokens refill at `refill_rate` tokens/second
  - Each call consumes 1 token
  - If bucket is empty: raise RateLimitExceededError

State is persisted in `rate_limit_buckets` so limits survive restarts.
The refill is calculated lazily on each consume() call using elapsed time
since last_refill — no background task needed.

Default limits (conservative for financial services):
  generate_decision:        5 per minute  (0.083/s, capacity 5)
  finalize_application:     10 per minute (0.167/s, capacity 10)
  submit_application:       30 per minute (0.5/s, capacity 30)
  default (all other tools): 60 per minute (1/s, capacity 60)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class RateLimitExceededError(Exception):
    def __init__(self, agent_id: str, action: str, retry_after_seconds: float):
        self.agent_id = agent_id
        self.action = action
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Rate limit exceeded for agent '{agent_id}' on action '{action}'. "
            f"Retry after {retry_after_seconds:.1f}s."
        )


# Default bucket configs: (capacity, refill_rate tokens/sec)
DEFAULT_LIMITS: dict[str, tuple[float, float]] = {
    "generate_decision":    (5.0,  5.0 / 60),
    "finalize_application": (10.0, 10.0 / 60),
    "submit_application":   (30.0, 30.0 / 60),
    "_default":             (60.0, 60.0 / 60),
}


class RateLimiter:
    """
    Token-bucket rate limiter backed by PostgreSQL.

    Usage:
        limiter = RateLimiter(pool)
        await limiter.consume(agent_id="agent-001", action="generate_decision")
        # raises RateLimitExceededError if limit hit
    """

    def __init__(self, pool: asyncpg.Pool, limits: Optional[dict[str, tuple[float, float]]] = None):
        self._pool = pool
        self._limits = limits or DEFAULT_LIMITS

    async def consume(self, agent_id: str, action: str, tokens: float = 1.0) -> None:
        """
        Consume tokens from the bucket. Raises RateLimitExceededError if empty.
        Creates the bucket on first use.
        """
        capacity, refill_rate = self._limits.get(action, self._limits["_default"])

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Upsert bucket, lock row for update
                row = await conn.fetchrow(
                    """
                    INSERT INTO rate_limit_buckets
                        (agent_id, action, tokens, capacity, refill_rate, last_refill)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (agent_id, action) DO UPDATE
                        SET agent_id = EXCLUDED.agent_id  -- no-op, just to get lock
                    RETURNING tokens, capacity, refill_rate, last_refill
                    """,
                    agent_id, action, capacity, capacity, refill_rate,
                )

                # Calculate refill based on elapsed time
                now = datetime.now(timezone.utc)
                last_refill = row["last_refill"]
                if last_refill.tzinfo is None:
                    last_refill = last_refill.replace(tzinfo=timezone.utc)
                elapsed = (now - last_refill).total_seconds()
                refilled = min(
                    row["capacity"],
                    row["tokens"] + elapsed * row["refill_rate"],
                )

                if refilled < tokens:
                    # Calculate when enough tokens will be available
                    deficit = tokens - refilled
                    retry_after = deficit / row["refill_rate"]
                    raise RateLimitExceededError(agent_id, action, retry_after)

                new_tokens = refilled - tokens
                await conn.execute(
                    """
                    UPDATE rate_limit_buckets
                    SET tokens = $1, last_refill = NOW()
                    WHERE agent_id = $2 AND action = $3
                    """,
                    new_tokens, agent_id, action,
                )

    async def reset(self, agent_id: str, action: Optional[str] = None) -> None:
        """Reset bucket(s) to full capacity. Used in tests and admin operations."""
        async with self._pool.acquire() as conn:
            if action:
                await conn.execute(
                    """
                    UPDATE rate_limit_buckets
                    SET tokens = capacity, last_refill = NOW()
                    WHERE agent_id = $1 AND action = $2
                    """,
                    agent_id, action,
                )
            else:
                await conn.execute(
                    """
                    UPDATE rate_limit_buckets
                    SET tokens = capacity, last_refill = NOW()
                    WHERE agent_id = $1
                    """,
                    agent_id,
                )

    async def get_status(self, agent_id: str, action: str) -> dict:
        """Return current bucket status for an agent/action pair."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT tokens, capacity, refill_rate, last_refill
                FROM rate_limit_buckets
                WHERE agent_id = $1 AND action = $2
                """,
                agent_id, action,
            )
        if row is None:
            capacity, refill_rate = self._limits.get(action, self._limits["_default"])
            return {"tokens": capacity, "capacity": capacity, "refill_rate": refill_rate}

        now = datetime.now(timezone.utc)
        last_refill = row["last_refill"]
        if last_refill.tzinfo is None:
            last_refill = last_refill.replace(tzinfo=timezone.utc)
        elapsed = (now - last_refill).total_seconds()
        current = min(row["capacity"], row["tokens"] + elapsed * row["refill_rate"])
        return {
            "tokens": round(current, 4),
            "capacity": row["capacity"],
            "refill_rate": row["refill_rate"],
        }
