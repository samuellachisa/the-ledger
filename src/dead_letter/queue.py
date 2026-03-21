"""
DeadLetterQueue: Persistent store for events that failed processing after all retries.

Problem
-------
The projection daemon and saga manager both have retry logic. When retries are
exhausted they previously just logged and skipped — the event was silently lost
from the processor's perspective. In a financial system this is unacceptable:
a failed compliance projection update or a stuck saga step must be visible to
operators.

Design
------
- Failed events are written to `dead_letter_events` with full context
- Operators can query unresolved entries via the MCP resource ledger://dead-letters
- After fixing the underlying bug, operators call resolve() to mark entries done
  and can trigger a projection rebuild or saga replay as needed
- DeadLetterQueue is injected into ProjectionDaemon and SagaManager
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

import asyncpg

from src.event_store import StoredEvent

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def write(
        self,
        event: StoredEvent,
        source: str,           # 'projection' | 'saga'
        processor_name: str,
        retry_count: int,
        error: Exception,
    ) -> None:
        """Persist a failed event to the dead letter table."""
        error_type = type(error).__name__
        error_message = str(error)[:2000]  # cap length

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO dead_letter_events
                    (event_id, global_position, aggregate_type, event_type,
                     payload, source, processor_name, retry_count,
                     error_type, error_message)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT DO NOTHING
                """,
                event.event_id,
                event.global_position,
                event.aggregate_type,
                event.event_type,
                dict(event.payload),
                source,
                processor_name,
                retry_count,
                error_type,
                error_message,
            )
        logger.error(
            "Dead letter: %s/%s event %s failed in %s after %d retries: %s: %s",
            event.aggregate_type, event.event_type, event.event_id,
            processor_name, retry_count, error_type, error_message,
        )

    async def list_unresolved(
        self,
        source: Optional[str] = None,
        processor_name: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """List unresolved dead letter entries for operator inspection."""
        async with self._pool.acquire() as conn:
            query = """
                SELECT dead_letter_id, event_id, global_position, aggregate_type,
                       event_type, source, processor_name, retry_count,
                       error_type, error_message, created_at, resolved_at
                FROM dead_letter_events
                WHERE resolved_at IS NULL
            """
            args = []
            if source:
                args.append(source)
                query += f" AND source = ${len(args)}"
            if processor_name:
                args.append(processor_name)
                query += f" AND processor_name = ${len(args)}"
            args.append(limit)
            query += f" ORDER BY created_at DESC LIMIT ${len(args)}"
            rows = await conn.fetch(query, *args)
        return [dict(r) for r in rows]

    async def resolve(
        self,
        dead_letter_id: UUID,
        resolved_by: str,
    ) -> bool:
        """Mark a dead letter entry as resolved. Returns True if found."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE dead_letter_events
                SET resolved_at = NOW(), resolved_by = $1
                WHERE dead_letter_id = $2 AND resolved_at IS NULL
                """,
                resolved_by, dead_letter_id,
            )
        updated = result.split()[-1] != "0"
        if updated:
            logger.info("Dead letter %s resolved by %s", dead_letter_id, resolved_by)
        return updated

    async def count_unresolved(self) -> dict[str, int]:
        """Return unresolved counts grouped by processor_name."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT processor_name, COUNT(*) as count
                FROM dead_letter_events
                WHERE resolved_at IS NULL
                GROUP BY processor_name
                """,
            )
        return {r["processor_name"]: r["count"] for r in rows}
