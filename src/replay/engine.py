"""
ReplayEngine: Targeted event reprocessing for dead-letter entries and position ranges.

Problem
-------
The dead letter queue captures failed events, but there was no mechanism to
replay them after fixing the bug. A full projection rebuild replays everything
— expensive and unnecessary when only one event failed. This engine provides
surgical replay: re-feed specific events to a specific processor.

Design
------
- replay_dead_letter: load one DLQ entry → call handler in transaction → resolve
- replay_range: load events by global_position range → call handler per event
- Each event gets its own transaction so one failure doesn't block others
- On success, dead letter entries are marked resolved automatically
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable
from uuid import UUID

import asyncpg

from src.dead_letter.queue import DeadLetterQueue
from src.event_store import EventStore

logger = logging.getLogger(__name__)


@dataclass
class ReplayedEvent:
    """Lightweight event object reconstructed from a dead-letter or raw DB row."""
    event_id: UUID
    aggregate_id: UUID
    aggregate_type: str
    event_type: str
    global_position: int
    payload: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


HandlerFn = Callable[[asyncpg.Connection, ReplayedEvent], Awaitable[None]]


class ReplayEngine:
    """Replays events from the dead-letter queue or by global position range."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        event_store: EventStore,
        dead_letter: DeadLetterQueue,
    ) -> None:
        self._pool = pool
        self._event_store = event_store
        self._dead_letter = dead_letter

    async def replay_dead_letter(
        self,
        dead_letter_id: UUID,
        processor_name: str,
        handler_fn: HandlerFn,
    ) -> dict:
        """
        Replay a single dead-letter entry.

        Loads the entry, reconstructs a ReplayedEvent, calls handler_fn inside
        a transaction, then marks the entry resolved on success.

        Returns {"status": "replayed", ...} or {"status": "failed", "error": str}
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT dead_letter_id, event_id, aggregate_type, event_type,
                       global_position, payload
                FROM dead_letter_events
                WHERE dead_letter_id = $1
                """,
                dead_letter_id,
            )

        if row is None:
            return {"status": "failed", "error": f"dead_letter_id {dead_letter_id} not found"}

        payload = dict(row["payload"])
        # aggregate_id may be stored in payload or we fall back to a zero UUID
        raw_id = payload.get("aggregate_id") or payload.get("stream_id")
        try:
            aggregate_id = UUID(str(raw_id)) if raw_id else UUID(int=0)
        except (ValueError, AttributeError):
            aggregate_id = UUID(int=0)

        event = ReplayedEvent(
            event_id=row["event_id"],
            aggregate_id=aggregate_id,
            aggregate_type=row["aggregate_type"],
            event_type=row["event_type"],
            global_position=row["global_position"],
            payload=payload,
        )

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await handler_fn(conn, event)

            await self._dead_letter.resolve(dead_letter_id, resolved_by="replay-engine")
            logger.info("Replayed dead letter %s (%s)", dead_letter_id, event.event_type)
            return {
                "status": "replayed",
                "dead_letter_id": str(dead_letter_id),
                "event_type": event.event_type,
            }
        except Exception as exc:
            logger.error("Replay failed for dead letter %s: %s", dead_letter_id, exc)
            return {"status": "failed", "error": str(exc)}

    async def replay_range(
        self,
        after_position: int,
        before_position: int,
        processor_name: str,
        handler_fn: HandlerFn,
    ) -> dict:
        """
        Replay all events with global_position in (after_position, before_position).

        Each event gets its own transaction. Failures are collected without
        aborting the rest of the run.

        Returns {"replayed": int, "failed": int, "errors": list}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, aggregate_id, aggregate_type, event_type,
                       global_position, payload, metadata
                FROM events
                WHERE global_position > $1 AND global_position < $2
                ORDER BY global_position ASC
                """,
                after_position, before_position,
            )

        replayed = 0
        failed = 0
        errors: list[dict] = []

        for row in rows:
            event = ReplayedEvent(
                event_id=row["event_id"],
                aggregate_id=row["aggregate_id"],
                aggregate_type=row["aggregate_type"],
                event_type=row["event_type"],
                global_position=row["global_position"],
                payload=dict(row["payload"]),
                metadata=dict(row["metadata"]) if row["metadata"] else {},
            )
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await handler_fn(conn, event)
                replayed += 1
            except Exception as exc:
                failed += 1
                errors.append({
                    "global_position": event.global_position,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "error": str(exc),
                })
                logger.error(
                    "replay_range: failed at position %d (%s): %s",
                    event.global_position, event.event_type, exc,
                )

        logger.info(
            "replay_range [%d, %d] via %s: %d replayed, %d failed",
            after_position, before_position, processor_name, replayed, failed,
        )
        return {"replayed": replayed, "failed": failed, "errors": errors}
