"""
ledger/event_store.py
EventStore Core Implementation
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional, TypeVar
from uuid import UUID, uuid4
import asyncpg
from ledger.schema.events import DomainEvent, EVENT_REGISTRY, OptimisticConcurrencyError

logger = logging.getLogger(__name__)

_RETRY_BASE_MS = 10
_RETRY_FACTOR = 2
_RETRY_MAX_ATTEMPTS = 3

class StoredEvent:
    def __init__(self, row: asyncpg.Record):
        self.event_id = row["event_id"]
        self.stream_id = row["stream_id"]
        self.event_type = row["event_type"]
        self.event_version = row["event_version"]
        self.stream_position = row["stream_position"]
        self.global_position = row["global_position"]
        self.payload = dict(row["payload"])
        self.metadata = dict(row["metadata"])
        self.recorded_at = row["recorded_at"]

def extract_aggregate_type(stream_id: str) -> str:
    """
    Map the 7 stream prefixes to aggregate_types.
    Prefixes: loan-{id}, docpkg-{id}, agent-{type}-{session_id}, credit-{id}, fraud-{id}, compliance-{id}, audit-{entity_type}-{id}
    """
    if stream_id.startswith("loan-"): return "LoanApplication"
    if stream_id.startswith("docpkg-"): return "DocumentPackage"
    if stream_id.startswith("agent-"): return "AgentSession"
    if stream_id.startswith("credit-"): return "CreditRecord"
    if stream_id.startswith("fraud-"): return "FraudRecord"
    if stream_id.startswith("compliance-"): return "ComplianceRecord"
    if stream_id.startswith("audit-"): return "AuditLedger"
    raise ValueError(f"Invalid stream prefix for stream_id: {stream_id}")

class EventStore:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def append(
        self,
        stream_id: str,
        events: list[DomainEvent],
        expected_version: int,
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
    ) -> int:
        if not events:
            return expected_version
            
        aggregate_type = extract_aggregate_type(stream_id)
        
        # Gas Town session management parsing
        if aggregate_type == "AgentSession":
            if expected_version == 0:
                first_event = events[0]
                if first_event.event_type != "AgentSessionStarted":
                    raise ValueError("AgentSessionStarted must be the first event for an AgentSession stream.")
                    
            for ev in events:
                if "context_source" not in ev.metadata:
                    # Provide default per requirement
                    ev.metadata["context_source"] = "fresh"
                elif not (ev.metadata["context_source"] == "fresh" or ev.metadata["context_source"].startswith("prior_session_replay:")):
                    raise ValueError("Invalid context_source metadata")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                new_version = expected_version + len(events)
                if expected_version == 0:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO event_streams (stream_id, aggregate_type, current_version, created_at)
                            VALUES ($1, $2, $3, NOW())
                            """,
                            stream_id, aggregate_type, new_version
                        )
                    except asyncpg.UniqueViolationError:
                        actual_v = await conn.fetchval(
                            "SELECT current_version FROM event_streams WHERE stream_id = $1", stream_id
                        )
                        raise OptimisticConcurrencyError(stream_id, expected_version, actual_v or -1)
                else:
                    locked = await conn.fetchrow(
                        "SELECT current_version FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                        stream_id
                    )
                    if locked is None or locked["current_version"] != expected_version:
                        actual_version = locked["current_version"] if locked else -1
                        raise OptimisticConcurrencyError(stream_id, expected_version, actual_version)
                    
                    await conn.execute(
                        "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                        new_version, stream_id
                    )

                for i, event in enumerate(events):
                    position = expected_version + i + 1
                    payload = event.to_payload()
                    metadata = dict(event.metadata)
                    if correlation_id:
                        metadata["correlation_id"] = str(correlation_id)
                    if causation_id:
                        metadata["causation_id"] = str(causation_id)

                    event_row = await conn.fetchrow(
                        """
                        INSERT INTO events (
                            event_id, stream_id, stream_position,
                            event_type, event_version, payload, metadata, recorded_at
                        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
                        RETURNING event_id, global_position
                        """,
                        event.event_id, stream_id, position,
                        event.event_type, event.event_version,
                        json.dumps(payload), json.dumps(metadata), event.recorded_at
                    )

                    await conn.execute(
                        """
                        INSERT INTO outbox (
                            event_id, destination, payload, published_at, attempts
                        ) VALUES ($1, $2, $3::jsonb, NULL, 0)
                        """,
                        event_row["event_id"], f"internal.{aggregate_type}", json.dumps({
                            "event_type": event.event_type, "payload": payload
                        })
                    )
                return new_version

    async def append_with_retry(
        self,
        stream_id: str,
        command_fn: Callable[[], Coroutine[Any, Any, tuple[list[DomainEvent], int]]],
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
    ) -> int:
        last_error = None
        for attempt in range(_RETRY_MAX_ATTEMPTS):
            if attempt > 0:
                delay_ms = _RETRY_BASE_MS * (_RETRY_FACTOR ** (attempt - 1))
                await asyncio.sleep(delay_ms / 1000)
            try:
                events, expected_version = await command_fn()
                if not events:
                    return expected_version
                return await self.append(
                    stream_id, events, expected_version,
                    correlation_id=correlation_id, causation_id=causation_id
                )
            except OptimisticConcurrencyError as e:
                last_error = e
        raise last_error

    async def load_stream(self, stream_id: str, from_position: int = 0, to_position: Optional[int] = None) -> list[StoredEvent]:
        async with self._pool.acquire() as conn:
            query = "SELECT * FROM events WHERE stream_id = $1 AND stream_position > $2"
            args = [stream_id, from_position]
            if to_position is not None:
                query += f" AND stream_position <= ${len(args) + 1}"
                args.append(to_position)
            query += " ORDER BY stream_position"
            rows = await conn.fetch(query, *args)
        return [StoredEvent(row) for row in rows]
        
    async def load_all(self, from_global_position: int = 0, event_types: Optional[list[str]] = None, batch_size: int = 100):
        # async generator for replay
        async with self._pool.acquire() as conn:
            current_pos = from_global_position
            while True:
                query = "SELECT * FROM events WHERE global_position > $1"
                args = [current_pos]
                if event_types:
                    query += f" AND event_type = ANY(${len(args) + 1})"
                    args.append(event_types)
                query += f" ORDER BY global_position ASC LIMIT ${len(args) + 1}"
                args.append(batch_size)
                
                rows = await conn.fetch(query, *args)
                if not rows:
                    break
                for row in rows:
                    yield StoredEvent(row)
                current_pos = rows[-1]["global_position"]

    async def stream_version(self, stream_id: str) -> int:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT current_version FROM event_streams WHERE stream_id = $1", stream_id
            )
        return result or 0

    async def archive_stream(self, stream_id: str) -> bool:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1 AND archived_at IS NULL",
                stream_id
            )
        return result.split()[-1] != "0"
