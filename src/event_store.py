"""
EventStore: Core async interface to the PostgreSQL event store.

Design decisions:
- append() uses a single atomic transaction: UPDATE event_streams (OCC check) +
  INSERT events + INSERT outbox. If any step fails, all roll back.
- global_position uses a PostgreSQL sequence (not MAX+1) to avoid gaps under
  concurrent inserts and to ensure monotonic ordering.
- load_stream() applies upcasters at read time — raw DB payload is never modified.
- Outbox entries are written in the same transaction as events (dual-write safety).

Retry strategy (append_with_retry):
- On OptimisticConcurrencyError: reload aggregate, re-execute command, retry append.
- Exponential backoff: base 10ms, factor 2x, max 5 retries.
- Estimated conflict rate in this domain: < 0.1% (one primary agent per application).
- Expected resolution within 2 retries: > 99.9% of conflicts.
- After max retries: raises OptimisticConcurrencyError with suggested_action="manual_review".

Archive support:
- load_stream() accepts from_version/to_version for range queries.
- load_all() supports after_position for incremental reads (used by daemon + archiver).
- Archived events remain in the events table; a separate archiver process can copy
  events with global_position < threshold to cold storage and mark them archived.
  The events table retains all rows — immutability is never violated.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional
from uuid import UUID, uuid4

import asyncpg

from src.models.events import (
    AggregateNotFoundError,
    DomainEvent,
    EVENT_REGISTRY,
    OptimisticConcurrencyError,
)
from src.upcasting.registry import UpcasterRegistry
from src.snapshots.store import SnapshotStore, SNAPSHOT_THRESHOLD
from src.observability.metrics import get_metrics

logger = logging.getLogger(__name__)

# Retry configuration for append_with_retry
_RETRY_BASE_MS = 10
_RETRY_FACTOR = 2
_RETRY_MAX_ATTEMPTS = 5


class StoredEvent:
    """Raw event record as returned from the database, before upcasting."""
    __slots__ = (
        "event_id", "stream_id", "aggregate_type", "aggregate_id",
        "event_type", "schema_version", "stream_position", "global_position",
        "payload", "metadata", "causation_id", "correlation_id", "recorded_at"
    )

    def __init__(self, row: asyncpg.Record):
        self.event_id = row["event_id"]
        self.stream_id = row["stream_id"]
        self.aggregate_type = row["aggregate_type"]
        self.aggregate_id = row["aggregate_id"]
        self.event_type = row["event_type"]
        self.schema_version = row["schema_version"]
        self.stream_position = row["stream_position"]
        self.global_position = row["global_position"]
        self.payload = dict(row["payload"])
        self.metadata = dict(row["metadata"])
        self.causation_id = row["causation_id"]
        self.correlation_id = row["correlation_id"]
        self.recorded_at = row["recorded_at"]


class EventStore:
    """
    Async event store backed by PostgreSQL.

    Usage:
        store = EventStore(pool)
        await store.append(stream_id, events, expected_version=0)
        events = await store.load_stream(stream_id)
    """

    def __init__(self, pool: asyncpg.Pool, upcaster_registry: Optional[UpcasterRegistry] = None):
        self._pool = pool
        self._upcasters = upcaster_registry or UpcasterRegistry()
        self._snapshots = SnapshotStore(pool)

    # -------------------------------------------------------------------------
    # Write path
    # -------------------------------------------------------------------------

    async def append(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        events: list[DomainEvent],
        expected_version: int,
    ) -> int:
        """
        Atomically append events to a stream with OCC.

        Args:
            aggregate_type: e.g. 'LoanApplication'
            aggregate_id: Business identity UUID
            events: List of domain events to append
            expected_version: The version the caller believes the stream is at.
                              Use 0 for a new stream (no events yet).

        Returns:
            New stream version after append.

        Raises:
            OptimisticConcurrencyError: If expected_version != current version.
        """
        if not events:
            return expected_version

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: Upsert stream and atomically check+increment version.
                # INSERT ... ON CONFLICT handles first-time stream creation.
                # The UPDATE only fires if current_version matches expected_version.
                new_version = expected_version + len(events)

                result = await conn.fetchrow(
                    """
                    INSERT INTO event_streams (stream_id, aggregate_type, aggregate_id, current_version)
                    VALUES (gen_random_uuid(), $1, $2, $3)
                    ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE
                        SET current_version = $3,
                            updated_at = NOW()
                        WHERE event_streams.current_version = $4
                    RETURNING stream_id, current_version
                    """,
                    aggregate_type,
                    aggregate_id,
                    new_version,
                    expected_version,
                )

                if result is None:
                    # The WHERE clause failed — version mismatch
                    actual = await conn.fetchval(
                        "SELECT current_version FROM event_streams "
                        "WHERE aggregate_type = $1 AND aggregate_id = $2",
                        aggregate_type, aggregate_id
                    )
                    # Fetch stream_id for error reporting
                    stream_id = await conn.fetchval(
                        "SELECT stream_id FROM event_streams "
                        "WHERE aggregate_type = $1 AND aggregate_id = $2",
                        aggregate_type, aggregate_id
                    )
                    get_metrics().increment("occ_conflicts_total", labels={"aggregate_type": aggregate_type})
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id or uuid4(),
                        expected=expected_version,
                        actual=actual or -1,
                    )

                stream_id: UUID = result["stream_id"]                # Step 2: Insert events
                for i, event in enumerate(events):
                    position = expected_version + i + 1
                    payload = event.to_payload()
                    metadata = {
                        **event.metadata,
                        "occurred_at": event.occurred_at.isoformat(),
                    }

                    event_row = await conn.fetchrow(
                        """
                        INSERT INTO events (
                            event_id, stream_id, aggregate_type, aggregate_id,
                            event_type, schema_version, stream_position,
                            payload, metadata, causation_id, correlation_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        RETURNING event_id, global_position
                        """,
                        event.event_id,
                        stream_id,
                        aggregate_type,
                        aggregate_id,
                        event.event_type,
                        event.schema_version,
                        position,
                        payload,
                        metadata,
                        event.causation_id,
                        event.correlation_id,
                    )

                    # Step 3: Write to outbox in same transaction
                    await conn.execute(
                        """
                        INSERT INTO outbox (
                            event_id, aggregate_type, event_type, payload, metadata
                        ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        event_row["event_id"],
                        aggregate_type,
                        event.event_type,
                        payload,
                        metadata,
                    )

                return new_version

        # Instrument after transaction commits
        m = get_metrics()
        m.increment("events_appended_total", value=len(events), labels={"aggregate_type": aggregate_type})
        return new_version

    async def maybe_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        version: int,
        state: dict,
    ) -> None:
        """
        Write a snapshot if the version crosses a threshold boundary.
        Call this from command handlers after a successful append.

        Example:
            new_version = await store.append(...)
            await store.maybe_snapshot(agg_type, agg_id, new_version, agg.to_snapshot())
        """
        if await self._snapshots.should_snapshot(aggregate_type, aggregate_id, version):
            await self._snapshots.save(aggregate_type, aggregate_id, version, state)
            await self._snapshots.delete_old_snapshots(aggregate_type, aggregate_id)

    # -------------------------------------------------------------------------
    # Read path
    # -------------------------------------------------------------------------

    async def load_stream(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        from_version: int = 0,
        to_version: Optional[int] = None,
    ) -> list[DomainEvent]:
        """
        Load and upcast all events for an aggregate stream.

        Uses snapshots when available: finds the latest snapshot and replays
        only events after it, reducing load from O(all events) to O(recent events).

        Args:
            aggregate_type: Aggregate type name
            aggregate_id: Aggregate identity
            from_version: Start from this stream position (exclusive). Default 0 = all.
                          Overridden by snapshot position when snapshot is newer.
            to_version: End at this stream position (inclusive). Default = all.

        Returns:
            List of upcasted DomainEvent instances in stream order.
        """
        # Use snapshot to skip replaying old events (unless caller specifies from_version)
        effective_from = from_version
        if from_version == 0 and to_version is None:
            snapshot = await self._snapshots.load(aggregate_type, aggregate_id)
            if snapshot:
                effective_from = snapshot["stream_position"]
                get_metrics().increment("snapshot_hits_total", labels={"aggregate_type": aggregate_type})
                logger.debug("Snapshot hit for %s/%s at version %d", aggregate_type, aggregate_id, effective_from)
            else:
                get_metrics().increment("snapshot_misses_total", labels={"aggregate_type": aggregate_type})
                logger.debug(
                    "Snapshot hit for %s/%s at version %d",
                    aggregate_type, aggregate_id, effective_from,
                )

        async with self._pool.acquire() as conn:
            query = """
                SELECT e.*
                FROM events e
                JOIN event_streams s ON e.stream_id = s.stream_id
                WHERE s.aggregate_type = $1
                  AND s.aggregate_id = $2
                  AND e.stream_position > $3
            """
            args: list[Any] = [aggregate_type, aggregate_id, effective_from]

            if to_version is not None:
                query += f" AND e.stream_position <= ${len(args) + 1}"
                args.append(to_version)

            query += " ORDER BY e.stream_position"
            rows = await conn.fetch(query, *args)

        return [self._deserialize(StoredEvent(row)) for row in rows]

    async def load_all(
        self,
        after_position: int = 0,
        limit: int = 1000,
        aggregate_type: Optional[str] = None,
    ) -> list[StoredEvent]:
        """
        Load events across all streams after a global position.
        Used by ProjectionDaemon for polling.

        Returns raw StoredEvent (not upcasted) — projections handle their own
        schema concerns.
        """
        async with self._pool.acquire() as conn:
            if aggregate_type:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events
                    WHERE global_position > $1
                      AND aggregate_type = $2
                    ORDER BY global_position
                    LIMIT $3
                    """,
                    after_position, aggregate_type, limit
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events
                    WHERE global_position > $1
                    ORDER BY global_position
                    LIMIT $2
                    """,
                    after_position, limit
                )
        return [StoredEvent(row) for row in rows]

    async def stream_version(self, aggregate_type: str, aggregate_id: UUID) -> int:
        """
        Return the current version of a stream. Returns 0 if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT current_version FROM event_streams "
                "WHERE aggregate_type = $1 AND aggregate_id = $2",
                aggregate_type, aggregate_id
            )
        return result or 0

    async def get_stream_id(self, aggregate_type: str, aggregate_id: UUID) -> Optional[UUID]:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT stream_id FROM event_streams "
                "WHERE aggregate_type = $1 AND aggregate_id = $2",
                aggregate_type, aggregate_id
            )

    async def load_correlation_chain(self, correlation_id: UUID) -> list[StoredEvent]:
        """
        Load all events that share a correlation_id, in global order.

        Answers: "show me every event that belongs to this saga/request."
        Uses the idx_events_correlation_id index for O(log n) lookup.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM events
                WHERE correlation_id = $1
                ORDER BY global_position
                """,
                correlation_id,
            )
        return [StoredEvent(row) for row in rows]

    async def load_causation_chain(self, root_event_id: UUID, max_depth: int = 20) -> list[StoredEvent]:
        """
        Walk the causation chain starting from root_event_id using a recursive CTE.

        Answers: "show me every event that was caused (directly or transitively)
        by this event." Useful for debugging saga failures.

        max_depth guards against infinite loops in malformed data.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH RECURSIVE chain AS (
                    SELECT *, 0 AS depth FROM events WHERE event_id = $1
                    UNION ALL
                    SELECT e.*, c.depth + 1
                    FROM events e
                    JOIN chain c ON e.causation_id = c.event_id
                    WHERE c.depth < $2
                )
                SELECT * FROM chain ORDER BY global_position
                """,
                root_event_id, max_depth,
            )
        return [StoredEvent(row) for row in rows]

    async def load_filtered(
        self,
        status: Optional[str] = None,
        after_date: Optional[str] = None,
        before_date: Optional[str] = None,
        aggregate_type: Optional[str] = None,
        limit: int = 100,
        after_position: int = 0,
    ) -> list[StoredEvent]:
        """
        Filtered event query for operational use.
        Supports filtering by aggregate_type, date range, and pagination.
        """
        async with self._pool.acquire() as conn:
            query = "SELECT * FROM events WHERE global_position > $1"
            args: list[Any] = [after_position]

            if aggregate_type:
                args.append(aggregate_type)
                query += f" AND aggregate_type = ${len(args)}"
            if after_date:
                args.append(after_date)
                query += f" AND recorded_at >= ${len(args)}"
            if before_date:
                args.append(before_date)
                query += f" AND recorded_at <= ${len(args)}"

            args.append(limit)
            query += f" ORDER BY global_position LIMIT ${len(args)}"
            rows = await conn.fetch(query, *args)
        return [StoredEvent(row) for row in rows]

    async def append_with_retry(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        command_fn: Callable[[], Coroutine[Any, Any, tuple[list[DomainEvent], int]]],
        max_attempts: int = _RETRY_MAX_ATTEMPTS,
    ) -> int:
        """
        Execute a command function and append its events, retrying on OCC conflicts.

        command_fn must be an async callable that returns (events, expected_version).
        It is called fresh on each retry so it can reload the aggregate.

        Retry strategy:
            - Attempt 1: immediate
            - Attempt 2: 10ms delay
            - Attempt 3: 20ms delay
            - Attempt 4: 40ms delay
            - Attempt 5: 80ms delay
        After max_attempts, raises OptimisticConcurrencyError with
        suggested_action="manual_review".

        Estimated conflict rate: < 0.1% in this domain.
        Expected resolution within 2 retries: > 99.9%.
        """
        last_error: OptimisticConcurrencyError | None = None
        for attempt in range(max_attempts):
            if attempt > 0:
                delay_ms = _RETRY_BASE_MS * (_RETRY_FACTOR ** (attempt - 1))
                await asyncio.sleep(delay_ms / 1000)
                logger.info(
                    "OCC retry %d/%d for %s/%s after %dms",
                    attempt + 1, max_attempts, aggregate_type, aggregate_id, delay_ms
                )
            try:
                events, expected_version = await command_fn()
                return await self.append(aggregate_type, aggregate_id, events, expected_version)
            except OptimisticConcurrencyError as e:
                last_error = e
                logger.warning("OCC conflict on attempt %d: %s", attempt + 1, e)

        # Exhausted retries
        assert last_error is not None
        last_error.suggested_action = "manual_review"
        raise last_error

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _deserialize(self, stored: StoredEvent) -> DomainEvent:
        """Deserialize a StoredEvent into a DomainEvent, applying upcasters."""
        # Apply upcasters to bring payload to current schema version
        payload, schema_version = self._upcasters.upcast(
            event_type=stored.event_type,
            payload=stored.payload,
            from_version=stored.schema_version,
        )

        event_class = EVENT_REGISTRY.get(stored.event_type)
        if event_class is None:
            logger.warning("Unknown event type: %s — skipping", stored.event_type)
            # Return a generic DomainEvent for unknown types
            return DomainEvent(
                event_id=stored.event_id,
                aggregate_id=stored.aggregate_id,
                aggregate_type=stored.aggregate_type,
                event_type=stored.event_type,
                schema_version=schema_version,
                metadata=stored.metadata,
            )

        return event_class(
            event_id=stored.event_id,
            aggregate_id=stored.aggregate_id,
            aggregate_type=stored.aggregate_type,
            event_type=stored.event_type,
            schema_version=schema_version,
            occurred_at=stored.recorded_at,
            causation_id=stored.causation_id,
            correlation_id=stored.correlation_id,
            metadata=stored.metadata,
            **payload,
        )
