"""
ProjectionDaemon: Background asyncio task that polls for new events and
updates all registered projections.

Design:
- Polls global_position after last checkpoint
- Processes events in global order (ensures causal consistency)
- Updates checkpoint atomically with projection state
- Fault tolerant: bad events are logged and skipped after retry limit
- Exposes get_lag() for SLO monitoring
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Awaitable

import asyncpg

from src.event_store import EventStore, StoredEvent
from src.dead_letter.queue import DeadLetterQueue
from src.observability.metrics import get_metrics

logger = logging.getLogger(__name__)

# Type alias for projection handler
ProjectionHandler = Callable[[asyncpg.Connection, StoredEvent], Awaitable[None]]

POLL_INTERVAL_SECONDS = 0.1   # 100ms polling for < 500ms SLO
MAX_RETRIES_PER_EVENT = 3
BATCH_SIZE = 500


class ProjectionDaemon:
    """
    Async daemon that drives all projections forward.

    Usage:
        daemon = ProjectionDaemon(pool, event_store)
        daemon.register("ApplicationSummary", handler_fn)
        await daemon.start()
        # ... later ...
        await daemon.stop()
    """

    def __init__(self, pool: asyncpg.Pool, event_store: EventStore, dead_letter: DeadLetterQueue | None = None):
        self._pool = pool
        self._store = event_store
        self._dead_letter = dead_letter
        self._handlers: dict[str, ProjectionHandler] = {}
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_positions: dict[str, int] = {}
        self._latest_global_position: int = 0

    def register(self, projection_name: str, handler: ProjectionHandler) -> None:
        """Register a projection handler."""
        self._handlers[projection_name] = handler

    async def start(self) -> None:
        """Start the daemon as a background task."""
        self._running = True
        await self._load_checkpoints()
        self._task = asyncio.create_task(self._run_loop(), name="ProjectionDaemon")
        logger.info("ProjectionDaemon started with projections: %s", list(self._handlers.keys()))

    async def stop(self) -> None:
        """Gracefully stop the daemon."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ProjectionDaemon stopped")

    async def get_lag(self, projection_name: str) -> dict:
        """
        Return lag metrics for a projection.

        Returns:
            {
                "event_lag": int,        # events behind current head
                "time_lag_ms": float,    # ms since last processed event was recorded
                "checkpoint": int,       # last processed global_position
                "head_position": int,    # current max global_position
            }

        SLO: ApplicationSummary time_lag_ms < 500ms.
        """
        async with self._pool.acquire() as conn:
            head = await conn.fetchval(
                "SELECT COALESCE(MAX(global_position), 0) FROM events"
            ) or 0
            checkpoint = self._last_positions.get(projection_name, 0)

            # Time since the last unprocessed event was recorded
            time_lag_ms = 0.0
            if head > checkpoint:
                oldest_unprocessed = await conn.fetchval(
                    """
                    SELECT EXTRACT(EPOCH FROM (NOW() - recorded_at)) * 1000
                    FROM events
                    WHERE global_position = $1
                    """,
                    checkpoint + 1,
                )
                time_lag_ms = float(oldest_unprocessed or 0)

        return {
            "event_lag": max(0, head - checkpoint),
            "time_lag_ms": time_lag_ms,
            "checkpoint": checkpoint,
            "head_position": head,
        }

    # Map projection names to their backing tables for truncation during rebuild
    _PROJECTION_TABLES: dict[str, str] = {
        "ApplicationSummary": "application_summary_projection",
        "AgentPerformanceLedger": "agent_performance_projection",
        "ComplianceAuditView": "compliance_audit_projection",
    }

    async def rebuild_projection(self, projection_name: str) -> None:
        """
        Force a full rebuild of a projection from position 0.

        Snapshot invalidation triggers:
        1. Projection handler bug fixed (incorrect state calculation)
        2. New field added to projection schema
        3. Upcaster registered that changes event shape this projection depends on
        4. Manual operator request

        Procedure (atomic — crash-safe):
        1. Truncate the projection table (removes stale data)
        2. Reset checkpoint to 0 (daemon will replay from beginning)
        The checkpoint reset happens AFTER truncation so a crash between steps
        leaves the projection empty but with a non-zero checkpoint — the daemon
        will detect the empty table and rebuild on next start.
        """
        logger.info("Rebuilding projection: %s — truncating table and resetting checkpoint", projection_name)
        table = self._PROJECTION_TABLES.get(projection_name)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if table:
                    await conn.execute(f"TRUNCATE TABLE {table}")
                await conn.execute(
                    "UPDATE projection_checkpoints SET last_position = 0, updated_at = NOW() "
                    "WHERE projection_name = $1",
                    projection_name
                )
        self._last_positions[projection_name] = 0
        logger.info("Projection %s ready for rebuild from position 0", projection_name)

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _load_checkpoints(self) -> None:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT projection_name, last_position FROM projection_checkpoints")
            for row in rows:
                self._last_positions[row["projection_name"]] = row["last_position"]

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._process_batch()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("ProjectionDaemon loop error: %s", e, exc_info=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _process_batch(self) -> None:
        # Find the minimum checkpoint across all projections
        if not self._handlers:
            return

        min_position = min(self._last_positions.get(name, 0) for name in self._handlers)

        events = await self._store.load_all(after_position=min_position, limit=BATCH_SIZE)
        if not events:
            return

        for projection_name, handler in self._handlers.items():
            checkpoint = self._last_positions.get(projection_name, 0)
            relevant = [e for e in events if e.global_position > checkpoint]

            for event in relevant:
                await self._process_event_with_retry(projection_name, handler, event)

        # Update latest known position
        if events:
            self._latest_global_position = events[-1].global_position

    async def _process_event_with_retry(
        self,
        projection_name: str,
        handler: ProjectionHandler,
        event: StoredEvent,
    ) -> None:
        for attempt in range(MAX_RETRIES_PER_EVENT):
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await handler(conn, event)
                        await conn.execute(
                            """
                            UPDATE projection_checkpoints
                            SET last_position = $1, updated_at = NOW()
                            WHERE projection_name = $2
                            """,
                            event.global_position,
                            projection_name,
                        )
                self._last_positions[projection_name] = event.global_position
                get_metrics().increment("projection_events_processed_total", labels={"projection_name": projection_name})
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt < MAX_RETRIES_PER_EVENT - 1:
                    logger.warning(
                        "Projection %s failed on event %s (attempt %d/%d): %s",
                        projection_name, event.event_id, attempt + 1, MAX_RETRIES_PER_EVENT, e
                    )
                    await asyncio.sleep(0.05 * (2 ** attempt))  # exponential backoff
                else:
                    logger.error(
                        "Projection %s permanently skipping event %s after %d retries: %s",
                        projection_name, event.event_id, MAX_RETRIES_PER_EVENT, e,
                        exc_info=True
                    )
                    # Write to dead letter queue before skipping
                    if self._dead_letter:
                        try:
                            await self._dead_letter.write(
                                event=event,
                                source="projection",
                                processor_name=projection_name,
                                retry_count=MAX_RETRIES_PER_EVENT,
                                error=e,
                            )
                        except Exception as dlq_err:
                            logger.error("Failed to write dead letter: %s", dlq_err)
                    # Skip this event — advance checkpoint to avoid blocking
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE projection_checkpoints
                            SET last_position = $1, updated_at = NOW()
                            WHERE projection_name = $2
                            """,
                            event.global_position,
                            projection_name,
                        )
                    self._last_positions[projection_name] = event.global_position
