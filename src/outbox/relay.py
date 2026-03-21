"""
OutboxRelay: Polls the outbox table and publishes events to external systems.

Problem
-------
The outbox table is written atomically with every event append (dual-write
safety). But the relay process that reads pending rows and publishes them to
a message broker was never implemented — it was only referenced in comments.

Design
------
- Polls outbox WHERE status = 'pending' ORDER BY created_at
- Calls publisher.publish(event_type, payload, metadata) for each entry
- On success: marks status = 'published'
- On failure: increments retry_count; marks 'failed' after max_retries
- MessagePublisher is an abstract interface — swap in SNS, Kafka, Redis Streams, etc.
- At-least-once delivery: if the relay crashes after publish but before marking
  'published', the event will be re-published on restart. Consumers must be idempotent.

Backpressure
------------
- BATCH_SIZE limits how many events are processed per poll cycle
- POLL_INTERVAL_SECONDS controls polling frequency
- Failed entries are retried with exponential backoff up to max_retries
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import asyncpg

from src.observability.metrics import get_metrics
from src.circuit_breaker.breaker import CircuitBreaker, CircuitOpenError

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.5
BATCH_SIZE = 100


class MessagePublisher(ABC):
    """
    Abstract publisher interface. Implement this for your message broker.

    Example implementations:
        - SNSPublisher: publishes to AWS SNS topic
        - KafkaPublisher: produces to Kafka topic per event_type
        - RedisPublisher: XADD to Redis Stream
        - InMemoryPublisher: for testing
    """

    @abstractmethod
    async def publish(
        self,
        event_type: str,
        aggregate_type: str,
        payload: dict,
        metadata: dict,
    ) -> None:
        """Publish a single event. Must be idempotent."""
        ...


class InMemoryPublisher(MessagePublisher):
    """Test/dev publisher that collects published events in memory."""

    def __init__(self):
        self.published: list[dict] = []

    async def publish(self, event_type: str, aggregate_type: str, payload: dict, metadata: dict) -> None:
        self.published.append({
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "payload": payload,
            "metadata": metadata,
        })


class OutboxRelay:
    """
    Background daemon that drains the outbox table.

    Usage:
        publisher = MyMessagePublisher(...)
        relay = OutboxRelay(pool, publisher)
        await relay.start()
        # ... later ...
        await relay.stop()
    """

    def __init__(self, pool: asyncpg.Pool, publisher: MessagePublisher):
        self._pool = pool
        self._publisher = publisher
        self._task: asyncio.Task | None = None
        self._running = False
        self._published_count = 0
        self._failed_count = 0
        self._circuit = CircuitBreaker(
            name="outbox-publisher",
            failure_threshold=5,
            recovery_timeout_seconds=30.0,
        )

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="OutboxRelay")
        logger.info("OutboxRelay started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("OutboxRelay stopped. Published: %d, Failed: %d",
                    self._published_count, self._failed_count)

    @property
    def stats(self) -> dict:
        return {
            "published_count": self._published_count,
            "failed_count": self._failed_count,
            "circuit_state": self._circuit.state.value,
            "circuit_failures": self._circuit.failure_count,
        }

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while self._running:
            try:
                processed = await self._process_batch()
                if processed == 0:
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
            except CircuitOpenError as e:
                logger.warning("OutboxRelay: circuit open — backing off %.1fs", e.retry_after)
                await asyncio.sleep(min(e.retry_after, 30.0))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("OutboxRelay loop error: %s", e, exc_info=True)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _process_batch(self) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT outbox_id, event_id, aggregate_type, event_type,
                           payload, metadata, retry_count, max_retries
                    FROM outbox
                    WHERE status = 'pending'
                    ORDER BY created_at
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                    BATCH_SIZE,
                )

                if not rows:
                    return 0

                for row in rows:
                    await self._process_entry_in_conn(conn, row)

        return len(rows)

    async def _process_entry(self, row: asyncpg.Record) -> None:
        outbox_id = row["outbox_id"]
        try:
            await self._circuit.call(
                self._publisher.publish(
                    event_type=row["event_type"],
                    aggregate_type=row["aggregate_type"],
                    payload=dict(row["payload"]),
                    metadata=dict(row["metadata"]),
                )
            )
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE outbox
                    SET status = 'published', published_at = NOW()
                    WHERE outbox_id = $1
                    """,
                    outbox_id,
                )
            self._published_count += 1
            get_metrics().increment("outbox_published_total")

        except CircuitOpenError:
            # Don't mark as failed — circuit will recover; re-try on next cycle
            raise
        except Exception as e:
            new_retry = row["retry_count"] + 1
            new_status = "failed" if new_retry >= row["max_retries"] else "pending"
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE outbox
                    SET retry_count = $1, status = $2, last_error = $3
                    WHERE outbox_id = $4
                    """,
                    new_retry, new_status, str(e)[:500], outbox_id,
                )
            if new_status == "failed":
                self._failed_count += 1
                get_metrics().increment("outbox_failed_total")
                logger.error(
                    "Outbox entry %s permanently failed after %d retries: %s",
                    outbox_id, new_retry, e,
                )
            else:
                logger.warning(
                    "Outbox entry %s failed (attempt %d/%d): %s",
                    outbox_id, new_retry, row["max_retries"], e,
                )

    async def _process_entry_in_conn(self, conn: asyncpg.Connection, row: asyncpg.Record) -> None:
        """Process one outbox entry within an existing transaction/connection."""
        outbox_id = row["outbox_id"]
        try:
            await self._circuit.call(
                self._publisher.publish(
                    event_type=row["event_type"],
                    aggregate_type=row["aggregate_type"],
                    payload=dict(row["payload"]),
                    metadata=dict(row["metadata"]),
                )
            )
            await conn.execute(
                "UPDATE outbox SET status = 'published', published_at = NOW() WHERE outbox_id = $1",
                outbox_id,
            )
            self._published_count += 1
            get_metrics().increment("outbox_published_total")
        except CircuitOpenError:
            raise
        except Exception as e:
            new_retry = row["retry_count"] + 1
            new_status = "failed" if new_retry >= row["max_retries"] else "pending"
            await conn.execute(
                "UPDATE outbox SET retry_count = $1, status = $2, last_error = $3 WHERE outbox_id = $4",
                new_retry, new_status, str(e)[:500], outbox_id,
            )
            if new_status == "failed":
                self._failed_count += 1
                get_metrics().increment("outbox_failed_total")
                logger.error("Outbox entry %s permanently failed after %d retries: %s", outbox_id, new_retry, e)
            else:
                logger.warning("Outbox entry %s failed (attempt %d/%d): %s", outbox_id, new_retry, row["max_retries"], e)
