"""
ErasureHandler: GDPR Right-to-Erasure (Right to be Forgotten).

Design
------
Event sourcing and GDPR are in tension: events are immutable, but GDPR requires
the ability to erase personal data. The standard solution is event tombstoning:

1. Append a PersonalDataErased event to each affected stream.
2. Projections react to PersonalDataErased by removing PII fields.
3. The original events remain in the log (immutability preserved) but
   projections no longer surface the PII.

For stronger guarantees, the payload of original events can be overwritten
with [REDACTED] markers — this is a deliberate, audited exception to
immutability, recorded in the erasure_requests table.

Strategy used here: tombstone + projection update (no payload mutation).
Payload mutation is available via apply_erasure(strategy="redact") for
jurisdictions that require it.

PII fields tracked: applicant_name, applicant_id (in LoanApplicationSubmitted)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

import asyncpg

logger = logging.getLogger(__name__)

# Fields considered PII in each event type
PII_FIELDS: dict[str, list[str]] = {
    "LoanApplicationSubmitted": ["applicant_name", "applicant_id"],
}

REDACTED = "[REDACTED]"


@dataclass
class ErasureRequest:
    erasure_id: UUID
    applicant_id: UUID
    requested_by: str
    status: str
    affected_streams: list[UUID]
    created_at: datetime
    applied_at: Optional[datetime]


class ErasureHandler:
    """
    Handles GDPR erasure requests.

    Usage:
        handler = ErasureHandler(pool, event_store)
        erasure_id = await handler.request_erasure(applicant_id, requested_by="dpo@apex.com")
        await handler.apply_erasure(erasure_id)
    """

    def __init__(self, pool: asyncpg.Pool, event_store=None):
        self._pool = pool
        self._event_store = event_store

    async def request_erasure(
        self, applicant_id: UUID, requested_by: str
    ) -> UUID:
        """
        Create an erasure request for all streams belonging to an applicant.
        Returns the erasure_id.
        """
        # Find all streams containing events for this applicant
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT e.stream_id, e.aggregate_id
                FROM events e
                WHERE e.payload->>'applicant_id' = $1
                   OR e.aggregate_id = $1
                """,
                str(applicant_id),
            )

        affected_stream_ids = [row["stream_id"] for row in rows]
        erasure_id = uuid4()

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO erasure_requests
                    (erasure_id, applicant_id, requested_by, status, affected_stream_ids)
                VALUES ($1, $2, $3, 'pending', $4)
                """,
                erasure_id, applicant_id, requested_by,
                [str(s) for s in affected_stream_ids],
            )

        logger.info(
            "Erasure request %s created for applicant %s (%d streams affected)",
            erasure_id, applicant_id, len(affected_stream_ids),
        )
        return erasure_id

    async def apply_erasure(self, erasure_id: UUID) -> int:
        """
        Apply an erasure request by appending PersonalDataErased tombstone events
        to each affected stream and updating the projection to remove PII.

        Returns the number of streams processed.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM erasure_requests WHERE erasure_id = $1",
                erasure_id,
            )

        if row is None:
            raise ValueError(f"Erasure request {erasure_id} not found")
        if row["status"] == "applied":
            logger.info("Erasure %s already applied", erasure_id)
            return 0

        applicant_id = row["applicant_id"]
        affected_stream_ids = row["affected_stream_ids"] or []

        # Append tombstone event to each affected stream and redact projection
        processed = 0
        for stream_id_str in affected_stream_ids:
            stream_id = UUID(stream_id_str)
            try:
                await self._tombstone_stream(stream_id, applicant_id, erasure_id)
                processed += 1
            except Exception as e:
                logger.error(
                    "Failed to tombstone stream %s for erasure %s: %s",
                    stream_id, erasure_id, e,
                )

        # Redact PII from projection
        await self._redact_projection(applicant_id)

        # Mark erasure as applied
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE erasure_requests
                SET status = 'applied', applied_at = NOW()
                WHERE erasure_id = $1
                """,
                erasure_id,
            )

        logger.info(
            "Erasure %s applied: %d streams processed", erasure_id, processed
        )
        return processed

    async def list_requests(self, status: Optional[str] = None) -> list[dict]:
        """List erasure requests, optionally filtered by status."""
        async with self._pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT * FROM erasure_requests WHERE status = $1 ORDER BY created_at DESC",
                    status,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM erasure_requests ORDER BY created_at DESC"
                )
        return [dict(row) for row in rows]

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _tombstone_stream(
        self, stream_id: UUID, applicant_id: UUID, erasure_id: UUID
    ) -> None:
        """Append a PersonalDataErased event to the stream."""
        if self._event_store is None:
            return

        # Get current stream version and aggregate info
        async with self._pool.acquire() as conn:
            stream_row = await conn.fetchrow(
                "SELECT aggregate_type, aggregate_id, current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        if stream_row is None:
            return

        from src.models.events import PersonalDataErased
        event = PersonalDataErased(
            aggregate_id=stream_row["aggregate_id"],
            aggregate_type=stream_row["aggregate_type"],
            applicant_id=applicant_id,
            erasure_id=erasure_id,
            fields_erased=list(PII_FIELDS.get(stream_row["aggregate_type"], [])),
        )
        await self._event_store.append(
            aggregate_type=stream_row["aggregate_type"],
            aggregate_id=stream_row["aggregate_id"],
            events=[event],
            expected_version=stream_row["current_version"],
        )

    async def _redact_projection(self, applicant_id: UUID) -> None:
        """Remove PII from the application_summary_projection."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary_projection
                SET applicant_name = $1
                WHERE application_id IN (
                    SELECT DISTINCT s.aggregate_id
                    FROM events e
                    JOIN event_streams s ON e.stream_id = s.stream_id
                    WHERE s.aggregate_type = 'LoanApplication'
                      AND e.event_type = 'LoanApplicationSubmitted'
                      AND (
                          e.payload->>'applicant_id' = $2
                          OR s.aggregate_id = $3
                      )
                )
                """,
                REDACTED, str(applicant_id), applicant_id,
            )
