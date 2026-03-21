"""
StreamArchiver: Marks terminal loan application streams as archived.

Design
------
- archive_streams(): marks streams as archived where updated_at < before_date
  and the application is in a terminal state (FinalApproved/Denied/Referred/Withdrawn).
- list_archivable(): returns streams eligible for archival without modifying them.
- restore_stream(): unmarks the archived flag for a stream.

The events table is NEVER modified — immutability is preserved.
Archival only sets archived_at on event_streams. A separate cold-storage
process can copy events to cheaper storage using the archived_at flag.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

# Terminal statuses — streams in these states are eligible for archival
TERMINAL_STATUSES = {"FinalApproved", "Denied", "Referred", "Withdrawn"}


@dataclass
class ArchivableStream:
    stream_id: UUID
    aggregate_id: UUID
    aggregate_type: str
    current_version: int
    updated_at: datetime
    archived_at: Optional[datetime]


class StreamArchiver:
    """
    Archives terminal event streams to reduce hot-storage footprint.

    Usage:
        archiver = StreamArchiver(pool)
        candidates = await archiver.list_archivable(before_date=datetime(2025, 1, 1))
        count = await archiver.archive_streams(before_date=datetime(2025, 1, 1))
        await archiver.restore_stream(aggregate_id)
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def list_archivable(self, before_date: datetime) -> list[ArchivableStream]:
        """
        Return streams eligible for archival:
        - updated_at < before_date
        - application is in a terminal status
        - not already archived
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.stream_id, s.aggregate_id, s.aggregate_type,
                       s.current_version, s.updated_at, s.archived_at
                FROM event_streams s
                JOIN application_summary_projection p ON s.aggregate_id = p.application_id
                WHERE s.updated_at < $1
                  AND p.status = ANY($2::text[])
                  AND s.archived_at IS NULL
                ORDER BY s.updated_at
                """,
                before_date, list(TERMINAL_STATUSES),
            )
        return [
            ArchivableStream(
                stream_id=row["stream_id"],
                aggregate_id=row["aggregate_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                updated_at=row["updated_at"],
                archived_at=row["archived_at"],
            )
            for row in rows
        ]

    async def archive_streams(
        self, before_date: datetime, dry_run: bool = False
    ) -> int:
        """
        Archive all eligible streams updated before before_date.

        Args:
            before_date: Archive streams not updated since this date.
            dry_run: If True, return count without modifying anything.

        Returns:
            Number of streams archived (or would be archived in dry_run mode).
        """
        candidates = await self.list_archivable(before_date)
        if dry_run:
            logger.info("Dry run: %d streams would be archived", len(candidates))
            return len(candidates)

        if not candidates:
            return 0

        stream_ids = [c.stream_id for c in candidates]
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = ANY($1::uuid[])
                  AND archived_at IS NULL
                """,
                stream_ids,
            )

        count = int(result.split()[-1])
        logger.info("Archived %d streams (before_date=%s)", count, before_date)
        return count

    async def archive_stream(self, aggregate_type: str, aggregate_id: UUID) -> bool:
        """
        Archive a single stream by aggregate_type + aggregate_id.
        Returns True if the stream was found and newly archived.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NOW(), updated_at = NOW()
                WHERE aggregate_type = $1
                  AND aggregate_id = $2
                  AND archived_at IS NULL
                """,
                aggregate_type,
                aggregate_id,
            )
        archived = result.split()[-1] != "0"
        if archived:
            logger.info("Archived stream %s/%s", aggregate_type, aggregate_id)
        return archived

    async def restore_stream(self, aggregate_id: UUID) -> bool:
        """
        Unmark a stream as archived. Returns True if the stream was found and restored.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NULL
                WHERE aggregate_id = $1 AND archived_at IS NOT NULL
                """,
                aggregate_id,
            )
        restored = result.split()[-1] != "0"
        if restored:
            logger.info("Stream for aggregate %s restored from archive", aggregate_id)
        return restored
