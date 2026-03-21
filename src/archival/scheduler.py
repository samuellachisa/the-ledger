"""
ArchivalScheduler: Periodically archives old event streams to cold storage.

Runs as a background asyncio task. Archives streams whose last event is older
than ARCHIVAL_AGE_DAYS and that are in a terminal state (Approved/Denied/Referred).

Environment:
    ARCHIVAL_INTERVAL_HOURS  — how often to run (default 24)
    ARCHIVAL_AGE_DAYS        — archive streams older than this (default 90)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import asyncpg

from src.archival.archiver import StreamArchiver

logger = logging.getLogger(__name__)

ARCHIVAL_INTERVAL_HOURS = int(os.environ.get("ARCHIVAL_INTERVAL_HOURS", "24"))
ARCHIVAL_AGE_DAYS = int(os.environ.get("ARCHIVAL_AGE_DAYS", "90"))


class ArchivalScheduler:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._archiver = StreamArchiver(pool)
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ArchivalScheduler")
        logger.info("ArchivalScheduler started (interval=%dh, age=%dd)", ARCHIVAL_INTERVAL_HOURS, ARCHIVAL_AGE_DAYS)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict:
        """Archive all eligible streams. Returns count of archived streams."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT s.aggregate_type, s.aggregate_id
                FROM event_streams s
                JOIN application_summary_projection p ON p.application_id = s.aggregate_id
                WHERE s.archived_at IS NULL
                  AND s.aggregate_type = 'LoanApplication'
                  AND p.status IN ('Approved', 'Denied', 'Referred')
                  AND s.updated_at < NOW() - ($1 || ' days')::interval
                LIMIT 100
                """,
                str(ARCHIVAL_AGE_DAYS),
            )

        archived = 0
        for row in rows:
            try:
                await self._archiver.archive_stream(row["aggregate_type"], row["aggregate_id"])
                archived += 1
            except Exception as e:
                logger.error("Failed to archive %s/%s: %s", row["aggregate_type"], row["aggregate_id"], e)

        if archived:
            logger.info("ArchivalScheduler: archived %d streams", archived)
        return {"archived": archived}

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("ArchivalScheduler error: %s", e, exc_info=True)
            await asyncio.sleep(ARCHIVAL_INTERVAL_HOURS * 3600)
