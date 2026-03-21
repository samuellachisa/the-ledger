"""
ErasureScheduler: Periodically applies pending GDPR erasure requests.

Runs as a background asyncio task. Picks up erasure_requests with status='pending'
and applies them, ensuring no request is left unenforced.

Environment:
    ERASURE_INTERVAL_MINUTES — how often to run (default 60)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional
from uuid import UUID

import asyncpg

from src.erasure.handler import ErasureHandler
from src.event_store import EventStore

logger = logging.getLogger(__name__)

ERASURE_INTERVAL_MINUTES = int(os.environ.get("ERASURE_INTERVAL_MINUTES", "60"))


class ErasureScheduler:
    def __init__(self, pool: asyncpg.Pool, event_store: EventStore):
        self._pool = pool
        self._handler = ErasureHandler(pool, event_store)
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ErasureScheduler")
        logger.info("ErasureScheduler started (interval=%dm)", ERASURE_INTERVAL_MINUTES)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict:
        """Apply all pending erasure requests. Returns count processed."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT erasure_id FROM erasure_requests WHERE status = 'pending' LIMIT 50"
            )

        processed = 0
        errors = 0
        for row in rows:
            try:
                await self._handler.apply_erasure(row["erasure_id"])
                processed += 1
            except Exception as e:
                errors += 1
                logger.error("ErasureScheduler: failed to apply erasure %s: %s", row["erasure_id"], e)

        if processed or errors:
            logger.info("ErasureScheduler: processed=%d errors=%d", processed, errors)
        return {"processed": processed, "errors": errors}

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("ErasureScheduler error: %s", e, exc_info=True)
            await asyncio.sleep(ERASURE_INTERVAL_MINUTES * 60)
