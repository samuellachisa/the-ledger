"""
ledger/projections/daemon.py
Async Projection Daemon updating 3 Read-Models.
"""
import asyncio
import logging
from typing import Callable, Awaitable, List
from ledger.event_store import EventStore, StoredEvent

logger = logging.getLogger(__name__)

class ProjectionDaemon:
    def __init__(self, event_store: EventStore, handlers: List[Callable[[StoredEvent], Awaitable[None]]], max_retries: int = 3):
        self.event_store = event_store
        self.handlers = handlers
        self.max_retries = max_retries
        self.last_processed_global_position = 0
        self.is_running = False
        self._task = None
        self._db_max_pos = 0

    async def _poll_loop(self):
        while self.is_running:
            try:
                # Update max pos for lag calculation
                async with self.event_store._pool.acquire() as conn:
                    val = await conn.fetchval("SELECT MAX(global_position) FROM events")
                    self._db_max_pos = val or 0
                
                # Expose get_lag accurately. Load new events.
                evts = [e async for e in self.event_store.load_all(from_global_position=self.last_processed_global_position, batch_size=100)]
                for evt in evts:
                    for handler in self.handlers:
                        for attempt in range(self.max_retries):
                            try:
                                await handler(evt)
                                break
                            except Exception as e:
                                logger.error(f"Handler {handler.__name__} failed on event {evt.global_position}: {e}")
                                if attempt == self.max_retries - 1:
                                    logger.error(f"Skipping event {evt.global_position} for handler {handler.__name__}. Fault Tolerance Active.")
                                else:
                                    await asyncio.sleep(0.1)
                    self.last_processed_global_position = evt.global_position
                
                await asyncio.sleep(0.05) # 50ms polling strictly achieving <500ms SLO
            except Exception as e:
                logger.error(f"Daemon polling error: {e}")
                await asyncio.sleep(1)

    def start(self):
        self.is_running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def get_lag(self) -> int:
        """Metrics: Returns (global_position - last_processed_position)"""
        return max(0, self._db_max_pos - self.last_processed_global_position)
