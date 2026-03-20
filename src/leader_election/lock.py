"""
LeaderElection: PostgreSQL advisory lock-based leader election.

Problem
-------
Running multiple instances of ProjectionDaemon or SagaManager (e.g. during
a rolling deploy) causes double-processing: both instances poll the same
events and apply them to projections, creating duplicate rows or incorrect
aggregate state.

Design
------
- Uses pg_try_advisory_lock(lock_id) — a session-level advisory lock.
- Only one instance can hold the lock at a time.
- The lock is automatically released when the connection closes (crash-safe).
- Leader polls to renew its lock; followers poll to detect leader failure.
- lock_id is a stable integer derived from the component name (hash).

Usage
-----
    election = LeaderElection(pool, "ProjectionDaemon")
    async with election:
        # Only runs on the leader
        await daemon.start()

    # Or manual:
    if await election.try_acquire():
        await daemon.start()
        # hold until stop
        await election.release()
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SECONDS = 5.0
LOCK_NAMESPACE = 0x4150_4558  # "APEX" in hex — upper 32 bits of the 64-bit lock key


def _lock_id(name: str) -> int:
    """Derive a stable 64-bit lock ID from a component name."""
    h = int(hashlib.md5(name.encode()).hexdigest()[:8], 16) & 0xFFFF_FFFF
    return (LOCK_NAMESPACE << 32) | h


class LeaderElection:
    """
    Advisory-lock-based leader election for a named component.

    Args:
        pool: asyncpg connection pool.
        name: Component name (e.g. "ProjectionDaemon"). Must be unique per component.
        heartbeat_interval: How often to log leader status (seconds).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        name: str,
        heartbeat_interval: float = HEARTBEAT_INTERVAL_SECONDS,
    ):
        self._pool = pool
        self._name = name
        self._lock_id = _lock_id(name)
        self._heartbeat_interval = heartbeat_interval
        self._conn: Optional[asyncpg.Connection] = None
        self._is_leader = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def try_acquire(self) -> bool:
        """
        Attempt to acquire the leader lock.
        Returns True if this instance is now the leader.
        The lock is held on a dedicated connection for the lifetime of leadership.
        """
        if self._conn is None:
            self._conn = await self._pool.acquire()

        result = await self._conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", self._lock_id
        )
        self._is_leader = bool(result)
        if self._is_leader:
            logger.info("LeaderElection[%s]: acquired lock (id=%d)", self._name, self._lock_id)
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name=f"LeaderHeartbeat-{self._name}"
            )
        else:
            logger.debug("LeaderElection[%s]: lock held by another instance", self._name)
        return self._is_leader

    async def release(self) -> None:
        """Release the leader lock."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._conn:
            try:
                await self._conn.execute(
                    "SELECT pg_advisory_unlock($1)", self._lock_id
                )
            except Exception:
                pass
            await self._pool.release(self._conn)
            self._conn = None

        self._is_leader = False
        logger.info("LeaderElection[%s]: lock released", self._name)

    async def wait_for_leadership(self, poll_interval: float = 2.0) -> None:
        """
        Block until this instance acquires the leader lock.
        Useful for standby instances that should take over when the leader dies.
        """
        while not await self.try_acquire():
            await asyncio.sleep(poll_interval)

    async def __aenter__(self) -> "LeaderElection":
        await self.wait_for_leadership()
        return self

    async def __aexit__(self, *_) -> None:
        await self.release()

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Periodically verify we still hold the lock (detect connection loss)."""
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                if self._conn:
                    still_held = await self._conn.fetchval(
                        """
                        SELECT COUNT(*) > 0
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND classid = ($1 >> 32)::int
                          AND objid = ($1 & x'FFFFFFFF'::bigint)::int
                          AND granted = true
                        """,
                        self._lock_id,
                    )
                    if not still_held:
                        logger.error(
                            "LeaderElection[%s]: lock lost! Marking as follower.", self._name
                        )
                        self._is_leader = False
                        return
                    logger.debug("LeaderElection[%s]: heartbeat OK", self._name)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("LeaderElection[%s]: heartbeat error: %s", self._name, e)
