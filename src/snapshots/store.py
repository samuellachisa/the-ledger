"""
SnapshotStore: Periodic aggregate state snapshots to avoid full stream replay.

Design
------
Without snapshots, loading a LoanApplication with 15 events requires reading
and replaying all 15 events every time. For high-volume applications this
becomes a linear scan problem.

With snapshots:
  1. Write a snapshot every SNAPSHOT_THRESHOLD events
  2. On load: find the latest snapshot, replay only events AFTER it
  3. Snapshot data is the aggregate's serializable state dict

Snapshot schema versioning
--------------------------
snapshot_data includes a schema_version field. If the aggregate's state
shape changes, bump SNAPSHOT_SCHEMA_VERSION and add a migration in
_migrate_snapshot_data(). Old snapshots are still readable — they just
get migrated on load.

Immutability
------------
Snapshots are derived data. The event log remains the source of truth.
A snapshot can always be discarded and rebuilt from events.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

SNAPSHOT_THRESHOLD = 10   # Take a snapshot every N events
SNAPSHOT_SCHEMA_VERSION = 1


class SnapshotStore:
    """
    Reads and writes aggregate snapshots.

    Usage (in EventStore.load_stream):
        snapshot = await snapshot_store.load(aggregate_type, aggregate_id)
        if snapshot:
            aggregate = MyAggregate.from_snapshot(snapshot["data"])
            from_version = snapshot["stream_position"]
        else:
            aggregate = MyAggregate(aggregate_id)
            from_version = 0
        # load only events after from_version
        events = await event_store.load_stream(..., from_version=from_version)
        for e in events:
            aggregate.apply(e)
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def load(
        self, aggregate_type: str, aggregate_id: UUID
    ) -> Optional[dict[str, Any]]:
        """
        Load the latest snapshot for an aggregate.

        Returns:
            {
                "stream_position": int,
                "schema_version": int,
                "data": dict,          # aggregate state
            }
            or None if no snapshot exists.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_position, schema_version, snapshot_data
                FROM aggregate_snapshots
                WHERE aggregate_type = $1 AND aggregate_id = $2
                ORDER BY stream_position DESC
                LIMIT 1
                """,
                aggregate_type, aggregate_id,
            )
        if row is None:
            return None

        data = dict(row["snapshot_data"])
        data = self._migrate_snapshot_data(data, from_version=row["schema_version"])
        return {
            "stream_position": row["stream_position"],
            "schema_version": row["schema_version"],
            "data": data,
        }

    async def save(
        self,
        aggregate_type: str,
        aggregate_id: UUID,
        stream_position: int,
        state: dict[str, Any],
    ) -> None:
        """
        Persist a snapshot. Silently skips if one already exists at this position.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO aggregate_snapshots
                    (aggregate_type, aggregate_id, stream_position, snapshot_data, schema_version)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (aggregate_type, aggregate_id, stream_position) DO NOTHING
                """,
                aggregate_type, aggregate_id, stream_position,
                state, SNAPSHOT_SCHEMA_VERSION,
            )
        logger.debug(
            "Snapshot saved: %s/%s at version %d", aggregate_type, aggregate_id, stream_position
        )

    async def should_snapshot(
        self, aggregate_type: str, aggregate_id: UUID, current_version: int
    ) -> bool:
        """
        Returns True if a new snapshot should be taken.
        Snapshots every SNAPSHOT_THRESHOLD events, but only if no snapshot
        exists at or after the current threshold boundary.
        """
        if current_version % SNAPSHOT_THRESHOLD != 0:
            return False
        # Check if we already have a snapshot at this version
        async with self._pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT 1 FROM aggregate_snapshots
                WHERE aggregate_type = $1 AND aggregate_id = $2
                  AND stream_position = $3
                """,
                aggregate_type, aggregate_id, current_version,
            )
        return exists is None

    async def delete_old_snapshots(
        self, aggregate_type: str, aggregate_id: UUID, keep: int = 2
    ) -> None:
        """
        Prune old snapshots, keeping only the N most recent.
        Called periodically to prevent unbounded growth.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM aggregate_snapshots
                WHERE aggregate_type = $1 AND aggregate_id = $2
                  AND stream_position NOT IN (
                      SELECT stream_position FROM aggregate_snapshots
                      WHERE aggregate_type = $1 AND aggregate_id = $2
                      ORDER BY stream_position DESC
                      LIMIT $3
                  )
                """,
                aggregate_type, aggregate_id, keep,
            )

    # -------------------------------------------------------------------------
    # Snapshot migration
    # -------------------------------------------------------------------------

    @staticmethod
    def _migrate_snapshot_data(data: dict, from_version: int) -> dict:
        """
        Migrate snapshot data from an older schema version to current.
        Add migration steps here when aggregate state shape changes.
        """
        # v1 → current: no migration needed yet
        return data
