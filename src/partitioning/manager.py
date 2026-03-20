"""
PartitionManager: PostgreSQL range partitioning for the events table.

Problem
-------
The events table grows unboundedly. At 10k events/day that's ~3.6M rows/year.
Queries that scan by recorded_at (archival, replay, monitoring) degrade linearly.
PostgreSQL declarative partitioning by month keeps each partition small and
allows old partitions to be detached and moved to cold storage without downtime.

Design
------
- events table is partitioned by recorded_at (RANGE, monthly)
- Partitions are named: events_YYYY_MM
- ensure_partitions(months_ahead): creates partitions for current + N future months
- drop_old_partitions(before_date): detaches and drops partitions older than date
- list_partitions(): returns all current partition names + row counts

Schema note
-----------
The base events table must be created as:
    CREATE TABLE events (...) PARTITION BY RANGE (recorded_at);
This is a schema migration — existing unpartitioned tables need a one-time
migration (pg_partman or manual copy). This manager handles the ongoing
partition lifecycle after that migration.

For this project we create a partitioned shadow table events_partitioned
to avoid breaking the existing schema, and provide a migration helper.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class PartitionManager:
    """
    Manages monthly range partitions for the events table.

    Usage:
        manager = PartitionManager(pool)
        await manager.ensure_partitions(months_ahead=3)
        partitions = await manager.list_partitions()
        await manager.drop_old_partitions(before_date=date(2024, 1, 1))
    """

    TABLE = "events"

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def ensure_partitions(self, months_ahead: int = 3) -> list[str]:
        """
        Create monthly partitions for the current month + months_ahead future months.
        Safe to call repeatedly — uses IF NOT EXISTS.

        Returns list of partition names that were created.
        """
        created = []
        today = date.today()
        # Start from first day of current month
        current = date(today.year, today.month, 1)

        for _ in range(months_ahead + 1):
            name = self._partition_name(current)
            next_month = self._next_month(current)
            created_now = await self._create_partition(name, current, next_month)
            if created_now:
                created.append(name)
            current = next_month

        return created

    async def list_partitions(self) -> list[dict]:
        """
        List all partitions of the events table with their row counts.

        Returns list of dicts: {name, from_date, to_date, row_count}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    child.relname AS partition_name,
                    pg_get_expr(child.relpartbound, child.oid) AS bounds,
                    pg_stat_user_tables.n_live_tup AS row_count
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                LEFT JOIN pg_stat_user_tables ON pg_stat_user_tables.relname = child.relname
                WHERE parent.relname = $1
                ORDER BY child.relname
                """,
                self.TABLE,
            )
        return [
            {
                "name": row["partition_name"],
                "bounds": row["bounds"],
                "row_count": row["row_count"] or 0,
            }
            for row in rows
        ]

    async def drop_old_partitions(
        self, before_date: date, dry_run: bool = False
    ) -> list[str]:
        """
        Detach and drop partitions whose end date is before before_date.

        Args:
            before_date: Drop partitions ending before this date.
            dry_run: If True, return names without dropping.

        Returns list of partition names dropped (or would be dropped).
        """
        partitions = await self.list_partitions()
        to_drop = []

        for p in partitions:
            # Partition name format: events_YYYY_MM
            parts = p["name"].split("_")
            if len(parts) >= 3:
                try:
                    year, month = int(parts[-2]), int(parts[-1])
                    partition_end = self._next_month(date(year, month, 1))
                    if partition_end <= before_date:
                        to_drop.append(p["name"])
                except (ValueError, IndexError):
                    pass

        if dry_run:
            logger.info("Dry run: would drop %d partitions: %s", len(to_drop), to_drop)
            return to_drop

        for name in to_drop:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"ALTER TABLE {self.TABLE} DETACH PARTITION {name}"
                )
                await conn.execute(f"DROP TABLE IF EXISTS {name}")
            logger.info("Dropped partition %s", name)

        return to_drop

    async def get_partition_stats(self) -> dict:
        """Return summary stats across all partitions."""
        partitions = await self.list_partitions()
        total_rows = sum(p["row_count"] for p in partitions)
        return {
            "partition_count": len(partitions),
            "total_rows": total_rows,
            "partitions": partitions,
        }

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _partition_name(self, month_start: date) -> str:
        return f"{self.TABLE}_{month_start.year:04d}_{month_start.month:02d}"

    def _next_month(self, d: date) -> date:
        if d.month == 12:
            return date(d.year + 1, 1, 1)
        return date(d.year, d.month + 1, 1)

    async def _create_partition(
        self, name: str, from_date: date, to_date: date
    ) -> bool:
        """Create a partition. Returns True if created, False if already exists."""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {name}
                    PARTITION OF {self.TABLE}
                    FOR VALUES FROM ('{from_date}') TO ('{to_date}')
                    """
                )
            logger.info("Partition %s ensured (%s → %s)", name, from_date, to_date)
            return True
        except Exception as e:
            # Table already exists as a partition — not an error
            if "already exists" in str(e) or "already a partition" in str(e):
                return False
            logger.error("Failed to create partition %s: %s", name, e)
            raise
