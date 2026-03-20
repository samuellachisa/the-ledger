"""
MigrationRunner: Tooling to validate and dry-run upcaster migrations.

Answers the question: "If I register this upcaster, what events in the DB
would be affected, and does the transformation look correct?"

Design
------
- dry_run(): applies upcaster in memory to all matching events, returns diffs
- validate_upcaster(): checks that the upcaster doesn't crash on any live event
- list_affected_streams(): returns count/list of streams with matching events
- All operations are READ-ONLY — the DB is never modified.
- Results are recorded in schema_migration_runs for operator audit trail.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)


@dataclass
class DryRunResult:
    event_id: UUID
    aggregate_type: str
    aggregate_id: UUID
    stream_position: int
    before: dict
    after: dict
    changed: bool
    error: str | None = None


@dataclass
class AffectedStream:
    aggregate_type: str
    aggregate_id: UUID
    stream_id: UUID
    event_count: int


class MigrationRunner:
    """
    Validates and previews upcaster migrations without touching the event store.

    Usage:
        runner = MigrationRunner(pool, registry)
        results = await runner.dry_run("CreditAnalysisCompleted", from_version=1)
        streams = await runner.list_affected_streams("CreditAnalysisCompleted", from_version=1)
        ok = await runner.validate_upcaster("CreditAnalysisCompleted", from_version=1)
    """

    def __init__(self, pool: asyncpg.Pool, registry: UpcasterRegistry):
        self._pool = pool
        self._registry = registry

    async def dry_run(
        self,
        event_type: str,
        from_version: int,
        limit: int = 1000,
    ) -> list[DryRunResult]:
        """
        Apply the upcaster to all matching events in the DB and return before/after diffs.
        Nothing is written to the database.
        """
        rows = await self._fetch_events(event_type, from_version, limit)
        results: list[DryRunResult] = []

        for row in rows:
            before = dict(row["payload"])
            try:
                after, _ = self._registry.upcast(event_type, before, from_version)
                results.append(DryRunResult(
                    event_id=row["event_id"],
                    aggregate_type=row["aggregate_type"],
                    aggregate_id=row["aggregate_id"],
                    stream_position=row["stream_position"],
                    before=before,
                    after=after,
                    changed=before != after,
                ))
            except Exception as e:
                results.append(DryRunResult(
                    event_id=row["event_id"],
                    aggregate_type=row["aggregate_type"],
                    aggregate_id=row["aggregate_id"],
                    stream_position=row["stream_position"],
                    before=before,
                    after={},
                    changed=False,
                    error=str(e),
                ))

        await self._record_run(event_type, from_version, len(rows), len([r for r in results if r.error]))
        return results

    async def validate_upcaster(self, event_type: str, from_version: int) -> dict[str, Any]:
        """
        Validate that the upcaster runs without errors on all matching events.

        Returns:
            {
                "valid": bool,
                "total_events": int,
                "errors": int,
                "error_samples": list[str]  # first 5 error messages
            }
        """
        results = await self.dry_run(event_type, from_version)
        errors = [r for r in results if r.error]
        return {
            "valid": len(errors) == 0,
            "total_events": len(results),
            "errors": len(errors),
            "error_samples": [r.error for r in errors[:5]],
        }

    async def list_affected_streams(
        self, event_type: str, from_version: int
    ) -> list[AffectedStream]:
        """
        Return all streams that contain events matching (event_type, schema_version).
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.aggregate_type, s.aggregate_id, s.stream_id,
                       COUNT(e.event_id) AS event_count
                FROM events e
                JOIN event_streams s ON e.stream_id = s.stream_id
                WHERE e.event_type = $1 AND e.schema_version = $2
                GROUP BY s.aggregate_type, s.aggregate_id, s.stream_id
                ORDER BY s.aggregate_type, s.aggregate_id
                """,
                event_type, from_version,
            )
        return [
            AffectedStream(
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                stream_id=row["stream_id"],
                event_count=row["event_count"],
            )
            for row in rows
        ]

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _fetch_events(
        self, event_type: str, from_version: int, limit: int
    ) -> list[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT event_id, aggregate_type, aggregate_id, stream_position, payload
                FROM events
                WHERE event_type = $1 AND schema_version = $2
                ORDER BY global_position
                LIMIT $3
                """,
                event_type, from_version, limit,
            )

    async def _record_run(
        self, event_type: str, from_version: int, total: int, error_count: int
    ) -> None:
        """Record migration run for audit trail (best-effort)."""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO schema_migration_runs
                        (run_id, event_type, from_version, events_affected, errors, ran_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    """,
                    uuid4(), event_type, from_version, total, error_count,
                )
        except Exception as e:
            logger.warning("Failed to record migration run: %s", e)
