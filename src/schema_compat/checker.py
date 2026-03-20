"""
SchemaCompatibilityChecker: Validates that a new upcaster is backward-compatible
with existing snapshots stored in aggregate_snapshots.

Problem
-------
When a new upcaster is registered (e.g. CreditAnalysisCompleted v2→v3), any
existing snapshots that were taken at v2 will be loaded and then have events
replayed on top. If the upcaster changes field names or removes fields that
the snapshot depends on, the aggregate will silently get wrong state.

Design
------
- load_snapshot_schemas(): reads all distinct snapshot schemas from the DB
- check_upcaster_compatibility(): applies the upcaster to snapshot data and
  checks that required fields are still present
- check_field_removal(): detects if the upcaster removes a field that appears
  in existing snapshots
- check_field_rename(): detects potential renames (field disappears + new field appears)

This is a static analysis tool — it does not modify any data.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

import asyncpg

from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)


@dataclass
class CompatibilityIssue:
    severity: str  # "error" | "warning"
    issue_type: str
    description: str
    affected_snapshots: int = 0


class SchemaCompatibilityChecker:
    """
    Checks upcaster compatibility against existing snapshots.

    Usage:
        checker = SchemaCompatibilityChecker(pool, registry)
        issues = await checker.check_upcaster_compatibility(
            "CreditAnalysisCompleted", from_version=2
        )
        for issue in issues:
            print(issue.severity, issue.description)
    """

    def __init__(self, pool: asyncpg.Pool, registry: UpcasterRegistry):
        self._pool = pool
        self._registry = registry

    async def check_upcaster_compatibility(
        self,
        event_type: str,
        from_version: int,
    ) -> list[CompatibilityIssue]:
        """
        Run all compatibility checks for a given upcaster.

        Returns list of CompatibilityIssue (empty = all clear).
        """
        issues: list[CompatibilityIssue] = []

        # 1. Check the upcaster is actually registered
        key = (event_type, from_version)
        if key not in self._registry._upcasters:
            issues.append(CompatibilityIssue(
                severity="error",
                issue_type="MissingUpcaster",
                description=f"No upcaster registered for {event_type} v{from_version}→v{from_version+1}",
            ))
            return issues

        # 2. Load sample events from DB to test against
        sample_payloads = await self._load_sample_payloads(event_type, from_version)
        if not sample_payloads:
            issues.append(CompatibilityIssue(
                severity="warning",
                issue_type="NoSampleData",
                description=f"No events found for {event_type} v{from_version} — cannot validate",
            ))
            return issues

        # 3. Apply upcaster and check for crashes
        crash_count = 0
        field_removals: set[str] = set()
        all_before_fields: set[str] = set()
        all_after_fields: set[str] = set()

        for payload in sample_payloads:
            all_before_fields.update(payload.keys())
            try:
                after, _ = self._registry.upcast(event_type, payload, from_version)
                all_after_fields.update(after.keys())
            except Exception as e:
                crash_count += 1
                if crash_count == 1:
                    issues.append(CompatibilityIssue(
                        severity="error",
                        issue_type="UpcasterCrash",
                        description=f"Upcaster crashed on sample payload: {e}",
                        affected_snapshots=crash_count,
                    ))

        if crash_count > 1:
            # Update count on existing issue
            for issue in issues:
                if issue.issue_type == "UpcasterCrash":
                    issue.affected_snapshots = crash_count

        # 4. Detect field removals
        removed = all_before_fields - all_after_fields
        if removed:
            # Check if any snapshot uses these fields
            snapshot_count = await self._count_snapshots_with_fields(
                event_type, from_version, list(removed)
            )
            issues.append(CompatibilityIssue(
                severity="error" if snapshot_count > 0 else "warning",
                issue_type="FieldRemoval",
                description=f"Fields removed by upcaster: {sorted(removed)}. "
                            f"{snapshot_count} snapshots may be affected.",
                affected_snapshots=snapshot_count,
            ))

        # 5. Detect potential renames (field disappears + new field appears)
        added = all_after_fields - all_before_fields
        if removed and added:
            issues.append(CompatibilityIssue(
                severity="warning",
                issue_type="PotentialRename",
                description=f"Possible field rename: {sorted(removed)} → {sorted(added)}. "
                            "Verify snapshots don't depend on old field names.",
            ))

        if not issues:
            logger.info(
                "SchemaCompatibilityChecker: %s v%d is compatible (%d samples checked)",
                event_type, from_version, len(sample_payloads),
            )

        return issues

    async def check_all_upcasters(self) -> dict[str, list[CompatibilityIssue]]:
        """Run compatibility checks for all registered upcasters."""
        results = {}
        for event_type, from_version in self._registry._upcasters:
            key = f"{event_type}_v{from_version}"
            results[key] = await self.check_upcaster_compatibility(event_type, from_version)
        return results

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _load_sample_payloads(
        self, event_type: str, schema_version: int, limit: int = 100
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT payload FROM events
                WHERE event_type = $1 AND schema_version = $2
                LIMIT $3
                """,
                event_type, schema_version, limit,
            )
        return [dict(row["payload"]) for row in rows]

    async def _count_snapshots_with_fields(
        self, aggregate_type: str, schema_version: int, fields: list[str]
    ) -> int:
        """Count snapshots that contain any of the given fields in their data."""
        if not fields:
            return 0
        async with self._pool.acquire() as conn:
            # Check if snapshot_data contains any of the removed fields as keys
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM aggregate_snapshots
                WHERE aggregate_type = $1
                  AND schema_version = $2
                  AND snapshot_data ?| $3
                """,
                aggregate_type, schema_version, fields,
            )
        return count or 0
