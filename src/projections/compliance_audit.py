"""
ComplianceAuditView Projection: Temporal compliance state with point-in-time queries.

Stores a snapshot at each compliance event, enabling:
    get_state_at(application_id, timestamp) → compliance state at that moment
"""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

from src.event_store import StoredEvent


def _parse_dt(val) -> datetime | None:
    """Coerce a value to a timezone-aware datetime for asyncpg TIMESTAMPTZ."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


async def handle_compliance_audit(conn: asyncpg.Connection, event: StoredEvent) -> None:
    """Build temporal compliance snapshots."""
    if event.aggregate_type != "ComplianceRecord":
        return

    payload = event.payload
    record_id = event.aggregate_id
    occurred_at = _parse_dt(event.metadata.get("occurred_at") or event.recorded_at)

    if event.event_type == "ComplianceRecordCreated":
        from uuid import UUID as _UUID
        app_id_val = payload.get("application_id")
        if isinstance(app_id_val, str):
            app_id_val = _UUID(app_id_val)
        await conn.execute(
            """
            INSERT INTO compliance_audit_projection
                (record_id, application_id, as_of_timestamp, status,
                 checks_passed, checks_failed, officer_id, notes)
            VALUES ($1, $2, $3, 'InProgress', '[]', '[]', $4, '')
            ON CONFLICT DO NOTHING
            """,
            record_id,
            app_id_val,
            occurred_at,
            payload.get("officer_id"),
        )

    elif event.event_type in ("ComplianceCheckPassed", "ComplianceCheckFailed"):
        # Get current state and append new snapshot
        latest = await conn.fetchrow(
            """
            SELECT * FROM compliance_audit_projection
            WHERE record_id = $1
            ORDER BY as_of_timestamp DESC
            LIMIT 1
            """,
            record_id,
        )
        if not latest:
            return

        import json
        passed = list(latest["checks_passed"])
        failed = list(latest["checks_failed"])

        if event.event_type == "ComplianceCheckPassed":
            check = payload.get("check_name")
            if check not in passed:
                passed.append(check)
        else:
            check = payload.get("check_name")
            if check not in failed:
                failed.append(check)

        await conn.execute(
            """
            INSERT INTO compliance_audit_projection
                (record_id, application_id, as_of_timestamp, status,
                 checks_passed, checks_failed, officer_id, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
            """,
            record_id,
            latest["application_id"],
            occurred_at,
            latest["status"],
            passed,
            failed,
            payload.get("officer_id", latest["officer_id"]),
            latest["notes"],
        )

    elif event.event_type == "ComplianceRecordFinalized":
        latest = await conn.fetchrow(
            """
            SELECT * FROM compliance_audit_projection
            WHERE record_id = $1
            ORDER BY as_of_timestamp DESC
            LIMIT 1
            """,
            record_id,
        )
        if not latest:
            return

        await conn.execute(
            """
            INSERT INTO compliance_audit_projection
                (record_id, application_id, as_of_timestamp, status,
                 checks_passed, checks_failed, officer_id, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
            """,
            record_id,
            latest["application_id"],
            occurred_at,
            payload.get("overall_status"),
            latest["checks_passed"],
            latest["checks_failed"],
            payload.get("officer_id", latest["officer_id"]),
            payload.get("notes", ""),
        )


async def get_state_at(
    conn: asyncpg.Connection,
    application_id,
    timestamp: datetime,
) -> dict | None:
    """
    Temporal query: return compliance state as it was at a given timestamp.
    This is the key feature of the ComplianceAuditView.
    """
    ts = _parse_dt(timestamp) or timestamp
    row = await conn.fetchrow(
        """
        SELECT cap.*
        FROM compliance_audit_projection cap
        WHERE cap.application_id = $1
          AND cap.as_of_timestamp <= $2
        ORDER BY cap.as_of_timestamp DESC
        LIMIT 1
        """,
        application_id,
        ts,
    )
    return dict(row) if row else None


async def get_compliance_history(conn: asyncpg.Connection, application_id) -> list[dict]:
    """Return full compliance history for an application."""
    rows = await conn.fetch(
        """
        SELECT cap.*
        FROM compliance_audit_projection cap
        WHERE cap.application_id = $1
        ORDER BY cap.as_of_timestamp ASC
        """,
        application_id,
    )
    return [dict(r) for r in rows]
