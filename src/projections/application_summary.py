"""
ApplicationSummary Projection: Current state of all loan applications.
SLO: < 500ms lag from event to projection update.
"""
from __future__ import annotations

import asyncpg

from src.event_store import StoredEvent


async def handle_application_summary(conn: asyncpg.Connection, event: StoredEvent) -> None:
    """Update the application_summary_projection based on incoming events."""
    payload = event.payload
    agg_id = event.aggregate_id

    if event.event_type == "LoanApplicationSubmitted":
        await conn.execute(
            """
            INSERT INTO application_summary_projection
                (application_id, applicant_name, loan_amount, status, submitted_at, current_version)
            VALUES ($1, $2, $3, 'Submitted', $4, 1)
            ON CONFLICT (application_id) DO UPDATE
                SET applicant_name = EXCLUDED.applicant_name,
                    loan_amount = EXCLUDED.loan_amount,
                    status = EXCLUDED.status,
                    submitted_at = EXCLUDED.submitted_at,
                    current_version = application_summary_projection.current_version + 1,
                    updated_at = NOW()
            """,
            agg_id,
            payload.get("applicant_name"),
            payload.get("loan_amount"),
            event.metadata.get("occurred_at"),
        )

    elif event.event_type == "CreditAnalysisRequested":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'AwaitingAnalysis', current_version = current_version + 1, updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "CreditAnalysisCompleted":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'UnderReview',
                credit_score = $2,
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
            payload.get("credit_score"),
        )

    elif event.event_type == "ComplianceCheckRequested":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'ComplianceCheck', current_version = current_version + 1, updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "DecisionGenerated":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'PendingDecision',
                confidence_score = $2,
                last_decision_at = NOW(),
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
            payload.get("confidence_score"),
        )

    elif event.event_type == "ApplicationApproved":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'FinalApproved',
                last_decision_at = NOW(),
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "ApplicationDenied":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'Denied',
                last_decision_at = NOW(),
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "ApplicationReferred":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'Referred',
                last_decision_at = NOW(),
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "ApplicationWithdrawn":
        await conn.execute(
            """
            UPDATE application_summary_projection
            SET status = 'Withdrawn',
                current_version = current_version + 1,
                updated_at = NOW()
            WHERE application_id = $1
            """,
            agg_id,
        )

    elif event.event_type == "ComplianceRecordFinalized":
        # Update compliance_passed on the linked application
        # We need to find the application linked to this compliance record
        await conn.execute(
            """
            UPDATE application_summary_projection asp
            SET compliance_passed = $2,
                current_version = current_version + 1,
                updated_at = NOW()
            FROM events e
            JOIN events ce ON ce.aggregate_id = $1::uuid
                AND ce.event_type = 'ComplianceCheckRequested'
            WHERE asp.application_id = ce.aggregate_id
              AND asp.application_id = e.aggregate_id
            """,
            agg_id,
            payload.get("overall_status") == "Passed",
        )


async def get_application_summary(conn: asyncpg.Connection, application_id) -> dict | None:
    """Query the application summary projection."""
    row = await conn.fetchrow(
        "SELECT * FROM application_summary_projection WHERE application_id = $1",
        application_id,
    )
    return dict(row) if row else None


async def list_applications(
    conn: asyncpg.Connection,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    if status:
        rows = await conn.fetch(
            "SELECT * FROM application_summary_projection WHERE status = $1 "
            "ORDER BY submitted_at DESC LIMIT $2 OFFSET $3",
            status, limit, offset
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM application_summary_projection "
            "ORDER BY submitted_at DESC LIMIT $1 OFFSET $2",
            limit, offset
        )
    return [dict(r) for r in rows]
