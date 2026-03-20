"""
AgentPerformanceLedger Projection: Metrics per agent model version.
Tracks decision counts, approval rates, and average confidence scores.
"""
from __future__ import annotations

import asyncpg

from src.event_store import StoredEvent


async def handle_agent_performance(conn: asyncpg.Connection, event: StoredEvent) -> None:
    """Update agent performance metrics on decision events."""
    if event.event_type != "DecisionGenerated":
        return

    payload = event.payload
    model_version = payload.get("model_version", "unknown")
    outcome = payload.get("outcome", "")
    confidence = payload.get("confidence_score", 0.0)

    approved = 1 if outcome == "APPROVE" else 0
    denied = 1 if outcome == "DENY" else 0
    referred = 1 if outcome == "REFER" else 0

    await conn.execute(
        """
        INSERT INTO agent_performance_projection
            (model_version, total_decisions, approved_count, denied_count,
             referred_count, avg_confidence, updated_at)
        VALUES ($1, 1, $2, $3, $4, $5, NOW())
        ON CONFLICT (model_version) DO UPDATE
            SET total_decisions = agent_performance_projection.total_decisions + 1,
                approved_count  = agent_performance_projection.approved_count + $2,
                denied_count    = agent_performance_projection.denied_count + $3,
                referred_count  = agent_performance_projection.referred_count + $4,
                avg_confidence  = (
                    agent_performance_projection.avg_confidence *
                    agent_performance_projection.total_decisions + $5
                ) / (agent_performance_projection.total_decisions + 1),
                updated_at = NOW()
        """,
        model_version, approved, denied, referred, confidence,
    )


async def get_agent_performance(conn: asyncpg.Connection, model_version: str) -> dict | None:
    row = await conn.fetchrow(
        "SELECT * FROM agent_performance_projection WHERE model_version = $1",
        model_version,
    )
    return dict(row) if row else None


async def list_agent_performance(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM agent_performance_projection ORDER BY total_decisions DESC"
    )
    return [dict(r) for r in rows]
