"""
MCP Resources (Query Side): Read from projections only — never from event streams directly.

Resources:
    ledger://applications                       — all applications (paginated)
    ledger://applications/{id}                  — current application state
    ledger://applications/{id}/audit-trail      — full event history (exception: auditors)
    ledger://applications/{id}/compliance       — temporal compliance history
    ledger://agents/performance                 — decision metrics per model version
    ledger://integrity/{id}                     — hash chain verification result
"""
from __future__ import annotations

import json
import logging
from uuid import UUID

import asyncpg

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.projections.agent_performance import get_agent_performance, list_agent_performance
from src.projections.application_summary import get_application_summary, list_applications
from src.projections.compliance_audit import get_compliance_history

logger = logging.getLogger(__name__)


async def read_applications(conn: asyncpg.Connection) -> list[dict]:
    """ledger://applications — all applications."""
    return await list_applications(conn)


async def read_application(conn: asyncpg.Connection, application_id: UUID) -> dict | None:
    """ledger://applications/{id} — current state from projection."""
    return await get_application_summary(conn, application_id)


async def read_audit_trail(event_store: EventStore, application_id: UUID) -> list[dict]:
    """
    ledger://applications/{id}/audit-trail

    Justified exception to the projection-only rule: audit trail consumers
    (regulators, auditors) need the raw event sequence, not a derived view.
    """
    events = await event_store.load_stream("LoanApplication", application_id)
    return [
        {
            "event_type": e.event_type,
            "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
            "payload": e.to_payload(),
            "schema_version": e.schema_version,
        }
        for e in events
    ]


async def read_compliance(conn: asyncpg.Connection, application_id: UUID) -> list[dict]:
    """ledger://applications/{id}/compliance — temporal compliance history."""
    return await get_compliance_history(conn, application_id)


async def read_agent_performance(conn: asyncpg.Connection) -> list[dict]:
    """ledger://agents/performance — metrics per model version."""
    return await list_agent_performance(conn)


async def read_integrity(
    pool: asyncpg.Pool,
    event_store: EventStore,
    application_id: UUID,
) -> dict:
    """ledger://integrity/{id} — hash chain verification."""
    return await run_integrity_check(pool, event_store, application_id)
