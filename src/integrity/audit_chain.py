"""
Audit Chain Integrity: Cryptographic hash chain verification for AuditLedger.

Provides:
- run_integrity_check: Verify hash chain for an application
- get_audit_trail: Load full audit trail with raw vs upcasted comparison
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from uuid import UUID

import asyncpg

from src.aggregates.audit_ledger import AuditLedgerAggregate, GENESIS_HASH
from src.event_store import EventStore
from src.models.events import IntegrityError

logger = logging.getLogger(__name__)


async def run_integrity_check(
    pool: asyncpg.Pool,
    event_store: EventStore,
    application_id: UUID,
    checked_by: str = "system",
) -> dict:
    """
    Verify the cryptographic hash chain for an application's audit ledger.

    Returns a result dict with:
        chain_valid: bool
        entries_checked: int
        broken_at_sequence: int | None
        details: list of per-entry results
    """
    async with pool.acquire() as conn:
        entries = await conn.fetch(
            """
            SELECT ledger_entry_id, event_id, event_type, event_hash,
                   chain_hash, sequence_number, recorded_at
            FROM audit_ledger_projection
            WHERE application_id = $1
            ORDER BY sequence_number
            """,
            application_id,
        )

    if not entries:
        return {
            "chain_valid": True,
            "entries_checked": 0,
            "broken_at_sequence": None,
            "details": [],
            "message": "No audit entries found for this application",
        }

    prev_hash = GENESIS_HASH
    details = []
    broken_at = None

    for entry in entries:
        expected_chain = hashlib.sha256(
            (prev_hash + entry["event_hash"]).encode()
        ).hexdigest()

        valid = expected_chain == entry["chain_hash"]
        details.append({
            "sequence": entry["sequence_number"],
            "event_type": entry["event_type"],
            "event_hash": entry["event_hash"][:16] + "...",
            "chain_hash": entry["chain_hash"][:16] + "...",
            "valid": valid,
        })

        if not valid and broken_at is None:
            broken_at = entry["sequence_number"]
            logger.error(
                "Hash chain broken at sequence %d for application %s",
                entry["sequence_number"], application_id
            )

        prev_hash = entry["chain_hash"]

    return {
        "chain_valid": broken_at is None,
        "entries_checked": len(entries),
        "broken_at_sequence": broken_at,
        "details": details,
    }


async def record_audit_entry(
    pool: asyncpg.Pool,
    application_id: UUID,
    event_id: UUID,
    event_type: str,
    event_payload: dict,
    sequence_number: int,
    prev_chain_hash: str,
) -> str:
    """
    Record a new entry in the audit ledger projection.
    Returns the new chain_hash.
    """
    canonical = json.dumps(
        {"event_id": str(event_id), "payload": event_payload},
        sort_keys=True, default=str
    )
    event_hash = hashlib.sha256(canonical.encode()).hexdigest()
    chain_hash = hashlib.sha256((prev_chain_hash + event_hash).encode()).hexdigest()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO audit_ledger_projection
                (application_id, event_id, event_type, event_hash, chain_hash, sequence_number)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (application_id, sequence_number) DO NOTHING
            """,
            application_id, event_id, event_type, event_hash, chain_hash, sequence_number,
        )

    return chain_hash


async def get_last_chain_hash(pool: asyncpg.Pool, application_id: UUID) -> tuple[str, int]:
    """Return (last_chain_hash, last_sequence_number) for an application."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT chain_hash, sequence_number
            FROM audit_ledger_projection
            WHERE application_id = $1
            ORDER BY sequence_number DESC
            LIMIT 1
            """,
            application_id,
        )
    if row:
        return row["chain_hash"], row["sequence_number"]
    return GENESIS_HASH, 0
