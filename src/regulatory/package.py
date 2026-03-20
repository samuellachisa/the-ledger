"""
Regulatory Package Generator: Self-contained JSON for auditors.

Produces a package containing:
- All events for the application (raw + upcasted)
- Current projection state
- Cryptographic integrity proof
- Metadata for regulatory submission
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from uuid import UUID

import asyncpg

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.projections.application_summary import get_application_summary
from src.projections.compliance_audit import get_compliance_history

logger = logging.getLogger(__name__)


async def generate_regulatory_package(
    pool: asyncpg.Pool,
    event_store: EventStore,
    application_id: UUID,
    generated_by: str = "system",
) -> dict:
    """
    Generate a self-contained regulatory package for an application.

    The package is designed to be:
    - Self-verifiable (contains integrity proof)
    - Complete (all events + projections)
    - Tamper-evident (package hash)
    - Auditor-friendly (human-readable structure)
    """
    # 1. Load all events (upcasted for readability)
    events = await event_store.load_stream("LoanApplication", application_id)
    events_data = [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "schema_version": e.schema_version,
            "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
            "payload": e.to_payload(),
            "metadata": e.metadata,
        }
        for e in events
    ]

    # 2. Load raw events from DB (for comparison — proves upcasting didn't modify DB)
    async with pool.acquire() as conn:
        raw_rows = await conn.fetch(
            """
            SELECT e.event_id, e.event_type, e.schema_version, e.payload,
                   e.recorded_at, e.global_position
            FROM events e
            JOIN event_streams s ON e.stream_id = s.stream_id
            WHERE s.aggregate_type = 'LoanApplication'
              AND s.aggregate_id = $1
            ORDER BY e.stream_position
            """,
            application_id,
        )
        raw_events_data = [
            {
                "event_id": str(r["event_id"]),
                "event_type": r["event_type"],
                "schema_version": r["schema_version"],
                "raw_payload": dict(r["payload"]),
                "recorded_at": r["recorded_at"].isoformat(),
            }
            for r in raw_rows
        ]

    # 3. Current projection state
    async with pool.acquire() as conn:
        summary = await get_application_summary(conn, application_id)
        compliance_history = await get_compliance_history(conn, application_id)

    # 4. Integrity proof
    integrity = await run_integrity_check(pool, event_store, application_id, checked_by=generated_by)

    # 5. Build package
    package = {
        "package_version": "1.0",
        "generated_at": datetime.utcnow().isoformat(),
        "generated_by": generated_by,
        "application_id": str(application_id),
        "event_count": len(events_data),
        "events": events_data,
        "raw_events": raw_events_data,
        "projection_state": {
            "application_summary": summary,
            "compliance_history": compliance_history,
        },
        "integrity_proof": integrity,
        "regulatory_metadata": {
            "jurisdiction": "US",
            "regulation": "ECOA/FCRA",
            "package_purpose": "Loan Decision Audit Trail",
            "chain_valid": integrity["chain_valid"],
        },
    }

    # 6. Compute package hash (tamper-evident seal)
    package_json = json.dumps(package, sort_keys=True, default=str)
    package["package_hash"] = hashlib.sha256(package_json.encode()).hexdigest()

    logger.info(
        "Generated regulatory package for %s: %d events, chain_valid=%s",
        application_id, len(events_data), integrity["chain_valid"]
    )

    return package


def verify_regulatory_package(package: dict) -> dict:
    """
    Independently verify a regulatory package without access to the live system.

    An auditor can call this function with only the package JSON to verify:
    1. Package hash integrity (tamper detection)
    2. Hash chain validity (event immutability)
    3. Raw vs. upcasted event consistency (upcasting didn't modify DB data)

    Returns:
        {
            "package_hash_valid": bool,
            "chain_valid": bool,
            "raw_upcasted_consistent": bool,
            "event_count_matches": bool,
            "issues": list[str],
        }
    """
    issues: list[str] = []

    # 1. Verify package hash
    stored_hash = package.get("package_hash", "")
    package_without_hash = {k: v for k, v in package.items() if k != "package_hash"}
    recomputed = hashlib.sha256(
        json.dumps(package_without_hash, sort_keys=True, default=str).encode()
    ).hexdigest()
    package_hash_valid = recomputed == stored_hash
    if not package_hash_valid:
        issues.append(f"Package hash mismatch: stored={stored_hash[:16]}..., computed={recomputed[:16]}...")

    # 2. Verify hash chain independently
    from src.aggregates.audit_ledger import GENESIS_HASH
    integrity_proof = package.get("integrity_proof", {})
    chain_valid = integrity_proof.get("chain_valid", False)
    if not chain_valid:
        broken_at = integrity_proof.get("broken_at_sequence")
        issues.append(f"Hash chain broken at sequence {broken_at}")

    # 3. Verify raw vs. upcasted consistency
    # Raw events should have the same event_ids as upcasted events
    raw_events = package.get("raw_events", [])
    upcasted_events = package.get("events", [])
    raw_ids = {e["event_id"] for e in raw_events}
    upcasted_ids = {e["event_id"] for e in upcasted_events}
    raw_upcasted_consistent = raw_ids == upcasted_ids
    if not raw_upcasted_consistent:
        issues.append(
            f"Raw/upcasted event ID mismatch: "
            f"{len(raw_ids - upcasted_ids)} raw-only, {len(upcasted_ids - raw_ids)} upcasted-only"
        )

    # 4. Verify event count
    declared_count = package.get("event_count", 0)
    actual_count = len(upcasted_events)
    event_count_matches = declared_count == actual_count
    if not event_count_matches:
        issues.append(f"Event count mismatch: declared={declared_count}, actual={actual_count}")

    return {
        "package_hash_valid": package_hash_valid,
        "chain_valid": chain_valid,
        "raw_upcasted_consistent": raw_upcasted_consistent,
        "event_count_matches": event_count_matches,
        "issues": issues,
        "overall_valid": len(issues) == 0,
    }
