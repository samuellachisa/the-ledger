"""
Ledger-path audit helper (narrative / MCP demo).

This module verifies a **metadata-based** hash chain on events loaded through
`ledger.event_store.EventStore`: each event may carry `metadata["chain_hash"]`
computed as SHA-256(previous_chain + canonical_event). It is used by
`tests/test_integrity.py` for the lightweight ledger schema.

For **production** tamper detection against `audit_ledger_projection`, use
`src.integrity.audit_chain.run_integrity_check` (pool + `src.event_store.EventStore`
+ application UUID) — that path reads the materialized audit table built from
domain events and matches `AuditLedgerAggregate` / `GENESIS_HASH`.
"""
import hashlib
import json
from dataclasses import dataclass

from ledger.event_store import EventStore


@dataclass
class IntegrityCheckResult:
    events_verified: int
    chain_valid: bool
    tamper_detected: bool


async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str) -> IntegrityCheckResult:
    """Verify optional per-event ``metadata['chain_hash']`` for a ledger stream key ``{entity_type}-{entity_id}``."""
    stream_id = f"{entity_type}-{entity_id}"
    events = await store.load_stream(stream_id)

    if not events:
        return IntegrityCheckResult(0, True, False)

    previous_hash = "GENESIS"
    verified = 0
    tampered = False

    for event in events:
        canonical = json.dumps(
            {"event_id": str(event.event_id), "payload": event.payload},
            sort_keys=True,
            default=str,
        )

        expected_hash = hashlib.sha256((previous_hash + canonical).encode()).hexdigest()
        stored_hash = event.metadata.get("chain_hash")

        if stored_hash and stored_hash != expected_hash:
            tampered = True
            break

        previous_hash = expected_hash
        verified += 1

    return IntegrityCheckResult(
        events_verified=verified,
        chain_valid=not tampered,
        tamper_detected=tampered,
    )
