"""
ledger/integrity/audit_chain.py
Cryptographic Audit Chain mapping SHA-256 blocks evaluating immutability.
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
            sort_keys=True, default=str
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
        tamper_detected=tampered
    )
