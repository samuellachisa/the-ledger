"""
AuditLedgerAggregate: Cryptographic hash chain for tamper-evident audit trail.

Each entry's chain_hash = SHA-256(prev_chain_hash + event_hash).
Breaking any historical event breaks all subsequent chain hashes.
"""
from __future__ import annotations

import hashlib
from uuid import UUID

from src.aggregates.base import AggregateRoot
from src.models.events import (
    AuditEntryRecorded,
    IntegrityCheckCompleted,
    IntegrityError,
)

GENESIS_HASH = "0" * 64  # Initial hash for the first entry in the chain


class AuditLedgerAggregate(AggregateRoot):
    aggregate_type = "AuditLedger"

    def __init__(self, aggregate_id: UUID):
        super().__init__(aggregate_id)
        self.application_id: UUID | None = None
        self.entries: list[dict] = []  # {sequence, event_hash, chain_hash}
        self.last_chain_hash: str = GENESIS_HASH
        self.sequence_counter: int = 0

    # -------------------------------------------------------------------------
    # Commands
    # -------------------------------------------------------------------------

    def record_entry(
        self,
        application_id: UUID,
        action: str,
        actor: str,
        details: dict,
        source_event_id: UUID,
        event_payload: dict,
    ) -> None:
        """Record an auditable action with cryptographic chaining."""
        event_hash = self._compute_event_hash(source_event_id, event_payload)
        chain_hash = self._compute_chain_hash(self.last_chain_hash, event_hash)

        self._raise_event(AuditEntryRecorded(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            action=action,
            actor=actor,
            details=details,
            source_event_id=source_event_id,
            event_hash=event_hash,
            chain_hash=chain_hash,
        ))

    def run_integrity_check(self, application_id: UUID, checked_by: str) -> bool:
        """
        Verify the hash chain from genesis to current.
        Returns True if chain is valid, False if broken.
        Raises IntegrityError with the sequence number where chain breaks.
        """
        prev_hash = GENESIS_HASH
        for entry in self.entries:
            expected_chain = self._compute_chain_hash(prev_hash, entry["event_hash"])
            if expected_chain != entry["chain_hash"]:
                self._raise_event(IntegrityCheckCompleted(
                    aggregate_id=self.aggregate_id,
                    application_id=application_id,
                    chain_valid=False,
                    entries_checked=entry["sequence"],
                    broken_at_sequence=entry["sequence"],
                    checked_by=checked_by,
                ))
                raise IntegrityError(
                    sequence_number=entry["sequence"],
                    expected_hash=expected_chain,
                    actual_hash=entry["chain_hash"],
                )
            prev_hash = entry["chain_hash"]

        self._raise_event(IntegrityCheckCompleted(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            chain_valid=True,
            entries_checked=len(self.entries),
            broken_at_sequence=None,
            checked_by=checked_by,
        ))
        return True

    # -------------------------------------------------------------------------
    # Event Handlers
    # -------------------------------------------------------------------------

    def when_AuditEntryRecorded(self, event: AuditEntryRecorded) -> None:
        if self.application_id is None:
            self.application_id = event.application_id
        self.sequence_counter += 1
        self.entries.append({
            "sequence": self.sequence_counter,
            "event_hash": event.event_hash,
            "chain_hash": event.chain_hash,
            "action": event.action,
        })
        self.last_chain_hash = event.chain_hash

    def when_IntegrityCheckCompleted(self, event: IntegrityCheckCompleted) -> None:
        pass  # Integrity checks are recorded but don't change ledger state

    # -------------------------------------------------------------------------
    # Reconstruction
    # -------------------------------------------------------------------------

    @classmethod
    def load(cls, aggregate_id: UUID, events: list) -> "AuditLedgerAggregate":
        """Reconstruct ledger state by replaying *events* in order.

        Accepts the list returned by ``EventStore.load_stream()``.  After
        replay the full hash chain is restored in ``entries`` and
        ``last_chain_hash`` reflects the tip of the chain, ready for the
        next ``record_entry()`` call.  ``original_version`` is set so it
        can be passed directly as ``expected_version`` to
        ``EventStore.append()``.
        """
        instance = cls(aggregate_id)
        for event in events:
            instance.apply(event)
        instance.original_version = instance.version
        instance.pending_events = []
        return instance

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _compute_event_hash(event_id: UUID, payload: dict) -> str:
        import json
        canonical = json.dumps(
            {"event_id": str(event_id), "payload": payload},
            sort_keys=True, default=str
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _compute_chain_hash(prev_chain_hash: str, event_hash: str) -> str:
        combined = prev_chain_hash + event_hash
        return hashlib.sha256(combined.encode()).hexdigest()
