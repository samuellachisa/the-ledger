"""Base aggregate class implementing the Event Sourcing aggregate pattern."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from src.models.events import DomainEvent


class AggregateRoot:
    """
    Base class for all aggregates.

    Pattern:
        1. Load aggregate: replay events via apply()
        2. Execute command: validate rules, call _raise_event()
        3. _raise_event() appends to pending_events and calls apply()
        4. Command handler appends pending_events to EventStore

    Aggregates NEVER call other aggregates directly.
    They communicate only via events.
    """

    aggregate_type: str = ""

    def __init__(self, aggregate_id: UUID):
        self.aggregate_id = aggregate_id
        self.version: int = 0          # Current stream version (for OCC)
        self.pending_events: list[DomainEvent] = []

    def _raise_event(self, event: DomainEvent) -> None:
        """Record a new event and apply it to update state."""
        self.apply(event)
        self.pending_events.append(event)

    def apply(self, event: DomainEvent) -> None:
        """
        Dispatch event to the appropriate when_* handler.
        Called both during replay (load) and when raising new events.
        """
        handler_name = f"when_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        # Unknown event types are silently ignored (forward compatibility)
        self.version += 1

    @classmethod
    def load(cls, aggregate_id: UUID, events: list[DomainEvent]) -> "AggregateRoot":
        """Reconstruct aggregate state by replaying events."""
        instance = cls(aggregate_id)
        for event in events:
            instance.apply(event)
        # After replay, version reflects the stream version; pending_events is empty
        instance.pending_events = []
        return instance

    def clear_pending(self) -> list[DomainEvent]:
        """Return and clear pending events after successful append."""
        events = list(self.pending_events)
        self.pending_events = []
        return events
